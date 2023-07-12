use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{
    RollingUpdateStatefulSetStrategy, StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy,
};
use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Container, PersistentVolumeClaim, PodSpec, PodTemplateSpec, Service, ServicePort, ServiceSpec,
    VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use tracing::{debug, error};

use utils::consts::{DEFAULT_BACKUP_DIR, DEFAULT_DATA_DIR};

use crate::controller::consts::FIELD_MANAGER;
use crate::controller::Controller;
use crate::crd::{Cluster, StorageSpec};

/// CRD `XlineCluster` controller
pub(crate) struct ClusterController {
    /// Kubernetes client
    pub(crate) kube_client: Client,
    /// Cluster api
    pub(crate) cluster_api: Api<Cluster>,
    /// The kubernetes cluster dns suffix
    pub(crate) cluster_suffix: String,
}

/// All possible errors
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// Missing an object in cluster
    #[error("Missing object key {0} in cluster")]
    MissingObject(&'static str),
    /// Kube error
    #[error("Kubernetes api error")]
    Kube(#[from] kube::Error),
    /// Backup PV mount path is already mounted
    #[error("The path {0} is internally used in the xline operator and cannot be mounted.")]
    CannotMount(&'static str),
}

/// Controller result
type Result<T> = std::result::Result<T, Error>;

impl ClusterController {
    /// Extract service ports
    fn extract_service_ports(cluster: &Arc<Cluster>) -> Result<Vec<ServicePort>> {
        // expose all the container pods
        let service_ports = cluster
            .spec
            .container
            .ports
            .as_ref()
            .map(|v| {
                v.iter()
                    .map(|port| ServicePort {
                        name: port.name.clone(),
                        port: port.container_port,
                        ..ServicePort::default()
                    })
                    .collect()
            })
            .ok_or(Error::MissingObject(".spec.container.ports"))?;
        Ok(service_ports)
    }

    /// Extract persistent volume claims
    fn extract_pvcs(cluster: &Arc<Cluster>) -> Vec<PersistentVolumeClaim> {
        let mut pvcs = Vec::new();
        // check if the backup type is PV, add the pvc to pvcs
        if let Some(spec) = cluster.spec.backup.as_ref() {
            if let StorageSpec::Pvc { pvc } = spec.storage.clone() {
                pvcs.push(pvc);
            }
        }
        // check if the data pvc if specified, add the pvc to pvcs
        if let Some(pvc) = cluster.spec.data.as_ref() {
            pvcs.push(pvc.clone());
        }
        // extend the user defined pvcs
        if let Some(spec_pvcs) = cluster.spec.pvcs.clone() {
            pvcs.extend(spec_pvcs);
        }
        pvcs
    }

    /// Extract owner reference
    fn extract_owner_ref(cluster: &Arc<Cluster>) -> OwnerReference {
        // unwrap controller_owner_ref is always safe
        let Some(owner_ref) = cluster.controller_owner_ref(&()) else { unreachable!() };
        owner_ref
    }

    /// Extract name, namespace
    fn extract_id(cluster: &Arc<Cluster>) -> Result<(&str, &str)> {
        let namespace = cluster
            .metadata
            .namespace
            .as_deref()
            .ok_or(Error::MissingObject(".metadata.namespace"))?;
        let name = cluster
            .metadata
            .name
            .as_deref()
            .ok_or(Error::MissingObject(".metadata.name"))?;
        Ok((namespace, name))
    }

    /// Build the metadata which shares between all subresources
    fn build_metadata(namespace: &str, name: &str, owner_ref: OwnerReference) -> ObjectMeta {
        let mut labels: BTreeMap<String, String> = BTreeMap::new();
        let _: Option<_> = labels.insert("app".to_owned(), name.to_owned());
        ObjectMeta {
            labels: Some(labels.clone()),            // it is used in selector
            name: Some(name.to_owned()),             // all subresources share the same name
            namespace: Some(namespace.to_owned()),   // all subresources share the same namespace
            owner_references: Some(vec![owner_ref]), // allow k8s GC to automatically clean up itself
            ..ObjectMeta::default()
        }
    }

    /// Apply headless service
    async fn apply_headless_service(
        &self,
        namespace: &str,
        name: &str,
        metadata: &ObjectMeta,
        service_ports: Vec<ServicePort>,
    ) -> Result<()> {
        let api: Api<Service> = Api::namespaced(self.kube_client.clone(), namespace);
        let _: Service = api
            .patch(
                name,
                &PatchParams::apply(FIELD_MANAGER),
                &Patch::Apply(Service {
                    metadata: metadata.clone(),
                    spec: Some(ServiceSpec {
                        cluster_ip: None,
                        ports: Some(service_ports),
                        selector: metadata.labels.clone(),
                        ..ServiceSpec::default()
                    }),
                    ..Service::default()
                }),
            )
            .await?;
        Ok(())
    }

    /// Apply the statefulset in k8s to reconcile cluster
    async fn apply_statefulset(
        &self,
        namespace: &str,
        name: &str,
        cluster: &Arc<Cluster>,
        pvcs: Vec<PersistentVolumeClaim>,
        metadata: &ObjectMeta,
    ) -> Result<()> {
        let api: Api<StatefulSet> = Api::namespaced(self.kube_client.clone(), namespace);
        let mut container = cluster.spec.container.clone();
        let backup = cluster.spec.backup.clone();
        let data = cluster.spec.data.clone();

        // mount backup volume to `DEFAULT_BACKUP_PV_MOUNT_PATH` in container
        let backup_mount = if let Some(spec) = backup {
            let backup_pvc_name = match spec.storage {
                StorageSpec::S3 { .. } => None,
                StorageSpec::Pvc { pvc } => Some(
                    pvc.metadata
                        .name
                        .ok_or(Error::MissingObject(".spec.backup.pvc.metadata.name"))?,
                ),
            };
            backup_pvc_name.map(|pvc_name| VolumeMount {
                mount_path: DEFAULT_BACKUP_DIR.to_owned(),
                name: pvc_name,
                ..VolumeMount::default()
            })
        } else {
            None
        };
        // mount data volume to `DEFAULT_DATA_DIR` in container
        let data_mount = if let Some(pvc) = data {
            Some(VolumeMount {
                mount_path: DEFAULT_DATA_DIR.to_owned(),
                name: pvc
                    .metadata
                    .name
                    .ok_or(Error::MissingObject(".spec.data.metadata.name"))?,
                ..VolumeMount::default()
            })
        } else {
            None
        };

        let mut mounts = Vec::new();
        // check if the container has specified volume_mounts before
        if let Some(spec_mounts) = container.volume_mounts {
            // if the container mount the dir used in operator, return error
            if spec_mounts
                .iter()
                .any(|mount| mount.mount_path.starts_with(DEFAULT_BACKUP_DIR))
            {
                return Err(Error::CannotMount(DEFAULT_BACKUP_DIR));
            }
            if spec_mounts
                .iter()
                .any(|mount| mount.mount_path.starts_with(DEFAULT_DATA_DIR))
            {
                return Err(Error::CannotMount(DEFAULT_DATA_DIR));
            }
            // extend the mounts
            mounts.extend(spec_mounts);
        }
        if let Some(mount) = backup_mount {
            mounts.push(mount);
        }
        if let Some(mount) = data_mount {
            mounts.push(mount);
        }
        // override the container volume_mounts
        container.volume_mounts = Some(mounts);

        let _: StatefulSet = api
            .patch(
                name,
                &PatchParams::apply(FIELD_MANAGER),
                &Patch::Apply(StatefulSet {
                    metadata: metadata.clone(),
                    spec: Some(StatefulSetSpec {
                        replicas: Some(cluster.spec.size),
                        selector: LabelSelector {
                            match_expressions: None,
                            match_labels: metadata.labels.clone(),
                        },
                        service_name: name.to_owned(),
                        volume_claim_templates: Some(pvcs),
                        update_strategy: Some(StatefulSetUpdateStrategy {
                            rolling_update: Some(RollingUpdateStatefulSetStrategy {
                                max_unavailable: Some(IntOrString::String("50%".to_owned())), // allow a maximum of half the cluster quorum shutdown when performing a rolling update
                                partition: None,
                            }),
                            ..StatefulSetUpdateStrategy::default()
                        }),
                        template: PodTemplateSpec {
                            metadata: Some(ObjectMeta {
                                labels: metadata.labels.clone(),
                                ..ObjectMeta::default()
                            }),
                            spec: Some(PodSpec {
                                init_containers: Some(vec![]), // TODO publish sidecar operator to registry
                                containers: vec![container], // TODO inject the sidecar operator container here
                                volumes: None,
                                ..PodSpec::default()
                            }),
                        },
                        ..StatefulSetSpec::default()
                    }),
                    ..StatefulSet::default()
                }),
            )
            .await?;
        Ok(())
    }

    /// Apply the cron job to trigger backup
    async fn apply_backup_cron_job(
        &self,
        namespace: &str,
        name: &str,
        size: i32,
        cron: &str,
        metadata: &ObjectMeta,
    ) -> Result<()> {
        let api: Api<CronJob> = Api::namespaced(self.kube_client.clone(), namespace);
        let trigger_cmd = vec![
            "/bin/sh".to_owned(),
            "-ecx".to_owned(),
            format!(
                "curl {name}-$((RANDOM % {size})).{name}.{namespace}.svc.{}/backup",
                self.cluster_suffix
            ), // choose a node randomly
        ];
        let _: CronJob = api
            .patch(
                name,
                &PatchParams::apply(FIELD_MANAGER),
                &Patch::Apply(CronJob {
                    metadata: metadata.clone(),
                    spec: Some(CronJobSpec {
                        concurrency_policy: Some("Forbid".to_owned()), // A backup cron job cannot run concurrently
                        schedule: cron.to_owned(),
                        job_template: JobTemplateSpec {
                            spec: Some(JobSpec {
                                template: PodTemplateSpec {
                                    spec: Some(PodSpec {
                                        containers: vec![Container {
                                            name: format!("{name}-backup-cronjob"),
                                            image_pull_policy: Some("IfNotPresent".to_owned()),
                                            image: Some("curlimages/curl".to_owned()),
                                            command: Some(trigger_cmd),
                                            ..Container::default()
                                        }],
                                        restart_policy: Some("OnFailure".to_owned()),
                                        ..PodSpec::default()
                                    }),
                                    ..PodTemplateSpec::default()
                                },
                                ..JobSpec::default()
                            }),
                            ..JobTemplateSpec::default()
                        },
                        ..CronJobSpec::default()
                    }),
                    ..CronJob::default()
                }),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Controller<Cluster> for ClusterController {
    type Error = Error;

    fn api(&self) -> &Api<Cluster> {
        &self.cluster_api
    }

    async fn reconcile_once(&self, cluster: &Arc<Cluster>) -> Result<()> {
        debug!(
            "Reconciling cluster: \n{}",
            serde_json::to_string_pretty(cluster.as_ref()).unwrap_or_default()
        );
        let (namespace, name) = Self::extract_id(cluster)?;
        let owner_ref = Self::extract_owner_ref(cluster);
        let pvcs = Self::extract_pvcs(cluster);
        let service_ports = Self::extract_service_ports(cluster)?;
        let metadata = Self::build_metadata(namespace, name, owner_ref);

        self.apply_headless_service(namespace, name, &metadata, service_ports)
            .await?;
        self.apply_statefulset(namespace, name, cluster, pvcs, &metadata)
            .await?;
        if let Some(spec) = cluster.spec.backup.as_ref() {
            Box::pin(self.apply_backup_cron_job(
                namespace,
                name,
                cluster.spec.size,
                spec.cron.as_str(),
                &metadata,
            ))
            .await?;
        }
        Ok(())
    }

    fn handle_error(&self, resource: &Arc<Cluster>, err: &Self::Error) {
        error!("{:?} reconciliation error: {}", resource.metadata.name, err);
    }
}