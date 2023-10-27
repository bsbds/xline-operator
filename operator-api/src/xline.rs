use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{AttachParams, AttachedProcess};
use kube::Api;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use std::process::{Child, Command};

#[derive(Debug, Clone)]
pub struct XlineConfig {
    pub name: String,
    pub executable: String,
    pub storage_engine: String,
    pub data_dir: String,
    pub is_leader: bool,
    pub additional: Option<String>,
}

impl XlineConfig {
    fn gen_start_cmd(&self, members: &HashMap<String, String>) -> String {
        let mut start_cmd = format!(
            "{} --name {} --members {} --storage-engine {} --data-dir {}",
            self.executable,
            self.name,
            members
                .iter()
                .map(|(name, addr)| format!("{name}={addr}"))
                .collect::<Vec<_>>()
                .join(","),
            self.storage_engine,
            self.data_dir,
        );
        if self.is_leader {
            start_cmd.push(' ');
            start_cmd.push_str("--is-leader");
        }
        if let Some(additional) = &self.additional {
            start_cmd.push(' ');
            let pat: &[_] = &['\'', '"'];
            start_cmd.push_str(additional.trim_matches(pat));
        }
        start_cmd
    }
}

/// xline handle abstraction
#[async_trait]
pub trait XlineHandle: Debug + Send + Sync + 'static {
    /// start a xline node
    async fn start(&mut self, members: &HashMap<String, String>) -> anyhow::Result<()>; // we dont care about what failure happened when start, it just failed

    /// kill a xline node
    async fn kill(&mut self) -> anyhow::Result<()>;
}

/// Local xline handle, it will execute the xline in the local
/// machine with the start_cmd
#[derive(Debug)]
pub struct LocalXlineHandle {
    config: XlineConfig,
    child_proc: Option<Child>,
}

impl LocalXlineHandle {
    /// New a local xline handle
    pub fn new(config: XlineConfig) -> Self {
        Self {
            config,
            child_proc: None,
        }
    }
}

#[async_trait]
impl XlineHandle for LocalXlineHandle {
    async fn start(&mut self, members: &HashMap<String, String>) -> anyhow::Result<()> {
        self.kill().await?;
        let cmd = self.config.gen_start_cmd(members);
        let mut cmds = cmd.split_whitespace();
        let Some((exe, args)) = cmds
            .next()
            .map(|exe| (exe, cmds.collect::<Vec<_>>())) else {
            unreachable!("the start_cmd must be valid");
        };
        let proc = Command::new(exe).args(args).spawn()?;
        self.child_proc = Some(proc);
        Ok(())
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        if let Some(mut proc) = self.child_proc.take() {
            return Ok(proc.kill()?);
        }
        Ok(())
    }
}

/// K8s xline handle, it will execute the xline start_cmd
/// in pod
pub struct K8sXlineHandle {
    /// the pod name
    pod_name: String,
    /// the container name of xline
    container_name: String,
    /// k8s pods api
    pods_api: Api<Pod>,
    /// the attached process of xline
    process: Option<AttachedProcess>,
    /// the xline config
    config: XlineConfig,
}

impl Debug for K8sXlineHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sXlineHandle")
            .field("pod_name", &self.pod_name)
            .field("container_name", &self.container_name)
            .field("pods_api", &self.pods_api)
            .field("config", &self.config)
            .finish()
    }
}

impl K8sXlineHandle {
    /// New with default k8s client
    pub async fn new_with_default(
        pod_name: String,
        container_name: String,
        namespace: &str,
        config: XlineConfig,
    ) -> Self {
        let client = kube::Client::try_default()
            .await
            .unwrap_or_else(|_ig| unreachable!("it must be setup in k8s environment"));
        Self {
            pod_name,
            container_name,
            pods_api: Api::namespaced(client, namespace),
            process: None,
            config,
        }
    }

    /// New with the provided k8s client
    pub fn new_with_client(
        pod_name: String,
        container_name: String,
        client: kube::Client,
        namespace: &str,
        config: XlineConfig,
    ) -> Self {
        Self {
            pod_name,
            container_name,
            pods_api: Api::namespaced(client, namespace),
            process: None,
            config,
        }
    }
}

#[async_trait]
impl XlineHandle for K8sXlineHandle {
    async fn start(&mut self, members: &HashMap<String, String>) -> anyhow::Result<()> {
        self.kill().await?;
        let cmd = self.config.gen_start_cmd(members);
        let cmds: Vec<&str> = cmd.split_whitespace().collect();
        let process = self
            .pods_api
            .exec(
                &self.pod_name,
                cmds,
                &AttachParams::default().container(&self.container_name),
            )
            .await?;
        self.process = Some(process);
        Ok(())
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        if let Some(process) = self.process.take() {
            process.abort();
        }
        Ok(())
    }
}