use std::collections::HashMap;

use anyhow::Result;
use clippy_utilities::OverflowArithmetic;
use flume::Receiver;
use kube::api::DeleteParams;
use kube::{Api, CustomResourceExt};
use operator_api::HeartbeatStatus;
use tracing::{debug, error};

use crate::crd::Cluster;

/// State of sidecar operators
pub(crate) struct SidecarState {
    /// map for each sidecar operator and its status
    statuses: HashMap<String, HeartbeatStatus>,
    /// Receiver for heartbeat status
    status_rx: Receiver<HeartbeatStatus>,
    /// maximum interval between accepted `HeartbeatStatus`
    heartbeat_period: u64,
    /// Api for Cluster
    cluster_api: Api<Cluster>,
    /// unreachable cache
    unreachable: HashMap<String, usize>,
    /// unreachable counter threshold
    unreachable_thresh: usize,
}

impl SidecarState {
    /// Creates a new `SidecarState`
    pub(crate) fn new(
        status_rx: Receiver<HeartbeatStatus>,
        heartbeat_period: u64,
        cluster_api: Api<Cluster>,
        unreachable_thresh: usize,
    ) -> Self {
        Self {
            statuses: HashMap::new(),
            status_rx,
            heartbeat_period,
            cluster_api,
            unreachable: HashMap::new(),
            unreachable_thresh,
        }
    }

    /// Task that update the state received from sidecar operators
    pub(crate) async fn state_update_task(mut self) -> Result<()> {
        let spec_size = self.get_spec_size().await.unwrap_or(2);
        let majority = spec_size.overflow_add(1) / 2;
        debug!("spec.size: {spec_size}, majority: {majority}");

        loop {
            while let Ok(status) = self.status_rx.recv_async().await {
                debug!("reveived status: {status:?}");
                let _prev = self.statuses.insert(status.id.clone(), status);

                let mut statuses = self.statuses.values().collect::<Vec<_>>();
                statuses.sort();
                statuses.reverse();

                debug!("sorted statuses: {statuses:?}");

                let latest = statuses
                    .first()
                    .unwrap_or_else(|| unreachable!("there should be at lease one status"));
                // take timestamps that within the period from the latest
                let accepted = statuses.iter().take_while(|s| {
                    s.timestamp.overflow_add(self.heartbeat_period) >= latest.timestamp
                });

                // the current accepted status is leass than half
                if accepted.clone().count() < majority {
                    continue;
                }

                let reachable_counts = accepted.flat_map(|s| s.reachable_ids.iter()).fold(
                    HashMap::new(),
                    |mut map, id| {
                        let v = map.entry(id).or_insert(0);
                        *v = v.overflow_add(1);
                        map
                    },
                );

                debug!("reachable_counts: {reachable_counts:?}");

                for id in self.statuses.keys() {
                    let count = reachable_counts.get(id).copied().unwrap_or_else(|| 0);
                    // the sidecar operator is considered offline
                    if count < majority {
                        // if already in unreachable cache, increment the counter.
                        // we would consider the recovery is failed if the counter reach
                        // the threshold.
                        if let Some(cnt) = self.unreachable.get_mut(id) {
                            *cnt = cnt.overflow_add(1);
                            if *cnt == self.unreachable_thresh {
                                error!("failed to recover the operator: {id}");
                                let _ignore = self.unreachable.remove(id);
                                // TODO: notify the administrator
                            }
                        }
                        // otherwise delete the pod, which will trigger k8s to recreate it
                        else {
                            debug!("{id} is unreachable, count: {count}, deleteing the pod");
                            if let Err(e) =
                                self.cluster_api.delete(id, &DeleteParams::default()).await
                            {
                                error!("failed to delete pod {id}, {e}");
                            }
                            let _ignore = self.unreachable.insert(id.clone(), 0);
                        }
                    // if recoverd, remove it from the cache
                    } else if self.unreachable.remove(id).is_some() {
                        debug!("operator {id} recoverd");
                    }
                }
            }
        }
    }

    // get the cluster size in the specification
    async fn get_spec_size(&self) -> Result<usize> {
        let cluster = self.cluster_api.get(Cluster::crd_name()).await?;
        Ok(cluster
            .spec
            .size
            .try_into()
            .unwrap_or_else(|_| unreachable!("the spec size should not be negative")))
    }
}
