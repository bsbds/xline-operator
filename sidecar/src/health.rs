use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt, TryFutureExt};
use operator_api::HeartbeatStatus;
use tokio::time::{interval, Interval};
use tracing::debug;

const DEPLOY_OPERATOR_ROUTE: &'static str = "/status";
const MEMBER_HEALTH_ROUTE: &'static str = "/health";

/// Struct for building the heartbeat task
#[derive(Debug)]
pub(crate) struct Health {
    /// id of this sidecar operator
    id: String,
    /// sidecar operators
    members: HashMap<String, String>,
    /// deploy operator addr
    deploy_operator_url: String,
    /// interval between two heartbeat
    send_interval: Interval,
    /// Client used to send heartbeat
    client: reqwest::Client,
}

impl Health {
    /// Creates a new `Heartbeat`
    pub(crate) fn new(
        id: String,
        members: HashMap<String, String>,
        deploy_operator_addr: &str,
        heartbeat_interval: Duration,
        client_timeout: Duration,
    ) -> Result<Self> {
        let deploy_operator_url = format!("http://{deploy_operator_addr}{DEPLOY_OPERATOR_ROUTE}");
        let members = members
            .into_iter()
            .map(|(mid, addr)| (mid, format!("http://{addr}{MEMBER_HEALTH_ROUTE}")))
            .collect();

        let client = reqwest::Client::builder().timeout(client_timeout).build()?;

        Ok(Self {
            id,
            send_interval: interval(heartbeat_interval),
            members,
            deploy_operator_url,
            client,
        })
    }

    /// Task that periodically probe status from other sidecar operators and send to deploy operator
    pub(crate) async fn probe_task(mut self) -> Result<()> {
        debug!("started probe task, with config: {self:?}");
        loop {
            let _instant = self.send_interval.tick().await;
            let status = self.probe().await?;
            self.send_heartbeat(status).await?;
        }
    }

    /// Probe the health status of other sidecar operators
    async fn probe(&self) -> Result<HeartbeatStatus> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {e}"))
            .as_secs();

        debug!("probe timestamp: {timestamp}");

        let mut status: Vec<_> = self
            .members
            .iter()
            .map(|(id, addr)| self.client.get(addr).send().map_ok(|_resp| id.clone()))
            .collect::<FuturesUnordered<_>>()
            .filter_map(|result| future::ready(result.ok()))
            .collect()
            .await;

        // make sure that the sidecar operator itself is in status
        if !status.contains(&self.id) {
            status.push(self.id.clone());
        }

        Ok(HeartbeatStatus::new(self.id.clone(), timestamp, status))
    }

    /// Send heartbeat to deploy operator
    async fn send_heartbeat(&self, status: HeartbeatStatus) -> Result<()> {
        debug!("sending heartbeat status: {status:?}");
        let _resp = self
            .client
            .post(self.deploy_operator_url.clone())
            .json(&status)
            .send()
            .await?;

        Ok(())
    }
}
