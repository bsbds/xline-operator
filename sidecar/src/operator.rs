#![allow(dead_code)] // TODO remove when it is implemented

use std::net::SocketAddr;

use anyhow::Result;
use axum::{routing::get, Router};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::controller::Controller;
use crate::controller::Error;
use crate::health::Health;

/// Sidecar operator
#[derive(Debug)]
pub struct Operator {
    /// Operator config
    config: Config,
}

impl Operator {
    /// Constructor
    #[must_use]
    #[inline]
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Run operator
    ///
    /// # Errors
    ///
    /// Return Err when run failed
    #[inline]
    pub async fn run(&self) -> Result<()> {
        let (graceful_tx, mut graceful_rx) = tokio::sync::oneshot::channel();
        let forceful_shutdown = async {
            info!("press ctrl+c to shut down gracefully");
            let _ctrl_c = tokio::signal::ctrl_c().await;
            let _r = graceful_tx.send(());
            info!("graceful shutdown already requested, press ctrl+c again to force shut down");
            let _ctrl_c_c = tokio::signal::ctrl_c().await;
        };
        tokio::pin!(forceful_shutdown);

        let listen_addr = self.config.status_listen_addr.parse()?;
        let _ws_handle = tokio::spawn(Self::web_server(listen_addr));

        let op_health = Health::new(
            self.config.name.clone(),
            self.config.members.clone(),
            &self.config.deploy_op_addr,
            self.config.heartbeat_interval,
            self.config.client_timeout,
        )?;
        let _hb_handle = tokio::spawn(op_health.probe_task());

        let mut controller = Controller::new(self.config.clone());
        #[allow(clippy::integer_arithmetic)] // this error originates in the macro `tokio::select`
        loop {
            tokio::select! {
                _ = &mut forceful_shutdown => {
                    warn!("forceful shutdown");
                    break
                }
                res = controller.reconcile_once(&mut graceful_rx) => {
                    match res {
                        Ok(instant) => {
                            debug!("successfully reconcile the cluster states within {:?}", instant.elapsed());
                        }
                        Err(err) => {
                            if err == Error::Shutdown {
                                info!("graceful shutdown");
                                break
                            }
                            error!("reconcile failed, error: {}", err);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Run a server that exposes this operator's status
    async fn web_server(listen_addr: SocketAddr) -> Result<()> {
        let status = Router::new().route(
            "/health",
            get(|| async {
                debug!("received health request");
            }),
        );

        axum::Server::bind(&listen_addr)
            .serve(status.into_make_service())
            .await?;

        Ok(())
    }
}
