#![allow(dead_code)] // TODO remove when it is implemented

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// Sidecar operator config
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// Name of this node
    pub name: String,
    /// Operators
    pub members: HashMap<String, String>,
    /// Deploy operators address
    pub deploy_op_addr: String,
    /// Status server listen address
    pub status_listen_addr: String,
    /// Check cluster health interval
    pub check_interval: Duration,
    /// Send heartbeat interval
    pub heartbeat_interval: Duration,
    /// Timeout for the http client
    pub client_timeout: Duration,
    /// Backup storage config
    pub backup: Option<Backup>,
}

/// Backup storage config
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Backup {
    /// S3 storage
    S3 {
        /// S3 path
        path: String,
        /// S3 secret
        secret: String,
    },
    /// PV storage
    PV {
        /// Mounted path of pv
        path: PathBuf,
    },
}

impl Config {
    /// Constructor
    #[must_use]
    #[inline]
    #[allow(clippy::too_many_arguments)] // only called once
    pub fn new(
        name: String,
        members: HashMap<String, String>,
        deploy_op_addr: String,
        status_listen_addr: String,
        check_interval: Duration,
        heartbeat_interval: Duration,
        client_timeout: Duration,
        backup: Option<Backup>,
    ) -> Self {
        Self {
            name,
            members,
            deploy_op_addr,
            status_listen_addr,
            check_interval,
            heartbeat_interval,
            client_timeout,
            backup,
        }
    }
}
