use serde::{Deserialize, Serialize};

/// Heartbeat status
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatStatus {
    /// the id of the sidecar operator
    pub id: String,
    /// the timestamp of this status
    pub timestamp: u64,
    /// reachable sidecar operator ids
    pub reachable_ids: Vec<String>,
}

impl HeartbeatStatus {
    /// Creates a new `HeartbeatStatus`
    pub fn new(id: String, timestamp: u64, reachable_ids: Vec<String>) -> Self {
        Self {
            id,
            timestamp,
            reachable_ids,
        }
    }
}
