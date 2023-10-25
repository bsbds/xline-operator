/// Default backup PV mount path in container, this path cannot be mounted by user
pub const DEFAULT_BACKUP_DIR: &str = "/xline-backup";
/// Default xline data dir, this path cannot be mounted by user
pub const DEFAULT_DATA_DIR: &str = "/usr/local/xline/data-dir";
/// the URL ROUTE that sidecar sends heartbeat status to
pub const OPERATOR_MONITOR_ROUTE: &str = "/monitor";
/// the URL ROUTE of each sidecar for backup
pub const SIDECAR_BACKUP_ROUTE: &str = "/backup";
