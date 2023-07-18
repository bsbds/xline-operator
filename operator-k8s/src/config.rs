use clap::Parser;

/// Xline operator config
#[derive(Debug, Parser)]
#[non_exhaustive]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// The namespace to deploy
    #[arg(long, default_value = "default")]
    pub namespace: String,
    /// Enable operator to work in all namespaces, the 'namespace' will be ignored when it is true
    #[arg(long, default_value = "false")]
    pub cluster_wide: bool,
    /// Whether to create CRD regardless of current version on k8s
    #[arg(long, default_value = "false")]
    pub create_crd: bool,
    /// The kubernetes cluster DNS suffix, default is 'cluster.local'
    #[arg(long, default_value = "cluster.local")]
    pub cluster_suffix: String,
}