use std::path::PathBuf;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct OxenAppData {
    pub path: PathBuf,
    pub config: Config,
}

impl OxenAppData {
    pub fn new(path: PathBuf) -> OxenAppData {
        OxenAppData {
            path,
            config: Config::default(),
        }
    }

    /// Construct with an explicit server config.
    pub fn with_config(path: PathBuf, config: Config) -> OxenAppData {
        OxenAppData { path, config }
    }
}
