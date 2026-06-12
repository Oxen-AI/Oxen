use std::path::PathBuf;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct OxenAppData {
    pub path: PathBuf,
    pub config: Config,
    /// When true, relax some checks to facilitate testing. Never enable in normal operation.
    pub test_mode: bool,
}

impl OxenAppData {
    #[cfg(test)]
    pub fn new(path: PathBuf) -> OxenAppData {
        OxenAppData {
            path,
            config: Config::default(),
            test_mode: false,
        }
    }
}
