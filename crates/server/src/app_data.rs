use std::path::PathBuf;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct OxenAppData {
    pub path: PathBuf,
    pub config: Config,
}

impl OxenAppData {
    #[cfg(test)]
    pub fn new(path: PathBuf) -> OxenAppData {
        OxenAppData {
            path,
            config: Config::default(),
        }
    }
}
