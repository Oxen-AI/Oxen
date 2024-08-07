use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::OxenError;
use crate::model::Remote;
use crate::util;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryConfig {
    // this is the current remote name
    pub remote_name: Option<String>,
    pub remotes: Vec<Remote>,
    // write the version if it is past v0.18.4
    pub min_version: Option<String>,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl RepositoryConfig {
    pub fn new() -> Self {
        RepositoryConfig {
            remote_name: None,
            remotes: Vec::new(),
            min_version: None,
        }
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        let contents = util::fs::read_from_path(&path)?;
        let remote_config: RepositoryConfig = toml::from_str(&contents)?;
        Ok(remote_config)
    }
}