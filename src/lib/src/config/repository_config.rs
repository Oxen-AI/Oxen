use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::constants::DEFAULT_VNODE_SIZE;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote};
use crate::storage::StorageConfig;
use crate::util;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryConfig {
    // this is the current remote name
    pub remote_name: Option<String>,
    pub remotes: Vec<Remote>,
    // If the user clones a subtree, we store the paths here so that we know we don't have the full tree
    pub subtree_paths: Option<Vec<PathBuf>>,
    // If the user clones with a depth, we store the depth here so that we know we don't have the full tree
    pub depth: Option<i32>,
    // write the version if it is past v0.18.4
    pub min_version: Option<String>,
    pub vnode_size: Option<u64>,
    /// Storage configuration
    pub storage: Option<StorageConfig>,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        RepositoryConfig {
            vnode_size: Some(DEFAULT_VNODE_SIZE),
            ..Self::new()
        }
    }
}

impl RepositoryConfig {
    pub fn new() -> Self {
        RepositoryConfig {
            remote_name: None,
            remotes: Vec::new(),
            subtree_paths: None,
            depth: None,
            min_version: None,
            vnode_size: None,
            storage: None,
        }
    }

    pub fn from_repo(repo: &LocalRepository) -> Result<Self, OxenError> {
        let path = util::fs::config_filepath(&repo.path);
        Self::from_file(&path)
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        let contents = util::fs::read_from_path(&path)?;
        let remote_config: RepositoryConfig = toml::from_str(&contents)?;
        Ok(remote_config)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        util::fs::write_to_path(&path, toml)?;
        Ok(())
    }

    pub fn vnode_size(&self) -> u64 {
        self.vnode_size.unwrap_or(DEFAULT_VNODE_SIZE)
    }
}
