use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use utoipa::ToSchema;

use crate::constants::DEFAULT_VNODE_SIZE;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote};
use crate::storage::StorageConfig;
use crate::util;

/// A sort of registry for known [`MerkleStore`] implementations that can be used by [`LocalRepository`].
/// This enum serves as a configuration option in a repository's `config.toml`
#[derive(Serialize, Deserialize, Debug, Clone, Copy, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MerkleStoreKind {
    /// The [`FileBackend`] store.
    File,
}

/// The default is the original custom file format based Merkle tree node storage.
impl Default for MerkleStoreKind {
    fn default() -> Self {
        Self::File
    }
}

/// Caller provided something that doesn't map to any known [`MerkleStoreKind`].
/// Contains the type name of the provided value.
#[derive(Debug, Error)]
#[error("No known configuration. Unknown MerkleStore kind: {0}")]
pub struct UnknownMerkleStoreKind(String);

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
    /// Flag for repositories stored on virtual file systems
    pub vfs: Option<bool>,
    /// Flag for remote-mode repositories
    pub remote_mode: Option<bool>,
    /// Currently used only for remote mode
    pub workspace_name: Option<String>,
    pub workspaces: Option<Vec<String>>,
    /// The type of Merkle tree node storage that the repository uses.
    pub merkle_store_kind: MerkleStoreKind,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        RepositoryConfig {
            remote_name: None,
            remotes: Vec::new(),
            subtree_paths: None,
            depth: None,
            min_version: None,
            vnode_size: Some(DEFAULT_VNODE_SIZE),
            storage: None,
            vfs: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
            merkle_store_kind: MerkleStoreKind::default(),
        }
    }
}

impl RepositoryConfig {
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
