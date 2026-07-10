use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::constants::{DEFAULT_VNODE_SIZE, MIN_OXEN_VERSION};
use crate::core::db::merkle_node::MerkleNodeBackend;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote};
use crate::storage::StorageConfig;
use crate::util;
use crate::util::fs::AtomicFile;

#[derive(Debug, Error)]
pub enum RepoConfigError {
    #[error("[RepositoryConfig] Failed to read config: {0}")]
    Read(Box<OxenError>),

    #[error("[RepositoryConfig] TOML read error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("[RepositoryConfig] TOML write error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("[RepositoryConfig] Failed to write config: {0}")]
    Write(Box<OxenError>),

    #[error("Cannot obtain current directory.")]
    CurDir,
}

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
    /// Which engine backs this repo's Merkle node store. The authoritative record of the repo's
    /// backend: written at `init` and updated by the file ↔ LMDB migration. `None` for repos whose
    /// config predates the field, which fall back to on-disk evidence (see `create_merkle_node_store`).
    pub merkle_node_backend: Option<MerkleNodeBackend>,
}

impl Default for RepositoryConfig {
    /// Default matches what `oxen init` writes to a fresh repo's `config.toml`: the current
    /// `MIN_OXEN_VERSION`, the default vnode size, and `None` for every per-repo override.
    fn default() -> Self {
        RepositoryConfig {
            remote_name: None,
            remotes: Vec::new(),
            subtree_paths: None,
            depth: None,
            min_version: Some(MIN_OXEN_VERSION.to_string()),
            vnode_size: Some(DEFAULT_VNODE_SIZE),
            storage: None,
            vfs: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
            merkle_node_backend: None,
        }
    }
}

impl RepositoryConfig {
    /// Serialize a repository config as a TOML formatted string.
    pub fn to_toml(&self) -> Result<String, RepoConfigError> {
        let toml = toml::to_string(&self)?;
        Ok(toml)
    }

    /// Parse the TOML string as a repository config.
    pub fn from_toml(toml: &str) -> Result<Self, RepoConfigError> {
        let remote_config: RepositoryConfig = toml::from_str(toml)?;
        Ok(remote_config)
    }

    /// Load the repository's config.
    pub fn from_repo(repo: &LocalRepository) -> Result<Self, RepoConfigError> {
        Self::from_file(util::fs::config_filepath(&repo.path))
    }

    /// Load a repository config from a specific file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, RepoConfigError> {
        let contents =
            util::fs::read_from_path(&path).map_err(|e| RepoConfigError::Read(Box::from(e)))?;
        Self::from_toml(&contents)
    }

    /// Save a repository config to the specified file.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), RepoConfigError> {
        let toml = self.to_toml()?;
        AtomicFile::new(path.as_ref())
            .write(toml.as_bytes())
            .map_err(|e| RepoConfigError::Write(Box::from(e)))?;
        Ok(())
    }

    /// The repository config's virtual node size.
    pub fn vnode_size(&self) -> u64 {
        self.vnode_size.unwrap_or(DEFAULT_VNODE_SIZE)
    }

    /// Loads a repository config from the current active working directory.
    pub(crate) fn from_current_dir() -> Result<Self, RepoConfigError> {
        let Some(repo_dir) = util::fs::get_repo_root_from_current_dir() else {
            return Err(RepoConfigError::CurDir);
        };
        let config_path = util::fs::config_filepath(&repo_dir);
        RepositoryConfig::from_file(&config_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageKind;
    use std::path::PathBuf;

    fn parse(toml_str: &str) -> RepositoryConfig {
        toml::from_str(toml_str).expect("test fixture must parse")
    }

    #[test]
    fn parses_canonical_storage_section() {
        let toml = r#"
            remotes = []

            [storage]
            kind = "local"
            versions_path = "/mnt/nfs/customer/.oxen/versions/files"
        "#;
        let config = parse(toml);
        let storage = config.storage.expect("storage section parsed");
        assert_eq!(storage.kind, StorageKind::Local);
        assert_eq!(
            storage.versions_path,
            Some(PathBuf::from("/mnt/nfs/customer/.oxen/versions/files"))
        );
    }

    #[test]
    fn promotes_legacy_settings_path_to_versions_path() {
        // The pre-rename TOML shape that customers could have on disk:
        // `[storage] type = "local"` plus `[storage.settings] path = "..."`.
        // The custom Deserialize ignores the legacy `type` key (kind defaults
        // to "local") and promotes `settings.path` into `versions_path`.
        let toml = r#"
            remotes = []

            [storage]
            type = "local"

            [storage.settings]
            path = "/mnt/nfs/customer/.oxen/versions/files"
        "#;
        let config = parse(toml);
        let storage = config.storage.expect("legacy storage section parsed");
        assert_eq!(storage.kind, StorageKind::Local);
        assert_eq!(
            storage.versions_path,
            Some(PathBuf::from("/mnt/nfs/customer/.oxen/versions/files"))
        );
    }

    #[test]
    fn legacy_config_round_trips_into_canonical_shape() {
        // After load+save, the legacy `[storage.settings]` subtable disappears
        // and `versions_path` shows up in `[storage]`.
        let legacy = r#"
            remotes = []

            [storage]
            type = "local"

            [storage.settings]
            path = "/mnt/nfs/customer/.oxen/versions/files"
        "#;
        let config = parse(legacy);
        let serialized = toml::to_string(&config).expect("re-serialize");

        assert!(
            serialized.contains("kind = \"local\""),
            "expected canonical `kind` key in:\n{serialized}"
        );
        assert!(
            serialized.contains("versions_path = \"/mnt/nfs/customer/.oxen/versions/files\""),
            "expected promoted `versions_path` in:\n{serialized}"
        );
        assert!(
            !serialized.contains("[storage.settings]"),
            "legacy subtable must not be re-emitted; got:\n{serialized}"
        );
        assert!(
            !serialized.contains("type ="),
            "legacy `type` key must not be re-emitted; got:\n{serialized}"
        );
    }

    #[test]
    fn top_level_versions_path_wins_over_legacy_settings_path() {
        let toml = r#"
            remotes = []

            [storage]
            kind = "local"
            versions_path = "/preferred/path"

            [storage.settings]
            path = "/should/be/ignored"
        "#;
        let config = parse(toml);
        let storage = config.storage.expect("storage section parsed");
        assert_eq!(
            storage.versions_path,
            Some(PathBuf::from("/preferred/path"))
        );
    }

    #[test]
    fn missing_storage_section_stays_none() {
        let toml = r#"
            remotes = []
        "#;
        let config = parse(toml);
        assert!(config.storage.is_none());
    }

    #[test]
    fn storage_section_without_versions_path_serializes_kind_only() {
        let toml = r#"
            remotes = []

            [storage]
            kind = "local"
        "#;
        let config = parse(toml);
        let serialized = toml::to_string(&config).expect("re-serialize");
        assert!(
            serialized.contains("kind = \"local\""),
            "expected `kind` in:\n{serialized}"
        );
        assert!(
            !serialized.contains("versions_path"),
            "absent versions_path must be skipped on serialize; got:\n{serialized}"
        );
    }
}
