use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use utoipa::ToSchema;

use crate::constants::DEFAULT_VNODE_SIZE;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote};
use crate::storage::StorageConfig;
use crate::util;

/// A sort of registry for known [`MerkleStore`] implementations that can be used by [`LocalRepository`].
/// This enum serves as a configuration option in a repository's `config.toml`
#[derive(Serialize, Deserialize, Debug, Clone, Copy, ToSchema)]
// TODO: remove Serialize + Deserialize derives. These are only necessary because `LocalRepository`
//       requires them. Those bounds will eventually be dropped.
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
    #[serde(default)]
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
