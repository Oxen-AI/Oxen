use std::path::Path;

use crate::config::RepositoryConfig;
use crate::config::repository_config::MerkleStoreKind;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::storage::StorageConfig;
use crate::util;

pub fn init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    if hidden_dir.exists() {
        return Err(OxenError::RepoAlreadyExistsAtPath(path.to_path_buf()));
    }

    // Cleanup the .oxen dir if init fails
    match init_with_version_default(path, MinOxenVersion::LATEST, false) {
        Ok(result) => Ok(result),
        Err(error) => {
            util::fs::remove_dir_all(hidden_dir)?;
            Err(error)
        }
    }
}

// default storage backend is local
pub fn init_with_version_default(
    path: &Path,
    version: MinOxenVersion,
    is_vfs: bool,
) -> Result<LocalRepository, OxenError> {
    init_with_version_and_merkle_store(path, version, is_vfs, MerkleStoreKind::default())
}

/// Sync init variant that lets the caller pick the [`MerkleStoreKind`].
pub fn init_with_version_and_merkle_store(
    path: &Path,
    version: MinOxenVersion,
    is_vfs: bool,
    merkle_store_kind: MerkleStoreKind,
) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    util::fs::create_dir_all(hidden_dir)?;
    if util::fs::config_filepath(path).try_exists()? {
        return Err(OxenError::RepoAlreadyExistsAtPath(path.to_path_buf()));
    }

    let config = RepositoryConfig {
        min_version: Some(version.to_string()),
        vfs: if is_vfs { Some(true) } else { None },
        merkle_store_kind,
        ..Default::default()
    };
    let repo = LocalRepository::new(path, config)?;
    repo.save()?;

    Ok(repo)
}

pub async fn init_with_version_and_storage_config(
    path: &Path,
    version: MinOxenVersion,
    storage_config: Option<StorageConfig>,
    is_vfs: bool,
) -> Result<LocalRepository, OxenError> {
    init_with_version_storage_and_merkle_store(
        path,
        version,
        storage_config,
        is_vfs,
        MerkleStoreKind::default(),
    )
    .await
}

/// Async init variant that lets the caller pick both the [`StorageConfig`] and the
/// [`MerkleStoreKind`]. Source of truth for the CLI's `oxen init` plumbing.
pub async fn init_with_version_storage_and_merkle_store(
    path: &Path,
    version: MinOxenVersion,
    storage_config: Option<StorageConfig>,
    is_vfs: bool,
    merkle_store_kind: MerkleStoreKind,
) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    util::fs::create_dir_all(hidden_dir)?;
    if util::fs::config_filepath(path).try_exists()? {
        return Err(OxenError::RepoAlreadyExistsAtPath(path.to_path_buf()));
    }

    let config = RepositoryConfig {
        min_version: Some(version.to_string()),
        storage: storage_config,
        vfs: if is_vfs { Some(true) } else { None },
        merkle_store_kind,
        ..Default::default()
    };
    let repo = LocalRepository::new(path, config)?;
    repo.save()?;

    let version_store = repo.version_store();
    version_store.init().await?;

    Ok(repo)
}
