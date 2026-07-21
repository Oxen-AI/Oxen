use std::path::Path;

use crate::config::RepositoryConfig;
use crate::core::db::merkle_node::DEFAULT_MERKLE_NODE_BACKEND;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::storage::StorageConfig;
use crate::util;

pub fn init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    if hidden_dir.exists() {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    // Cleanup the .oxen dir if init fails
    match init_with_version_default(path, MinOxenVersion::LATEST) {
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
) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    util::fs::create_dir_all(hidden_dir)?;
    if util::fs::config_filepath(path).try_exists()? {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    let config = RepositoryConfig {
        min_version: Some(version.to_string()),
        merkle_node_backend: Some(DEFAULT_MERKLE_NODE_BACKEND),
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
) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    util::fs::create_dir_all(hidden_dir)?;
    if util::fs::config_filepath(path).try_exists()? {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    let config = RepositoryConfig {
        min_version: Some(version.to_string()),
        storage: storage_config,
        merkle_node_backend: Some(DEFAULT_MERKLE_NODE_BACKEND),
        ..Default::default()
    };
    let repo = LocalRepository::new(path, config)?;
    repo.save()?;

    let version_store = repo.version_store();
    version_store.init().await?;

    Ok(repo)
}
