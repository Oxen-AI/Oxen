use std::path::Path;

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
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    util::fs::create_dir_all(hidden_dir)?;
    if util::fs::config_filepath(path).try_exists()? {
        return Err(OxenError::RepoAlreadyExistsAtPath(path.to_path_buf()));
    }

    let repo = LocalRepository::new_from_version(path, version.to_string(), None, is_vfs)?;
    repo.save()?;

    Ok(repo)
}

pub async fn init_with_version_and_storage_config(
    path: &Path,
    version: MinOxenVersion,
    storage_config: Option<StorageConfig>,
    is_vfs: bool,
) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    util::fs::create_dir_all(hidden_dir)?;
    if util::fs::config_filepath(path).try_exists()? {
        return Err(OxenError::RepoAlreadyExistsAtPath(path.to_path_buf()));
    }

    let repo =
        LocalRepository::new_from_version(path, version.to_string(), storage_config, is_vfs)?;
    repo.save()?;

    let version_store = repo.version_store();
    version_store.init().await?;

    Ok(repo)
}
