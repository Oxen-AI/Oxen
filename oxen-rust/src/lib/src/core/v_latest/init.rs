use std::path::Path;

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::StorageOpts;
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

    let repo = LocalRepository::new_from_version(path, version.to_string(), None)?;
    repo.save()?;

    Ok(repo)
}

pub async fn init_with_version_and_storage_opts(
    path: &Path,
    version: MinOxenVersion,
    storage_opts: Option<StorageOpts>,
) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    util::fs::create_dir_all(hidden_dir)?;
    if util::fs::config_filepath(path).try_exists()? {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    let repo = LocalRepository::new_from_version(path, version.to_string(), storage_opts)?;
    repo.save()?;

    let version_store = repo.version_store()?;
    version_store.init().await?;

    Ok(repo)
}
