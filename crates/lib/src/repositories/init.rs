//! # oxen init
//!
//! Initialize a local oxen repository
//!

use std::path::Path;

use crate::constants::MIN_OXEN_VERSION;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::StorageOpts;

/// # Initialize an Empty Oxen Repository
/// ```ignore
/// use liboxen::repositories;
/// use std::path::Path;
///
/// let base_dir = Path::new("repo_dir_init");
/// let repo = repositories::init(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
/// ```
pub fn init(path: &Path) -> Result<LocalRepository, OxenError> {
    init_with_version(path, MIN_OXEN_VERSION)
}

pub fn init_with_version(
    path: &Path,
    version: MinOxenVersion,
) -> Result<LocalRepository, OxenError> {
    match version {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::init_with_version_default(path, version),
    }
}

pub async fn init_with_storage_opts(
    path: &Path,
    storage_opts: Option<StorageOpts>,
) -> Result<LocalRepository, OxenError> {
    init_with_version_and_storage_opts(path, MIN_OXEN_VERSION, storage_opts).await
}

pub async fn init_with_version_and_storage_opts(
    path: &Path,
    version: MinOxenVersion,
    storage_opts: Option<StorageOpts>,
) -> Result<LocalRepository, OxenError> {
    match version {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::init_with_version_and_storage_opts(path, version, storage_opts).await,
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    use crate::util;

    #[tokio::test]
    async fn test_command_init() -> Result<(), OxenError> {
        test::run_empty_dir_test(|repo_dir| {
            // Init repo
            repositories::init(repo_dir)?;

            // Init should create the .oxen directory
            let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
            let config_file = util::fs::config_filepath(repo_dir);
            assert!(hidden_dir.exists());
            assert!(config_file.exists());

            Ok(())
        })
    }

    #[test]
    fn test_repositories_not_set_as_remote_mode_by_default() -> Result<(), OxenError> {
        test::run_empty_dir_test(|repo_dir| {
            // Init repo
            let repo = repositories::init(repo_dir)?;
            assert!(!repo.is_remote_mode());

            Ok(())
        })
    }
}
