//! # oxen init
//!
//! Initialize a local oxen repository
//!

use std::path::Path;

use crate::config::repository_config::MerkleStoreKind;
use crate::constants::MIN_OXEN_VERSION;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::storage::StorageConfig;

/// # Initialize an Empty Oxen Repository
/// ```ignore
/// use liboxen::repositories;
/// use std::path::Path;
///
/// let base_dir = Path::new("repo_dir_init");
/// let repo = repositories::init(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
/// ```
pub fn init(path: impl AsRef<Path>) -> Result<LocalRepository, OxenError> {
    init_with_version(path, MIN_OXEN_VERSION)
}

pub fn init_with_version(
    path: impl AsRef<Path>,
    version: MinOxenVersion,
) -> Result<LocalRepository, OxenError> {
    init_with_version_and_merkle_store(path, version, MerkleStoreKind::default())
}

/// Like [`init_with_version`] but lets the caller pick the Merkle store backend.
pub fn init_with_version_and_merkle_store(
    path: impl AsRef<Path>,
    version: MinOxenVersion,
    merkle_store_kind: MerkleStoreKind,
) -> Result<LocalRepository, OxenError> {
    let path = path.as_ref();
    core::v_latest::init_with_version_and_merkle_store(path, version, false, merkle_store_kind)
}

pub async fn init_with_storage_config(
    path: impl AsRef<Path>,
    storage_config: Option<StorageConfig>,
) -> Result<LocalRepository, OxenError> {
    init_with_version_and_storage_config(path, MIN_OXEN_VERSION, storage_config).await
}

pub async fn init_with_version_and_storage_config(
    path: impl AsRef<Path>,
    version: MinOxenVersion,
    storage_config: Option<StorageConfig>,
) -> Result<LocalRepository, OxenError> {
    init_with_version_storage_and_merkle_store(
        path,
        version,
        storage_config,
        MerkleStoreKind::default(),
    )
    .await
}

/// Async init variant that lets the caller pick both the [`StorageConfig`] and the
/// [`MerkleStoreKind`]. This is the entry point the CLI's `oxen init` calls.
pub async fn init_with_version_storage_and_merkle_store(
    path: impl AsRef<Path>,
    version: MinOxenVersion,
    storage_config: Option<StorageConfig>,
    merkle_store_kind: MerkleStoreKind,
) -> Result<LocalRepository, OxenError> {
    let path = path.as_ref();
    core::v_latest::init_with_version_storage_and_merkle_store(
        path,
        version,
        storage_config,
        false,
        merkle_store_kind,
    )
    .await
}

#[cfg(test)]
mod tests {
    use crate::config::repository_config::MerkleStoreKind;
    use crate::constants::MIN_OXEN_VERSION;
    use crate::error::OxenError;
    use crate::model::LocalRepository;
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

    /// `repositories::init` (no kind arg) selects [`MerkleStoreKind::File`].
    #[test]
    fn test_init_default_merkle_store_kind_is_file() -> Result<(), OxenError> {
        test::run_empty_dir_test(|repo_dir| {
            let repo = repositories::init(repo_dir)?;
            assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::File);
            Ok(())
        })
    }

    /// `init_with_version_and_merkle_store` with [`MerkleStoreKind::Lmdb`]
    /// gives a repo whose `merkle_store_kind` getter reports `Lmdb`, persists
    /// that choice to `config.toml`, and reloads it on the next `from_dir`.
    #[test]
    fn test_init_with_lmdb_persists_kind() -> Result<(), OxenError> {
        // LMDB envs can't open on a VFS (e.g. Windows CI's ImDisk RAMDisk at
        // R:\test); route this test's repo dir off OXEN_TEST_RUN_DIR via the
        // dedicated guard helper.
        let guard = test::create_lmdb_safe_empty_dir()?;
        let repo_dir = guard.path();
        let repo = repositories::init::init_with_version_and_merkle_store(
            repo_dir,
            MIN_OXEN_VERSION,
            MerkleStoreKind::Lmdb,
        )?;
        assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::Lmdb);
        drop(repo); // close the LMDB env so we can reopen

        let reloaded = LocalRepository::from_dir(repo_dir)?;
        assert_eq!(reloaded.merkle_store_kind(), MerkleStoreKind::Lmdb);
        Ok(())
    }

    /// Async init path (used by the CLI) also respects an explicit
    /// [`MerkleStoreKind::Lmdb`].
    #[tokio::test]
    async fn test_init_with_storage_and_lmdb() -> Result<(), OxenError> {
        let guard = test::create_lmdb_safe_empty_dir()?;
        let repo = repositories::init::init_with_version_storage_and_merkle_store(
            guard.path(),
            MIN_OXEN_VERSION,
            None,
            MerkleStoreKind::Lmdb,
        )
        .await?;
        assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::Lmdb);
        Ok(())
    }
}
