//! Test helpers for preparing repository state.
//!
//! Includes both "fresh" (i.e. empty) repositories and functions for making
//! commits within a repository.
//!
//!
use std::path::PathBuf;

use crate::config::repository_config::MerkleStoreKind;
use crate::core::v_latest::init_with_version_and_merkle_store;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::repositories;
use crate::test::repo_guard::{RepoDirGuard, TestLocalRepo};
use crate::test::test_utils::run_async;
use crate::test::{
    add_txt_file_to_dir, create_prefixed_dir, create_repo_dir, generate_random_string,
    init_test_env, test_run_dir,
};

/// Base directory under which LMDB-backed test repos are rooted.
///
/// LMDB requires APIs that support the memory-mapped file semantics it requires. Many virtual
/// filesystems don't implement these APIs, so LMDB in general cannot work on a VFS. However,
/// RAM disks in Linux and macOS do support the mmap APIs LMDB needs.
///
/// On Windows, LMDB depends on NT memory-section APIs that are not implemented by RAM disks.
/// On Windows CI `OXEN_TEST_RUN_DIR=R:\test` is an ImDisk RAMDisk (a VFS) and opening an LMDB
/// env there fails with `Os { code: 1, .. }` → "Incorrect function." Routing LMDB-backed test
/// repos to the OS temp dir keeps the env on the host's real volume (NTFS on Windows runners).
///
/// This function mirrors `lmdb_test_root` in
/// `crates/liboxen/src/core/db/merkle_node/lmdb.rs`, used by the low-level [`LmdbBackend`] tests.
#[inline(always)]
fn lmdb_test_base() -> PathBuf {
    std::env::temp_dir().join("oxen-lmdb-tests")
}

/// Create a fresh empty dir suitable for an LMDB-backed test repo that deletes the directory on Drop.
/// The dir lives under [`lmdb_test_base`] rather than `OXEN_TEST_RUN_DIR`.
#[allow(dead_code)]
pub(crate) fn create_lmdb_safe_empty_dir() -> Result<RepoDirGuard, OxenError> {
    let dir = create_prefixed_dir(lmdb_test_base(), "dir")?;
    Ok(RepoDirGuard::new(dir))
}

/// Construct a fresh test repo dir, initialize a [`LocalRepository`] in it
/// with the given [`crate::config::repository_config::MerkleStoreKind`], and
/// return a [`TestLocalRepo`] that cleans the dir up on Drop. Mirrors the
/// repo shape produced by the former
/// `run_empty_local_repo_test_with_merkle_store` helper.
///
/// For `MerkleStoreKind::Lmdb` the repo dir is routed under
/// [`lmdb_test_base`] so the LMDB env never lands on a VFS like Windows CI's
/// ImDisk RAMDisk.
///
/// For tests that start on `File` but later transcode the same `repo.path`
/// into LMDB (i.e. the migration tests in
/// `m20260512000000_switch_merkle_store_to_lmdb`), use
/// [`init_lmdb_safe_test_repo_with_merkle_store`] instead — its `File` arm
/// also roots under [`lmdb_test_base`] so the later LMDB open succeeds on
/// Windows.
#[allow(dead_code)]
pub(crate) fn init_test_repo_with_merkle_store(
    kind: MerkleStoreKind,
) -> Result<TestLocalRepo, OxenError> {
    init_test_env();
    log::info!("<<<<< init_test_repo_with_merkle_store start ({kind:?})");
    let repo_dir = match kind {
        MerkleStoreKind::Lmdb => create_prefixed_dir(lmdb_test_base(), "dir")?,
        MerkleStoreKind::File => create_repo_dir(test_run_dir())?,
    };
    let guarded_repo = TestLocalRepo::new(|| {
        init_with_version_and_merkle_store(&repo_dir, MinOxenVersion::LATEST, false, kind)
    })?;
    log::info!(">>>>> init_test_repo_with_merkle_store ready");
    Ok(guarded_repo)
}

/// Variant of [`init_test_repo_with_merkle_store`] that roots **both** the
/// `File` and `Lmdb` arms under [`lmdb_test_base`] (the OS temp dir, i.e. NTFS
/// on Windows CI) instead of the usual `test_run_dir()`.
///
/// Use this for tests that start the repo on the file backend but later open
/// an LMDB env at the same `repo.path` — currently just the migration tests
/// in `m20260512000000_switch_merkle_store_to_lmdb`.
///
/// **Why this exists.** On the Windows CI runner `OXEN_TEST_RUN_DIR=R:\test`
/// is an ImDisk RAMDisk — a Win32-level virtual filesystem that does not
/// implement the NT memory-section APIs LMDB depends on. Opening an LMDB env
/// there fails with `STATUS_INVALID_DEVICE_REQUEST`, which `mdb_nt2win32`
/// translates to `ERROR_INVALID_FUNCTION` (Win32 code 1); the resulting
/// `Lmdb(Access(Io(Os { code: 1, .. })))` is reported as "Incorrect
/// function." The migration cannot work around this by redirecting LMDB to a
/// different path, because `LocalRepository::from_dir` (via
/// `lmdb::cache::get_or_open`) always opens the env at `repo.path`. The only
/// correct fix is to give the repo an NTFS-backed `repo.path` up front.
///
/// All other tests should keep using [`init_test_repo_with_merkle_store`] so
/// they continue to benefit from the RAMDisk on Windows CI.
#[allow(dead_code)]
pub(crate) fn init_lmdb_safe_test_repo_with_merkle_store(
    kind: MerkleStoreKind,
) -> Result<TestLocalRepo, OxenError> {
    init_test_env();
    log::info!("<<<<< init_lmdb_safe_test_repo_with_merkle_store start ({kind:?})");
    let prefix = match kind {
        MerkleStoreKind::Lmdb => "dir",
        MerkleStoreKind::File => "repo",
    };
    let repo_dir = create_prefixed_dir(lmdb_test_base(), prefix)?;
    let guarded_repo = TestLocalRepo::new(|| {
        init_with_version_and_merkle_store(&repo_dir, MinOxenVersion::LATEST, false, kind)
    })?;
    log::info!(">>>>> init_lmdb_safe_test_repo_with_merkle_store ready");
    Ok(guarded_repo)
}

/// Async variant of [`init_test_repo_with_merkle_store`] — also initializes
/// the version store, mirroring the former
/// `run_empty_local_repo_test_with_merkle_store_async` helper.
#[allow(dead_code)]
pub(crate) async fn init_test_repo_merkle_init_version_store_async(
    kind: MerkleStoreKind,
) -> Result<TestLocalRepo, OxenError> {
    let repo = init_test_repo_with_merkle_store(kind)?;
    repo.version_store().init().await?;
    Ok(repo)
}

#[allow(dead_code)]
pub(crate) fn apply_one_commit_local_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    let txt = generate_random_string(20);
    let file_path = add_txt_file_to_dir(&repo.path, &txt)?;
    run_async(async { repositories::add(repo, &file_path).await })??;
    repositories::commit(repo, "Init commit")?;
    Ok(())
}
