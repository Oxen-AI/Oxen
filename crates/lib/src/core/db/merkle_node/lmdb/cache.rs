//! Process-wide cache of open [`LmdbBackend`] instances, keyed by the repository root path.
//! Use the [`get_or_open`] function to manage creation and access to all [`LmdbBackend`]s.
//!
//! LMDB rejects opening the same database directory twice in one process (`mdb_env_open`
//! returns `MDB_BUSY`). Since a [`LocalRepository`] contains a Merkle tree store, it
//! may end up holding an [`Arc`] to an [`LmdbBackend`]. Creating another [`LocalRepoistory`]
//! at the same location while holding onto a previously created one will create a runtime `panic!`.
//!
//! The cache makes the constructing multiple `LocalRepository` instances pointing at the same
//! on-disk repo safe by maintaing a mapping of repository to a weak reference to the
//! [`LmdbBackend`] that it uses. Users of the cache only get [`Arc<LmdbBackend>`]. Overlapping
//! callers that want to re-create a [`LocalRepository`] or a [`LmdbBackend`] more specifically
//! must use this function to ensure that the process only ever has exactly one open LMDB per
//! repository.
//!
//! The cache does not hold the returned [`Arc`]. If the caller drops the only reference to the
//! `Arc`, then the [`LmdbBackend`] and its underlying [`heed::Env`] will also be dropped. At
//! which point, it is safe to re-open LMDB at the repository. The cache holds [`Weak`] references
//! to the [`LmdbBackend`] that it returns. This allows the process to deallocate the LMDB env
//! when it is possible, keeping the cache from holding onto unused LMDB environments and growing
//! without bound. If a requested [`LmdbBackend`] for a particular repository no longer exists,
//! it is recreated and put back into the cache. Each access of the cache incurs a sweep to
//! evict tombstoned [`Weak<LmdbBackend>`] references.
//!
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::core::db::merkle_node::lmdb::lmdb_backend::LmdbBackend;
use crate::core::db::merkle_node::lmdb::{DEFAULT_LMDB_MMAP_SIZE, LmdbError, lmdb_backend_options};
use crate::util;

/// Repository's root => Weak reference to its LMDB backend.
type LmdbCache = HashMap<PathBuf, Weak<LmdbBackend>>;

/// Process-wide cache of open LMDB backends, keyed by **canonicalized** repository root.
static LMDB_BACKEND_CACHE: OnceLock<Mutex<LmdbCache>> = OnceLock::new();

/// Single entry point for obtaining an [`Arc<LmdbBackend>`] for a repository.
///
/// Either returns the cached one (bumping its strong refcount) or opens a new LMDB
/// environment for the given [`LocalRepository`]'s root and installs a [`Weak`]
/// reference to it in the cache.
///
/// Using this cache is safe, provided that all LMDB env creation is managed through
/// this function.
///
/// Internally:
///   1. Canonicalizes `repo_root` so two unequal path shapes for the same physical repo.
///      This is critical to ensure that two repositories never have duplicate LMDB envs.
///   2. On cache hit, it increments the strong refcount and returns the Arc.
///   3. On cache miss:
///      a. Builds default LMDB open options [`lmdb_backend_options`]` with the default size
///         for the memory-mapped LMDB file ([`DEFAULT_LMDB_MMAP_SIZE`]).
///      b. `create_dir_all`s `.oxen/lmdb_merkle_tree_store/` to ensure the open works.
///      c. Calls [`LmdbBackend::new`] with these options and
///
/// # Concurrency
/// The mutex is held across the entire lookup-and-insert, which serializes
/// `LmdbBackend::new` calls process-wide. Opens are infrequent (one per repo
/// per process) and the serialization is what closes the race between
/// `Weak::upgrade` returning `None` and the subsequent `LmdbBackend::new`
/// call — without it, two threads could observe a dead `Weak`, both call
/// `LmdbBackend::new` on the same path, and one would hit LMDB's
/// "already open" error.
pub(crate) fn get_or_open(repo_root: &Path) -> Result<Arc<LmdbBackend>, LmdbError> {
    // ensure we always have the same key for identifical repositories
    let repo_root =
        util::fs::canonicalize(repo_root).map_err(|e| LmdbError::InitAbs(Box::new(e)))?;

    let cache = LMDB_BACKEND_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    // Recover the guard whether the mutex is poisoned or not. Poisoning
    // here is benign: it means an earlier opener panicked mid-`get_or_open`
    // — the worst the map can contain is a stale `Weak` (which the
    // tombstone sweep below prunes) or a missing entry (which causes a
    // reopen, also safe). There's no scenario where poisoned state lies
    // about which envs are shared.
    let mut guard = cache.lock().unwrap_or_else(|p| {
        log::error!("A previous acces of the LMDB cache had a panic while the mutex was locked. Attempting to clean up stale data, if applicable, and continue.");
        p.into_inner()
    });

    // check if we have created an LMDB backend for this repository
    // or if the one we created is still alive
    if let Some(weak) = guard.get(&repo_root)
        && let Some(strong) = weak.upgrade()
    {
        log::debug!("LMDB backend cache hit: {:?}", repo_root);
        return Ok(strong);
    }

    // Tombstone sweep: remove any of our weak references that no longer point
    // to a live LmdbBackend. This keeps the map size from growing unbounded.
    guard.retain(|_, w| w.strong_count() > 0);

    log::info!("LMDB backend cache miss: opening {}", repo_root.display());
    let options = lmdb_backend_options(DEFAULT_LMDB_MMAP_SIZE)?;
    let backend = Arc::new(LmdbBackend::new(repo_root.clone(), options)?);
    guard.insert(repo_root, Arc::downgrade(&backend));
    Ok(backend)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::config::repository_config::MerkleStoreKind;
    use crate::core::db::merkle_node::lmdb::cache;
    use crate::error::OxenError;
    use crate::test;

    /// Two `get_or_open` calls for the same canonical repo root return the
    /// same `Arc` — i.e. the underlying `LmdbBackend` (and its `Env`,
    /// `Database` handles) are shared. Pre-cache the second call would
    /// have hit LMDB's "already open in this program" error.
    #[test]
    fn test_cache_returns_same_backend_for_concurrent_opens() -> Result<(), OxenError> {
        let repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        let a = cache::get_or_open(&repo.path)?;
        let b = cache::get_or_open(&repo.path)?;
        assert!(
            Arc::ptr_eq(&a, &b),
            "expected same Arc<LmdbBackend> for two opens of the same path",
        );
        Ok(())
    }

    /// Two different canonical paths yield two distinct `LmdbBackend`s.
    #[test]
    fn test_cache_distinguishes_different_paths() -> Result<(), OxenError> {
        let repo_a = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        let repo_b = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        let a = cache::get_or_open(&repo_a.path)?;
        let b = cache::get_or_open(&repo_b.path)?;
        assert!(
            !Arc::ptr_eq(&a, &b),
            "two distinct repos must produce distinct LmdbBackends",
        );
        Ok(())
    }

    /// Drop the only strong handle, then verify the cache's `Weak` is a
    /// tombstone and a subsequent open yields a fresh `Arc`.
    #[test]
    fn test_cache_drops_backend_when_all_handles_released() -> Result<(), OxenError> {
        let mut repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        let repo_path = repo.path.clone();
        // Free the test helper's strong handle so this test owns the only
        // live reference at the point of `drop(a)` below.
        repo.drop_inner();

        let a = cache::get_or_open(&repo_path)?;
        let weak = Arc::downgrade(&a);
        drop(a);
        assert_eq!(
            weak.strong_count(),
            0,
            "dropping the last Arc must drop the LmdbBackend",
        );

        // Reopening must succeed (proves close-then-reopen) and must not
        // hand back the same allocation as before.
        let b = cache::get_or_open(&repo_path)?;
        assert!(
            weak.upgrade().is_none(),
            "the prior backend must be fully gone after the last Arc drops",
        );
        drop(b);
        Ok(())
    }

    /// Two open requests via path shapes that canonicalize to the same
    /// physical path collapse to one cache entry.
    #[test]
    fn test_cache_canonicalizes_path_shapes() -> Result<(), OxenError> {
        let repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        // Build a non-canonical variant: append a trailing `./.`.
        let nonsense = repo.path.join(".");
        let a = cache::get_or_open(&repo.path)?;
        let b = cache::get_or_open(&nonsense)?;
        assert!(
            Arc::ptr_eq(&a, &b),
            "different path shapes for the same physical repo must share the cache entry",
        );
        Ok(())
    }
}
