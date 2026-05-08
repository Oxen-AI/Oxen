use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

use lru::LruCache;
use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock};

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

/// Paired-lock cache for per-commit `dir_hash_db` RocksDB handles. Each field solves a
/// disjoint problem — see each field. A single process-global instance ([`CACHE`]) is used;
/// the public free functions below just delegate into it.
struct DirHashDbCache {
    /// LRU of read-only RocksDB handles. Every access takes `.write()` briefly — `get` on
    /// a hit (which bumps LRU recency and so requires `&mut`) or `put` on a miss. The inner
    /// `Arc<DB>` lets the caller clone-and-release the lock quickly so the actual RocksDB
    /// operation runs outside the cache lock. Read-only opens don't touch RocksDB's `LOCK`
    /// file, so multiple readers and a writer can hold separate handles to the same path
    /// without conflicting at the OS level.
    handles: RwLock<LruCache<PathBuf, Arc<DBWithThreadMode<MultiThreaded>>>>,

    /// LRU of writable RocksDB handles, each behind its own `RwLock`. RocksDB's on-disk
    /// `LOCK` file is exclusive, so opening the same path writable twice in one process
    /// fails with "lock hold by current process". Caching one handle per path and serving
    /// callers via `RwLock::write()` keeps in-process writers sequential without ever
    /// racing on the OS lock. Reads via [`Self::handles`] are unaffected.
    writers: RwLock<LruCache<PathBuf, Arc<RwLock<DBWithThreadMode<SingleThreaded>>>>>,

    /// Per-repo rebuild barriers. Readers of a repo's dir_hash_db hold `.read()` across
    /// their whole operation; rebuilders take `.write()`. When the rebuilder gets the write
    /// guard, every outstanding reader of *this repo* has released its cloned `Arc<DB>`,
    /// so popping the cache entry drops the last reference and closes the RocksDB — which
    /// Windows requires before `rename` on the directory will succeed. Rebuilds in other
    /// repos are not blocked.
    ///
    /// Entries are never removed from the map; one entry per repo-path ever touched this
    /// process, ~40 bytes each, which is negligible for any realistic workload. The outer
    /// `Mutex` is held only for the brief map lookup.
    rebuild_barriers: Mutex<HashMap<PathBuf, Arc<RwLock<()>>>>,
}

static CACHE: LazyLock<DirHashDbCache> = LazyLock::new(|| DirHashDbCache {
    handles: RwLock::new(LruCache::new(DB_CACHE_SIZE)),
    writers: RwLock::new(LruCache::new(DB_CACHE_SIZE)),
    rebuild_barriers: Mutex::new(HashMap::new()),
});

impl DirHashDbCache {
    /// Get or create the rebuild barrier for a repo, keyed by repo path.
    fn barrier_for(&self, repo_path: &Path) -> Result<Arc<RwLock<()>>, OxenError> {
        let mut map = self
            .rebuild_barriers
            .lock()
            .map_err(|e| OxenError::LockPoisoned(format!("dir_hash_db barriers: {e}").into()))?;
        Ok(map
            .entry(repo_path.to_path_buf())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone())
    }

    /// Run `operation` against the cached RocksDB for a commit, opening it read-only on first
    /// access. Holds this repo's rebuild barrier as a reader across the closure so an
    /// in-flight rebuilder of *this repo* waits for the operation (and its cloned `Arc<DB>`)
    /// to finish before proceeding. Rebuilds in other repos are unaffected.
    fn with_reader<F, T>(
        &self,
        repository: &LocalRepository,
        commit_id: &str,
        operation: F,
    ) -> Result<T, OxenError>
    where
        F: FnOnce(&DBWithThreadMode<MultiThreaded>) -> Result<T, OxenError>,
    {
        let barrier = self.barrier_for(&repository.path)?;
        let _barrier_guard = barrier
            .read()
            .map_err(|e| OxenError::LockPoisoned(format!("dir_hash_db access: {e}").into()))?;

        let dir_hashes_db = {
            let dir_hashes_db_dir = dir_hash_db_path_from_commit_id(repository, commit_id);

            // `get` rather than `peek` so cache hits bump the entry's LRU recency. That needs
            // `&mut` on the cache, hence `.write()` for every access (hit or miss). The clone
            // is cheap and the write guard drops before `operation` runs, so the actual
            // RocksDB work happens outside the cache lock.
            let mut cache_w = self
                .handles
                .write()
                .map_err(|e| OxenError::LockPoisoned(format!("dir_hash_db LRU: {e}").into()))?;

            if let Some(db) = cache_w.get(&dir_hashes_db_dir) {
                Arc::clone(db)
            } else {
                if !dir_hashes_db_dir.exists() {
                    return Err(OxenError::path_does_not_exist(&dir_hashes_db_dir));
                }

                let opts = db::key_val::opts::default();
                let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
                    DBWithThreadMode::open_for_read_only(&opts, &dir_hashes_db_dir, false)?;

                let db = Arc::new(dir_hashes_db);
                cache_w.put(dir_hashes_db_dir, db.clone());
                db
            }
        };

        operation(&dir_hashes_db)
    }

    /// Run `operation` against the cached writable RocksDB for a commit, opening it on
    /// first access. Holds this repo's rebuild barrier as a reader (so a concurrent
    /// rebuilder waits) and takes the per-handle `RwLock::write()` so multiple writers
    /// targeting the same commit's dir_hashes serialize in-process instead of racing on
    /// RocksDB's on-disk `LOCK` file.
    fn with_writer<F, T>(
        &self,
        repository: &LocalRepository,
        commit_id: &str,
        operation: F,
    ) -> Result<T, OxenError>
    where
        F: FnOnce(&DBWithThreadMode<SingleThreaded>) -> Result<T, OxenError>,
    {
        let barrier = self.barrier_for(&repository.path)?;
        let _barrier_guard = barrier
            .read()
            .map_err(|e| OxenError::LockPoisoned(format!("dir_hash_db access: {e}").into()))?;

        let path = dir_hash_db_path_from_commit_id(repository, commit_id);

        let db_lock = {
            let mut cache_w = self.writers.write().map_err(|e| {
                OxenError::LockPoisoned(format!("dir_hash_db writer LRU: {e}").into())
            })?;

            if let Some(db) = cache_w.get(&path) {
                Arc::clone(db)
            } else {
                let opts = db::key_val::opts::default();
                let db: DBWithThreadMode<SingleThreaded> =
                    DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
                let arc = Arc::new(RwLock::new(db));
                cache_w.put(path, Arc::clone(&arc));
                arc
            }
        };

        let db = db_lock
            .write()
            .map_err(|e| OxenError::LockPoisoned(format!("dir_hash_db writer: {e}").into()))?;
        operation(&db)
    }

    /// Take this repo's rebuild barrier exclusively, pop the cache entry for `commit_id`,
    /// and run `operation`. The write-guard wait ensures no reader of *this repo* holds a
    /// cloned `Arc<DB>`, so popping drops the last reference and closes the RocksDB —
    /// releasing the OS file handles that would otherwise prevent `rename` from succeeding
    /// on Windows. Readers and rebuilders of other repos are unaffected.
    fn with_entry_evicted<F, R>(
        &self,
        repo: &LocalRepository,
        commit_id: &str,
        operation: F,
    ) -> Result<R, OxenError>
    where
        F: FnOnce() -> Result<R, OxenError>,
    {
        let barrier = self.barrier_for(&repo.path)?;
        let _barrier_guard = barrier
            .write()
            .map_err(|e| OxenError::LockPoisoned(format!("dir_hash_db access: {e}").into()))?;

        let path = dir_hash_db_path_from_commit_id(repo, commit_id);
        {
            let mut handles = self
                .handles
                .write()
                .map_err(|e| OxenError::LockPoisoned(format!("dir_hash_db LRU: {e}").into()))?;
            handles.pop(&path);
        }
        {
            let mut writers = self.writers.write().map_err(|e| {
                OxenError::LockPoisoned(format!("dir_hash_db writer LRU: {e}").into())
            })?;
            writers.pop(&path);
        }

        operation()
    }

    /// Remove any cached handles (read-only and writable) whose on-disk path starts with
    /// `prefix`. Takes only the LRU write locks — does not take `rebuild_barrier`. Callers
    /// that need the rebuild guarantee should go through [`Self::with_entry_evicted`].
    fn evict_prefix(&self, prefix: &Path) -> Result<(), OxenError> {
        evict_prefix_from(&self.handles, prefix, "dir_hash_db LRU")?;
        evict_prefix_from(&self.writers, prefix, "dir_hash_db writer LRU")?;
        Ok(())
    }
}

fn evict_prefix_from<V>(
    cache: &RwLock<LruCache<PathBuf, V>>,
    prefix: &Path,
    label: &str,
) -> Result<(), OxenError> {
    let mut instances = cache
        .write()
        .map_err(|e| OxenError::LockPoisoned(format!("{label}: {e}").into()))?;

    let dbs_to_remove = instances
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();

    for db in dbs_to_remove {
        let _ = instances.pop(&db);
    }

    Ok(())
}

// Commit db is the directories per commit
// This helps us skip to a directory in the tree
// .oxen/history/{COMMIT_ID}/dir_hashes
pub fn dir_hash_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    let commit_id = &commit.id;
    dir_hash_db_path_from_commit_id(repo, commit_id)
}

pub fn dir_hash_db_path_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> PathBuf {
    let commit_id = commit_id.as_ref();
    util::fs::oxen_hidden_dir(&repo.path)
        .join(Path::new(HISTORY_DIR))
        .join(commit_id)
        .join(DIR_HASHES_DIR)
}

/// Removes all dir_hashes DB instances from cache whose path starts with the given prefix.
/// Used in test cleanup to release file handles before directory deletion.
pub fn remove_from_cache_with_children(db_path_prefix: impl AsRef<Path>) -> Result<(), OxenError> {
    CACHE.evict_prefix(db_path_prefix.as_ref())
}

pub fn with_dir_hash_db_manager<F, T>(
    repository: &LocalRepository,
    commit_id: &str,
    operation: F,
) -> Result<T, OxenError>
where
    F: FnOnce(&DBWithThreadMode<MultiThreaded>) -> Result<T, OxenError>,
{
    CACHE.with_reader(repository, commit_id, operation)
}

/// Run `operation` against a cached writable dir_hash_db handle for `commit_id`.
///
/// Concurrent in-process callers serialize on the inner `RwLock`, so multiple writers
/// targeting the same commit never collide on RocksDB's on-disk `LOCK` file. Writers
/// targeting different commits or different repos are not blocked.
pub fn with_dir_hash_db_writer<F, T>(
    repository: &LocalRepository,
    commit_id: &str,
    operation: F,
) -> Result<T, OxenError>
where
    F: FnOnce(&DBWithThreadMode<SingleThreaded>) -> Result<T, OxenError>,
{
    CACHE.with_writer(repository, commit_id, operation)
}

/// Run `operation` with `commit_id`'s entry evicted from the dir_hash_db cache.
///
/// The repo's rebuild barrier is taken exclusively so no concurrent reader of this repo
/// holds a cloned `Arc<DB>`; popping the cache entry then drops the last reference, closing
/// the RocksDB and releasing OS file handles on the directory. The closure is free to rename
/// or replace the directory (on Windows this requires no process hold files inside it open).
/// The next reader after release reopens the DB on cache miss. Readers and rebuilders of
/// other repos are not blocked.
pub fn with_entry_evicted<F, R>(
    repo: &LocalRepository,
    commit_id: &str,
    operation: F,
) -> Result<R, OxenError>
where
    F: FnOnce() -> Result<R, OxenError>,
{
    CACHE.with_entry_evicted(repo, commit_id, operation)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use crate::core::db::key_val::str_val_db;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    /// Regression for the "lock hold by current process" RocksDB error: concurrent in-process
    /// writers targeting the same commit's `dir_hash_db` must not race on the on-disk `LOCK`.
    /// `with_dir_hash_db_writer` serializes them via the cached handle's inner `RwLock`. If a
    /// future change goes back to opening the DB directly, simultaneous opens collide and at
    /// least one of these threads errors out.
    #[tokio::test]
    async fn test_concurrent_writers_share_cached_handle() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit_id = repositories::commits::head_commit(&repo)?.id;

            const N: usize = 8;
            let start = Arc::new(Barrier::new(N));
            let handles: Vec<_> = (0..N)
                .map(|i| {
                    let repo = repo.clone();
                    let commit_id = commit_id.clone();
                    let start = Arc::clone(&start);
                    thread::spawn(move || -> Result<(), OxenError> {
                        start.wait();
                        super::with_dir_hash_db_writer(&repo, &commit_id, |db| {
                            str_val_db::put(db, format!("k{i}"), &format!("v{i}"))
                        })
                    })
                })
                .collect();

            for h in handles {
                h.join().expect("writer thread panicked")?;
            }

            // Confirm every writer's value actually landed.
            super::with_dir_hash_db_manager(&repo, &commit_id, |db| {
                for i in 0..N {
                    let got: Option<String> = str_val_db::get(db, format!("k{i}"))?;
                    assert_eq!(got.as_deref(), Some(&*format!("v{i}")));
                }
                Ok(())
            })
        })
        .await
    }
}
