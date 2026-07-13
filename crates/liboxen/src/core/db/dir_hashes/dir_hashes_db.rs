use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

use parking_lot::{Mutex, RwLock};
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};

/// Paired cache for per-commit `dir_hash_db` RocksDB handles. The two fields solve disjoint
/// problems — see each field. A single process-global instance ([`CACHE`]) is used; the public
/// free functions below just delegate into it.
struct DirHashDbCache {
    /// Weak-ref registry of open read-only DB handles. The strong `Arc<DB>` lives only as long
    /// as some [`with_reader`](DirHashDbCache::with_reader) call holds it; when the last caller
    /// drops, RocksDB closes and the entry becomes a tombstone that the next opener prunes.
    /// There is no capacity cap, so an in-use entry can never be evicted — every concurrent
    /// reader of the same commit shares one handle instead of accumulating parallel RO opens.
    handles: Mutex<HashMap<PathBuf, Weak<DBWithThreadMode<MultiThreaded>>>>,

    /// Per-repo rebuild barriers. Readers of a repo's dir_hash_db hold `.read()` across their
    /// whole operation; rebuilders take `.write()`. When the rebuilder gets the write guard,
    /// every outstanding reader of *this repo* has released its cloned `Arc<DB>`, so the
    /// underlying RocksDB has already closed on that last drop — which Windows requires before
    /// `rename` on the directory will succeed. Rebuilds in other repos are not blocked.
    ///
    /// Entries are never removed from the map; one entry per repo-path ever touched this
    /// process, ~40 bytes each, which is negligible for any realistic workload. The outer
    /// `Mutex` is held only for the brief map lookup.
    rebuild_barriers: Mutex<HashMap<PathBuf, Arc<RwLock<()>>>>,
}

static CACHE: LazyLock<DirHashDbCache> = LazyLock::new(|| DirHashDbCache {
    handles: Mutex::new(HashMap::new()),
    rebuild_barriers: Mutex::new(HashMap::new()),
});

impl DirHashDbCache {
    /// Get or create the rebuild barrier for a repo, keyed by repo path.
    fn barrier_for(&self, repo_path: &Path) -> Arc<RwLock<()>> {
        let mut map = self.rebuild_barriers.lock();
        if let Some(barrier) = map.get(repo_path) {
            return barrier.clone();
        }
        map.entry(repo_path.to_path_buf())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Run `operation` against the cached RocksDB for a commit, opening it read-only on first
    /// access. Holds this repo's rebuild barrier as a reader across the closure so an in-flight
    /// rebuilder of *this repo* waits for the operation (and its cloned `Arc<DB>`) to finish
    /// before proceeding. Rebuilds in other repos are unaffected.
    fn with_reader<F, T>(
        &self,
        repository: &LocalRepository,
        commit_id: &String,
        operation: F,
    ) -> Result<T, OxenError>
    where
        F: FnOnce(&DBWithThreadMode<MultiThreaded>) -> Result<T, OxenError>,
    {
        let barrier = self.barrier_for(&repository.path);
        let _barrier_guard = barrier.read();

        let dir_hashes_db_dir = dir_hash_db_path_from_commit_id(repository, commit_id);
        let dir_hashes_db = self.open_reader(&dir_hashes_db_dir)?;
        operation(&dir_hashes_db)
    }

    /// Return the shared read-only handle for `dir_hashes_db_dir`, opening it on first access.
    /// The dir must already exist — dir_hashes are written when a commit is created, and readers
    /// error out (rather than materialize an empty dir) if the target commit has none on disk.
    fn open_reader(
        &self,
        dir_hashes_db_dir: &Path,
    ) -> Result<Arc<DBWithThreadMode<MultiThreaded>>, OxenError> {
        let mut handles = self.handles.lock();
        if let Some(strong) = lookup_live(&handles, dir_hashes_db_dir) {
            return Ok(strong);
        }
        if !dir_hashes_db_dir.exists() {
            return Err(OxenError::path_does_not_exist(dir_hashes_db_dir));
        }
        let opts = db::key_val::opts::default();
        let db = Arc::new(DBWithThreadMode::<MultiThreaded>::open_for_read_only(
            &opts,
            dir_hashes_db_dir,
            false,
        )?);
        handles.insert(dir_hashes_db_dir.to_path_buf(), Arc::downgrade(&db));
        handles.retain(|_, weak| weak.strong_count() > 0);
        Ok(db)
    }

    /// Take this repo's rebuild barrier exclusively, then run `operation`. The write-guard wait
    /// ensures no reader of *this repo* holds a cloned `Arc<DB>`; without any strong reference,
    /// the underlying RocksDB has already closed, so the OS file handles blocking `rename` on
    /// Windows are released. The closure is free to rename or replace the directory; the next
    /// reader reopens on cache miss. Readers and rebuilders of other repos are not blocked.
    fn with_exclusive_access<F, R>(
        &self,
        repo: &LocalRepository,
        operation: F,
    ) -> Result<R, OxenError>
    where
        F: FnOnce() -> Result<R, OxenError>,
    {
        let barrier = self.barrier_for(&repo.path);
        let _barrier_guard = barrier.write();
        operation()
    }

    /// Remove tombstone entries under `prefix` from the registry. Live entries (someone still
    /// holds the `Arc`) are unaffected; the DB closes when the last strong reference drops.
    fn evict_prefix(&self, prefix: &Path) {
        self.handles
            .lock()
            .retain(|key, _| !key.starts_with(prefix));
    }
}

fn lookup_live(
    handles: &HashMap<PathBuf, Weak<DBWithThreadMode<MultiThreaded>>>,
    dir_hashes_db_dir: &Path,
) -> Option<Arc<DBWithThreadMode<MultiThreaded>>> {
    handles.get(dir_hashes_db_dir)?.upgrade()
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

/// Removes tombstone entries under `db_path_prefix` from the registry. Live entries are
/// unaffected; the DB closes when the last strong reference drops. Used in test cleanup.
pub fn remove_from_cache_with_children(db_path_prefix: impl AsRef<Path>) -> Result<(), OxenError> {
    CACHE.evict_prefix(db_path_prefix.as_ref());
    Ok(())
}

pub fn with_dir_hash_db_manager<F, T>(
    repository: &LocalRepository,
    commit_id: &String,
    operation: F,
) -> Result<T, OxenError>
where
    F: FnOnce(&DBWithThreadMode<MultiThreaded>) -> Result<T, OxenError>,
{
    CACHE.with_reader(repository, commit_id, operation)
}

/// Run `operation` under exclusive access to `repo`'s dir_hash_dbs — no reader of this repo can
/// hold an `Arc<DB>` for the duration. See [`DirHashDbCache::with_exclusive_access`].
pub fn with_exclusive_access<F, R>(repo: &LocalRepository, operation: F) -> Result<R, OxenError>
where
    F: FnOnce() -> Result<R, OxenError>,
{
    CACHE.with_exclusive_access(repo, operation)
}
