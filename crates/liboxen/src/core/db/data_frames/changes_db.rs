//! Cached RocksDB handles for workspace data-frame change tracking.
//!
//! Row and column edits on a workspace data frame are recorded into small
//! RocksDB databases under `.oxen/mods/duckdb/<hash>/{row,column}_changes`.
//! RocksDB grants a single, process-exclusive lock per database directory (via
//! its `LOCK` file), so opening the same path a second time *within one
//! process* fails with:
//!
//! ```text
//! lock hold by current process ... LOCK: No locks available
//! ```
//!
//! That is exactly what happened when two oxen-server requests edited (or one
//! edited while another diffed) the same data frame concurrently: each call
//! used to `DB::open` the change-tracking database fresh, and the second open
//! collided with the lock the first still held.
//!
//! This module keeps one shared handle per path in a global LRU cache, mirroring
//! [`crate::core::staged::staged_db_manager`]. Every caller that touches the
//! same change-tracking database in this process shares that one handle, so they
//! never race on the file lock. RocksDB's `DB` is internally synchronized
//! (`Send + Sync`, with `&self` reads and writes), so a shared `Arc<DB>` is safe
//! to use concurrently — and, unlike a guard, it can be held across `.await`
//! points in the async restore paths.

use lru::LruCache;
use parking_lot::RwLock;
use rocksdb::DB;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use crate::core::db;
use crate::core::db::data_frames::DataFrameError;
use crate::error::OxenError;

const CHANGES_DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

/// Global LRU cache of open change-tracking RocksDB handles, keyed by db path.
static CHANGES_DB_INSTANCES: LazyLock<RwLock<LruCache<PathBuf, Arc<DB>>>> =
    LazyLock::new(|| RwLock::new(LruCache::new(CHANGES_DB_CACHE_SIZE)));

/// Remove a single change-tracking db from the cache. The underlying handle is
/// closed once the last outstanding `Arc` is dropped.
pub fn remove_from_cache(db_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let mut instances = CHANGES_DB_INSTANCES.write();
    let _ = instances.pop(&db_path.as_ref().to_path_buf());
    Ok(())
}

/// Remove every cached change-tracking db whose path lives under `prefix`.
/// Mainly used for test cleanup so handles are released before the temp
/// directory is deleted.
pub fn remove_from_cache_with_children(prefix: impl AsRef<Path>) -> Result<(), OxenError> {
    let prefix = prefix.as_ref();
    let mut instances = CHANGES_DB_INSTANCES.write();
    let to_remove: Vec<PathBuf> = instances
        .iter()
        .map(|(key, _)| key.clone())
        .filter(|key| key.starts_with(prefix))
        .collect();
    for key in to_remove {
        let _ = instances.pop(&key);
    }
    Ok(())
}

/// Return the shared handle for an existing change-tracking db, opening and
/// caching it on first use. The db directory is created if it does not exist,
/// so this is the right entry point for write paths (recording row/column
/// changes) that always need a db to write into.
pub fn get_changes_db(db_path: &Path) -> Result<Arc<DB>, DataFrameError> {
    if let Some(db) = lookup_cached(db_path) {
        return Ok(db);
    }
    open_and_cache(db_path)
}

/// Like [`get_changes_db`], but returns `None` when the db does not yet exist
/// on disk instead of creating it. Use for read paths (diffs, view decoration)
/// so that merely reading a data frame never materializes an empty
/// change-tracking db on disk.
pub fn try_get_changes_db(db_path: &Path) -> Result<Option<Arc<DB>>, DataFrameError> {
    if let Some(db) = lookup_cached(db_path) {
        return Ok(Some(db));
    }
    // Don't create a db just to read from it.
    if !db_path.exists() {
        return Ok(None);
    }
    open_and_cache(db_path).map(Some)
}

/// Fast path: hand back an existing handle under a short-lived read lock.
fn lookup_cached(db_path: &Path) -> Option<Arc<DB>> {
    let cache_r = CHANGES_DB_INSTANCES.read();
    cache_r.peek(&db_path.to_path_buf()).cloned()
}

/// Slow path: open the db under the cache write lock and store the handle.
/// Re-checks the cache first, since another thread may have opened the same
/// path while we waited for the write lock.
fn open_and_cache(db_path: &Path) -> Result<Arc<DB>, DataFrameError> {
    let key = db_path.to_path_buf();

    let mut cache_w = CHANGES_DB_INSTANCES.write();
    if let Some(db) = cache_w.get(&key) {
        return Ok(db.clone());
    }

    if !db_path.exists() {
        std::fs::create_dir_all(db_path).map_err(DataFrameError::FailCreateDfDbDir)?;
    }

    let opts = db::key_val::opts::default();
    let db = Arc::new(DB::open(&opts, dunce::simplified(db_path))?);
    cache_w.put(key, db.clone());
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use std::thread;

    #[test]
    fn test_get_changes_db_shares_one_handle_per_path() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let db_path = data_dir.join("row_changes");

            // Two opens of the same path return the *same* shared handle rather
            // than two competing RocksDB locks.
            let a = get_changes_db(&db_path)?;
            let b = get_changes_db(&db_path)?;
            assert!(
                Arc::ptr_eq(&a, &b),
                "repeated opens of the same path must reuse one cached handle"
            );

            remove_from_cache(&db_path)?;
            Ok(())
        })
    }

    #[test]
    fn test_concurrent_opens_do_not_hit_the_lock_error() -> Result<(), OxenError> {
        // Regression test for "lock hold by current process ... No locks
        // available": before caching, the second concurrent `DB::open` on the
        // same path failed. Now every thread shares the cached handle and can
        // read/write without colliding on the file lock.
        //
        // Spawn more threads than the cache can hold open at once
        // (`CHANGES_DB_CACHE_SIZE`) so the contention comfortably exceeds the
        // pool of concurrent handles the cache is sized for.
        const NUM_THREADS: usize = CHANGES_DB_CACHE_SIZE.get() + 8;

        test::run_empty_dir_test(|data_dir| {
            let db_path = data_dir.join("row_changes");
            // Open once up front so a live handle is held while the threads run.
            let _held = get_changes_db(&db_path)?;

            thread::scope(|scope| {
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|i| {
                        let db_path = db_path.clone();
                        scope.spawn(move || -> Result<(), OxenError> {
                            let db = get_changes_db(&db_path)?;
                            db.put(format!("key-{i}"), format!("val-{i}"))?;
                            Ok(())
                        })
                    })
                    .collect();
                for handle in handles {
                    handle
                        .join()
                        .expect("worker thread panicked")
                        .expect("concurrent open/write must not hit the RocksDB lock error");
                }
            });

            // Every concurrent write is visible through the shared handle.
            let db = get_changes_db(&db_path)?;
            for i in 0..NUM_THREADS {
                let value = db.get(format!("key-{i}"))?;
                assert_eq!(value.as_deref(), Some(format!("val-{i}").as_bytes()));
            }

            remove_from_cache(&db_path)?;
            Ok(())
        })
    }

    #[test]
    fn test_try_get_changes_db_does_not_create_missing_db() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let db_path = data_dir.join("row_changes");

            // Reading a db that was never written must not materialize it.
            assert!(try_get_changes_db(&db_path)?.is_none());
            assert!(
                !db_path.exists(),
                "try_get_changes_db must not create the db directory on a read miss"
            );

            // Once a writer creates it, the reader sees the same handle/data.
            get_changes_db(&db_path)?.put("k", "v")?;
            let db = try_get_changes_db(&db_path)?.expect("db exists after write");
            assert_eq!(db.get("k")?.as_deref(), Some(b"v".as_ref()));

            remove_from_cache(&db_path)?;
            Ok(())
        })
    }
}
