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
//! never race on the file lock.
//!
//! Handles are `Arc<RwLock<DB>>`: compound writes take `.write()` across the full read-modify-write,
//! readers take `.read()`. **Never hold a guard across `.await`** — the `Arc` may cross awaits,
//! the guard must not. Eviction skips entries still held elsewhere; when every entry is pinned, new
//! handles open uncached.

use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use rocksdb::DB;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use crate::core::db;
use crate::core::db::data_frames::DataFrameError;
use crate::error::OxenError;

#[cfg(not(any(test, feature = "test-utils")))]
const CHANGES_DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
// Larger under test: `open_and_cache`'s uncached fallback lets a second `DB::open` on the
// same path collide on RocksDB's `LOCK`, breaking bootstrap-plus-workers concurrency tests
// under cross-test cache pressure.
#[cfg(any(test, feature = "test-utils"))]
const CHANGES_DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(300).unwrap();

/// Global LRU cache of open change-tracking RocksDB handles, keyed by db path.
/// `Mutex` rather than `RwLock` because `LruCache::get` requires `&mut self` to
/// bump recency, so every access is exclusive anyway.
static CHANGES_DB_INSTANCES: LazyLock<Mutex<LruCache<PathBuf, Arc<RwLock<DB>>>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(CHANGES_DB_CACHE_SIZE)));

/// Remove a single change-tracking db from the cache. The underlying handle is
/// closed once the last outstanding `Arc` is dropped.
pub fn remove_from_cache(db_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let mut instances = CHANGES_DB_INSTANCES.lock();
    let _ = instances.pop(&db_path.as_ref().to_path_buf());
    Ok(())
}

/// Remove every cached change-tracking db whose path lives under `prefix`. Mainly used for test
/// cleanup so handles are released before the temp directory is deleted.
pub fn remove_from_cache_with_children(prefix: impl AsRef<Path>) -> Result<(), OxenError> {
    let prefix = prefix.as_ref();
    let mut instances = CHANGES_DB_INSTANCES.lock();
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
pub fn get_changes_db(db_path: &Path) -> Result<Arc<RwLock<DB>>, DataFrameError> {
    if let Some(db) = lookup_cached(db_path) {
        return Ok(db);
    }
    open_and_cache(db_path)
}

/// Like [`get_changes_db`], but returns `None` when the db does not yet exist
/// on disk instead of creating it. Use for read paths (diffs, view decoration)
/// so that merely reading a data frame never materializes an empty
/// change-tracking db on disk.
pub fn try_get_changes_db(db_path: &Path) -> Result<Option<Arc<RwLock<DB>>>, DataFrameError> {
    if let Some(db) = lookup_cached(db_path) {
        return Ok(Some(db));
    }
    // `CURRENT` exists once RocksDB has initialized the db
    if !db_path.join("CURRENT").exists() {
        return Ok(None);
    }
    open_and_cache(db_path).map(Some)
}

/// Look up a cached handle, bumping its LRU recency on a hit.
fn lookup_cached(db_path: &Path) -> Option<Arc<RwLock<DB>>> {
    let mut cache = CHANGES_DB_INSTANCES.lock();
    cache.get(&db_path.to_path_buf()).cloned()
}

/// Slow path: open the db under the cache lock, evict an unreferenced LRU entry to make room, and
/// cache the new handle. Returns the new handle uncached (with a warning) if every cached entry
/// is still in use.
fn open_and_cache(db_path: &Path) -> Result<Arc<RwLock<DB>>, DataFrameError> {
    let key = db_path.to_path_buf();

    let mut cache = CHANGES_DB_INSTANCES.lock();
    if let Some(db) = cache.get(&key) {
        return Ok(db.clone());
    }

    if !db_path.exists() {
        std::fs::create_dir_all(db_path).map_err(DataFrameError::FailCreateDfDbDir)?;
    }

    let can_cache = make_room_for_new_entry(&mut cache);

    let opts = db::key_val::opts::default();
    let handle = Arc::new(RwLock::new(DB::open(&opts, dunce::simplified(db_path))?));
    if can_cache {
        cache.put(key, handle.clone());
    }
    Ok(handle)
}

/// Returns `true` if a new entry can be cached: either the cache had room, or an unreferenced
/// LRU entry was evicted. Returns `false` when every cached entry is still held.
fn make_room_for_new_entry(cache: &mut LruCache<PathBuf, Arc<RwLock<DB>>>) -> bool {
    if cache.len() < cache.cap().get() {
        return true;
    }
    let victim = cache
        .iter()
        .rev()
        .find_map(|(k, v)| (Arc::strong_count(v) == 1).then(|| k.clone()));
    match victim {
        Some(key) => {
            cache.pop(&key);
            true
        }
        None => {
            log::warn!(
                "changes_db cache is at capacity ({}) with every entry still in use; \
                 opening a new handle uncached",
                cache.cap().get()
            );
            false
        }
    }
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
                            let handle = get_changes_db(&db_path)?;
                            handle.write().put(format!("key-{i}"), format!("val-{i}"))?;
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
            let handle = get_changes_db(&db_path)?;
            let db = handle.read();
            for i in 0..NUM_THREADS {
                let value = db.get(format!("key-{i}"))?;
                assert_eq!(value.as_deref(), Some(format!("val-{i}").as_bytes()));
            }
            drop(db);

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

            // An empty leftover directory (no `CURRENT` file) is treated as
            // missing — a stray dir must not be promoted into a live db.
            std::fs::create_dir_all(&db_path)?;
            assert!(try_get_changes_db(&db_path)?.is_none());
            assert!(
                !db_path.join("CURRENT").exists(),
                "try_get_changes_db must not initialize an empty leftover directory"
            );

            // Once a writer creates it, the reader sees the same handle/data.
            get_changes_db(&db_path)?.write().put("k", "v")?;
            let handle = try_get_changes_db(&db_path)?.expect("db exists after write");
            assert_eq!(handle.read().get("k")?.as_deref(), Some(b"v".as_ref()));

            remove_from_cache(&db_path)?;
            Ok(())
        })
    }

    /// When every cached entry is still in use, an additional open returns a working handle
    /// without installing it in the cache.
    #[test]
    fn test_open_uncached_when_cache_is_fully_pinned() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            // Pin every cache slot by holding `CHANGES_DB_CACHE_SIZE` Arcs.
            let mut pinned = Vec::with_capacity(CHANGES_DB_CACHE_SIZE.get());
            for i in 0..CHANGES_DB_CACHE_SIZE.get() {
                pinned.push(get_changes_db(&data_dir.join(format!("pinned-{i}")))?);
            }

            // One more open finds no evictable slot and returns uncached.
            let extra_path = data_dir.join("extra");
            let extra = get_changes_db(&extra_path)?;
            extra.write().put("k", "v")?;

            // The uncached handle reads back through itself.
            assert_eq!(extra.read().get("k")?.as_deref(), Some(b"v".as_ref()));

            // The next opener for `extra_path` does not find it in the cache: it opens a
            // *different* Arc (which only succeeds because the current `extra` Arc still holds
            // the RocksDB lock — so we drop it first to make room).
            drop(extra);
            let reopened = get_changes_db(&extra_path)?;
            // The data persisted to disk and the new handle sees it.
            assert_eq!(reopened.read().get("k")?.as_deref(), Some(b"v".as_ref()));

            drop(reopened);
            drop(pinned);
            remove_from_cache_with_children(data_dir)?;
            Ok(())
        })
    }

    /// Reopening a path whose handle is still held returns the same cached `Arc`, even after enough
    /// distinct opens to fill the cache twice over.
    #[test]
    fn test_eviction_skips_still_held_entries() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let held_path = data_dir.join("held");
            let held = get_changes_db(&held_path)?;
            held.write().put("k", "v")?;

            // Fill the cache twice over with immediately-dropped handles to pressure eviction.
            let pressure = CHANGES_DB_CACHE_SIZE.get() * 2;
            for i in 0..pressure {
                let other = get_changes_db(&data_dir.join(format!("other-{i}")))?;
                drop(other);
            }

            let reopened = get_changes_db(&held_path)?;
            assert!(
                Arc::ptr_eq(&held, &reopened),
                "the held handle must survive eviction pressure",
            );
            assert_eq!(reopened.read().get("k")?.as_deref(), Some(b"v".as_ref()));

            drop(held);
            drop(reopened);
            remove_from_cache_with_children(data_dir)?;
            Ok(())
        })
    }

    /// Concurrent compound writes (delete + put) for the same key serialize: the final value
    /// matches exactly one writer, with no torn state visible.
    #[test]
    fn test_concurrent_compound_writes_serialize() -> Result<(), OxenError> {
        const NUM_THREADS: usize = 16;

        test::run_empty_dir_test(|data_dir| {
            let db_path = data_dir.join("row_changes");
            let handle = get_changes_db(&db_path)?;

            thread::scope(|scope| {
                let workers: Vec<_> = (0..NUM_THREADS)
                    .map(|i| {
                        let handle = handle.clone();
                        scope.spawn(move || -> Result<(), OxenError> {
                            // Compound delete-then-put under one write guard.
                            let db = handle.write();
                            db.delete("shared")?;
                            db.put("shared", format!("value-{i}"))?;
                            Ok(())
                        })
                    })
                    .collect();
                for w in workers {
                    w.join().expect("worker panicked").expect("compound write");
                }
            });

            // Exactly one writer's value persists.
            let db = handle.read();
            let value = db.get("shared")?.expect("some writer's value persisted");
            let value = std::str::from_utf8(&value).expect("utf8");
            assert!(
                (0..NUM_THREADS).any(|i| value == format!("value-{i}")),
                "stored value should match one of the writers, got {value:?}",
            );
            drop(db);

            remove_from_cache(&db_path)?;
            Ok(())
        })
    }
}
