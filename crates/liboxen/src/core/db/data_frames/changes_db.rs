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
//! This module keeps one shared handle per path in a process-wide weak-ref registry.
//! Every caller for the same path receives the same `Arc<RwLock<DB>>` for as long
//! as at least one caller holds it; the DB closes when the last `Arc` drops, and
//! the map entry becomes a tombstone that the next insert prunes. There is no
//! capacity cap, so an in-use entry can never be evicted. A brief LOCK collision
//! is still possible when an open races the tail of a concurrent close (RocksDB
//! releases the OS lock in its `Drop`, after `strong_count` already hit zero); see
//! [`get_changes_db`] for the bounded-retry that waits it out.
//!
//! Handles are `Arc<RwLock<DB>>`: compound writes take `.write()` across the full
//! read-modify-write, readers take `.read()`. **Never hold a guard across `.await`**
//! — the `Arc` may cross awaits, the guard must not.

use parking_lot::{Mutex, RwLock};
use rocksdb::DB;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};
use std::thread::sleep;
use std::time::Duration;

use crate::core::db;
use crate::core::db::data_frames::DataFrameError;
use crate::error::OxenError;

/// Weak-ref registry of open change-tracking DB handles, keyed by db path.
static CHANGES_DB_INSTANCES: LazyLock<Mutex<HashMap<PathBuf, Weak<RwLock<DB>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// How long `get_changes_db` waits out a concurrent close before surfacing a LOCK error.
/// See the retry loop in [`get_changes_db`] for the race this covers.
const OPEN_RETRIES: u32 = 100;
const OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(2);

/// Remove this path's tombstone entry from the registry. Live entries (someone still
/// holds the `Arc`) are unaffected; the DB closes when the last strong reference drops.
pub fn remove_from_cache(db_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let mut instances = CHANGES_DB_INSTANCES.lock();
    instances.remove(db_path.as_ref());
    Ok(())
}

/// Remove tombstone entries under `prefix` from the registry. Live entries are unaffected;
/// the DB closes when the last strong reference drops.
pub fn remove_from_cache_with_children(prefix: impl AsRef<Path>) -> Result<(), OxenError> {
    let prefix = prefix.as_ref();
    let mut instances = CHANGES_DB_INSTANCES.lock();
    instances.retain(|key, _| !key.starts_with(prefix));
    Ok(())
}

/// Return the shared handle for a change-tracking db, opening it on first use.
/// The db directory is created if it does not exist, so this is the right entry
/// point for write paths (recording row/column changes) that always need a db to
/// write into.
///
/// Waits out a concurrent close: when another thread drops the last strong `Arc`, its
/// `strong_count` hits zero *before* RocksDB's `Drop` releases the OS `LOCK` file, so
/// a follow-up open here can briefly race the tail of that close. If `DB::open` fails
/// with a LOCK-collision error we retry a bounded number of times, re-checking the
/// registry each round in case another opener won the race.
pub fn get_changes_db(db_path: &Path) -> Result<Arc<RwLock<DB>>, DataFrameError> {
    let mut attempts = 0;
    loop {
        let mut instances = CHANGES_DB_INSTANCES.lock();
        if let Some(weak) = instances.get(db_path)
            && let Some(strong) = weak.upgrade()
        {
            return Ok(strong);
        }
        std::fs::create_dir_all(db_path).map_err(DataFrameError::FailCreateDfDbDir)?;
        let opts = db::key_val::opts::default();
        match DB::open(&opts, dunce::simplified(db_path)) {
            Ok(db) => {
                let handle = Arc::new(RwLock::new(db));
                instances.insert(db_path.to_path_buf(), Arc::downgrade(&handle));
                instances.retain(|_, weak| weak.strong_count() > 0);
                return Ok(handle);
            }
            Err(err) if is_lock_collision(&err) => {
                drop(instances);
                attempts += 1;
                if attempts >= OPEN_RETRIES {
                    return Err(err.into());
                }
                sleep(OPEN_RETRY_INTERVAL);
            }
            Err(err) => return Err(err.into()),
        }
    }
}

/// True if `err` is a RocksDB LOCK-file collision — i.e. another opener still holds the
/// per-directory LOCK. The check is by error-message match rather than a distinct
/// `ErrorKind` because RocksDB surfaces this as a plain `IOError` across platforms
/// (Unix "While lock file: …/LOCK: Resource temporarily unavailable",
/// Windows "Failed to create lock file: …\\LOCK: The process cannot access the file…").
fn is_lock_collision(err: &rocksdb::Error) -> bool {
    let msg = err.to_string();
    msg.contains("LOCK") || msg.contains("lock file")
}

/// Like [`get_changes_db`], but returns `None` when the db does not yet exist
/// on disk instead of creating it. Use for read paths (diffs, view decoration)
/// so that merely reading a data frame never materializes an empty
/// change-tracking db on disk.
pub fn try_get_changes_db(db_path: &Path) -> Result<Option<Arc<RwLock<DB>>>, DataFrameError> {
    // Scoped so the guard drops before the `get_changes_db` re-lock on the miss path.
    {
        let instances = CHANGES_DB_INSTANCES.lock();
        if let Some(weak) = instances.get(db_path)
            && let Some(strong) = weak.upgrade()
        {
            return Ok(Some(strong));
        }
    }
    // `CURRENT` exists once RocksDB has initialized the db
    if !db_path.join("CURRENT").exists() {
        return Ok(None);
    }
    get_changes_db(db_path).map(Some)
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
        const NUM_THREADS: usize = 16;

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

    /// Every concurrent opener for the same path returns the same shared `Arc`, so a
    /// distinct set of unrelated opens between two calls cannot break identity.
    #[test]
    fn test_repeated_opens_return_same_arc_under_pressure() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let held_path = data_dir.join("held");
            let held = get_changes_db(&held_path)?;
            held.write().put("k", "v")?;

            // Open (and drop) many unrelated handles between the two `get_changes_db` calls
            // on `held_path`.
            for i in 0..256 {
                let other = get_changes_db(&data_dir.join(format!("other-{i}")))?;
                drop(other);
            }

            let reopened = get_changes_db(&held_path)?;
            assert!(
                Arc::ptr_eq(&held, &reopened),
                "the held handle must survive registry pressure",
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
