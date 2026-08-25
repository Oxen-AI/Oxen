//! Process-global per-data-frame write lock: serializes the row operations that mutate a single
//! workspace data frame's staged table.
//!
//! # Why it exists
//!
//! Opening a DuckDB database file that this process already has open yields a second, independent
//! database rather than joining the first, because DuckDB's single-writer protection is a file lock
//! that excludes other processes. The two then diverge, and whichever folds its state into the file
//! last is the version that survives, so the other's rows are gone while both callers were told
//! they succeeded. That is true of any write, a lone `INSERT` included, so making a write atomic in
//! SQL would not remove the need for this lock.
//!
//! What normally keeps one database per file is the connection cache in
//! [`crate::core::db::data_frames::df_db`]. A cache entry is a connection behind a mutex, so
//! writers that find the same entry are serialized by it. That is incidental, and it stops holding
//! the moment the entry is gone: the cache evicts under LRU pressure, and `rename`, workspace
//! delete, and repo delete evict by hand. An eviction while one writer is still inside its
//! operation hands the next writer its own database over the same file. This lock keys on the same
//! identity the connection cache does, and its entry lives for as long as any caller holds it, so
//! exclusion holds regardless of what the cache is currently holding.
//!
//! # Scope and ordering
//!
//! One lock per data frame, keyed by the path of its staged DuckDB file, which is the identity of a
//! (repository, workspace, data frame path) triple. Writes to different data frames, different
//! workspaces, or different repositories never contend.
//!
//! It composes with [`crate::core::repo_locks`] rather than replacing it: a write entry point still
//! takes the repo's shared write reservation, which is what makes writes block against exclusive
//! maintenance operations. This lock only orders writers against each other.
//!
//! Two ordering rules keep it deadlock-free, and both are structural rather than conventional:
//!
//! - It is always taken *before* the DuckDB connection lock, never the other way around, because
//!   [`with_data_frame_write`] wraps whole row operations and the connection lock is taken inside
//!   them.
//! - It cannot be held across an `.await`, because [`with_data_frame_write`] takes a synchronous
//!   closure. Nothing that indexes or rebuilds the table (all of which is async) can be waiting on
//!   this lock, so no cycle with the read path, which indexes on demand, is possible.
//!
//! shortcut: an in-process lock, which serializes writers only within one oxen-server process. If
//! the server is ever run as more than one process per repository, this needs to become a lock the
//! processes share, such as a lock file on the workspace directory. Note that a row operation that
//! is atomic in DuckDB is *not* an alternative: the hazard is two databases over one file, which no
//! single statement addresses.
//!
//! The registry reclaims an entry once its last user is done, so it holds one small mutex per data
//! frame currently being written rather than one per data frame ever written. The reclaim runs from
//! a `Drop`, so it covers a panic inside the guarded work as well as a clean return.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;

static REGISTRY: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Holds a caller's reference to one data frame's lock, and drops the registry entry with it once
/// no other caller wants it. Reclaiming from `Drop` covers the unwind path, so a panic inside the
/// guarded work cannot leave the entry behind.
struct Reclaim<'a> {
    db_path: &'a Path,
    lock: Arc<Mutex<()>>,
}

impl Drop for Reclaim<'_> {
    fn drop(&mut self) {
        // A count of two means the map's reference and this one, so nobody else wants this lock.
        // Observing that while holding the registry lock is what makes removal safe: every clone
        // is taken under this same lock, so a caller blocked on the data frame's mutex, or one
        // that has cloned but not yet locked, is already counted here. The next caller for this
        // path simply creates a fresh entry.
        let mut registry = REGISTRY.lock();
        if Arc::strong_count(&self.lock) == 2 {
            registry.remove(self.db_path);
        }
    }
}

/// Run `work` with exclusive access to the data frame whose staged DuckDB table lives at
/// `db_path`, blocking until any other row write on that same data frame finishes.
pub fn with_data_frame_write<T>(db_path: &Path, work: impl FnOnce() -> T) -> T {
    // The registry guard is released at the end of this statement, before the data frame's own
    // lock is taken: holding both would serialize unrelated data frames.
    let reclaim = Reclaim {
        db_path,
        lock: REGISTRY
            .lock()
            .entry(db_path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone(),
    };

    // Declared after `reclaim` so it drops before it: the entry becomes reclaimable only once this
    // caller has released the data frame.
    let _guard = reclaim.lock.lock();
    work()
}

/// Whether the registry currently holds a lock for `db_path`. Keyed on one path rather than
/// reporting a total, so a sibling test writing to its own data frame cannot perturb it.
#[cfg(test)]
fn registry_holds(db_path: &Path) -> bool {
    REGISTRY.lock().contains_key(db_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Writers on one data frame run one at a time: a second writer cannot enter while the first
    // is inside its critical section.
    #[test]
    fn test_writes_to_one_data_frame_do_not_overlap() {
        let db_path = PathBuf::from("/data-frame-locks-test/one");
        let inside = Arc::new(AtomicUsize::new(0));
        let max_inside = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let db_path = db_path.clone();
                let inside = inside.clone();
                let max_inside = max_inside.clone();
                scope.spawn(move || {
                    for _ in 0..500 {
                        with_data_frame_write(&db_path, || {
                            let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
                            max_inside.fetch_max(now, Ordering::SeqCst);
                            std::hint::spin_loop();
                            inside.fetch_sub(1, Ordering::SeqCst);
                        });
                    }
                });
            }
        });

        assert_eq!(
            max_inside.load(Ordering::SeqCst),
            1,
            "two row writes on the same data frame overlapped"
        );
    }

    // The registry tracks the data frames being written, not every one ever written: an entry
    // exists while a write is in flight and is gone once the writers finish.
    #[test]
    fn test_registry_reclaims_a_data_frame_once_its_writers_finish() {
        let db_path = PathBuf::from("/data-frame-locks-test/reclaimed");

        with_data_frame_write(&db_path, || {
            assert!(
                registry_holds(&db_path),
                "a write in flight must hold its registry entry"
            );
        });
        assert!(
            !registry_holds(&db_path),
            "the registry kept a lock for a data frame with no writers"
        );

        // Same under contention, where the last writer out is the one that reclaims.
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let db_path = db_path.clone();
                scope.spawn(move || {
                    for _ in 0..100 {
                        with_data_frame_write(&db_path, std::hint::spin_loop);
                    }
                });
            }
        });
        assert!(
            !registry_holds(&db_path),
            "the registry kept a lock after concurrent writers finished"
        );
    }

    // A panic inside the guarded work reclaims the entry too, and leaves the lock usable rather
    // than held by the unwound caller.
    #[test]
    fn test_a_panic_inside_the_guarded_work_still_reclaims() {
        let db_path = PathBuf::from("/data-frame-locks-test/panicked");

        // The panic below is deliberate, so its output in the test log is expected.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_data_frame_write(&db_path, || {
                panic!("deliberate panic under the data frame lock")
            });
        }));
        assert!(outcome.is_err(), "the panic should have propagated");

        assert!(
            !registry_holds(&db_path),
            "a panic left its registry entry behind"
        );

        // Still acquirable: `parking_lot`'s mutex does not poison, and the unwind released it.
        with_data_frame_write(&db_path, || {});
    }

    // Each data frame gets its own lock, so a write to one never waits on a write to another.
    // Nesting proves it: two keys sharing a lock would deadlock here.
    #[test]
    fn test_writes_to_different_data_frames_take_different_locks() {
        let b = PathBuf::from("/data-frame-locks-test/b");
        let reached_inner =
            with_data_frame_write(&PathBuf::from("/data-frame-locks-test/a"), || {
                with_data_frame_write(&b, || true)
            });
        assert!(reached_inner);
    }
}
