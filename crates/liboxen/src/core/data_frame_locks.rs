//! Process-global per-data-frame write lock — serializes the read-modify-write row operations on
//! a single workspace data frame's staged table.
//!
//! # Why it exists
//!
//! Appending, updating, or deleting a row reads the staged DuckDB table, derives new contents, and
//! writes them back. Two of those running at once on the same table produce last-writer-wins: the
//! loser's rows are discarded while both callers are told they succeeded. The cached DuckDB
//! connection ([`crate::core::db::data_frames::df_db`]) serializes writes only while both callers
//! share the cached connection — its cache evicts under LRU pressure, and an eviction between two
//! in-flight row writes hands each caller its own connection to the same file, at which point
//! nothing serializes them. This lock keys on the same identity the connection cache does but is
//! never evicted, so exclusion holds regardless of what the cache is currently holding.
//!
//! # Scope and ordering
//!
//! One lock per data frame, keyed by the path of its staged DuckDB file — the identity of a
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
//!   this lock, so no cycle with the read path — which indexes on demand — is possible.
//!
//! shortcut: an in-process lock, which serializes writers only within one oxen-server process. If
//! the server is ever run as more than one process per repository, this needs to become a lock the
//! processes share (a lock file on the workspace directory, or a row op that is atomic in DuckDB
//! itself).
//!
//! The registry never evicts: it holds one small mutex per data frame the process has written to,
//! for the life of the process.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;

static REGISTRY: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Run `work` with exclusive access to the data frame whose staged DuckDB table lives at
/// `db_path`, blocking until any other row write on that same data frame finishes.
pub fn with_data_frame_write<T>(db_path: &Path, work: impl FnOnce() -> T) -> T {
    // The registry guard is released at the end of this statement, before the data frame's own
    // lock is taken — holding both would serialize unrelated data frames.
    let lock = REGISTRY
        .lock()
        .entry(db_path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();
    let _guard = lock.lock();
    work()
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
