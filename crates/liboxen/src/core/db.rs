//! Interacting with the oxen databases
//!

use std::thread::sleep;
use std::time::Duration;

pub mod data_frames;
pub mod dir_hashes;
pub mod key_val;
pub mod merkle_node;
pub(crate) mod weak_cache;

/// How long a weak-handle registry waits out a concurrent close before surfacing a LOCK error.
pub const OPEN_RETRIES: u32 = 100;
pub const OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(2);

/// True if `err` is a RocksDB LOCK-file collision, i.e. another opener still holds the
/// per-directory LOCK. Matched on the message rather than a distinct `ErrorKind` because
/// RocksDB surfaces every lock failure as a plain `IOError`:
/// Unix "While lock file: …/LOCK: Resource temporarily unavailable",
/// same-process "lock hold by current process, acquire time …: …/LOCK: No locks available",
/// Windows "Failed to create lock file: …\\LOCK: <system message>" (the Windows text is
/// locale-dependent, so that prefix also covers other failures to open the LOCK file).
pub fn is_lock_collision(err: &rocksdb::Error) -> bool {
    let msg = err.to_string();
    msg.contains("While lock file")
        || msg.contains("lock hold by current process")
        || msg.contains("Failed to create lock file")
}

/// Runs `open`, waiting out a LOCK collision so an open that races the tail of a concurrent close
/// succeeds once that close releases the OS lock. Surfaces the collision as an error only after
/// [`OPEN_RETRIES`] attempts.
pub(crate) fn open_with_lock_retry<T>(
    mut open: impl FnMut() -> Result<T, rocksdb::Error>,
) -> Result<T, rocksdb::Error> {
    let mut attempts = 0;
    loop {
        match open() {
            Err(err) if is_lock_collision(&err) => {
                attempts += 1;
                if attempts >= OPEN_RETRIES {
                    return Err(err);
                }
                sleep(OPEN_RETRY_INTERVAL);
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::{DBWithThreadMode, MultiThreaded};

    use super::*;
    use crate::error::OxenError;
    use crate::test;
    #[cfg(unix)]
    use crate::util;

    #[test]
    fn test_is_lock_collision_on_a_held_lock() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let db_path = dir.join("db");
            let _held: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&key_val::opts::default(), &db_path)?;

            let err = DBWithThreadMode::<MultiThreaded>::open(&key_val::opts::default(), &db_path)
                .expect_err("a second open of a locked path must fail");
            assert!(
                is_lock_collision(&err),
                "expected a LOCK collision, got: {err}"
            );

            Ok(())
        })
    }

    /// An unrelated failure whose path happens to contain "LOCK" is not a collision.
    #[test]
    fn test_is_lock_collision_ignores_lock_in_the_path() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let db_path = dir.join("LOCK").join("db");
            let mut opts = key_val::opts::default();
            opts.create_if_missing(false);

            let err = DBWithThreadMode::<MultiThreaded>::open(&opts, &db_path)
                .expect_err("opening a missing db without create_if_missing must fail");
            let msg = err.to_string();
            assert!(msg.contains("LOCK"), "path missing from the error: {msg}");
            assert!(!is_lock_collision(&err), "misread as a collision: {msg}");

            let retried =
                open_with_lock_retry(|| DBWithThreadMode::<MultiThreaded>::open(&opts, &db_path))
                    .expect_err("a non-collision failure must not be retried into success");
            assert!(
                !is_lock_collision(&retried),
                "the retry changed which error surfaced: {retried}"
            );

            Ok(())
        })
    }

    /// A LOCK path RocksDB cannot open as a file is not a collision. Unix only: the same setup
    /// reports "Access is denied" on Windows, which is indistinguishable from a sharing
    /// violation without matching localized system text.
    #[cfg(unix)]
    #[test]
    fn test_is_lock_collision_ignores_an_unopenable_lock_path() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let db_path = dir.join("db");
            util::fs::create_dir_all(db_path.join("LOCK"))?;

            let err = DBWithThreadMode::<MultiThreaded>::open(&key_val::opts::default(), &db_path)
                .expect_err("a db whose LOCK path is a directory must fail to open");
            let msg = err.to_string();
            assert!(
                msg.contains("while open a file for lock"),
                "expected a failure to open the LOCK file, got: {msg}"
            );
            assert!(!is_lock_collision(&err), "misread as a collision: {msg}");

            Ok(())
        })
    }
}
