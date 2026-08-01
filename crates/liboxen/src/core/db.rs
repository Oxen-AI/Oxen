//! Interacting with the oxen databases
//!

use std::time::Duration;

pub mod data_frames;
pub mod dir_hashes;
pub mod key_val;
pub mod merkle_node;

/// How long a weak-handle registry waits out a concurrent close before surfacing a LOCK error.
pub const OPEN_RETRIES: u32 = 100;
pub const OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(2);

/// True if `err` is a RocksDB LOCK-file collision — i.e. another opener still holds the
/// per-directory LOCK. The check is by error-message match rather than a distinct
/// `ErrorKind` because RocksDB surfaces this as a plain `IOError` across platforms
/// (Unix "While lock file: …/LOCK: Resource temporarily unavailable",
/// same-process "lock hold by current process, acquire time …: …/LOCK: No locks available",
/// Windows "Failed to create lock file: …\\LOCK: The process cannot access the file…").
pub fn is_lock_collision(err: &rocksdb::Error) -> bool {
    let msg = err.to_string();
    msg.contains("LOCK") || msg.contains("lock file")
}
