//! All Oxen LMDB envs are opened `WithoutTls` (heed's `MDB_NOTLS`), which ties each reader's
//! lock-table slot to the txn object rather than to the thread. We choose it for two reasons, and
//! `Send` is NOT one of them: (1) it lifts LMDB's "one read txn per thread" restriction, so a
//! store's read methods can nest/compose — e.g. a recursive tree read, or a read closure that
//! calls another read helper on the same env — without clobbering a per-thread slot; and (2) it is
//! the configuration LMDB recommends for thread-pool callers, which is exactly how we run: the
//! layer is sync and async callers wrap each operation (open txn → work → commit/abort) in one
//! `spawn_blocking`. The read txn itself never needs to be `Send` — it lives and dies inside the
//! closure, and only the owned data we copy out crosses back.
//!
//! TXN-LIFETIME RULE (applies to BOTH `RoTxn` and `RwTxn`): a transaction must never be held
//! across `.await`. An `RwTxn` is `!Send` so the compiler enforces this. An `RoTxn<WithoutTls>`
//! is `Send` (a side effect of `MDB_NOTLS`, not a goal), so the compiler does NOT — but the rule
//! still holds, and is MORE load-bearing under a fixed, non-resizable map: a long-lived reader
//! pins old B-tree pages and prevents the writer from reclaiming free pages, so a stale reader can
//! push an otherwise-healthy repo toward `MapFull` prematurely (there is no resize to escape the
//! free-page pressure). Copy what you need out into owned `Bytes` and drop the txn promptly. An
//! `RwTxn` lives entirely in one `spawn_blocking` closure.

use heed::{RoTxn, RwTxn, WithoutTls};

use super::lmdb_env::LmdbEnv;
use super::lmdb_error::LmdbLayerError;

// The raw txn primitives below are intentionally module-private: `with_read_txn` /
// `with_write_txn` are the only sanctioned read/write entry point. Routing every transaction
// through the brackets makes the txn-lifetime rule structural (a bare txn can never escape) and
// removes the forgotten-commit footgun (commit is folded into the write bracket).

/// Open a read-only transaction. Keep it short (see the txn-lifetime rule above).
fn read_txn(lmdb_env: &LmdbEnv) -> Result<RoTxn<'_, WithoutTls>, LmdbLayerError> {
    lmdb_env.read_txn().map_err(LmdbLayerError::Txn)
}

/// Open the env's single write transaction (LMDB allows one at a time). Commit with `commit` or
/// drop to abort.
fn write_txn(lmdb_env: &LmdbEnv) -> Result<RwTxn<'_>, LmdbLayerError> {
    lmdb_env.write_txn().map_err(LmdbLayerError::Txn)
}

/// Commit a write transaction, mapping the heed error into the leaf type.
fn commit(txn: RwTxn<'_>) -> Result<(), LmdbLayerError> {
    txn.commit().map_err(LmdbLayerError::Txn)
}

/// Run `f` inside a read transaction and return its value; the txn is dropped when `f` returns.
///
/// The closure cannot leak the txn (its lifetime is local to this call), so the only way to keep
/// a read result is to copy it out into owned data — which is exactly what `LmdbDb` reads do. This
/// structurally enforces the txn-lifetime rule: the txn cannot escape into a `.await`. `E` is
/// generic so the closure can return a domain error (e.g. `OxenError`) directly; the opening error
/// converts into it via `From<LmdbLayerError>`.
pub fn with_read_txn<R, E>(
    lmdb_env: &LmdbEnv,
    f: impl FnOnce(&RoTxn<'_, WithoutTls>) -> Result<R, E>,
) -> Result<R, E>
where
    E: From<LmdbLayerError>,
{
    let txn = read_txn(lmdb_env)?;
    f(&txn)
}

/// Run `f` inside a write transaction, then COMMIT iff `f` returns `Ok` — the durability boundary
/// for the whole batch. If `f` returns `Err` (or panics), the txn drops without committing, so
/// every write in the closure is rolled back atomically.
///
/// This is the canonical write shape: a logical unit of work (e.g. one commit's nodes) is one
/// closure, hence one txn, hence one atomic commit. Like `with_read_txn`, the txn cannot escape
/// the closure, and `E` is generic so the closure can return a domain error directly.
pub fn with_write_txn<R, E>(
    lmdb_env: &LmdbEnv,
    f: impl FnOnce(&mut RwTxn<'_>) -> Result<R, E>,
) -> Result<R, E>
where
    E: From<LmdbLayerError>,
{
    let mut txn = write_txn(lmdb_env)?;
    let value = f(&mut txn)?;
    commit(txn)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use super::*;
    use crate::lmdb::lmdb_db::LmdbDb;
    use crate::lmdb::lmdb_env::{LmdbEnvConfig, open_lmdb_env};

    /// A domain-style error to show `with_*_txn` works with a closure error type that is not
    /// `LmdbLayerError`, as long as it converts from it (mirrors a wrapper returning `OxenError`).
    #[derive(Debug)]
    struct Rollback;

    impl From<LmdbLayerError> for Rollback {
        fn from(_: LmdbLayerError) -> Self {
            Rollback
        }
    }

    fn test_lmdb_env() -> (tempfile::TempDir, LmdbEnv) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let lmdb_env =
            open_lmdb_env(dir.path(), &LmdbEnvConfig::new(1, ByteSize::mib(16))).expect("open env");
        (dir, lmdb_env)
    }

    #[test]
    fn with_write_txn_commits_on_ok() {
        let (_dir, lmdb_env) = test_lmdb_env();
        let db =
            with_write_txn(&lmdb_env, |txn| LmdbDb::open(&lmdb_env, txn, "kv")).expect("open db");

        with_write_txn(&lmdb_env, |txn| db.put(txn, b"k", b"v")).expect("put");

        let value = with_read_txn(&lmdb_env, |txn| db.get(txn, b"k")).expect("get");
        assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    }

    #[test]
    fn with_write_txn_aborts_on_err() {
        let (_dir, lmdb_env) = test_lmdb_env();
        let db =
            with_write_txn(&lmdb_env, |txn| LmdbDb::open(&lmdb_env, txn, "kv")).expect("open db");

        // The closure writes, then returns Err — the write must roll back when the txn aborts.
        let result: Result<(), Rollback> = with_write_txn(&lmdb_env, |txn| {
            db.put(txn, b"k", b"v")?;
            Err(Rollback)
        });
        assert!(result.is_err());

        let present = with_read_txn(&lmdb_env, |txn| db.contains(txn, b"k")).expect("contains");
        assert!(
            !present,
            "a closure that returned Err must leave nothing committed"
        );
    }
}
