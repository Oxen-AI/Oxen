//! The `LmdbStore` lifecycle trait: the shared env lifecycle every LMDB-backed store gets for
//! free. An implementor supplies only the env + primary-database hook; the trait default-implements
//! `read`/`write` (the transaction brackets pre-bound to the store's database) and `snapshot_to`
//! (env copy). The point is correctness via shared code — the lifecycle is written once here, not
//! re-derived per store where it could drift (e.g. forgetting `CompactionOption::Disabled` on a
//! snapshot). A store's own domain operations stay inherent methods written over
//! `self.read`/`self.write`.
//!
//! This is distinct from a future cross-engine store trait (e.g. File vs LMDB): that contract's
//! impls necessarily differ by engine and so can't be defaulted, whereas `LmdbStore` is
//! cross-LMDB-store and shares one impl.

use std::path::{Path, PathBuf};

use heed::{RoTxn, RwTxn, WithoutTls};

use super::lmdb_db::LmdbDb;
use super::lmdb_env::{LmdbEnv, copy_lmdb_env_to_dir};
use super::lmdb_error::LmdbLayerError;
use super::txn::{with_read_txn, with_write_txn};

/// Shared lifecycle for an LMDB-backed store: one env, one primary database, the transaction
/// brackets, and env snapshotting — supplied as defaults so every store behaves identically.
///
/// An implementor provides only [`LmdbStore::lmdb_env`] and [`LmdbStore::lmdb_db`]; it then calls
/// `self.read` / `self.write` from its own inherent domain methods.
// `pub(crate)` + `#[allow(dead_code)]`: consumer-less until the first real implementor lands — a
// test impl exercises the defaults meanwhile. Drop the `allow` (and the ones on
// `copy_lmdb_env_to_dir` / `LMDB_DATA_FILE`) once an implementor exists.
#[allow(dead_code)]
pub(crate) trait LmdbStore {
    /// The store's environment (one logical store per env).
    fn lmdb_env(&self) -> &LmdbEnv;

    /// The store's primary database.
    fn lmdb_db(&self) -> &LmdbDb;

    /// Run `f` inside a read transaction with the store's database pre-bound. The txn cannot escape
    /// the closure (see the txn-lifetime rule in `txn`); copy out what you need. `E` is generic so
    /// the closure can return a domain error (e.g. `OxenError`) directly.
    fn read<R, E>(
        &self,
        f: impl FnOnce(&LmdbDb, &RoTxn<'_, WithoutTls>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<LmdbLayerError>,
    {
        with_read_txn(self.lmdb_env(), |txn| f(self.lmdb_db(), txn))
    }

    /// Run `f` inside a write transaction with the store's database pre-bound, committing iff `f`
    /// returns `Ok` (one logical unit of work = one closure = one atomic commit).
    fn write<R, E>(&self, f: impl FnOnce(&LmdbDb, &mut RwTxn<'_>) -> Result<R, E>) -> Result<R, E>
    where
        E: From<LmdbLayerError>,
    {
        with_write_txn(self.lmdb_env(), |txn| f(self.lmdb_db(), txn))
    }

    /// Snapshot the store's env into `dst_dir` (point-in-time consistent), returning the copied
    /// data file's path. See `copy_lmdb_env_to_dir` for the snapshot/compaction semantics.
    fn snapshot_to(&self, dst_dir: &Path) -> Result<PathBuf, LmdbLayerError> {
        copy_lmdb_env_to_dir(self.lmdb_env(), dst_dir)
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use tempfile::TempDir;

    use super::*;
    use crate::lmdb::lmdb_env::{LmdbEnvConfig, open_lmdb_env};

    /// A minimal `LmdbStore` to exercise the default methods until a real store implements it.
    struct TestStore {
        lmdb_env: LmdbEnv,
        db: LmdbDb,
    }

    impl LmdbStore for TestStore {
        fn lmdb_env(&self) -> &LmdbEnv {
            &self.lmdb_env
        }

        fn lmdb_db(&self) -> &LmdbDb {
            &self.db
        }
    }

    fn test_store() -> (TempDir, TestStore) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let lmdb_env =
            open_lmdb_env(dir.path(), &LmdbEnvConfig::new(1, ByteSize::mib(16))).expect("open env");
        let db =
            with_write_txn(&lmdb_env, |txn| LmdbDb::open(&lmdb_env, txn, "data")).expect("open db");
        (dir, TestStore { lmdb_env, db })
    }

    /// `write` commits on `Ok` and `read` sees the committed value — both through the database the
    /// hook pre-binds, so the store never touches a raw txn.
    #[test]
    fn read_and_write_go_through_the_brackets() {
        let (_dir, store) = test_store();
        store
            .write(|db, txn| db.put(txn, b"key", b"value"))
            .expect("write");
        let value = store
            .read(|db, txn| db.get(txn, b"key"))
            .expect("read")
            .expect("value present");
        assert_eq!(value.as_ref(), b"value");
    }

    /// `snapshot_to` copies committed state; reopening the snapshot yields the same data.
    #[test]
    fn snapshot_to_copies_committed_state() {
        let (_dir, store) = test_store();
        store
            .write(|db, txn| db.put(txn, b"key", b"value"))
            .expect("write");

        let dst = tempfile::tempdir().expect("create temp dir");
        let data_file = store.snapshot_to(dst.path()).expect("snapshot");
        assert!(data_file.exists());

        let copied = open_lmdb_env(dst.path(), &LmdbEnvConfig::new(1, ByteSize::mib(16)))
            .expect("open snapshot");
        let copied_db =
            with_write_txn(&copied, |txn| LmdbDb::open(&copied, txn, "data")).expect("open db");
        let value = with_read_txn(&copied, |txn| copied_db.get(txn, b"key"))
            .expect("read")
            .expect("value present");
        assert_eq!(value.as_ref(), b"value");
    }
}
