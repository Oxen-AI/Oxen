//! The `LmdbStore` lifecycle trait: the shared env lifecycle every LMDB-backed store gets for
//! free. An implementor names its env's map size and its database, and holds an `LmdbSlot`
//! recording where the env lives. The trait opens the env on first use and default-implements
//! `read`/`write` (the transaction brackets pre-bound to the store's database) and `snapshot_to`
//! (env copy). The point is correctness via shared code — the lifecycle is written once here, not
//! re-derived per store where it could drift (e.g. forgetting `CompactionOption::Disabled` on a
//! snapshot). A store's own domain operations stay inherent methods written over
//! `self.read`/`self.write`.
//!
//! This is distinct from a future cross-engine store trait (e.g. File vs LMDB): that contract's
//! impls necessarily differ by engine and so can't be defaulted, whereas `LmdbStore` is
//! cross-LMDB-store and shares one impl.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use bytesize::ByteSize;
use heed::{RoTxn, RwTxn, WithoutTls};

use super::env_registry::open_shared_env;
use super::lmdb_db::{LmdbDb, open_db};
use super::lmdb_env::{LmdbEnv, copy_lmdb_env_to_dir};
use super::lmdb_error::LmdbLayerError;
use super::txn::{with_read_txn, with_write_txn};

/// Where a store's env lives, and its env and database once the first read, write, or snapshot has
/// opened them. Holding the opened handles keeps the env live.
pub(crate) struct LmdbSlot {
    env_dir: PathBuf,
    handles: OnceLock<LmdbHandles>,
}

struct LmdbHandles {
    env: Arc<LmdbEnv>,
    db: LmdbDb,
}

impl LmdbSlot {
    /// A slot for the env at `env_dir`. Nothing is opened or created on disk until first use.
    pub(crate) fn new(env_dir: PathBuf) -> Self {
        LmdbSlot {
            env_dir,
            handles: OnceLock::new(),
        }
    }
}

impl fmt::Debug for LmdbSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LmdbSlot")
            .field("env_dir", &self.env_dir)
            .finish_non_exhaustive()
    }
}

/// `store`'s env and database, opening the env (and creating it if absent) on the first call and
/// returning the cached handles after.
fn opened<S: LmdbStore + ?Sized>(store: &S) -> Result<&LmdbHandles, LmdbLayerError> {
    let slot = store.lmdb_slot();
    if let Some(handles) = slot.handles.get() {
        return Ok(handles);
    }
    let env = open_shared_env(&slot.env_dir, S::LMDB_MAP_SIZE)?;
    let db = open_db(&env, S::LMDB_DB_NAME)?;
    // A racing caller may have filled the slot first. `get_or_init` keeps whichever handles landed
    // and drops ours, which reference the same shared env.
    Ok(slot.handles.get_or_init(|| LmdbHandles { env, db }))
}

/// Shared lifecycle for an LMDB-backed store: one env opened on first use holding one database,
/// the transaction brackets, and env snapshotting, supplied as defaults so every store behaves
/// identically.
///
/// An implementor names its map size and database and hands over its slot, then calls
/// `self.read` / `self.write` from its own inherent domain methods. The env is not opened (or
/// created on disk) until the first `read`, `write`, or `snapshot_to`.
pub(crate) trait LmdbStore {
    /// The env's fixed map size (see `lmdb_env`).
    const LMDB_MAP_SIZE: ByteSize;

    /// The name of the env's one database.
    const LMDB_DB_NAME: &'static str;

    /// The slot recording where the store's env lives (one logical store per env).
    fn lmdb_slot(&self) -> &LmdbSlot;

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
        let handles = opened(self)?;
        with_read_txn(&handles.env, |txn| f(&handles.db, txn))
    }

    /// Run `f` inside a write transaction with the store's database pre-bound, committing iff `f`
    /// returns `Ok` (one logical unit of work = one closure = one atomic commit).
    fn write<R, E>(&self, f: impl FnOnce(&LmdbDb, &mut RwTxn<'_>) -> Result<R, E>) -> Result<R, E>
    where
        E: From<LmdbLayerError>,
    {
        let handles = opened(self)?;
        with_write_txn(&handles.env, |txn| f(&handles.db, txn))
    }

    /// Snapshot the store's env into `dst_dir` (point-in-time consistent), returning the copied
    /// data file's path. See `copy_lmdb_env_to_dir` for the snapshot/compaction semantics.
    fn snapshot_to(&self, dst_dir: &Path) -> Result<PathBuf, LmdbLayerError> {
        let handles = opened(self)?;
        copy_lmdb_env_to_dir(&handles.env, dst_dir)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::lmdb::lmdb_env::open_lmdb_env;

    const TEST_MAP_SIZE: ByteSize = ByteSize::mib(16);

    /// A minimal `LmdbStore` to exercise the default methods.
    struct TestStore {
        lmdb: LmdbSlot,
    }

    impl LmdbStore for TestStore {
        const LMDB_MAP_SIZE: ByteSize = TEST_MAP_SIZE;
        const LMDB_DB_NAME: &'static str = "data";

        fn lmdb_slot(&self) -> &LmdbSlot {
            &self.lmdb
        }
    }

    fn test_store() -> (TempDir, TestStore) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = TestStore {
            lmdb: LmdbSlot::new(dir.path().join("env")),
        };
        (dir, store)
    }

    /// `write` commits on `Ok` and `read` sees the committed value — both through the database
    /// the hook pre-binds, so the store never touches a raw txn. The first of them opens the env.
    #[test]
    fn read_and_write_go_through_the_brackets() {
        let (_dir, store) = test_store();
        assert!(
            !store.lmdb.env_dir.exists(),
            "a store that has not been read or written opens no env"
        );
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

        let copied = open_lmdb_env(dst.path(), TEST_MAP_SIZE).expect("open snapshot");
        let copied_db =
            with_write_txn(&copied, |txn| LmdbDb::open(&copied, txn, "data")).expect("open db");
        let value = with_read_txn(&copied, |txn| copied_db.get(txn, b"key"))
            .expect("read")
            .expect("value present");
        assert_eq!(value.as_ref(), b"value");
    }
}
