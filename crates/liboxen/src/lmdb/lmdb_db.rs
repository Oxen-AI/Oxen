//! A wrapper over `heed::Database`: raw-bytes keys, opaque-bytes values. No per-record framing
//! lives here — stores built on this layer choose how to encode their keys into bytes and how to
//! decode value bytes themselves.
//!
//! KEY ORDERING: keys are ordered by LMDB's lexicographic byte comparison. That is correct for the
//! equality-only point lookups and for ordered scans over byte/string keys should one ever be
//! needed (UTF-8 byte order equals code-point order). A key that needs *numeric* range scans — a
//! multi-byte integer scanned in numeric order — would have to be encoded big-endian so byte order
//! matches numeric order, so this layer does not model it (and exposes no scan API).
//!
//! READ SAFETY: every read COPIES the value out of the read txn into owned `bytes::Bytes` before
//! returning. No method hands a caller a slice that borrows the mmap, so a returned value can
//! outlive the short `RoTxn` it was read under — callers never have to keep a txn open just to
//! hold onto a value.

use bytes::Bytes;
use heed::types::{Bytes as HeedBytes, DecodeIgnore};
use heed::{Database, RoIter, RoTxn, RwTxn, WithoutTls};

use super::lmdb_env::LmdbEnv;
use super::lmdb_error::LmdbLayerError;
use super::txn::with_write_txn;

/// Handle to one named sub-database: raw-bytes keys, opaque-bytes values copied out on read.
/// Cheap to clone.
#[derive(Clone)]
pub struct LmdbDb {
    db: Database<HeedBytes, HeedBytes>,
}

impl LmdbDb {
    /// Open or create the named sub-database within `lmdb_env` (inside a write txn, per heed).
    pub fn open(
        lmdb_env: &LmdbEnv,
        txn: &mut RwTxn<'_>,
        name: &str,
    ) -> Result<Self, LmdbLayerError> {
        let db = lmdb_env
            .create_database::<HeedBytes, HeedBytes>(txn, Some(name))
            .map_err(|source| LmdbLayerError::OpenDb {
                name: name.to_string(),
                source,
            })?;
        Ok(LmdbDb { db })
    }

    /// Fetch a value, COPIED OUT into owned `Bytes`. `None` if absent.
    pub fn get(
        &self,
        txn: &RoTxn<'_, WithoutTls>,
        key: &[u8],
    ) -> Result<Option<Bytes>, LmdbLayerError> {
        let value = self.db.get(txn, key).map_err(LmdbLayerError::Read)?;
        Ok(value.map(Bytes::copy_from_slice))
    }

    /// Existence check without decoding/copying the value.
    pub fn contains(
        &self,
        txn: &RoTxn<'_, WithoutTls>,
        key: &[u8],
    ) -> Result<bool, LmdbLayerError> {
        let found = self
            .db
            .remap_data_type::<DecodeIgnore>()
            .get(txn, key)
            .map_err(LmdbLayerError::Read)?;
        Ok(found.is_some())
    }

    /// Store opaque value bytes under `key`.
    pub fn put(&self, txn: &mut RwTxn<'_>, key: &[u8], value: &[u8]) -> Result<(), LmdbLayerError> {
        self.db.put(txn, key, value).map_err(LmdbLayerError::Write)
    }

    /// Delete a key; returns whether it existed.
    pub fn delete(&self, txn: &mut RwTxn<'_>, key: &[u8]) -> Result<bool, LmdbLayerError> {
        self.db.delete(txn, key).map_err(LmdbLayerError::Write)
    }

    /// Number of entries in the database.
    pub fn len(&self, txn: &RoTxn<'_, WithoutTls>) -> Result<u64, LmdbLayerError> {
        self.db.len(txn).map_err(LmdbLayerError::Read)
    }

    /// Delete every entry in the database.
    pub fn clear(&self, txn: &mut RwTxn<'_>) -> Result<(), LmdbLayerError> {
        self.db.clear(txn).map_err(LmdbLayerError::Write)
    }

    /// Iterate every entry, COPYING each key and value out into owned `Bytes`. LMDB walks keys in
    /// lexicographic byte order; that order carries no semantic meaning for content-address keys,
    /// so treat this as a full scan whose ordering callers should not rely on. The copied-out
    /// items can outlive the txn if collected.
    pub fn iter<'txn>(
        &self,
        txn: &'txn RoTxn<'_, WithoutTls>,
    ) -> Result<LmdbDbIter<'txn>, LmdbLayerError> {
        let inner = self.db.iter(txn).map_err(LmdbLayerError::Read)?;
        Ok(LmdbDbIter { inner })
    }
}

/// Open or create a single named sub-database in `lmdb_env`, running the heed-required write txn.
///
/// A convenience over [`LmdbDb::open`] for the common case of opening one database when a store is
/// constructed. To open several databases together, run them inside a single [`with_write_txn`]
/// closure with [`LmdbDb::open`] instead of paying one write txn per database.
pub fn open_db(lmdb_env: &LmdbEnv, name: &str) -> Result<LmdbDb, LmdbLayerError> {
    with_write_txn(lmdb_env, |txn| LmdbDb::open(lmdb_env, txn, name))
}

/// Full-scan iterator over a [`LmdbDb`], yielding owned `(key, value)` pairs (both copied out of
/// the txn).
pub struct LmdbDbIter<'txn> {
    inner: RoIter<'txn, HeedBytes, HeedBytes>,
}

impl Iterator for LmdbDbIter<'_> {
    type Item = Result<(Bytes, Bytes), LmdbLayerError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        Some(
            item.map(|(key, value)| (Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)))
                .map_err(LmdbLayerError::Read),
        )
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use tempfile::TempDir;

    use super::*;
    use crate::lmdb::lmdb_env::{LmdbEnvConfig, open_lmdb_env};
    use crate::lmdb::txn::{with_read_txn, with_write_txn};

    fn test_lmdb_env() -> (TempDir, LmdbEnv) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let lmdb_env =
            open_lmdb_env(dir.path(), &LmdbEnvConfig::new(1, ByteSize::mib(16))).expect("open env");
        (dir, lmdb_env)
    }

    #[test]
    fn put_get_contains_delete_round_trip() {
        let (_dir, lmdb_env) = test_lmdb_env();
        let db =
            with_write_txn(&lmdb_env, |txn| LmdbDb::open(&lmdb_env, txn, "kv")).expect("open db");
        with_write_txn(&lmdb_env, |txn| {
            db.put(txn, b"alpha", b"one")?;
            db.put(txn, b"beta", b"two")
        })
        .expect("put");

        // contains/len/get under one read snapshot; the value is copied out, so it is usable
        // after the txn is gone.
        let value = with_read_txn(&lmdb_env, |txn| {
            assert!(db.contains(txn, b"alpha")?);
            assert!(!db.contains(txn, b"gamma")?);
            assert_eq!(db.get(txn, b"gamma")?, None);
            assert_eq!(db.len(txn)?, 2);
            db.get(txn, b"alpha")
        })
        .expect("get")
        .expect("alpha present");
        assert_eq!(value.as_ref(), b"one");

        let deleted_again = with_write_txn(&lmdb_env, |txn| {
            assert!(db.delete(txn, b"alpha")?);
            db.delete(txn, b"alpha")
        })
        .expect("delete");
        assert!(!deleted_again);

        let remaining = with_read_txn(&lmdb_env, |txn| {
            assert_eq!(db.get(txn, b"alpha")?, None);
            db.len(txn)
        })
        .expect("len");
        assert_eq!(remaining, 1);
    }

    #[test]
    fn clear_empties_the_db() {
        let (_dir, lmdb_env) = test_lmdb_env();
        let db =
            with_write_txn(&lmdb_env, |txn| LmdbDb::open(&lmdb_env, txn, "kv")).expect("open db");
        with_write_txn(&lmdb_env, |txn| {
            db.put(txn, b"a", b"1")?;
            db.put(txn, b"b", b"2")?;
            db.clear(txn)
        })
        .expect("populate then clear");

        let len = with_read_txn(&lmdb_env, |txn| db.len(txn)).expect("len");
        assert_eq!(len, 0);
    }

    /// `iter` visits every entry, copies keys and values out as owned `Bytes`, and (as a property
    /// of LMDB's B-tree) yields them in lexicographic key byte order.
    #[test]
    fn iter_yields_every_entry() {
        let (_dir, lmdb_env) = test_lmdb_env();
        let db =
            with_write_txn(&lmdb_env, |txn| LmdbDb::open(&lmdb_env, txn, "kv")).expect("open db");
        // Inserted out of order to show iteration order is the database's, not insertion order.
        with_write_txn(&lmdb_env, |txn| {
            db.put(txn, b"c", b"3")?;
            db.put(txn, b"a", b"1")?;
            db.put(txn, b"b", b"2")
        })
        .expect("put");

        let entries: Vec<(Bytes, Bytes)> =
            with_read_txn(&lmdb_env, |txn| db.iter(txn)?.collect::<Result<_, _>>())
                .expect("iterate");
        // Owned `Bytes` outlive the txn, and LMDB yields keys in lexicographic byte order.
        assert_eq!(
            entries,
            vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
                (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
                (Bytes::from_static(b"c"), Bytes::from_static(b"3")),
            ]
        );
    }

    #[test]
    fn open_db_opens_a_usable_db() {
        let (_dir, lmdb_env) = test_lmdb_env();
        // open_db runs the write txn itself, unlike the txn-taking LmdbDb::open used above.
        let db = open_db(&lmdb_env, "kv").expect("open db");
        with_write_txn(&lmdb_env, |txn| db.put(txn, b"k", b"v")).expect("put");
        let value = with_read_txn(&lmdb_env, |txn| db.get(txn, b"k"))
            .expect("get")
            .expect("k present");
        assert_eq!(value.as_ref(), b"v");
    }
}
