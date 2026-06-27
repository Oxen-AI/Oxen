//! LMDB-backed [`MerkleNodeStore`]: both of a node's blobs live as two keys in one LMDB env at
//! `.oxen/tree/nodes_lmdb`, instead of two files per node on disk. The msgpack + lookup-table
//! framing is unchanged — only *where the two blobs live* differs from
//! [`FsMerkleNodeStore`](super::fs_merkle_node_store::FsMerkleNodeStore).
//!
//! Key layout: a node's two blobs share its [`MerkleHash`] and are distinguished by a one-byte
//! tag suffix — `hash_le(16) ‖ 0` holds the `node` blob, `hash_le(16) ‖ 1` holds `children`.
//! `write_node` puts both under one write transaction, so a node is never observable with only one
//! blob (the same atomicity the FS backend gets from writing both files before anything reads).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use bytesize::ByteSize;

use crate::constants;
use crate::error::OxenError;
use crate::lmdb::store::LmdbStore;
use crate::lmdb::{LmdbDb, LmdbEnv, LmdbEnvConfig, open_db, open_shared_env};
use crate::model::MerkleHash;

use super::merkle_node_db::MerkleDbError;
use super::merkle_node_store::MerkleNodeStore;

/// One database in the env holds every node's blobs.
const NODES_DB_NAME: &str = "nodes";
/// Single database, so a `max_dbs` of 1 is sufficient.
const MAX_DBS: u32 = 1;
/// Sparse upper bound on the env's mapped size — LMDB reserves this much address space but only
/// occupies what is written, so it is sized generously to avoid `MDB_MAP_FULL` on large repos.
const MERKLE_NODE_MAP_SIZE: ByteSize = ByteSize::gib(256);

/// Tag byte selecting a node's `node` blob within its composite key.
const NODE_TAG: u8 = 0;
/// Tag byte selecting a node's `children` blob within its composite key.
const CHILDREN_TAG: u8 = 1;
/// 16-byte little-endian hash followed by the 1-byte tag.
const KEY_LEN: usize = 17;

/// Stores each node's two blobs as two tagged keys in a single LMDB env.
pub(crate) struct LmdbMerkleNodeStore {
    env: Arc<LmdbEnv>,
    db: LmdbDb,
}

// Manual `Debug` (the `MerkleNodeStore` trait requires it) — the LMDB env/db handles aren't
// usefully printable, so emit just the type name.
impl std::fmt::Debug for LmdbMerkleNodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbMerkleNodeStore")
            .finish_non_exhaustive()
    }
}

impl LmdbMerkleNodeStore {
    /// Open (creating if absent) the LMDB merkle node env for the repo rooted at `repo_path`.
    pub(crate) fn new(repo_path: &Path) -> Result<Self, OxenError> {
        let dir = Self::env_dir(repo_path);
        let config = LmdbEnvConfig::new(MAX_DBS, MERKLE_NODE_MAP_SIZE);
        let env = open_shared_env(&dir, &config)?;
        let db = open_db(&env, NODES_DB_NAME)?;
        Ok(Self { env, db })
    }

    fn env_dir(repo_path: &Path) -> PathBuf {
        repo_path
            .join(constants::OXEN_HIDDEN_DIR)
            .join(constants::TREE_DIR)
            .join(constants::NODES_LMDB_DIR)
    }

    /// `hash_le(16) ‖ tag(1)` — the composite key for one of a node's two blobs.
    fn key(hash: &MerkleHash, tag: u8) -> [u8; KEY_LEN] {
        let mut key = [0u8; KEY_LEN];
        key[..16].copy_from_slice(&hash.to_le_bytes());
        key[16] = tag;
        key
    }
}

impl LmdbStore for LmdbMerkleNodeStore {
    fn lmdb_env(&self) -> &LmdbEnv {
        &self.env
    }

    fn lmdb_db(&self) -> &LmdbDb {
        &self.db
    }
}

impl MerkleNodeStore for LmdbMerkleNodeStore {
    fn exists(&self, hash: &MerkleHash) -> Result<bool, MerkleDbError> {
        self.read(|db, txn| {
            Ok(db.contains(txn, &Self::key(hash, NODE_TAG))?
                && db.contains(txn, &Self::key(hash, CHILDREN_TAG))?)
        })
    }

    fn read_node(&self, hash: &MerkleHash) -> Result<Bytes, MerkleDbError> {
        self.read(|db, txn| {
            db.get(txn, &Self::key(hash, NODE_TAG))?
                .ok_or(MerkleDbError::MissingNodeDir(*hash))
        })
    }

    fn read_children(&self, hash: &MerkleHash) -> Result<Bytes, MerkleDbError> {
        self.read(|db, txn| {
            db.get(txn, &Self::key(hash, CHILDREN_TAG))?
                .ok_or(MerkleDbError::MissingNodeDir(*hash))
        })
    }

    fn node_byte_sizes(&self, hash: &MerkleHash) -> Result<(u64, u64), MerkleDbError> {
        self.read(|db, txn| {
            let node = db
                .get(txn, &Self::key(hash, NODE_TAG))?
                .ok_or(MerkleDbError::MissingNodeDir(*hash))?;
            // `write_node` always writes both keys, so a missing children key means a childless
            // node whose blob is empty; treat it as zero-length rather than an error.
            let children_len = db
                .get(txn, &Self::key(hash, CHILDREN_TAG))?
                .map_or(0, |blob| blob.len() as u64);
            Ok((node.len() as u64, children_len))
        })
    }

    fn list_hashes(&self) -> Result<Vec<MerkleHash>, MerkleDbError> {
        self.read(|db, txn| {
            let mut hashes = Vec::new();
            // Each node contributes two keys; count the node-tagged one so each hash appears once.
            for item in db.iter(txn)? {
                let (key, _value) = item?;
                if key.len() == KEY_LEN && key[16] == NODE_TAG {
                    let mut hash_le = [0u8; 16];
                    hash_le.copy_from_slice(&key[..16]);
                    hashes.push(MerkleHash::new(u128::from_le_bytes(hash_le)));
                }
            }
            Ok(hashes)
        })
    }

    fn write_node(
        &self,
        hash: &MerkleHash,
        node: Bytes,
        children: Bytes,
    ) -> Result<(), MerkleDbError> {
        // Both blobs in one write txn: the commit is atomic, so a node is never half-present.
        self.write(|db, txn| {
            db.put(txn, &Self::key(hash, NODE_TAG), node.as_ref())?;
            db.put(txn, &Self::key(hash, CHILDREN_TAG), children.as_ref())?;
            Ok(())
        })
    }

    fn delete(&self, hash: &MerkleHash) -> Result<(), MerkleDbError> {
        self.write(|db, txn| {
            db.delete(txn, &Self::key(hash, NODE_TAG))?;
            db.delete(txn, &Self::key(hash, CHILDREN_TAG))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::error::OxenError;

    use super::*;

    fn test_store() -> (tempfile::TempDir, LmdbMerkleNodeStore) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LmdbMerkleNodeStore::new(dir.path()).expect("open lmdb merkle node store");
        (dir, store)
    }

    /// The LMDB backend must satisfy the same contract as the file backend: report absence,
    /// persist both blobs atomically, read them back unchanged, handle a childless node, report a
    /// missing node as `MissingNodeDir`, enumerate, and delete idempotently.
    #[test]
    fn lmdb_store_satisfies_the_contract() -> Result<(), OxenError> {
        let (_dir, store) = test_store();

        let hash = MerkleHash::new(0x1234_5678_9abc_def0);
        let node = Bytes::from_static(b"node blob: header + lookup table");
        let children = Bytes::from_static(b"children blob: concatenated child nodes");

        assert!(
            !store.exists(&hash)?,
            "node should not exist before writing"
        );

        store.write_node(&hash, node.clone(), children.clone())?;

        assert!(store.exists(&hash)?, "node should exist after writing");
        assert_eq!(store.read_node(&hash)?, node);
        assert_eq!(store.read_children(&hash)?, children);
        assert_eq!(
            store.node_byte_sizes(&hash)?,
            (node.len() as u64, children.len() as u64)
        );

        // A childless node (empty children blob) round-trips.
        let leaf = MerkleHash::new(0x42);
        store.write_node(&leaf, Bytes::from_static(b"leaf"), Bytes::new())?;
        assert!(store.read_children(&leaf)?.is_empty());
        assert_eq!(store.node_byte_sizes(&leaf)?, (4, 0));

        // Enumeration returns exactly the written hashes.
        let listed: HashSet<MerkleHash> = store.list_hashes()?.into_iter().collect();
        assert_eq!(listed, HashSet::from([hash, leaf]));

        // A never-written node is absent and reads as MissingNodeDir.
        let missing = MerkleHash::new(0xdead_beef);
        assert!(!store.exists(&missing)?);
        assert!(matches!(
            store.read_node(&missing),
            Err(MerkleDbError::MissingNodeDir(_))
        ));
        assert!(matches!(
            store.node_byte_sizes(&missing),
            Err(MerkleDbError::MissingNodeDir(_))
        ));

        // Delete removes a node; deleting an absent node is idempotent.
        store.delete(&hash)?;
        assert!(!store.exists(&hash)?);
        store.delete(&missing)?;
        assert_eq!(store.list_hashes()?, vec![leaf]);

        Ok(())
    }

    /// The two backends are interchangeable: the same node written to each reads back identically,
    /// so the engine choice never changes observable behavior.
    #[test]
    fn lmdb_and_fs_backends_round_trip_identically() -> Result<(), OxenError> {
        use super::super::fs_merkle_node_store::FsMerkleNodeStore;

        let fs_dir = tempfile::tempdir().expect("create temp dir");
        let lmdb_dir = tempfile::tempdir().expect("create temp dir");
        let fs = FsMerkleNodeStore::new(fs_dir.path());
        let lmdb = LmdbMerkleNodeStore::new(lmdb_dir.path())?;

        let hash = MerkleHash::new(0x0bad_c0de_dead_beef);
        let node = Bytes::from_static(b"node blob bytes");
        let children = Bytes::from_static(b"children blob bytes");

        fs.write_node(&hash, node.clone(), children.clone())?;
        lmdb.write_node(&hash, node.clone(), children.clone())?;

        assert_eq!(fs.exists(&hash)?, lmdb.exists(&hash)?);
        assert_eq!(fs.read_node(&hash)?, lmdb.read_node(&hash)?);
        assert_eq!(fs.read_children(&hash)?, lmdb.read_children(&hash)?);
        assert_eq!(fs.node_byte_sizes(&hash)?, lmdb.node_byte_sizes(&hash)?);
        assert_eq!(fs.list_hashes()?, lmdb.list_hashes()?);
        Ok(())
    }
}
