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
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use bytesize::ByteSize;
use heed::{RoTxn, RwTxn, WithoutTls};

use crate::constants;
use crate::error::OxenError;
use crate::lmdb::lmdb_env::copy_lmdb_env_to_dir;
use crate::lmdb::{
    LmdbDb, LmdbEnv, LmdbEnvConfig, open_db, open_shared_env, with_read_txn, with_write_txn,
};
use crate::model::MerkleHash;

use super::merkle_node_db::MerkleDbError;
use super::merkle_node_store::MerkleNodeStore;

/// One database in the env holds every node's blobs.
const NODES_DB_NAME: &str = "nodes";
/// Single database, so a `max_dbs` of 1 is sufficient.
const MAX_DBS: u32 = 1;
/// Sparse upper bound on the env's mapped size — LMDB reserves this much address space but only
/// occupies what is written, so it is sized generously to avoid `MDB_MAP_FULL` on large repos.
const MERKLE_NODE_MAP_SIZE: ByteSize = ByteSize::mib(512);
/// LMDB's data file name within the env directory (a stable LMDB convention); used only to detect
/// an existing LMDB store without opening (and thereby creating) the env.
const LMDB_DATA_FILE: &str = "data.mdb";

/// Tag byte selecting a node's `node` blob within its composite key.
const NODE_TAG: u8 = 0;
/// Tag byte selecting a node's `children` blob within its composite key.
const CHILDREN_TAG: u8 = 1;
/// 16-byte little-endian hash followed by the 1-byte tag.
const KEY_LEN: usize = 17;

/// The opened env and its primary database, created together on first access.
struct Handles {
    env: Arc<LmdbEnv>,
    db: LmdbDb,
}

/// Stores each node's two blobs as two tagged keys in a single LMDB env. The env is opened
/// (creating it if absent) on the first read or write, so a store that is never touched opens no
/// env — as every workspace's does, since a workspace never reads or writes its own merkle nodes.
pub(crate) struct LmdbMerkleNodeStore {
    env_dir: PathBuf,
    handles: OnceLock<Handles>,
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
    /// Prepare the LMDB merkle node store for the repo rooted at `repo_path`. The env is opened
    /// (creating it if absent) on the first read or write, not here.
    pub(crate) fn new(repo_path: &Path) -> Result<Self, OxenError> {
        Self::new_at(&Self::env_dir(repo_path))
    }

    /// Prepare an LMDB merkle node store at an explicit env directory. Used by the FS→LMDB
    /// migration to build the env in a temp dir before atomically publishing it.
    pub(crate) fn new_at(env_dir: &Path) -> Result<Self, OxenError> {
        Ok(Self {
            env_dir: env_dir.to_path_buf(),
            handles: OnceLock::new(),
        })
    }

    /// The env and database, opened (creating the env if absent) on first call and cached after.
    fn handles(&self) -> Result<&Handles, MerkleDbError> {
        if let Some(handles) = self.handles.get() {
            return Ok(handles);
        }
        let config = LmdbEnvConfig::new(MAX_DBS, MERKLE_NODE_MAP_SIZE);
        let env = open_shared_env(&self.env_dir, &config)?;
        let db = open_db(&env, NODES_DB_NAME)?;
        // A racing caller may have initialized first; `get_or_init` keeps whichever handles are
        // stored and drops ours (both reference the same shared env).
        Ok(self.handles.get_or_init(|| Handles { env, db }))
    }

    /// Run `f` in a read transaction against the (lazily opened) env with the database bound.
    fn read<R>(
        &self,
        f: impl FnOnce(&LmdbDb, &RoTxn<'_, WithoutTls>) -> Result<R, MerkleDbError>,
    ) -> Result<R, MerkleDbError> {
        let handles = self.handles()?;
        with_read_txn(&handles.env, |txn| f(&handles.db, txn))
    }

    /// Run `f` in a write transaction against the (lazily opened) env with the database bound,
    /// committing iff `f` returns `Ok`.
    fn write<R>(
        &self,
        f: impl FnOnce(&LmdbDb, &mut RwTxn<'_>) -> Result<R, MerkleDbError>,
    ) -> Result<R, MerkleDbError> {
        let handles = self.handles()?;
        with_write_txn(&handles.env, |txn| f(&handles.db, txn))
    }

    /// Snapshot the (lazily opened) env into `dst_dir`, returning the copied data file's path.
    fn snapshot_to(&self, dst_dir: &Path) -> Result<PathBuf, MerkleDbError> {
        let handles = self.handles()?;
        Ok(copy_lmdb_env_to_dir(&handles.env, dst_dir)?)
    }

    /// Whether an LMDB merkle node env already exists on disk for `repo_path`. Checks the data file
    /// directly so the caller can pick a backend without opening (and creating) an env.
    pub(crate) fn exists_on_disk(repo_path: &Path) -> bool {
        Self::env_dir(repo_path).join(LMDB_DATA_FILE).exists()
    }

    /// The env directory for the repo rooted at `repo_path` (`.oxen/tree/nodes_lmdb`).
    pub(crate) fn env_dir(repo_path: &Path) -> PathBuf {
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
            // `write_node` always writes both keys (a childless node still gets an empty children
            // blob), so a missing children key is an incomplete record, not a childless node;
            // report it as missing rather than as a valid zero-length blob.
            let children = db
                .get(txn, &Self::key(hash, CHILDREN_TAG))?
                .ok_or(MerkleDbError::MissingNodeDir(*hash))?;
            Ok((node.len() as u64, children.len() as u64))
        })
    }

    fn list_hashes(&self) -> Result<Vec<MerkleHash>, MerkleDbError> {
        self.read(|db, txn| {
            let mut hashes = Vec::new();
            // Each complete node contributes two keys; key off the node-tagged one so each hash
            // appears once, and require the children key to also be present so an incomplete
            // record (node key without its children key) is not reported as a valid node.
            for item in db.iter(txn)? {
                let (key, _value) = item?;
                if key.len() == KEY_LEN && key[16] == NODE_TAG {
                    let mut hash_le = [0u8; 16];
                    hash_le.copy_from_slice(&key[..16]);
                    let hash = MerkleHash::new(u128::from_le_bytes(hash_le));
                    if db.contains(txn, &Self::key(&hash, CHILDREN_TAG))? {
                        hashes.push(hash);
                    }
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

    fn write_nodes(
        &self,
        nodes: Vec<(MerkleHash, Bytes, Bytes)>,
        overwrite_existing: bool,
    ) -> Result<Vec<MerkleHash>, MerkleDbError> {
        // The whole batch commits in one transaction, so LMDB fsyncs once for it. That single
        // fsync keeps a large unpack fast, even at tens of thousands of nodes. A node counts as
        // present only when both of its keys are there, matching `exists`, so if a node key ever
        // lost its children key, this writes it again.
        self.write(|db, txn| {
            let mut written = Vec::new();
            for (hash, node, children) in &nodes {
                let present = db.contains(txn, &Self::key(hash, NODE_TAG))?
                    && db.contains(txn, &Self::key(hash, CHILDREN_TAG))?;
                if !overwrite_existing && present {
                    continue;
                }
                db.put(txn, &Self::key(hash, NODE_TAG), node.as_ref())?;
                db.put(txn, &Self::key(hash, CHILDREN_TAG), children.as_ref())?;
                written.push(*hash);
            }
            Ok(written)
        })
    }

    fn delete(&self, hash: &MerkleHash) -> Result<(), MerkleDbError> {
        self.write(|db, txn| {
            db.delete(txn, &Self::key(hash, NODE_TAG))?;
            db.delete(txn, &Self::key(hash, CHILDREN_TAG))?;
            Ok(())
        })
    }

    fn snapshot_for_archive(&self, dst_dir: &Path) -> Result<Option<PathBuf>, MerkleDbError> {
        // `mdb_env_copy` writes a single, point-in-time-consistent `data.mdb` and no `lock.mdb`,
        // so the archive captures durable state without the live env's runtime lock file.
        let data_file = self.snapshot_to(dst_dir)?;
        Ok(Some(data_file))
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

    /// The env is opened only on first access: constructing a store that is never read or written
    /// creates no env on disk. This is what keeps an untouched workspace store — which never reads
    /// or writes its own merkle nodes — from opening an LMDB env at all.
    #[test]
    fn env_opens_lazily_on_first_access() -> Result<(), OxenError> {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = LmdbMerkleNodeStore::new(dir.path())?;

        assert!(
            !LmdbMerkleNodeStore::exists_on_disk(dir.path()),
            "constructing a store must not open (create) an env"
        );

        // Any access opens (creates) the env; a read is enough.
        store.list_hashes()?;
        assert!(
            LmdbMerkleNodeStore::exists_on_disk(dir.path()),
            "the first access must open the env"
        );
        Ok(())
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

    /// `write_nodes` commits the whole batch in one transaction, returns exactly the hashes it newly
    /// wrote, and skips nodes already present unless overwriting. This is the same batch contract the
    /// filesystem backend satisfies, checked here over LMDB's single transaction path.
    #[test]
    fn lmdb_write_nodes_batches_and_respects_existing() -> Result<(), OxenError> {
        use std::collections::HashSet;
        let (_dir, store) = test_store();

        let a = MerkleHash::new(0xa);
        let b = MerkleHash::new(0xb);
        let batch = vec![
            (
                a,
                Bytes::from_static(b"node-a"),
                Bytes::from_static(b"kids-a"),
            ),
            (b, Bytes::from_static(b"node-b"), Bytes::new()),
        ];

        // A fresh batch writes every node and reports both hashes.
        let written: HashSet<_> = store
            .write_nodes(batch.clone(), false)?
            .into_iter()
            .collect();
        assert_eq!(written, HashSet::from([a, b]));
        assert_eq!(store.read_node(&a)?, Bytes::from_static(b"node-a"));
        assert!(store.read_children(&b)?.is_empty());

        // Re-running without overwrite writes nothing.
        assert!(store.write_nodes(batch.clone(), false)?.is_empty());

        // Overwriting must change the stored blobs, so read all four back and confirm the new
        // bytes. b's children started empty.
        let replacement = vec![
            (
                a,
                Bytes::from_static(b"node-a2"),
                Bytes::from_static(b"kids-a2"),
            ),
            (
                b,
                Bytes::from_static(b"node-b2"),
                Bytes::from_static(b"kids-b2"),
            ),
        ];
        let rewritten: HashSet<_> = store.write_nodes(replacement, true)?.into_iter().collect();
        assert_eq!(rewritten, HashSet::from([a, b]));
        assert_eq!(store.read_node(&a)?, Bytes::from_static(b"node-a2"));
        assert_eq!(store.read_children(&a)?, Bytes::from_static(b"kids-a2"));
        assert_eq!(store.read_node(&b)?, Bytes::from_static(b"node-b2"));
        assert_eq!(store.read_children(&b)?, Bytes::from_static(b"kids-b2"));
        Ok(())
    }

    /// A record with its node key but no children key counts as absent, so `write_nodes` writes it
    /// even when `overwrite_existing` is false. Only corruption produces that state, since
    /// `write_node` writes both keys in one transaction. This test guards the presence check that
    /// requires both keys.
    #[test]
    fn lmdb_write_nodes_rewrites_a_half_written_node() -> Result<(), OxenError> {
        let (_dir, store) = test_store();
        let hash = MerkleHash::new(0xc);

        // Plant a broken record with the node key present and no children key.
        store.write(|db, txn| -> Result<(), MerkleDbError> {
            db.put(txn, &LmdbMerkleNodeStore::key(&hash, NODE_TAG), b"stale")?;
            Ok(())
        })?;
        assert!(
            !store.exists(&hash)?,
            "a node missing its children key is not present"
        );

        // With overwrite off, `write_nodes` does not skip the broken record. It writes both blobs.
        let batch = vec![(
            hash,
            Bytes::from_static(b"node-c"),
            Bytes::from_static(b"kids-c"),
        )];
        assert_eq!(store.write_nodes(batch, false)?, vec![hash]);
        assert_eq!(store.read_node(&hash)?, Bytes::from_static(b"node-c"));
        assert_eq!(store.read_children(&hash)?, Bytes::from_static(b"kids-c"));
        Ok(())
    }
}
