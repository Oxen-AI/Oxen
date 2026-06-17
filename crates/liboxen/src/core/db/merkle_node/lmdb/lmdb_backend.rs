use std::path::{Path, PathBuf};

use heed::byteorder::LE;
use heed::types::{Bytes, DecodeIgnore, U128};
use heed::{AnyTls, Database, Env, EnvOpenOptions, WithTls};

use crate::constants::OXEN_HIDDEN_DIR;
use crate::core::db::merkle_node::lmdb::LmdbError;
use crate::core::db::merkle_node::lmdb::value_structs::{LmdbLink, LmdbNode};
use crate::error::OxenError;
use crate::model::MerkleHash;

/// Keys are merkle hashes, which are `u128` values.
/// MUST USE LITTLE ENDIAN (LE) so the byte layout matches
/// `ContentHash::as_bytes` / `LocationHash::as_bytes`.
pub(super) type KeyLmdb = U128<LE>;

/// Values are serialized as bytes. Access requires deserialization into structs.
pub(super) type ValueLmdb = Bytes;

/// LMDB table name: storing merkle tree node byte
const DB_NODES: &str = "merkle_tree_nodes";
/// LMDB table name: storing parent & child relationships per-node
const DB_LINKS: &str = "merkle_links";

/// Merkle tree node storage that is backed by LMDB.
///
/// NOTE: DO NOT USE ON A VIRTUAL FILE SYSTEM !!
pub struct LmdbBackend {
    /// The filesystem location of the local repository.
    pub(super) repo_root: PathBuf,
    /// The LMDB environment that contains the [`Database`] fields.
    pub(super) lmdb_env: Env<WithTls>, // note: WithTls makes this !Send. Use AnyTls if need to send between threads.

    /// Stores every kind of merkle tree node: any concrete [`MerkleTreeNode`].
    /// This includes files nodes! Note that there is no other data but the Merkle
    /// tree node type (u8) and the msgpack-serialized bytes of the actual node.
    ///
    /// These values deserialize into a [`LmdbNode`]. The  zerocopy view on these
    /// values is the [`LmdbNodeRef`].
    ///
    /// In the LMDB environment, this has name of the [DB_NODES] constant.
    pub(super) merkle_tree_nodes: Database<KeyLmdb, ValueLmdb>,

    /// Stores the parent and children connections for each Merkle tree node.
    ///
    /// These values deserialize into a [`LmdbLink`]. The zerocopy view on these
    /// values is the [`LmdbLinkRef`].
    ///
    /// In the LMDB environment, this has name of the [DB_LINKS] constant.
    pub(super) merkle_links: Database<KeyLmdb, ValueLmdb>,
}

impl LmdbBackend {
    pub fn new(repo_root: PathBuf, options: EnvOpenOptions) -> Result<Self, OxenError> {
        let options = {
            let mut options = options;
            options.max_dbs(2);
            log::debug!("Config for LMDB backend: {options:?}");
            options
        };

        let lmdb_env = {
            let db_location = lmdb_dir_location(&repo_root);
            log::debug!("Opening LMDB backend at: {}", db_location.display());
            // SAFETY: LMDB uses a memory mapped file. If this file is modified in or out of process,
            //         it can cause undefined behavior. There is nothing else in the Oxen codebase that
            //         modifies this mmap'd file. Nothing else access `db_location` except for this code.
            //         This uses LMDB's own opening code, which does lock the file to check for and
            //         prevent concurrent access. The fact that it is a "hidden" file helps somewhat: it will
            //         be unlikely for someone or something to accidentily stumble upon it and mess things up.
            unsafe { options.open(db_location).map_err(LmdbError::Access)? }
        };

        // create these two key-value tables
        let mut wtxn = lmdb_env.write_txn().map_err(LmdbError::Access)?;
        let merkle_tree_nodes = lmdb_env
            .create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_NODES))
            .map_err(LmdbError::Write)?;
        let merkle_links = lmdb_env
            .create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_LINKS))
            .map_err(LmdbError::Write)?;
        wtxn.commit().map_err(LmdbError::Write)?;
        log::debug!(
            "Ensure tables '{DB_NODES}' (Merkle nodes) and '{DB_LINKS}' (tree connections) exist"
        );

        Ok(Self {
            repo_root,
            lmdb_env,
            merkle_tree_nodes,
            merkle_links,
        })
    }

    /// Private helper for creating a new LMDB write transaction.
    /// Returns [`LmdbError::Access`] on error.
    pub(in crate::core::db::merkle_node::lmdb) fn write_txn<'a>(
        lmdb_env: &'a heed::Env<heed::WithTls>,
    ) -> Result<heed::RwTxn<'a>, LmdbError> {
        lmdb_env.write_txn().map_err(LmdbError::Access)
    }

    /// Private helper for creating a new LMDB read transaction.
    /// Returns [`LmdbError::Access`] on error.
    pub(in crate::core::db::merkle_node::lmdb) fn read_txn<'a>(
        &'a self,
    ) -> Result<heed::RoTxn<'a, heed::WithTls>, LmdbError> {
        self.lmdb_env.read_txn().map_err(LmdbError::Access)
    }

    /// Retrieve the raw stored value bytes, or `None` if no entry exists for the key.
    ///
    /// The returned slice borrows directly into the LMDB read transaction's mmap
    /// region — no copy, no parse. Callers wrap it in [`super::value_structs::LmdbNodeRef`]
    /// or [`super::value_structs::LmdbLinkRef`] to get a typed zero-copy view.
    ///
    /// Generic over the `TLS` marker so the helper works with whatever thread local storage
    /// choice the [`Env`] was opened with. The `Deref` bound is what lets `db.get(rtxn, ..)`
    /// accept `&heed::RoTxn<'a, T>` — heed's `Database::get` requires `&RoTxn<'_, AnyTls>`
    /// and both `WithTls` and `WithoutTls` deref to it.
    ///
    /// Returns an [`LmdbError::Retrieve`] on error.
    #[inline(always)]
    pub(super) fn retrieve_bytes<'a, TLS>(
        rtxn: &'a heed::RoTxn<'a, TLS>,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: &MerkleHash,
    ) -> Result<Option<&'a [u8]>, LmdbError>
    where
        heed::RoTxn<'a, TLS>: std::ops::Deref<Target = heed::RoTxn<'a, AnyTls>>,
    {
        db.get(rtxn, &key.to_u128()).map_err(LmdbError::Retrieve)
    }

    /// True if the given key exists in the database. False otherwise.
    /// Does no decoding or other handling of the stored value.
    ///
    /// Generic over the `TLS` marker — see [`Self::retrieve`] for the rationale on the bound.
    ///
    /// Returns an [`LmdbError::Retrieve`] on error.
    #[inline(always)]
    pub(super) fn key_present<'a, TLS>(
        rtxn: &heed::RoTxn<'a, TLS>,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: &MerkleHash,
    ) -> Result<bool, LmdbError>
    where
        heed::RoTxn<'a, TLS>: std::ops::Deref<Target = heed::RoTxn<'a, AnyTls>>,
    {
        Ok(db
            .remap_data_type::<DecodeIgnore>()
            .get(rtxn, &key.to_u128())
            .map_err(LmdbError::Retrieve)?
            .is_some())
    }

    /// Serialize the value and put it into the database with the given key.
    /// Is an error if serialization fails (heed::Error::Encoding).
    ///
    /// Returns an [`LmdbError::Write`] on error.
    #[inline(always)]
    pub(super) fn put_serialized<'a, L, S: Fn(L) -> Vec<u8>>(
        wtxn: &mut heed::RwTxn<'a>,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: &MerkleHash,
        serialize: S,
        value: L,
    ) -> Result<(), LmdbError> {
        let as_bytes = serialize(value);
        db.put(wtxn, &key.to_u128(), &as_bytes)
            .map_err(LmdbError::Write)
    }

    /// Checks if a node with the given `hash` exists in the store.
    ///
    /// Unlike [`MerkleReader::exists`], this method will return `Ok(true)` for file nodes
    /// if the file exists in the tree.
    pub fn full_exists(&self, hash: &MerkleHash) -> Result<bool, LmdbError> {
        let exists = Self::key_present(&self.read_txn()?, &self.merkle_tree_nodes, hash)?;
        Ok(exists)
    }

    /// Retrieves the node with the given `hash` from the store. `None` means no such node exists.
    ///
    /// Unlike [`MerkleReader::get_node`], this method will return `Ok(Some(.))` for file nodes
    /// if the file exists in the tree.
    ///
    /// Internally reads the value bytes directly from LMDB's mmap and decodes via
    /// [`LmdbNode::decode`], which uses the zero-copy [`super::value_structs::LmdbNodeRef`]
    /// path to validate the header before copying out the msgpack tail.
    pub fn full_get_node(&self, hash: &MerkleHash) -> Result<Option<LmdbNode>, LmdbError> {
        let rtxn = self.read_txn()?;
        let Some(bytes) = Self::retrieve_bytes(&rtxn, &self.merkle_tree_nodes, hash)? else {
            return Ok(None);
        };
        Ok(Some(LmdbNode::decode(bytes)?))
    }

    /// Retreives the node's parent link (if present) and its children. Returns Ok(None)
    /// if there is no node for the given hash.
    pub fn get_links(&self, hash: &MerkleHash) -> Result<Option<LmdbLink>, LmdbError> {
        let rtxn = self.read_txn()?;
        let Some(bytes) = Self::retrieve_bytes(&rtxn, &self.merkle_links, hash)? else {
            return Ok(None);
        };
        Ok(Some(LmdbLink::decode(bytes)?))
    }
}

/// The name of the LMDB directory as it exists in the repository's `.oxen/` hidden directory.
/// LMDB's actual physical contents and lock files are stored within this directory.
const OXEN_LMDB_MERKLE_DIR: &str = "lmdb_merkle_tree_store";

/// The complete filepath to the LMDB file for the given repository.
pub(crate) fn lmdb_dir_location(repo_root: &Path) -> PathBuf {
    repo_root.join(OXEN_HIDDEN_DIR).join(OXEN_LMDB_MERKLE_DIR)
}

/// Only displays the repository root and LMDB environment settings.
impl std::fmt::Debug for LmdbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbBackend")
            .field("repo_root", &self.repo_root)
            .field("lmdb_env", &self.lmdb_env.info())
            .finish()
    }
}
