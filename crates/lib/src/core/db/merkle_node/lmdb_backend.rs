use std::path::{Path, PathBuf};

use futures::SinkExt;
use heed::byteorder::LE;
use heed::types::{Bytes, DecodeIgnore, U128};
use heed::{Database, Env, EnvOpenOptions};

use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_writer::MerkleWriteSession;
use crate::model::merkle_tree::node::MerkleTreeNode;

/// MUST USE LITTLE ENDIAN (LE) so the byte layout matches
/// `ContentHash::as_bytes` / `LocationHash::as_bytes`.
type KeyLmdb = U128<LE>;
type ValueLmdb = Bytes;

const DB_CONTENTS: &str = "contents";

/// Merkle tree node storage that is backed by LMDB.
pub struct LmdbBackend {
    repo_root: PathBuf,
    lmdb_env: Env,
    contents: Database<KeyLmdb, ValueLmdb>,
}

impl std::fmt::Debug for LmdbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbBackend")
            .field("repo_root", &self.repo_root)
            .field("lmdb_env", &self.lmdb_env.info())
            .finish()
    }
}

impl LmdbBackend {
    pub fn new(
        repo_root: PathBuf,
        db_location: &Path,
        options: EnvOpenOptions,
    ) -> Result<Self, OxenError> {
        let options = {
            let mut options = options;
            options.max_dbs(1);
            options
        };

        let lmdb_env = unsafe { options.open(db_location)? };

        let mut wtxn = lmdb_env.write_txn()?;
        let contents =
            lmdb_env.create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_CONTENTS))?;
        wtxn.commit()?;

        Ok(Self { repo_root, lmdb_env, contents })
    }

    /// Retrieve the value and decode it into a struct of type `T`.
    /// Error if decoding fails. None if no key exists.
    #[inline(always)]
    fn retrieve_from<T>(
        lmdb_env: &Env,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: u128,
    ) -> Result<Option<T>, heed::Error>
    where
        for<'de> T: serde::Deserialize<'de>,
    {
        let rtxn = lmdb_env.read_txn()?;
        let Some(bytes) = db.get(&rtxn, &key)? else {
            return Ok(None);
        };
        let payload =
            rmp_serde::from_slice(bytes).map_err(|e| heed::Error::Decoding(Box::new(e)))?;
        Ok(Some(payload))
    }

    /// True if the given key exists in the database. False otherwise.
    /// Does no decoding or other handling of the stored value.
    #[inline(always)]
    fn key_present(
        lmdb_env: &Env,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: u128,
    ) -> Result<bool, heed::Error> {
        let rtxn = lmdb_env.read_txn()?;
        Ok(db
            .remap_data_type::<DecodeIgnore>()
            .get(&rtxn, &key)?
            .is_some())
    }
}

impl MerkleReader for LmdbBackend {

    /// Checks if a node with the given `hash` exists in the store.
    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        let exists = Self::key_present(&self.lmdb_env, &self.contents, hash.into())?;
        Ok(exists)
    }

    /// Retrieves the node with the given `hash` from the store. `None` means no such node exists.
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, OxenError> {
        let maybe_node = Self::retrieve_from(&self.lmdb_env, &self.contents, hash.into())?;
        Ok(maybe_node)
    }

    /// Retrieves the children of the node with the given `hash` from the store.
    /// An empty vec means that either the node is a not a directory or virtual node or it is one
    /// but has no files.
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError> {

        let Some(node) = self.get_node(hash)? else {
            return Ok(Vec::with_capacity(0));
        };

        node.node()




        if !MerkleNodeDB::exists(&self.repo_path, hash) {
            return Ok(Vec::with_capacity(0));
        }
        let mut db = MerkleNodeDB::open_read_only(&self.repo_path, hash)?;
        let children = db.map()?;
        Ok(children)
    }

}

/// Merkle writer implementation for the [`LmdbBackend`].
impl MerkleWriter for LmdbBackend {

    /// Returns a new [`LmdbWriteSession`] that can be used to write nodes to the store.
    fn begin<'a>(&'a self) -> Result<Box<dyn MerkleWriteSession + 'a>, OxenError> {
        let wtxn = self.lmdb_env.write_txn()?;
        Ok(LmdbWriteSession {
            wtxn,
            contents: &self.contents,
        })
    }
}

pub struct LmdbWriteSession<'a> {
    wtxn: heed::RwTxn<'a>,
    contents: &'a Database<KeyLmdb, ValueLmdb>,
}

impl<'a> LmdbWriteSession<'a> {
    /// Serialize the value and put it into the database with the given key.
    /// Is an error if serialization fails (heed::Error::Encoding).
    #[inline(always)]
    fn put_serialized<T: serde::Serialize>(
        wtxn: &mut heed::RwTxn<'a>,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: u128,
        value: &T,
    ) -> Result<(), heed::Error> {
        let mut buf = Vec::new();
        value
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .map_err(|e| heed::Error::Encoding(Box::new(e)))?;
        db.put(wtxn, &key, &buf)
    }
}

impl<'a> MerkleWriteSession<'a> for LmdbWriteSession<'a> {
    fn create_node<'b>(
        &'b self,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Box<dyn NodeWriteSession + 'b>, OxenError> {

    }

    fn finish(self: Box<Self>) -> Result<(), OxenError> {
        self.wtxn.commit()?;
        Ok(())
    }
}

impl<'a> NodeWriteSession for LmdbNodeWriteSession<'a> {


    fn queue_location(&mut self, key: LocationHash, node: &MerkleTreeL) -> Result<(), Self::Error> {
        Self::put_serialized(&mut self.wtxn, self.locations, key.into(), node)
    }

    fn queue_content(
        &mut self,
        key: ContentHash,
        content: &NodeContent,
    ) -> Result<(), Self::Error> {
        Self::put_serialized(&mut self.wtxn, self.contents, key.into(), content)
    }

    fn queue_commit(&mut self, commit: &Root) -> Result<(), Self::Error> {
        Self::put_serialized(
            &mut self.wtxn,
            self.commits,
            commit.content_hash().into(),
            commit,
        )
    }

    fn finish(self) -> Result<(), Self::Error> {
        self.wtxn.commit()
    }
}
