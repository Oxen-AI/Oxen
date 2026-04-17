use heed::byteorder::LE;
use heed::types::{Bytes, DecodeIgnore, U128};
use heed::{Database, Env, EnvOpenOptions};

use serde::Serialize;

use crate::explore::hash::{HasHash, Hash, HexHash};
use crate::explore::merkle_writer::{MerkleWriter, WriteSession};
use crate::explore::paths::AbsolutePath;
use crate::explore::{
    lazy_merkle::{MerkleTreeL, Root},
    merkle_reader::MerkleReader,
};

/// MUST USE LITTLE ENDIAN (LE) BECAUSE THIS MATCHES RUST'S LAYOUT
type HashLmdb = U128<LE>;
type ValueLmdb = Bytes;

pub struct LmdbMerkleDB {
    repo_root: AbsolutePath,
    lmdb_env: Env,
}

impl LmdbMerkleDB {
    pub fn new(
        repo_root: &AbsolutePath,
        db_location: &AbsolutePath,
        options: &EnvOpenOptions,
    ) -> Result<Self, <Self as MerkleReader>::Error> {
        let lmdb_env = unsafe { options.open(db_location.as_path())? };

        let mut wtxn = lmdb_env.write_txn()?;
        lmdb_env.create_database::<HashLmdb, ValueLmdb>(&mut wtxn, None)?;
        wtxn.commit()?;

        Ok(Self {
            repo_root: repo_root.clone(),
            lmdb_env,
        })
    }

    /// Actually access and decode some data stored in LMDB.
    pub(crate) fn retrieve<T>(&self, hash: Hash) -> Result<Option<T>, <Self as MerkleReader>::Error>
    where
        for<'de> T: serde::Deserialize<'de>,
    {
        let rtxn = self.lmdb_env.read_txn()?;
        let db: Database<HashLmdb, ValueLmdb> = self
            .lmdb_env
            .open_database(&rtxn, None)?
            .expect("Invariant violated: database has not been created.");

        let Some(bytes) = db.get(&rtxn, &hash.into())? else {
            return Ok(None);
        };

        let payload =
            rmp_serde::from_slice(bytes).map_err(|e| heed::Error::Decoding(Box::new(e)))?;
        Ok(Some(payload))
    }
}

impl MerkleReader for LmdbMerkleDB {
    type Error = heed::Error;

    fn repository(&self) -> &AbsolutePath {
        &self.repo_root
    }

    /// If true, then there is a node in the Merkle tree that has this hash.
    ///
    /// This always means that either `self.node()` xor `self.commit()` will return a
    /// non-`None` value. Note that this is a mutually exclusive relationship: exactly
    /// one of `node` or `commit` is non-`None` if `exists` is `true`.
    fn exists(&self, hash: Hash) -> Result<bool, Self::Error> {
        let rtxn = self.lmdb_env.read_txn()?;
        let db: Database<HashLmdb, ValueLmdb> = self
            .lmdb_env
            .open_database(&rtxn, None)?
            .expect("Invariant violated: database has not been created.");

        Ok(db
            // We only care about whether or not this key exists.
            // This remap data type ignores the value.
            .remap_data_type::<DecodeIgnore>()
            .get(&rtxn, &hash.into())?
            .is_some())
    }

    /// Obtains a reference to the Merkle tree node for the given hash.
    ///
    /// Corresponds to a real file or directory under version control.
    /// None means there is no node with that hash.
    fn node(&self, hash: Hash) -> Result<Option<MerkleTreeL>, Self::Error> {
        match self.retrieve(hash) {
            Err(heed::Error::Decoding(err)) => {
                eprintln!(
                    "[ERROR] {} is not a merkle node, it is a commit (error: {err})",
                    HexHash::from(hash)
                );
                Ok(None)
            }
            other => other,
        }
    }

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Result<Option<Root>, Self::Error> {
        match self.retrieve(hash) {
            Err(heed::Error::Decoding(err)) => {
                eprintln!(
                    "[ERROR] {} is not a commit, it is a merkle node (error: {err})",
                    HexHash::from(hash)
                );
                Ok(None)
            }
            other => other,
        }
    }
}

impl MerkleWriter for LmdbMerkleDB {
    type Error = heed::Error;
    type Session<'a> = LmdbWriteSession<'a>;

    /// Opens a write transaction in the "unamed" LMDB database.
    fn write_session<'a>(&'a self) -> Result<Self::Session<'a>, Self::Error> {
        let mut wtxn = self.lmdb_env.write_txn()?;
        let db: Database<U128<LE>, Bytes> = self.lmdb_env.create_database(&mut wtxn, None)?;
        Ok(LmdbWriteSession { wtxn, db })
    }
}

pub struct LmdbWriteSession<'a> {
    wtxn: heed::RwTxn<'a>,
    db: Database<U128<LE>, Bytes>,
}

impl<'a> LmdbWriteSession<'a> {
    /// Serializes the node and puts it into the database with its hash as the key.
    pub(crate) fn put<T: HasHash + Serialize>(
        &mut self,
        node: &T,
    ) -> Result<(), <Self as WriteSession<'a>>::Error> {
        let data = {
            let mut buf = Vec::new();

            node.serialize(&mut rmp_serde::Serializer::new(&mut buf))
                .map_err(|e| heed::Error::Encoding(Box::new(e)))?;

            buf
        };
        self.db.put(&mut self.wtxn, &node.hash().into(), &data)
    }
}

impl<'a> WriteSession<'a> for LmdbWriteSession<'a> {
    type Error = heed::Error;

    fn queue_node(&mut self, node: &MerkleTreeL) -> Result<(), Self::Error> {
        self.put(node)
    }

    fn queue_commit(&mut self, commit: &Root) -> Result<(), Self::Error> {
        self.put(commit)
    }

    /// Commits the active write transaction.
    fn finish(self) -> Result<(), Self::Error> {
        self.wtxn.commit()
    }
}
