use heed::byteorder::LE;
use heed::types::{Bytes, DecodeIgnore, U128};
use heed::{Database, Env, EnvOpenOptions};

use crate::explore::merkle_writer::{MerkleWriter, WriteSession};
use crate::explore::new_path::AbsolutePath;
use crate::explore::{
    lazy_merkle::{MerkleTreeL, Root},
    merkle_reader::MerkleReader,
    scratch::{Hash, Repository},
};

/// MUST USE LITTLE ENDIAN (LE) BECAUSE THIS MATCHES RUST'S LAYOUT
type HashLmdb = U128<LE>;
type ValueLmdb = Bytes;

pub struct LmdbMerkleDB {
    repo: Repository,
    lmdb_env: Env,
}

impl LmdbMerkleDB {
    pub fn new(
        repo: Repository,
        db_location: AbsolutePath,
        options: &EnvOpenOptions,
    ) -> Result<Self, <Self as MerkleReader>::Error> {
        let lmdb_env = unsafe { options.open(db_location.as_path())? };

        let mut wtxn = lmdb_env.write_txn()?;
        lmdb_env.create_database::<HashLmdb, ValueLmdb>(&mut wtxn, None)?;
        wtxn.commit()?;

        Ok(Self { repo, lmdb_env })
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

    fn repository(&self) -> &Repository {
        &self.repo
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
        self.retrieve(hash)
    }

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Result<Option<Root>, Self::Error> {
        self.retrieve(hash)
    }
}

impl MerkleWriter for LmdbMerkleDB {
    type Error = heed::Error;
    type Session<'a> = LmdbWriteSession<'a>;

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

impl<'a> WriteSession<'a> for LmdbWriteSession<'a> {
    type Error = heed::Error;

    fn queue_write(&mut self, node: &MerkleTreeL) -> Result<(), Self::Error> {
        let data = {
            let mut buf = Vec::new();

            use serde::Serialize;
            node.serialize(&mut rmp_serde::Serializer::new(&mut buf))
                .map_err(|e| heed::Error::Encoding(Box::new(e)))?;

            buf
        };
        self.db.put(&mut self.wtxn, &node.hash().into(), &data)
    }

    fn finish(self) -> Result<(), Self::Error> {
        self.wtxn.commit()
    }
}
