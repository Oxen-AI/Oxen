use heed::byteorder::LE;
use heed::types::{Bytes, DecodeIgnore, U128};
use heed::{Database, Env, EnvOpenOptions};
use liboxen::util::oxen_date_format::deserialize;

use crate::explore::new_path::AbsolutePath;
use crate::explore::{
    lazy_merkle_lmdb::{MerkleMetadataStore, MerkleTreeL, Root},
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
    ) -> Result<Self, <Self as MerkleMetadataStore>::Error> {
        let lmdb_env = unsafe { options.open(db_location.as_path())? };

        let mut wtxn = lmdb_env.write_txn()?;
        lmdb_env.create_database::<HashLmdb, ValueLmdb>(&mut wtxn, None)?;
        wtxn.commit()?;

        Ok(Self { repo, lmdb_env })
    }
}

impl MerkleMetadataStore for LmdbMerkleDB {
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
    fn node(&self, hash: Hash) -> Result<Option<&MerkleTreeL<Self>>, Self::Error> {
        let rtxn = self.lmdb_env.read_txn()?;
        let db: Database<HashLmdb, ValueLmdb> = self
            .lmdb_env
            .open_database(&rtxn, None)?
            .expect("Invariant violated: database has not been created.");


        let Some(bytes) = db.get(&rtxn, &hash.into())? else {
            return Ok(None)
        };

        use rmp_serde;

        rmp_serde::from_slice

        heed::Error::Decoding()

        Ok()
    }

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, _hash: Hash) -> Result<Option<&Root<Self>>, Self::Error> {
        unimplemented!()
    }
}
