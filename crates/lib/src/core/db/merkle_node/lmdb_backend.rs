use heed::byteorder::LE;
use heed::types::{Bytes, DecodeIgnore, U128};
use heed::{Database, Env, EnvOpenOptions};

/// MUST USE LITTLE ENDIAN (LE) so the byte layout matches
/// `ContentHash::as_bytes` / `LocationHash::as_bytes`.
type KeyLmdb = U128<LE>;
type ValueLmdb = Bytes;

const DB_LOCATIONS: &str = "locations";
const DB_CONTENTS: &str = "contents";
const DB_COMMITS: &str = "commits";

pub struct LmdbBackend {
    repo_root: AbsolutePath,
    lmdb_env: Env,
    locations: Database<KeyLmdb, ValueLmdb>,
    contents: Database<KeyLmdb, ValueLmdb>,
    commits: Database<KeyLmdb, ValueLmdb>,
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
        repo_root: &AbsolutePath,
        db_location: &AbsolutePath,
        options: EnvOpenOptions,
    ) -> Result<Self, <Self as MerkleReader>::Error> {
        // Three named DBs: locations / contents / commits.
        let options = {
            let mut options = options;
            options.max_dbs(3);
            options
        };

        let lmdb_env = unsafe { options.open(db_location.as_path())? };

        let mut wtxn = lmdb_env.write_txn()?;
        let locations =
            lmdb_env.create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_LOCATIONS))?;
        let contents =
            lmdb_env.create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_CONTENTS))?;
        let commits =
            lmdb_env.create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_COMMITS))?;
        wtxn.commit()?;

        Ok(Self {
            repo_root: repo_root.clone(),
            lmdb_env,
            locations,
            contents,
            commits,
        })
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
    type Error = heed::Error;

    fn repository(&self) -> &AbsolutePath {
        &self.repo_root
    }

    fn location_exists(&self, location: LocationHash) -> Result<bool, Self::Error> {
        Self::key_present(&self.lmdb_env, &self.locations, location.into())
    }

    fn content_exists(&self, content: ContentHash) -> Result<bool, Self::Error> {
        Self::key_present(&self.lmdb_env, &self.contents, content.into())
    }

    fn commit_exists(&self, commit: ContentHash) -> Result<bool, Self::Error> {
        Self::key_present(&self.lmdb_env, &self.commits, commit.into())
    }

    fn node(&self, location: LocationHash) -> Result<Option<MerkleTreeL>, Self::Error> {
        Self::retrieve_from(&self.lmdb_env, &self.locations, location.into())
    }

    fn content(&self, content: ContentHash) -> Result<Option<NodeContent>, Self::Error> {
        Self::retrieve_from(&self.lmdb_env, &self.contents, content.into())
    }

    fn commit(&self, commit: ContentHash) -> Result<Option<Root>, Self::Error> {
        Self::retrieve_from(&self.lmdb_env, &self.commits, commit.into())
    }
}

impl MerkleWriter for LmdbBackend {
    type Error = heed::Error;
    type Session<'a> = LmdbWriteSession<'a>;

    fn write_session(&self) -> Result<Self::Session<'_>, Self::Error> {
        let wtxn = self.lmdb_env.write_txn()?;
        Ok(LmdbWriteSession {
            wtxn,
            locations: &self.locations,
            contents: &self.contents,
            commits: &self.commits,
        })
    }
}

pub struct LmdbWriteSession<'a> {
    wtxn: heed::RwTxn<'a>,
    locations: &'a Database<KeyLmdb, ValueLmdb>,
    contents: &'a Database<KeyLmdb, ValueLmdb>,
    commits: &'a Database<KeyLmdb, ValueLmdb>,
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

impl<'a> WriteSession<'a> for LmdbWriteSession<'a> {
    type Error = heed::Error;

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
