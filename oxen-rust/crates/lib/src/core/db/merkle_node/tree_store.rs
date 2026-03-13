use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, RwLock};

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions};
use lru::LruCache;

use crate::constants::{OXEN_HIDDEN_DIR, TREE_DIR, TREE_LMDB_DIR};
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash};

use super::node_record::{ChildEntry, NodeRecord};

const LMDB_MAP_SIZE: usize = 10 * 1024 * 1024 * 1024; // 10 GB virtual
const LMDB_MAX_DBS: u32 = 3; // nodes, children, dir_hashes
const STORE_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

static TREE_STORE_CACHE: LazyLock<RwLock<LruCache<PathBuf, Arc<TreeStore>>>> =
    LazyLock::new(|| RwLock::new(LruCache::new(STORE_CACHE_SIZE)));

pub struct TreeStore {
    env: Env,
    nodes_db: Database<Bytes, Bytes>,
    children_db: Database<Bytes, Bytes>,
    dir_hashes_db: Database<Bytes, Bytes>,
}

// Safety: heed::Env is Send + Sync. We only access databases through
// proper read/write transactions which handle synchronization internally.
unsafe impl Send for TreeStore {}
unsafe impl Sync for TreeStore {}

fn lmdb_path(repo: &LocalRepository) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(TREE_LMDB_DIR)
}

/// Get or create a TreeStore for the given repository.
pub fn get_tree_store(repo: &LocalRepository) -> Result<Arc<TreeStore>, OxenError> {
    let path = lmdb_path(repo);

    // Fast path: read lock cache check
    {
        let cache = TREE_STORE_CACHE.read().map_err(|e| {
            OxenError::basic_str(format!("Could not read tree store cache: {e:?}"))
        })?;
        if let Some(store) = cache.peek(&path) {
            return Ok(Arc::clone(store));
        }
    }

    // Slow path: write lock, check again, then open
    let mut cache = TREE_STORE_CACHE.write().map_err(|e| {
        OxenError::basic_str(format!("Could not write tree store cache: {e:?}"))
    })?;

    if let Some(store) = cache.get(&path) {
        return Ok(Arc::clone(store));
    }

    let store = Arc::new(TreeStore::open(&path)?);
    cache.put(path, Arc::clone(&store));
    Ok(store)
}

/// Remove a TreeStore from the cache, e.g. for test cleanup.
pub fn remove_tree_store_from_cache(repo_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let prefix = repo_path.as_ref();
    let mut cache = TREE_STORE_CACHE.write().map_err(|e| {
        OxenError::basic_str(format!("Could not write tree store cache: {e:?}"))
    })?;
    let keys_to_remove: Vec<PathBuf> = cache
        .iter()
        .filter(|(k, _)| k.starts_with(prefix))
        .map(|(k, _)| k.clone())
        .collect();
    for key in keys_to_remove {
        cache.pop(&key);
    }
    Ok(())
}

impl TreeStore {
    pub fn open(lmdb_dir: &Path) -> Result<Self, OxenError> {
        std::fs::create_dir_all(lmdb_dir)?;
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(LMDB_MAP_SIZE)
                .max_dbs(LMDB_MAX_DBS)
                .open(lmdb_dir)
                .map_err(|e| OxenError::basic_str(format!("Failed to open LMDB env: {e}")))?
        };
        let mut wtxn = env
            .write_txn()
            .map_err(|e| OxenError::basic_str(format!("Failed to create LMDB write txn: {e}")))?;
        let nodes_db = env
            .create_database(&mut wtxn, Some("nodes"))
            .map_err(|e| OxenError::basic_str(format!("Failed to create nodes db: {e}")))?;
        let children_db = env
            .create_database(&mut wtxn, Some("children"))
            .map_err(|e| OxenError::basic_str(format!("Failed to create children db: {e}")))?;
        let dir_hashes_db = env
            .create_database(&mut wtxn, Some("dir_hashes"))
            .map_err(|e| OxenError::basic_str(format!("Failed to create dir_hashes db: {e}")))?;
        wtxn.commit()
            .map_err(|e| OxenError::basic_str(format!("Failed to commit LMDB init txn: {e}")))?;

        Ok(Self {
            env,
            nodes_db,
            children_db,
            dir_hashes_db,
        })
    }

    // ---- Node / Children API ----

    pub fn node_exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB read txn error: {e}")))?;
        let exists = self
            .nodes_db
            .get(&rtxn, &hash.to_le_bytes())
            .map_err(|e| OxenError::basic_str(format!("LMDB get error: {e}")))?
            .is_some();
        Ok(exists)
    }

    pub fn get_node(&self, hash: &MerkleHash) -> Result<Option<NodeRecord>, OxenError> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB read txn error: {e}")))?;
        match self
            .nodes_db
            .get(&rtxn, &hash.to_le_bytes())
            .map_err(|e| OxenError::basic_str(format!("LMDB get error: {e}")))?
        {
            Some(bytes) => Ok(Some(NodeRecord::from_bytes(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn get_children(&self, hash: &MerkleHash) -> Result<Option<Vec<ChildEntry>>, OxenError> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB read txn error: {e}")))?;
        match self
            .children_db
            .get(&rtxn, &hash.to_le_bytes())
            .map_err(|e| OxenError::basic_str(format!("LMDB get error: {e}")))?
        {
            Some(bytes) => Ok(Some(ChildEntry::vec_from_bytes(bytes)?)),
            None => Ok(None),
        }
    }

    /// Write a node and its children atomically in one transaction.
    pub fn put_node_with_children(
        &self,
        hash: &MerkleHash,
        node_record: &NodeRecord,
        children: &[ChildEntry],
    ) -> Result<(), OxenError> {
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB write txn error: {e}")))?;
        self.nodes_db
            .put(&mut wtxn, &hash.to_le_bytes(), &node_record.to_bytes())
            .map_err(|e| OxenError::basic_str(format!("LMDB put node error: {e}")))?;
        self.children_db
            .put(
                &mut wtxn,
                &hash.to_le_bytes(),
                &ChildEntry::vec_to_bytes(children),
            )
            .map_err(|e| OxenError::basic_str(format!("LMDB put children error: {e}")))?;
        wtxn.commit()
            .map_err(|e| OxenError::basic_str(format!("LMDB commit error: {e}")))?;
        Ok(())
    }

    // ---- Dir Hashes API ----

    /// Construct the composite key: [commit_hash 16B][path UTF-8 bytes]
    fn dir_hash_key(commit_hash: &MerkleHash, path: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(16 + path.len());
        key.extend_from_slice(&commit_hash.to_le_bytes());
        key.extend_from_slice(path.as_bytes());
        key
    }

    /// Write a single dir_hash entry.
    pub fn put_dir_hash(
        &self,
        commit_hash: &MerkleHash,
        path: &str,
        dir_hash: &MerkleHash,
    ) -> Result<(), OxenError> {
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB write txn error: {e}")))?;
        let key = Self::dir_hash_key(commit_hash, path);
        self.dir_hashes_db
            .put(&mut wtxn, &key, &dir_hash.to_le_bytes())
            .map_err(|e| OxenError::basic_str(format!("LMDB put dir_hash error: {e}")))?;
        wtxn.commit()
            .map_err(|e| OxenError::basic_str(format!("LMDB commit error: {e}")))?;
        Ok(())
    }

    /// Write multiple dir_hash entries in a single transaction.
    pub fn put_dir_hashes_batch(
        &self,
        commit_hash: &MerkleHash,
        entries: &HashMap<PathBuf, MerkleHash>,
    ) -> Result<(), OxenError> {
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB write txn error: {e}")))?;
        for (path, hash) in entries {
            if let Some(path_str) = path.to_str() {
                let key = Self::dir_hash_key(commit_hash, path_str);
                self.dir_hashes_db
                    .put(&mut wtxn, &key, &hash.to_le_bytes())
                    .map_err(|e| OxenError::basic_str(format!("LMDB put dir_hash error: {e}")))?;
            } else {
                log::error!("Failed to convert path to string: {path:?}");
            }
        }
        wtxn.commit()
            .map_err(|e| OxenError::basic_str(format!("LMDB commit error: {e}")))?;
        Ok(())
    }

    /// Delete a dir_hash entry.
    pub fn delete_dir_hash(
        &self,
        commit_hash: &MerkleHash,
        path: &str,
    ) -> Result<bool, OxenError> {
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB write txn error: {e}")))?;
        let key = Self::dir_hash_key(commit_hash, path);
        let deleted = self
            .dir_hashes_db
            .delete(&mut wtxn, &key)
            .map_err(|e| OxenError::basic_str(format!("LMDB delete dir_hash error: {e}")))?;
        wtxn.commit()
            .map_err(|e| OxenError::basic_str(format!("LMDB commit error: {e}")))?;
        Ok(deleted)
    }

    /// List all dir_hashes for a commit using prefix iteration.
    pub fn list_dir_hashes(
        &self,
        commit_hash: &MerkleHash,
    ) -> Result<HashMap<PathBuf, MerkleHash>, OxenError> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB read txn error: {e}")))?;
        let prefix = commit_hash.to_le_bytes();
        let iter = self
            .dir_hashes_db
            .prefix_iter(&rtxn, &prefix)
            .map_err(|e| OxenError::basic_str(format!("LMDB prefix_iter error: {e}")))?;
        let mut result = HashMap::new();
        for item in iter {
            let (key, value) = item
                .map_err(|e| OxenError::basic_str(format!("LMDB iter error: {e}")))?;
            let path_bytes = &key[16..];
            let path_str = std::str::from_utf8(path_bytes).map_err(|e| {
                OxenError::basic_str(format!("Invalid UTF-8 in dir_hash path: {e}"))
            })?;
            let hash_bytes: [u8; 16] = value.try_into().map_err(|_| {
                OxenError::basic_str("dir_hash value is not 16 bytes")
            })?;
            let hash = MerkleHash::from_le_bytes(hash_bytes);
            result.insert(PathBuf::from(path_str), hash);
        }
        Ok(result)
    }

    /// Get a single dir_hash for a commit+path.
    pub fn get_dir_hash(
        &self,
        commit_hash: &MerkleHash,
        path: &str,
    ) -> Result<Option<MerkleHash>, OxenError> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB read txn error: {e}")))?;
        let key = Self::dir_hash_key(commit_hash, path);
        match self
            .dir_hashes_db
            .get(&rtxn, &key)
            .map_err(|e| OxenError::basic_str(format!("LMDB get dir_hash error: {e}")))?
        {
            Some(bytes) => {
                let hash_bytes: [u8; 16] = bytes.try_into().map_err(|_| {
                    OxenError::basic_str("dir_hash value is not 16 bytes")
                })?;
                Ok(Some(MerkleHash::from_le_bytes(hash_bytes)))
            }
            None => Ok(None),
        }
    }

    // ---- Iteration (for compress_full_tree) ----

    /// Iterate all nodes in the store. Returns (hash, NodeRecord) pairs.
    pub fn iter_all_nodes(&self) -> Result<Vec<(MerkleHash, NodeRecord)>, OxenError> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| OxenError::basic_str(format!("LMDB read txn error: {e}")))?;
        let iter = self
            .nodes_db
            .iter(&rtxn)
            .map_err(|e| OxenError::basic_str(format!("LMDB iter error: {e}")))?;
        let mut result = Vec::new();
        for item in iter {
            let (key, value) = item
                .map_err(|e| OxenError::basic_str(format!("LMDB iter error: {e}")))?;
            let hash_bytes: [u8; 16] = key.try_into().map_err(|_| {
                OxenError::basic_str("Node key is not 16 bytes")
            })?;
            let hash = MerkleHash::from_le_bytes(hash_bytes);
            let record = NodeRecord::from_bytes(value)?;
            result.push((hash, record));
        }
        Ok(result)
    }
}
