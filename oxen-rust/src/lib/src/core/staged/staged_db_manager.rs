use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, LazyLock};

use indicatif::ProgressBar;
use lru::LruCache;
use parking_lot::Mutex;
use rmp_serde::Serializer;
use rocksdb::{IteratorMode, DB};
use serde::Serialize;

use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{
    EMerkleTreeNode, FileNode, MerkleTreeNode, StagedMerkleTreeNode,
};
use crate::model::LocalRepository;
use crate::model::StagedEntryStatus;
use crate::util;

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DB instances with LRU eviction
static DB_INSTANCES: LazyLock<RwLock<LruCache<PathBuf, Arc<RwLock<DB>>>>> =
    LazyLock::new(|| RwLock::new(LruCache::new(DB_CACHE_SIZE)));

/// Removes a repository's DB instance from the cache.
pub fn remove_from_cache(repository_path: impl AsRef<std::path::Path>) -> Result<(), OxenError> {
    let staged_dir = util::fs::oxen_hidden_dir(repository_path).join(STAGED_DIR);
    let mut instances = DB_INSTANCES.write();
    let _ = instances.pop(&staged_dir); // drop immediately
    Ok(())
}

/// Removes a repository's DB instance and all its subdirectories from the cache.
/// This is mostly useful in test cleanup to ensure all DB instances are removed.
pub fn remove_from_cache_with_children(
    repository_path: impl AsRef<std::path::Path>,
) -> Result<(), OxenError> {
    let mut dbs_to_remove: Vec<PathBuf> = vec![];
    let mut instances = DB_INSTANCES.write();
    for (key, _) in instances.iter() {
        if key.starts_with(&repository_path) {
            dbs_to_remove.push(key.clone());
        }
    }
    for db in dbs_to_remove {
        let _ = instances.pop(&db); // drop immediately
    }
    Ok(())
}

#[derive(Clone)]
pub struct StagedDBManager {
    staged_db: Arc<RwLock<DB>>,
    repository: LocalRepository,
}

pub fn with_staged_db_manager<F, T>(
    repository: &LocalRepository,
    operation: F,
) -> Result<T, OxenError>
where
    F: FnOnce(&StagedDBManager) -> Result<T, OxenError>,
{
    let staged_db = {
        let staged_db_dir = util::fs::oxen_hidden_dir(&repository.path).join(STAGED_DIR);

        // 1. If staged db exists in cache, return the existing connection
        {
            let cache_r = DB_INSTANCES.read();
            if let Some(db_lock) = cache_r.peek(&staged_db_dir) {
                // Read lock guard is dropped here, return the existing connection
                return operation(&StagedDBManager {
                    staged_db: db_lock.clone(),
                    repository: repository.clone(),
                });
            }
        }

        // 2. If not exists, create the directory and open the db
        let mut cache_w = DB_INSTANCES.write();
        if let Some(db_lock) = cache_w.get(&staged_db_dir) {
            db_lock.clone()
        } else {
            // Cache miss: create directory and open DB
            if !staged_db_dir.exists() {
                std::fs::create_dir_all(&staged_db_dir).map_err(|e| {
                    log::error!("Failed to create staged db directory: {e}");
                    OxenError::basic_str(format!("Failed to create staged db directory: {e}"))
                })?;
            }
            let opts = db::key_val::opts::default();
            let db = DB::open(&opts, dunce::simplified(&staged_db_dir)).map_err(|e| {
                log::error!("Failed to open staged db: {e}");
                OxenError::basic_str(format!("Failed to open staged db: {e}"))
            })?;
            // Wrap the DB in an RwLock and store it in the cache
            let db_lock = Arc::new(RwLock::new(db));
            cache_w.put(staged_db_dir.clone(), db_lock.clone());
            db_lock
        }
    };

    let manager = StagedDBManager {
        staged_db,
        repository: repository.clone(),
    };

    // Execute the operation with our StagedDBManager instance
    operation(&manager)
}

impl StagedDBManager {
    /// Upsert a file node to the staged db
    pub fn upsert_file_node(
        &self,
        relative_path: impl AsRef<Path>,
        status: StagedEntryStatus,
        file_node: &FileNode,
    ) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
        let staged_file_node = StagedMerkleTreeNode {
            status,
            node: MerkleTreeNode::from_file(file_node.clone()),
        };
        // Get a write lock on the db
        let db_w = self.staged_db.write();
        self.upsert_staged_node(relative_path, &staged_file_node, Some(&db_w))?;

        Ok(Some(staged_file_node))
    }

    /// Upsert a staged node to the staged db
    pub fn upsert_staged_node(
        &self,
        path: impl AsRef<Path>,
        staged_node: &StagedMerkleTreeNode,
        db_w: Option<&parking_lot::RwLockWriteGuard<DB>>,
    ) -> Result<(), OxenError> {
        let key = path.as_ref().to_string_lossy().into_owned();
        let mut buf = Vec::new();
        staged_node
            .serialize(&mut Serializer::new(&mut buf))
            .map_err(|e| OxenError::basic_str(e.to_string()))?;

        match db_w {
            Some(write_guard) => {
                write_guard.put(key.as_bytes(), buf)?;
            }
            None => {
                let db_w = self.staged_db.write();
                db_w.put(key.as_bytes(), buf)?;
            }
        }
        Ok(())
    }

    /// upsert multiple staged nodes to the staged db
    pub fn upsert_staged_nodes(
        &self,
        staged_nodes: &HashMap<PathBuf, StagedMerkleTreeNode>,
    ) -> Result<(), OxenError> {
        let db_w = self.staged_db.write();
        for (key, staged_node) in staged_nodes.iter() {
            self.upsert_staged_node(key, staged_node, Some(&db_w))?;
        }
        Ok(())
    }

    /// Delete an entry from the staged db
    /// If db_w is provided, use that write lock; otherwise acquire a new one
    pub fn delete_entry_with_lock(
        &self,
        path: impl AsRef<Path>,
        db_w: Option<&parking_lot::RwLockWriteGuard<DB>>,
    ) -> Result<(), OxenError> {
        let key = path.as_ref().to_string_lossy();

        match db_w {
            Some(write_guard) => {
                write_guard.delete(key.as_bytes())?;
            }
            None => {
                let db_w = self.staged_db.write();
                db_w.delete(key.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Delete an entry from the staged db (convenience method)
    pub fn delete_entry(&self, path: impl AsRef<Path>) -> Result<(), OxenError> {
        self.delete_entry_with_lock(path, None)
    }

    /// Write a directory node to the staged db
    pub fn add_directory(
        &self,
        directory_path: impl AsRef<Path>,
        seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
    ) -> Result<(), OxenError> {
        let directory_path = directory_path.as_ref();
        let directory_path_str = directory_path.to_str().unwrap();
        let mut seen_dirs = seen_dirs.lock();
        if !seen_dirs.insert(directory_path.to_path_buf()) {
            return Ok(());
        }

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Added,
            node: MerkleTreeNode::default_dir_from_path(directory_path),
        };

        let mut buf = Vec::new();
        dir_entry
            .serialize(&mut Serializer::new(&mut buf))
            .map_err(|e| {
                OxenError::basic_str(format!("Failed to serialize directory entry: {e}"))
            })?;
        let db_w = self.staged_db.write();
        db_w.put(directory_path_str, &buf)?;

        Ok(())
    }

    /// True if the paths exists in the staged db. False means it does not exist.
    pub fn exists(&self, path: impl AsRef<Path>) -> Result<bool, OxenError> {
        let key = path.as_ref().to_string_lossy();
        Ok({
            let db_r = self.staged_db.read();
            // key_may_exist is a bloom filter that doesn't hit I/O
            // if it says it doesn't exist, it 100% does not exist
            if !db_r.key_may_exist(key.as_bytes()) {
                false
            } else {
                // otherwise, we have to confirm with actual read
                // we use get_pinned to avoid copying data -- we only want to know if it exists
                db_r.get_pinned(key.as_bytes())?.is_some()
            }
        })
    }

    /// Read a file node from the staged db
    pub fn read_from_staged_db(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
        let key = path.as_ref().to_string_lossy();

        let db_r = self.staged_db.read();
        let data = match db_r.get(key.as_bytes())? {
            Some(d) => d,
            None => return Ok(None),
        };
        match rmp_serde::from_slice(&data) {
            Ok(val) => Ok(Some(val)),
            Err(e) => {
                log::error!("Failed to deserialize data for key {key}: {e}");
                Err(OxenError::basic_str(format!(
                    "Failed to deserialize staged data: {e}"
                )))
            }
        }
    }

    /// Read all entries below a path from the staged db
    pub fn read_staged_entries_below_path(
        &self,
        start_path: impl AsRef<Path>,
        read_progress: &ProgressBar,
    ) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
        let db = self.staged_db.read();
        let start_path =
            util::fs::path_relative_to_dir(start_path.as_ref(), &self.repository.path)?;
        let mut total_entries = 0;
        let iter = db.iterator(IteratorMode::Start);
        let mut dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>> = HashMap::new();
        for item in iter {
            match item {
                // key = file path, value = EntryMetaData
                Ok((key, value)) => {
                    // log::debug!("Key is {key:?}, value is {value:?}");
                    let key =
                        str::from_utf8(&key).map_err(|e| OxenError::basic_str(e.to_string()))?;
                    let path = Path::new(key);
                    if !path.starts_with(&start_path) {
                        continue;
                    }

                    // Older versions may have a corrupted StagedMerkleTreeNode that was staged
                    // Ignore these when reading the staged db
                    let entry: Result<StagedMerkleTreeNode, rmp_serde::decode::Error> =
                        rmp_serde::from_slice(&value);
                    let Ok(entry) = entry else {
                        log::error!("read_staged_entries error decoding {key} path: {path:?}");
                        continue;
                    };
                    log::debug!("read_staged_entries key {key} entry: {entry} path: {path:?}");

                    if let EMerkleTreeNode::Directory(_) = &entry.node.node {
                        // add the dir as a key in dir_entries
                        log::debug!("read_staged_entries adding dir {path:?}");
                        dir_entries.entry(path.to_path_buf()).or_default();
                    }

                    // add the file or dir as an entry under its parent dir
                    if let Some(parent) = path.parent() {
                        log::debug!(
                            "read_staged_entries adding file {path:?} to parent {parent:?}"
                        );
                        dir_entries
                            .entry(parent.to_path_buf())
                            .or_default()
                            .push(entry);
                    }

                    total_entries += 1;
                    read_progress.set_message(format!("Found {total_entries} entries"));
                }
                Err(err) => {
                    log::error!("Could not get staged entry: {err}");
                }
            }
        }

        log::debug!(
            "read_staged_entries dir_entries.len(): {:?}",
            dir_entries.len()
        );
        if log::max_level() == log::Level::Debug {
            for (dir, entries) in dir_entries.iter() {
                log::debug!("commit dir_entries dir {dir:?}");
                for entry in entries.iter() {
                    log::debug!("\tcommit dir_entries entry {entry}");
                }
            }
        }

        Ok((dir_entries, total_entries))
    }

    /// Remove staged entries and parent dir from staged db
    /// Duplicate of rm::remove_staged_recursively, for workspaces use only
    pub fn remove_staged_recursively(
        &self,
        repo: &LocalRepository,
        paths: &HashSet<PathBuf>,
    ) -> Result<(), OxenError> {
        let db_w = self.staged_db.write();
        let iter = db_w.iterator(IteratorMode::Start);
        // Iterate over staged_db and check if the path starts with the given path
        for item in iter {
            match item {
                Ok((key, _)) => match str::from_utf8(&key) {
                    Ok(key) => {
                        log::debug!("considering key: {key:?}");
                        for path in paths {
                            let path = util::fs::path_relative_to_dir(path, &repo.path)?;
                            let db_path = PathBuf::from(key);
                            log::debug!("considering rm db_path: {db_path:?} for path: {path:?}");
                            if db_path.starts_with(&path) && path != PathBuf::from("") {
                                let mut parent = db_path.parent().unwrap_or(Path::new(""));
                                self.delete_entry_with_lock(&db_path, Some(&db_w))?;
                                while parent != Path::new("") {
                                    log::debug!("maybe cleaning up empty dir: {parent:?}");
                                    self.cleanup_empty_dirs_with_lock(parent, &db_w)?;
                                    parent = parent.parent().unwrap_or(Path::new(""));
                                    if parent == Path::new("") {
                                        self.cleanup_empty_dirs_with_lock(parent, &db_w)?;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(OxenError::basic_str(format!(
                            "Could not read utf8 val: {e}"
                        )));
                    }
                },
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not read iterate over db values",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Removes an empty directory from the staged db
    fn cleanup_empty_dirs_with_lock(
        &self,
        path: &Path,
        db_w: &parking_lot::RwLockWriteGuard<DB>,
    ) -> Result<(), OxenError> {
        let iter = db_w.iterator(IteratorMode::Start);
        let mut total = 0;
        for item in iter {
            match item {
                Ok((key, _)) => match str::from_utf8(&key) {
                    Ok(key) => {
                        log::debug!("considering key: {key:?}");
                        let db_path = PathBuf::from(key);
                        if db_path.starts_with(path) && path != db_path {
                            total += 1;
                        }
                    }
                    Err(e) => {
                        return Err(OxenError::basic_str(format!(
                            "Could not read utf8 val: {e}"
                        )));
                    }
                },
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not read iterate over db values",
                    ));
                }
            }
        }
        log::debug!("total sub paths for dir {path:?}: {total}");
        if total == 0 {
            log::debug!("removing empty dir: {path:?}");
            db_w.delete(path.to_string_lossy().as_bytes())?;
        }
        Ok(())
    }
}
