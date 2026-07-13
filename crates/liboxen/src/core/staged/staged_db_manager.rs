use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, LazyLock, Weak};
use std::thread::sleep;
use std::time::Duration;

use indicatif::ProgressBar;
use parking_lot::{Mutex, RwLock};
use rmp_serde::Serializer;
use rocksdb::{DB, IteratorMode};
use serde::Serialize;

use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::StagedEntryStatus;
use crate::model::merkle_tree::node::{
    EMerkleTreeNode, FileNode, MerkleTreeNode, StagedMerkleTreeNode,
};
use crate::util;

// Weak-ref registry of open staged DB handles, keyed by `.oxen/staged` dir. The strong
// `Arc<RwLock<DB>>` lives only as long as some [`StagedDBManager`] holds it; when the last
// caller drops, RocksDB closes and the entry becomes a tombstone that the next opener prunes.
// There is no capacity cap, so an in-use entry can never be evicted — the shared-Arc
// invariant that compound read-modify-write sequences rely on holds unconditionally.
// A brief LOCK collision is still possible when an open races the tail of a concurrent
// close (RocksDB releases the OS lock in its `Drop`, after `strong_count` already hit zero);
// see [`get_staged_db_manager`] for the bounded-retry that waits it out.
static DB_INSTANCES: LazyLock<Mutex<HashMap<PathBuf, Weak<RwLock<DB>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// How long `get_staged_db_manager` waits out a concurrent close before surfacing a LOCK error.
const OPEN_RETRIES: u32 = 100;
const OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(2);

/// Removes this repository's tombstone entry from the registry. Live entries (someone still
/// holds the `Arc`) are unaffected; the DB closes when the last strong reference drops.
pub fn remove_from_cache(repository_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let staged_dir = util::fs::oxen_hidden_dir(repository_path).join(STAGED_DIR);
    let mut instances = DB_INSTANCES.lock();
    instances.remove(&staged_dir);
    Ok(())
}

/// Removes tombstone entries under `repository_path` from the registry. Live entries are
/// unaffected; the DB closes when the last strong reference drops.
pub fn remove_from_cache_with_children(repository_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let repository_path = repository_path.as_ref();
    let mut instances = DB_INSTANCES.lock();
    instances.retain(|key, _| !key.starts_with(repository_path));
    Ok(())
}

#[derive(Clone)]
pub struct StagedDBManager {
    staged_db: Arc<RwLock<DB>>,
    repository: LocalRepository,
}

/// Returns a [`StagedDBManager`] to access RocksDB for the given repository.
///
/// Every concurrent caller for the same repo receives the same shared `Arc<RwLock<DB>>`
/// on the staged DB for as long as at least one `StagedDBManager` stays alive. You should
/// **drop the manager as soon as you are done with it** — the underlying RocksDB closes on
/// the last drop, so holding a manager keeps the DB open (and its per-directory `LOCK`
/// file held) longer than necessary.
///
/// **In async contexts**, ensure the manager is dropped before any `.await` points to avoid
/// holding the database handle across suspension points.
///
/// May briefly block on a concurrent close — see the module doc and [`open_staged_db`] for
/// the retry that covers it.
///
/// Easy ways to ensure the manager is dropped promptly:
///
/// **Call `drop()` explicitly** when you need the result in the same scope:
/// ```ignore
/// let mgr = get_staged_db_manager(repo)?;
/// let result = mgr.read_from_staged_db(path)?;
/// drop(mgr);
/// // ... continue working with result ...
/// ```
///
/// **Use a block scope** so the manager is dropped at the end of the block:
/// ```ignore
/// let result = {
///     let mgr = get_staged_db_manager(repo)?;
///     mgr.read_from_staged_db(path)?
/// }; // mgr is dropped here
/// // ... continue working with result ...
/// ```
pub fn get_staged_db_manager(repository: &LocalRepository) -> Result<StagedDBManager, OxenError> {
    let staged_db_dir = util::fs::oxen_hidden_dir(&repository.path).join(STAGED_DIR);
    let staged_db = open_staged_db(&staged_db_dir)?;
    Ok(StagedDBManager {
        staged_db,
        repository: repository.clone(),
    })
}

/// Return the shared staged-DB handle for `staged_db_dir`, retrying briefly on a
/// LOCK-collision race with a concurrent close (see module doc).
fn open_staged_db(staged_db_dir: &Path) -> Result<Arc<RwLock<DB>>, OxenError> {
    // Fast path: cache hit does no filesystem work.
    if let Some(strong) = lookup_live(staged_db_dir) {
        return Ok(strong);
    }
    // Miss path: ensure the dir exists once (idempotent, but no reason to repeat under retry),
    // then open with bounded LOCK-collision retry.
    util::fs::create_dir_all(staged_db_dir)?;
    let opts = db::key_val::opts::default();
    let mut attempts = 0;
    loop {
        let mut instances = DB_INSTANCES.lock();
        if let Some(weak) = instances.get(staged_db_dir)
            && let Some(strong) = weak.upgrade()
        {
            return Ok(strong);
        }
        match DB::open(&opts, dunce::simplified(staged_db_dir)) {
            Ok(db) => {
                let arc_db = Arc::new(RwLock::new(db));
                instances.insert(staged_db_dir.to_path_buf(), Arc::downgrade(&arc_db));
                instances.retain(|_, weak| weak.strong_count() > 0);
                return Ok(arc_db);
            }
            Err(err) if is_lock_collision(&err) => {
                drop(instances);
                attempts += 1;
                if attempts >= OPEN_RETRIES {
                    return Err(staged_db_open_failed(err));
                }
                sleep(OPEN_RETRY_INTERVAL);
            }
            Err(err) => return Err(staged_db_open_failed(err)),
        }
    }
}

fn lookup_live(staged_db_dir: &Path) -> Option<Arc<RwLock<DB>>> {
    let instances = DB_INSTANCES.lock();
    instances.get(staged_db_dir)?.upgrade()
}

/// True if `err` is a RocksDB LOCK-file collision — i.e. another opener still holds the
/// per-directory LOCK. The check is by error-message match rather than a distinct
/// `ErrorKind` because RocksDB surfaces this as a plain `IOError` across platforms
/// (Unix "While lock file: …/LOCK: Resource temporarily unavailable",
/// Windows "Failed to create lock file: …\\LOCK: The process cannot access the file…").
fn is_lock_collision(err: &rocksdb::Error) -> bool {
    let msg = err.to_string();
    msg.contains("LOCK") || msg.contains("lock file")
}

fn staged_db_open_failed(source: rocksdb::Error) -> OxenError {
    log::error!("Failed to open staged db: {source}");
    OxenError::basic_str(format!("Failed to open staged db: {source}"))
}

/// Normalizes a path to use forward slashes for use as a DB key.
/// This ensures cross-platform consistency since DB keys should be platform-agnostic.
fn normalize_key(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().replace('\\', "/")
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
        let key = normalize_key(&path);
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
        let key = normalize_key(&path);

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
        let key = normalize_key(&path);
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
        let key = normalize_key(&path);

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
                            if db_path.starts_with(&path) && path != Path::new("") {
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
            db_w.delete(normalize_key(path).as_bytes())?;
        }
        Ok(())
    }
}
