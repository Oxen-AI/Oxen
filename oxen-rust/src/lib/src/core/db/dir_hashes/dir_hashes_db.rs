use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

use lru::LruCache;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, RwLock};

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DB instances with LRU eviction
static DB_INSTANCES: LazyLock<RwLock<LruCache<PathBuf, Arc<DBWithThreadMode<MultiThreaded>>>>> =
    LazyLock::new(|| RwLock::new(LruCache::new(DB_CACHE_SIZE)));

// Commit db is the directories per commit
// This helps us skip to a directory in the tree
// .oxen/history/{COMMIT_ID}/dir_hashes
pub fn dir_hash_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    let commit_id = &commit.id;
    dir_hash_db_path_from_commit_id(repo, commit_id)
}

pub fn dir_hash_db_path_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> PathBuf {
    let commit_id = commit_id.as_ref();
    util::fs::oxen_hidden_dir(&repo.path)
        .join(Path::new(HISTORY_DIR))
        .join(commit_id)
        .join(DIR_HASHES_DIR)
}

pub fn with_dir_hash_db_manager<F, T>(
    repository: &LocalRepository,
    commit_id: &String,
    operation: F,
) -> Result<T, OxenError>
where
    F: FnOnce(&DBWithThreadMode<MultiThreaded>) -> Result<T, OxenError>,
{
    let dir_hashes_db = {
        let dir_hashes_db_dir = dir_hash_db_path_from_commit_id(repository, commit_id);

        // 1. If this dir_hashes db exists in cache, return the existing connection
        {
            let cache_r = match DB_INSTANCES.read() {
                Ok(cache_r) => cache_r,
                Err(e) => {
                    return Err(OxenError::basic_str(format!(
                        "Could not open LRU for dir hash db cache: {e:?}"
                    )));
                }
            };
            if let Some(db) = cache_r.peek(&dir_hashes_db_dir) {
                // Cache hit: return the existing connection
                let dir_hashes_db = Arc::clone(db);
                return operation(&dir_hashes_db);
            }
        }

        // 2. If not exists, create the directory and open the db
        let mut cache_w = match DB_INSTANCES.write() {
            Ok(cache_w) => cache_w,
            Err(e) => {
                return Err(OxenError::basic_str(format!(
                    "Could not open LRU for dir hash db cache: {e:?}"
                )));
            }
        };

        if let Some(db) = cache_w.get(&dir_hashes_db_dir) {
            Arc::clone(db)
        } else {
            // Cache miss: open a new connection to the db

            if !dir_hashes_db_dir.exists() {
                return Err(OxenError::basic_str(format!(
                    "Could not find dir_hashes db for commit {commit_id}"
                )));
            }

            let opts = db::key_val::opts::default();
            let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open_for_read_only(&opts, &dir_hashes_db_dir, false)?;

            // Wrap the DB in an Arc and store it in the cache
            let db = Arc::new(dir_hashes_db);
            cache_w.put(dir_hashes_db_dir, db.clone());

            db
        }
    };

    operation(&dir_hashes_db)
}
