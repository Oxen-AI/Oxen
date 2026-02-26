use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::Mutex;
use rocksdb::DB;

use crate::constants::WORKSPACE_COMMITS_DIR;
use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util;

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DB instances with LRU eviction
static DB_INSTANCES: LazyLock<Mutex<LruCache<PathBuf, Arc<DB>>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(DB_CACHE_SIZE)));

/// Removes a repository's workspace commits DB instance from the cache.
pub(crate) fn remove_from_cache(
    repository_path: impl AsRef<std::path::Path>,
) -> Result<(), OxenError> {
    let commits_dir = util::fs::oxen_hidden_dir(repository_path).join(WORKSPACE_COMMITS_DIR);
    let mut instances = DB_INSTANCES.lock();
    let _ = instances.pop(&commits_dir);
    Ok(())
}

/// Manages the RocksDB commit ID → workspace ID mapping for non-editable workspaces.
pub(crate) struct WorkspaceCommitManager {
    db: Arc<DB>,
}

/// Manages access to the commit ID → workspace ID mapping.
pub(crate) fn with_workspace_commit_manager<F, T>(
    repository: &LocalRepository,
    operation: F,
) -> Result<T, OxenError>
where
    F: FnOnce(&WorkspaceCommitManager) -> Result<T, OxenError>,
{
    // Get or create the DB instance from cache
    let commits_db = {
        let commits_dir = util::fs::oxen_hidden_dir(&repository.path).join(WORKSPACE_COMMITS_DIR);

        let mut instances = DB_INSTANCES.lock();
        if let Some(db) = instances.get(&commits_dir) {
            Ok::<Arc<DB>, OxenError>(db.clone())
        } else {
            // Ensure directory exists
            if !commits_dir.exists() {
                util::fs::create_dir_all(&commits_dir).map_err(|e| {
                    let msg = format!("Failed to create workspace commits directory: {e}");
                    log::error!("{msg}");
                    OxenError::basic_str(msg)
                })?;
            }

            let opts = db::key_val::opts::default();
            let db = DB::open(&opts, dunce::simplified(&commits_dir)).map_err(|e| {
                let msg = format!("Failed to open workspace commits database: {e}");
                log::error!("{msg}");
                OxenError::basic_str(msg)
            })?;
            let arc_db = Arc::new(db);
            instances.put(commits_dir, arc_db.clone());
            Ok(arc_db)
        }
    }?;

    let manager = WorkspaceCommitManager { db: commits_db };

    operation(&manager)
}

impl WorkspaceCommitManager {
    pub(crate) fn get_id_for_commit(&self, commit_id: &str) -> Result<Option<String>, OxenError> {
        let bytes = commit_id.as_bytes();
        match self.db.get(bytes) {
            Ok(Some(value)) => Ok(Some(String::from(str::from_utf8(&value)?))),
            Ok(None) => Ok(None),
            Err(err) => {
                log::error!("get_id_for_commit error finding workspace id for commit {commit_id}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub(crate) fn set_commit(&self, commit_id: &str, workspace_id: &str) -> Result<(), OxenError> {
        self.db.put(commit_id.as_bytes(), workspace_id.as_bytes())?;
        Ok(())
    }

    pub(crate) fn has_commit(&self, commit_id: &str) -> bool {
        matches!(self.db.get(commit_id.as_bytes()), Ok(Some(_)))
    }

    pub(crate) fn delete_commit(&self, commit_id: &str) -> Result<(), OxenError> {
        self.db.delete(commit_id.as_bytes())?;
        Ok(())
    }
}
