use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::Mutex;
use rocksdb::DB;

use crate::constants::WORKSPACE_NAMES_DIR;
use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util;

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DB instances with LRU eviction
static DB_INSTANCES: LazyLock<Mutex<LruCache<PathBuf, Arc<DB>>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(DB_CACHE_SIZE)));

/// Removes a repository's workspace names DB instance from the cache.
pub fn remove_from_cache(repository_path: impl AsRef<std::path::Path>) -> Result<(), OxenError> {
    let names_dir = util::fs::oxen_hidden_dir(repository_path).join(WORKSPACE_NAMES_DIR);
    let mut instances = DB_INSTANCES.lock();
    let _ = instances.pop(&names_dir);
    Ok(())
}

/// Manages the RocksDB workspace ID -> name mapping.
pub struct WorkspaceNameManager {
    db: Arc<DB>,
}

/// Manages access to the workspace ID -> name mapping.
pub fn with_workspace_name_manager<F, T>(
    repository: &LocalRepository,
    operation: F,
) -> Result<T, OxenError>
where
    F: FnOnce(&WorkspaceNameManager) -> Result<T, OxenError>,
{
    // Get or create the DB instance from cache
    let names_db = {
        let names_dir = util::fs::oxen_hidden_dir(&repository.path).join(WORKSPACE_NAMES_DIR);

        let mut instances = DB_INSTANCES.lock();
        if let Some(db) = instances.get(&names_dir) {
            Ok::<Arc<DB>, OxenError>(db.clone())
        } else {
            // Ensure directory exists
            if !names_dir.exists() {
                util::fs::create_dir_all(&names_dir).map_err(|e| {
                    let msg = format!("Failed to create workspace names directory: {e}");
                    log::error!("{msg}");
                    OxenError::basic_str(msg)
                })?;
            }

            let opts = db::key_val::opts::default();
            let db = DB::open(&opts, dunce::simplified(&names_dir)).map_err(|e| {
                let msg = format!("Failed to open workspace names database: {e}");
                log::error!("{msg}");
                OxenError::basic_str(msg)
            })?;
            let arc_db = Arc::new(db);
            instances.put(names_dir, arc_db.clone());
            Ok(arc_db)
        }
    }?;

    let manager = WorkspaceNameManager { db: names_db };

    operation(&manager)
}

impl WorkspaceNameManager {
    pub fn get_id_for_name(&self, name: &str) -> Result<Option<String>, OxenError> {
        let bytes = name.as_bytes();
        match self.db.get(bytes) {
            Ok(Some(value)) => Ok(Some(String::from(str::from_utf8(&value)?))),
            Ok(None) => Ok(None),
            Err(err) => {
                log::error!("get_id_for_name error finding workspace id for name {name}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn set_name(&self, name: &str, workspace_id: &str) -> Result<(), OxenError> {
        self.db.put(name.as_bytes(), workspace_id.as_bytes())?;
        Ok(())
    }

    pub fn has_name(&self, name: &str) -> bool {
        matches!(self.db.get(name.as_bytes()), Ok(Some(_)))
    }

    pub fn delete_name(&self, name: &str) -> Result<(), OxenError> {
        self.db.delete(name.as_bytes())?;
        Ok(())
    }
}
