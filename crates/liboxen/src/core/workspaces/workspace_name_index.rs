use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::{self, Utf8Error};
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use rocksdb::{DB, IteratorMode};

use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACE_NAME_INDEX_DIR, WORKSPACES_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::workspace::WorkspaceConfig;
use crate::util;

#[cfg(not(any(test, feature = "test-utils")))]
const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
// Larger under test: sibling-test load can evict this LRU's entry between two
// `get_index` calls in a concurrency test, handing out a fresh `Arc<RwLock<DB>>`
// that breaks `put_if_absent`'s shared-writer atomicity.
#[cfg(any(test, feature = "test-utils"))]
const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(300).unwrap();

// Static cache of DB instances with LRU eviction. The inner `RwLock<DB>` lets
// compound read-modify-write sequences (e.g. `put_if_absent`) run under exclusive
// access. Never hold the guard across `.await`.
static DB_INSTANCES: LazyLock<Mutex<LruCache<PathBuf, Arc<RwLock<DB>>>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(DB_CACHE_SIZE)));

#[derive(Debug, thiserror::Error)]
pub enum WsError {
    #[error("Error looking up workspace name '{0}': {1}")]
    LookupErr(String, rocksdb::Error),

    #[error("Key {0} mapped to non-UTF-8 value. Error: {1}")]
    NonUtf8Key(String, Utf8Error),

    #[error("Failed to create workspace name index directory: {0}")]
    CreateDirErr(Box<OxenError>),

    #[error("Failed to open workspace name index database: {0}")]
    OpenError(rocksdb::Error),

    #[error("Failed to put key {0} in workspace name index. Error: {1}")]
    PutError(String, rocksdb::Error),

    #[error("Failed to delete key {0} from workspace name index. Error: {1}")]
    DeleteError(String, rocksdb::Error),

    #[error("Error iterating workspace name index: {0}")]
    IterationError(rocksdb::Error),

    #[error("Error listing workspace directories: {0}")]
    ListWsErr(Box<OxenError>),
}

pub fn index_dir(repo: &LocalRepository) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(WORKSPACE_NAME_INDEX_DIR)
}

/// Check if the workspace name index DB exists for this repo.
pub fn index_exists(repo: &LocalRepository) -> bool {
    index_dir(repo).exists()
}

/// Removes this repository's workspace name index DB from the cache.
pub fn remove_from_cache(repo: &LocalRepository) {
    let dir = index_dir(repo);
    let mut instances = DB_INSTANCES.lock();
    let _ = instances.pop(&dir); // drop immediately
}

/// Removes from cache including children paths (useful for test cleanup).
pub fn remove_from_cache_with_children(repository_path: &Path) {
    let mut instances = DB_INSTANCES.lock();

    let dbs_to_remove = instances
        .iter()
        .filter_map(|(key, _)| {
            if key.starts_with(repository_path) {
                Some(key.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    for db_path in dbs_to_remove {
        let _ = instances.pop(&db_path); // drop immediately
    }
}

pub struct WorkspaceNameIndex {
    db: Arc<RwLock<DB>>,
}

/// Returns a [`WorkspaceNameIndex`] handle for the given repository.
///
/// The handle holds a reference-counted DB instance cached in a global LRU cache.
/// Drop it when you're done to avoid holding the DB open longer than necessary.
pub fn get_index(repo: &LocalRepository) -> Result<WorkspaceNameIndex, WsError> {
    let dir = index_dir(repo);

    let mut instances = DB_INSTANCES.lock();
    if let Some(db) = instances.get(&dir) {
        return Ok(WorkspaceNameIndex { db: db.clone() });
    }

    if !dir.exists() {
        util::fs::create_dir_all(&dir).map_err(|e| WsError::CreateDirErr(Box::new(e)))?;
    }

    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(&dir)).map_err(WsError::OpenError)?;
    let arc_db = Arc::new(RwLock::new(db));
    instances.put(dir, arc_db.clone());
    Ok(WorkspaceNameIndex { db: arc_db })
}

impl WorkspaceNameIndex {
    /// Get workspace ID by name. O(1).
    pub fn get_id_by_name(&self, name: &str) -> Result<Option<String>, WsError> {
        let db = self.db.read();
        match db.get(name.as_bytes()) {
            Ok(Some(value)) => Ok(Some(String::from(
                str::from_utf8(&value).map_err(|e| WsError::NonUtf8Key(name.to_string(), e))?,
            ))),
            Ok(None) => Ok(None),
            Err(err) => Err(WsError::LookupErr(name.to_string(), err)),
        }
    }

    /// Check if a name exists in the index. O(1).
    pub fn has_name(&self, name: &str) -> Result<bool, OxenError> {
        let db = self.db.read();
        match db.get_pinned(name.as_bytes()) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(err) => Err(OxenError::basic_str(format!(
                "Error checking workspace name index for '{name}': {err}"
            ))),
        }
    }

    /// Insert a name -> workspace_id mapping.
    pub fn put(&self, name: &str, workspace_id: &str) -> Result<(), WsError> {
        let db = self.db.write();
        Self::put_locked(&db, name, workspace_id)
    }

    /// Insert the (name, workspace_id) mapping if `name` is not already present.
    /// Returns `Ok(None)` on success, or `Ok(Some(existing_id))` if `name` was
    /// already taken. The get-then-put runs under a single write lock so two
    /// concurrent callers cannot both observe an absent name and both insert.
    pub fn put_if_absent(&self, name: &str, workspace_id: &str) -> Result<Option<String>, WsError> {
        let db = self.db.write();
        if let Some(existing) = db
            .get(name.as_bytes())
            .map_err(|e| WsError::LookupErr(name.to_string(), e))?
        {
            let existing_id = str::from_utf8(&existing)
                .map_err(|e| WsError::NonUtf8Key(name.to_string(), e))?
                .to_string();
            return Ok(Some(existing_id));
        }
        Self::put_locked(&db, name, workspace_id)?;
        Ok(None)
    }

    /// Remove a name from the index.
    pub fn delete(&self, name: &str) -> Result<(), WsError> {
        let db = self.db.write();
        db.delete(name.as_bytes())
            .map_err(|e| WsError::DeleteError(name.to_string(), e))?;
        Ok(())
    }

    /// Remove all entries from the index.
    pub fn clear(&self) -> Result<(), WsError> {
        let db = self.db.write();
        Self::clear_locked(&db)
    }

    /// List all (name, workspace_id) entries for debugging/testing.
    #[cfg(test)]
    pub fn list(&self) -> Result<Vec<(String, String)>, WsError> {
        let db = self.db.read();
        let iter = db.iterator(IteratorMode::Start);
        let mut results = Vec::new();
        for item in iter {
            match item {
                Ok((key, value)) => match (str::from_utf8(&key), str::from_utf8(&value)) {
                    (Ok(name), Ok(id)) => {
                        results.push((name.to_string(), id.to_string()));
                    }
                    _ => {
                        log::error!(
                            "Could not decode workspace name index entry into valid UTF-8 strings."
                        );
                    }
                },
                Err(err) => {
                    return Err(WsError::IterationError(err));
                }
            }
        }
        Ok(results)
    }

    /// Rebuild the index from existing workspace configs on disk.
    /// Clears all existing entries first, then scans `.oxen/workspaces/` directories.
    pub fn rebuild_from_disk(&self, repo: &LocalRepository) -> Result<(), WsError> {
        let workspaces_dir = repo.path.join(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR);
        let workspace_dirs = if workspaces_dir.exists() {
            util::fs::list_dirs_in_dir(&workspaces_dir)
                .map_err(|e| WsError::ListWsErr(Box::new(e)))?
        } else {
            Vec::new()
        };

        // Hold the write lock across clear + every put so a concurrent reader
        // never sees the index in a half-rebuilt state.
        let db = self.db.write();
        Self::clear_locked(&db)?;

        for workspace_dir in workspace_dirs {
            let config_path = workspace_dir
                .join(OXEN_HIDDEN_DIR)
                .join(crate::constants::WORKSPACE_CONFIG);
            if !config_path.exists() {
                continue;
            }

            let config_contents = match util::fs::read_from_path(&config_path) {
                Ok(contents) => contents,
                Err(e) => {
                    log::warn!(
                        "[Skip workspace in index] Could not read workspace config at {config_path:?}: {e}"
                    );
                    continue;
                }
            };

            let config: WorkspaceConfig = match toml::from_str(&config_contents) {
                Ok(config) => config,
                Err(e) => {
                    log::warn!(
                        "[Skip workspace in index] Could not parse workspace config at {config_path:?}: {e}"
                    );
                    continue;
                }
            };

            if let (Some(name), Some(id)) = (&config.workspace_name, &config.workspace_id) {
                Self::put_locked(&db, name, id)?;
            }
        }

        Ok(())
    }

    fn put_locked(db: &DB, name: &str, workspace_id: &str) -> Result<(), WsError> {
        db.put(name.as_bytes(), workspace_id.as_bytes())
            .map_err(|e| WsError::PutError(name.to_string(), e))
    }

    fn clear_locked(db: &DB) -> Result<(), WsError> {
        let iter = db.iterator(IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, _)) => {
                    db.delete(&key).map_err(|e| {
                        WsError::PutError(String::from_utf8_lossy(&key).to_string(), e)
                    })?;
                }
                Err(err) => {
                    return Err(WsError::IterationError(err));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_workspace_name_index_put_and_get() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            idx.put("my-workspace", "abc-123")?;
            let result = idx.get_id_by_name("my-workspace")?;
            assert_eq!(result, Some("abc-123".to_string()));

            let missing = idx.get_id_by_name("nonexistent")?;
            assert_eq!(missing, None);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_name_index_has_name() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            idx.put("exists", "id-1")?;
            assert!(idx.has_name("exists")?);
            assert!(!idx.has_name("does-not-exist")?);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_name_index_delete() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            idx.put("to-delete", "id-1")?;
            assert!(idx.has_name("to-delete")?);

            idx.delete("to-delete")?;
            assert!(!idx.has_name("to-delete")?);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_name_index_clear() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            idx.put("ws-1", "id-1")?;
            idx.put("ws-2", "id-2")?;
            assert_eq!(idx.list()?.len(), 2);

            idx.clear()?;
            assert_eq!(idx.list()?.len(), 0);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_name_index_list() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            idx.put("alpha", "id-a")?;
            idx.put("beta", "id-b")?;

            let entries = idx.list()?;
            assert_eq!(entries.len(), 2);
            assert!(entries.contains(&("alpha".to_string(), "id-a".to_string())));
            assert!(entries.contains(&("beta".to_string(), "id-b".to_string())));
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_put_if_absent_inserts_on_first_call() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            let result = idx.put_if_absent("alpha", "id-a")?;
            assert_eq!(result, None);
            assert_eq!(idx.get_id_by_name("alpha")?, Some("id-a".to_string()));
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_put_if_absent_returns_existing_on_collision() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            assert_eq!(idx.put_if_absent("alpha", "id-a")?, None);

            // Second insert for the same name must report the existing id and
            // must NOT overwrite the stored value.
            let result = idx.put_if_absent("alpha", "id-b")?;
            assert_eq!(result, Some("id-a".to_string()));
            assert_eq!(idx.get_id_by_name("alpha")?, Some("id-a".to_string()));
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_concurrent_put_if_absent_exactly_one_winner() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = std::sync::Arc::new(get_index(&repo)?);

            let mut handles = Vec::new();
            for i in 0..10 {
                let idx = idx.clone();
                let id = format!("id-{i}");
                handles.push(tokio::task::spawn_blocking(move || {
                    idx.put_if_absent("shared-name", &id)
                        .map(|existing| (id, existing))
                }));
            }

            let mut winner_count = 0;
            let mut winner_id = None;
            for handle in handles {
                let (id, existing) = handle.await.expect("task panic")?;
                if existing.is_none() {
                    winner_count += 1;
                    winner_id = Some(id);
                }
            }

            assert_eq!(winner_count, 1, "exactly one put_if_absent caller wins");
            let stored = idx.get_id_by_name("shared-name")?;
            assert_eq!(stored, winner_id, "the winner's id is the one persisted");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_name_index_rebuild_from_disk() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create a file and commit so we have a valid commit to use
            let test_file = repo.path.join("test.txt");
            util::fs::write_to_path(&test_file, "hello")?;
            repositories::add(&repo, &test_file).await?;
            let commit = repositories::commit(&repo, "init")?;

            // Create some workspaces (one named, one unnamed)
            repositories::workspaces::create_with_name(
                &repo,
                &commit,
                "ws-id-1",
                Some("named-ws".to_string()),
                true,
            )
            .await?;
            repositories::workspaces::create(&repo, &commit, "ws-id-2", true)?;

            // Now rebuild the index from disk in a fresh index
            let idx = get_index(&repo)?;
            idx.rebuild_from_disk(&repo)?;

            // Only the named workspace should be in the index
            let entries = idx.list()?;
            assert_eq!(entries.len(), 1);
            assert_eq!(idx.get_id_by_name("named-ws")?, Some("ws-id-1".to_string()));
            assert_eq!(idx.get_id_by_name("ws-id-2")?, None);
            Ok(())
        })
        .await
    }
}
