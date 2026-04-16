use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::Mutex;
use rocksdb::{DB, IteratorMode};

use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACE_NAME_INDEX_DIR, WORKSPACES_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::workspace::WorkspaceConfig;
use crate::util;

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DB instances with LRU eviction
static DB_INSTANCES: LazyLock<Mutex<LruCache<PathBuf, Arc<DB>>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(DB_CACHE_SIZE)));

fn index_dir(repo: &LocalRepository) -> PathBuf {
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
pub fn remove_from_cache(repository_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let dir = util::fs::oxen_hidden_dir(&repository_path)
        .join(WORKSPACES_DIR)
        .join(WORKSPACE_NAME_INDEX_DIR);
    let mut instances = DB_INSTANCES.lock();
    let _ = instances.pop(&dir); // drop immediately
    Ok(())
}

/// Removes from cache including children paths (useful for test cleanup).
pub fn remove_from_cache_with_children(repository_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let mut dbs_to_remove: Vec<PathBuf> = vec![];
    let mut instances = DB_INSTANCES.lock();
    for (key, _) in instances.iter() {
        if key.starts_with(&repository_path) {
            dbs_to_remove.push(key.clone());
        }
    }
    for db_path in dbs_to_remove {
        let _ = instances.pop(&db_path); // drop immediately
    }
    Ok(())
}

pub struct WorkspaceNameIndex {
    db: Arc<DB>,
}

/// Returns a [`WorkspaceNameIndex`] handle for the given repository.
///
/// The handle holds a reference-counted DB instance cached in a global LRU cache.
/// Drop it when you're done to avoid holding the DB open longer than necessary.
pub fn get_index(repo: &LocalRepository) -> Result<WorkspaceNameIndex, OxenError> {
    let dir = index_dir(repo);

    let mut instances = DB_INSTANCES.lock();
    if let Some(db) = instances.get(&dir) {
        return Ok(WorkspaceNameIndex { db: db.clone() });
    }

    if !dir.exists() {
        util::fs::create_dir_all(&dir).map_err(|e| {
            OxenError::basic_str(format!(
                "Failed to create workspace name index directory: {e}"
            ))
        })?;
    }

    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(&dir)).map_err(|e| {
        OxenError::basic_str(format!("Failed to open workspace name index database: {e}"))
    })?;
    let arc_db = Arc::new(db);
    instances.put(dir, arc_db.clone());
    Ok(WorkspaceNameIndex { db: arc_db })
}

impl WorkspaceNameIndex {
    /// Get workspace ID by name. O(1).
    pub fn get_id_by_name(&self, name: &str) -> Result<Option<String>, OxenError> {
        match self.db.get(name.as_bytes()) {
            Ok(Some(value)) => Ok(Some(String::from(str::from_utf8(&value)?))),
            Ok(None) => Ok(None),
            Err(err) => Err(OxenError::basic_str(format!(
                "Error looking up workspace name '{name}': {err}"
            ))),
        }
    }

    /// Check if a name exists in the index. O(1).
    pub fn has_name(&self, name: &str) -> bool {
        match self.db.get_pinned(name.as_bytes()) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(err) => {
                log::error!("Error checking workspace name index for '{name}': {err}");
                false
            }
        }
    }

    /// Insert a name -> workspace_id mapping.
    pub fn put(&self, name: &str, workspace_id: &str) -> Result<(), OxenError> {
        self.db.put(name.as_bytes(), workspace_id.as_bytes())?;
        Ok(())
    }

    /// Remove a name from the index.
    pub fn delete(&self, name: &str) -> Result<(), OxenError> {
        self.db.delete(name.as_bytes())?;
        Ok(())
    }

    /// Remove all entries from the index.
    pub fn clear(&self) -> Result<(), OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, _)) => {
                    self.db.delete(key)?;
                }
                Err(err) => {
                    return Err(OxenError::basic_str(format!(
                        "Error iterating workspace name index: {err}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// List all (name, workspace_id) entries. Primarily for debugging/testing.
    pub fn list(&self) -> Result<Vec<(String, String)>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut results = Vec::new();
        for item in iter {
            match item {
                Ok((key, value)) => match (str::from_utf8(&key), str::from_utf8(&value)) {
                    (Ok(name), Ok(id)) => {
                        results.push((name.to_string(), id.to_string()));
                    }
                    _ => {
                        log::error!("Could not decode workspace name index entry");
                    }
                },
                Err(err) => {
                    return Err(OxenError::basic_str(format!(
                        "Error iterating workspace name index: {err}"
                    )));
                }
            }
        }
        Ok(results)
    }

    /// Rebuild the index from existing workspace configs on disk.
    /// Clears all existing entries first, then scans `.oxen/workspaces/` directories.
    pub fn rebuild_from_disk(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        self.clear()?;

        let workspaces_dir = repo.path.join(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR);
        if !workspaces_dir.exists() {
            return Ok(());
        }

        let workspace_dirs = util::fs::list_dirs_in_dir(&workspaces_dir).map_err(|e| {
            OxenError::basic_str(format!("Error listing workspace directories: {e}"))
        })?;

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
                    log::warn!("Could not read workspace config at {config_path:?}: {e}");
                    continue;
                }
            };

            let config: WorkspaceConfig = match toml::from_str(&config_contents) {
                Ok(config) => config,
                Err(e) => {
                    log::warn!("Could not parse workspace config at {config_path:?}: {e}");
                    continue;
                }
            };

            if let (Some(name), Some(id)) = (&config.workspace_name, &config.workspace_id) {
                self.put(name, id)?;
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
            assert!(idx.has_name("exists"));
            assert!(!idx.has_name("does-not-exist"));
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_name_index_delete() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let idx = get_index(&repo)?;
            idx.put("to-delete", "id-1")?;
            assert!(idx.has_name("to-delete"));

            idx.delete("to-delete")?;
            assert!(!idx.has_name("to-delete"));
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
