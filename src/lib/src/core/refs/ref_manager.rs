use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::Mutex;
use rocksdb::{IteratorMode, DB};

use crate::constants::{HEAD_FILE, REFS_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};
use crate::repositories;
use crate::util;

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DB instances with LRU eviction
static DB_INSTANCES: LazyLock<Mutex<LruCache<PathBuf, Arc<DB>>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(DB_CACHE_SIZE)));

/// Removes a repository's DB instance from the cache.
pub fn remove_from_cache(repository_path: impl AsRef<std::path::Path>) -> Result<(), OxenError> {
    let refs_dir = util::fs::oxen_hidden_dir(repository_path).join(REFS_DIR);
    let mut instances = DB_INSTANCES.lock();
    let _ = instances.pop(&refs_dir); // drop immediately
    Ok(())
}

/// Removes a repository's DB instance and all its subdirectories from the cache.
/// This is mostly useful in test cleanup to ensure all DB instances are removed.
pub fn remove_from_cache_with_children(
    repository_path: impl AsRef<std::path::Path>,
) -> Result<(), OxenError> {
    let mut dbs_to_remove: Vec<PathBuf> = vec![];
    let mut instances = DB_INSTANCES.lock();
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

pub struct RefManager {
    refs_db: Arc<DB>,
    head_file: PathBuf,
    repository: LocalRepository,
}

pub fn with_ref_manager<F, T>(repository: &LocalRepository, operation: F) -> Result<T, OxenError>
where
    F: FnOnce(&RefManager) -> Result<T, OxenError>,
{
    // Get or create the DB instance from cache
    let refs_db = {
        let refs_dir = util::fs::oxen_hidden_dir(&repository.path).join(REFS_DIR);

        let mut instances = DB_INSTANCES.lock();
        if let Some(db) = instances.get(&refs_dir) {
            Ok::<Arc<DB>, OxenError>(db.clone())
        } else {
            // Ensure directory exists
            if !refs_dir.exists() {
                util::fs::create_dir_all(&refs_dir).map_err(|e| {
                    log::error!("Failed to create refs directory: {}", e);
                    OxenError::basic_str(format!("Failed to create refs directory: {}", e))
                })?;
            }

            let opts = db::key_val::opts::default();
            let db = DB::open(&opts, dunce::simplified(&refs_dir)).map_err(|e| {
                log::error!("Failed to open refs database: {}", e);
                OxenError::basic_str(format!("Failed to open refs database: {}", e))
            })?;
            let arc_db = Arc::new(db);
            instances.put(refs_dir, arc_db.clone());
            Ok(arc_db)
        }
    }?;

    let manager = RefManager {
        refs_db,
        head_file: util::fs::oxen_hidden_dir(&repository.path).join(HEAD_FILE),
        repository: repository.clone(),
    };

    // Execute the operation with our RefManager instance
    operation(&manager)
}

impl RefManager {
    // Read operations (from RefReader)

    pub fn has_branch(&self, name: &str) -> bool {
        let bytes = name.as_bytes();
        match self.refs_db.get(bytes) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => false,
        }
    }

    pub fn get_current_branch(&self) -> Result<Option<Branch>, OxenError> {
        let ref_name = self.read_head_ref()?;
        if ref_name.is_none() {
            return Ok(None);
        }

        let ref_name = ref_name.unwrap();
        if let Some(id) = self.get_commit_id_for_branch(&ref_name)? {
            Ok(Some(Branch {
                name: ref_name,
                commit_id: id,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_commit_id_for_branch(&self, name: &str) -> Result<Option<String>, OxenError> {
        let bytes = name.as_bytes();
        match self.refs_db.get(bytes) {
            Ok(Some(value)) => Ok(Some(String::from(str::from_utf8(&value)?))),
            Ok(None) => Ok(None),
            Err(err) => {
                log::error!(
                    "get_commit_id_for_branch error finding commit id for branch {}",
                    name
                );
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn head_commit_id(&self) -> Result<Option<String>, OxenError> {
        let head_ref = self.read_head_ref()?;

        if let Some(head_ref) = head_ref {
            if let Some(commit_id) = self.get_commit_id_for_branch(&head_ref)? {
                Ok(Some(commit_id))
            } else if repositories::commits::commit_id_exists(&self.repository, &head_ref)? {
                Ok(Some(head_ref))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn read_head_ref(&self) -> Result<Option<String>, OxenError> {
        if self.head_file.exists() {
            Ok(Some(util::fs::read_from_path(&self.head_file)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_branches(&self) -> Result<Vec<Branch>, OxenError> {
        let mut branch_names: Vec<Branch> = vec![];
        let maybe_head_ref = self.read_head_ref()?;
        let iter = self.refs_db.iterator(IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => match (str::from_utf8(&key), str::from_utf8(&value)) {
                    (Ok(key_str), Ok(value)) => {
                        if maybe_head_ref.is_some() {
                            let ref_name = String::from(key_str);
                            let id = String::from(value);
                            branch_names.push(Branch {
                                name: ref_name,
                                commit_id: id,
                            });
                        }
                    }
                    _ => {
                        return Err(OxenError::basic_str("Could not read utf8 val..."));
                    }
                },
                Err(err) => {
                    let err = format!("Error reading refs db\nErr: {err}");
                    return Err(OxenError::basic_str(err));
                }
            }
        }
        Ok(branch_names)
    }

    // Write operations (from RefWriter)

    pub fn set_head(&self, name: impl AsRef<str>) {
        let name = name.as_ref();
        util::fs::write_to_path(&self.head_file, name).expect("Could not write to head");
    }

    pub fn create_branch(
        &self,
        name: impl AsRef<str>,
        commit_id: impl AsRef<str>,
    ) -> Result<Branch, OxenError> {
        let name = name.as_ref();
        let commit_id = commit_id.as_ref();

        if self.is_invalid_branch_name(name) {
            let err = format!("'{name}' is not a valid branch name.");
            return Err(OxenError::basic_str(err));
        }

        if self.has_branch(name) {
            let err = format!("Branch already exists: {name}");
            Err(OxenError::basic_str(err))
        } else {
            self.set_branch_commit_id(name, commit_id)?;
            Ok(Branch {
                name: String::from(name),
                commit_id: String::from(commit_id),
            })
        }
    }

    fn is_invalid_branch_name(&self, name: &str) -> bool {
        // https://git-scm.com/docs/git-check-ref-format
        let invalid_substrings = vec!["..", "~", "^", ":", "?", "[", "*", "\\", " ", "@{"];
        for invalid in invalid_substrings {
            if name.contains(invalid) {
                return true;
            }
        }

        if name == "@" {
            return true;
        }

        if name.ends_with('.') {
            return true;
        }

        false
    }

    pub fn rename_branch(&self, old_name: &str, new_name: &str) -> Result<(), OxenError> {
        if !self.has_branch(old_name) {
            Err(OxenError::local_branch_not_found(old_name))
        } else if self.has_branch(new_name) {
            Err(OxenError::basic_str(format!(
                "Branch already exists: {new_name}"
            )))
        } else if self.is_invalid_branch_name(new_name) {
            Err(OxenError::basic_str(format!(
                "'{new_name}' is not a valid branch name."
            )))
        } else {
            let old_id = self.refs_db.get(old_name)?.unwrap();
            self.refs_db.delete(old_name)?;
            self.refs_db.put(new_name, old_id)?;
            Ok(())
        }
    }

    pub fn delete_branch(&self, name: &str) -> Result<Branch, OxenError> {
        let Some(branch) = self.get_branch_by_name(name)? else {
            let err = format!("Branch does not exist: {name}");
            return Err(OxenError::basic_str(err));
        };
        self.refs_db.delete(name)?;
        Ok(branch)
    }

    pub fn set_branch_commit_id(
        &self,
        name: impl AsRef<str>,
        commit_id: impl AsRef<str>,
    ) -> Result<(), OxenError> {
        let name = name.as_ref();
        let commit_id = commit_id.as_ref();
        self.refs_db.put(name, commit_id)?;
        Ok(())
    }

    pub fn set_head_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        let head_val = self.read_head_ref()?; // could be branch name or commit ID
        if let Some(head_val) = head_val {
            if self.has_branch(&head_val) {
                self.set_head_branch_commit_id(commit_id)?;
            } else {
                self.set_head(commit_id);
            }
        }
        Ok(())
    }

    pub fn set_head_branch_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        if let Some(head_ref) = self.read_head_ref()? {
            self.set_branch_commit_id(&head_ref, commit_id)?;
        }
        Ok(())
    }

    pub fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, OxenError> {
        match self.get_commit_id_for_branch(name) {
            Ok(Some(commit_id)) => Ok(Some(Branch {
                name: name.to_string(),
                commit_id: commit_id.to_string(),
            })),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::test;
    use crate::util;
    use std::thread;

    #[tokio::test]
    async fn test_concurrent_access() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Spawn multiple threads to read/write concurrently
            let mut handles = vec![];
            for i in 0..5 {
                let repo_clone = repo.clone();
                let handle = thread::spawn(move || {
                    // Each thread creates its own branch and reads all branches
                    with_ref_manager(&repo_clone, |manager| {
                        manager.create_branch(format!("branch-{}", i), format!("commit-{}", i))?;
                        manager.list_branches()
                    })
                });
                handles.push(handle);
            }

            // Wait for all threads and collect results
            let results: Vec<Result<Vec<Branch>, OxenError>> =
                handles.into_iter().map(|h| h.join().unwrap()).collect();

            // Verify all operations succeeded
            for result in results {
                assert!(result.is_ok());
            }

            // Verify final state
            with_ref_manager(&repo, |manager| {
                let branches = manager.list_branches()?;
                // Should have 6 branches (initial + 5 new ones)
                assert_eq!(branches.len(), 6);
                Ok(())
            })?;

            Ok(())
        }).await
    }

    #[tokio::test]
    async fn test_list_branches() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // add and commit a file
            let new_file = repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&repo, new_file).await?;
            repositories::commit(&repo, "Added a new file")?;

            repositories::branches::create_from_head(&repo, "feature/add-something")?;
            repositories::branches::create_from_head(&repo, "bug/something-is-broken")?;

            // Use with_ref_manager instead of creating RefReader directly
            with_ref_manager(&repo, |manager| {
                let branches = manager.list_branches()?;

                // We started with the main branch, then added two more
                assert_eq!(branches.len(), 3);

                assert!(branches.iter().any(|b| b.name == "feature/add-something"));
                assert!(branches.iter().any(|b| b.name == "bug/something-is-broken"));
                assert!(branches.iter().any(|b| b.name == "main"));

                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_default_head() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                assert_eq!(manager.read_head_ref()?.unwrap(), DEFAULT_BRANCH_NAME);
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_create_branch_set_head() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                let branch_name = "experiment/cat-dog";
                let commit_id = format!("{}", uuid::Uuid::new_v4());
                manager.create_branch(branch_name, &commit_id)?;
                manager.set_head(branch_name);
                assert_eq!(manager.head_commit_id()?, Some(commit_id));
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_list_branches_empty() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                // always start with a default branch
                let branches = manager.list_branches()?;
                assert_eq!(branches.len(), 1);
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_list_branches_one() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                let name = "my-branch";
                let commit_id = format!("{}", uuid::Uuid::new_v4());
                manager.create_branch(name, &commit_id)?;
                let branches = manager.list_branches()?;
                // we always start with "main" branch
                assert_eq!(branches.len(), 2);
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_list_branches_many() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                // we always start with a default branch
                manager.create_branch("name_1", "1")?;
                manager.create_branch("name_2", "2")?;
                manager.create_branch("name_3", "3")?;
                let branches = manager.list_branches()?;
                assert_eq!(branches.len(), 4);
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_create_branch_same_name() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                manager.create_branch("my-fun-name", "1")?;

                assert!(manager.create_branch("my-fun-name", "2").is_err());

                // We should still only have two branches, default one and this one
                let branches = manager.list_branches()?;
                assert_eq!(branches.len(), 2);
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_delete_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                let name = "my-branch-name";
                manager.create_branch(name, "1234")?;
                let og_branches = manager.list_branches()?;
                let og_branch_count = og_branches.len();

                // Delete branch
                manager.delete_branch(name)?;

                // Should have one less branch than after creation
                let branches = manager.list_branches()?;
                assert_eq!(branches.len(), og_branch_count - 1);
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_rename_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                let og_name = "my-branch-name";
                manager.create_branch(og_name, "1234")?;
                let og_branches = manager.list_branches()?;
                let og_branch_count = og_branches.len();

                // try to rename to invalid name
                let result = manager.rename_branch(og_name, "invalid~name");
                assert!(result.is_err());

                // try to rename to existing name
                let result = manager.rename_branch(og_name, "my-branch-name");
                assert!(result.is_err());

                // rename branch
                let new_name = "new-name";
                manager.rename_branch(og_name, new_name)?;

                // Should have same number of branches, and one with the new name
                let branches = manager.list_branches()?;
                assert_eq!(branches.len(), og_branch_count);
                assert!(branches.iter().any(|b| b.name == new_name));
                Ok(())
            })
        }).await
    }

    #[tokio::test]
    async fn test_invalid_branch_names() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                let result = manager.create_branch("my name", "1234");
                assert!(result.is_err());
                Ok(())
            })
        }).await
    }
}
