//! All RefManager methods are synchronous and acquire `parking_lot::RwLock` guards on
//! `refs_db`. Do not introduce `.await` while holding a guard — `parking_lot` is not
//! re-entrant or await-aware, and a held guard across an await will deadlock the runtime.
//! Per-operation `spawn_blocking` callers are fine because the guard lifetime is bounded
//! by the closure.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use rocksdb::{DB, IteratorMode};

use crate::constants::{HEAD_FILE, REFS_DIR};
use crate::core::db;
use crate::error::OxenError;
use crate::model::{Branch, Commit, LocalRepository};
use crate::repositories;
use crate::util;

const DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DB instances with LRU eviction. Each entry wraps the DB in a
// `RwLock` so compound read-modify-write sequences (e.g. `create_branch`'s
// "check exists, then put") serialize under exclusive access.
static DB_INSTANCES: LazyLock<Mutex<LruCache<PathBuf, Arc<RwLock<DB>>>>> =
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
    refs_db: Arc<RwLock<DB>>,
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
            Ok::<Arc<RwLock<DB>>, OxenError>(db.clone())
        } else {
            if !refs_dir.exists() {
                util::fs::create_dir_all(&refs_dir)?;
            }

            let opts = db::key_val::opts::default();
            let db = DB::open(&opts, dunce::simplified(&refs_dir)).map_err(|source| {
                log::error!("Failed to open refs database at {refs_dir:?}: {source}");
                OxenError::RefsDbOpenFailed {
                    path: refs_dir.clone(),
                    source,
                }
            })?;
            let arc_db = Arc::new(RwLock::new(db));
            instances.put(refs_dir, arc_db.clone());
            Ok(arc_db)
        }
    }?;

    let manager = RefManager {
        refs_db,
        head_file: util::fs::oxen_hidden_dir(&repository.path).join(HEAD_FILE),
        repository: repository.clone(),
    };

    operation(&manager)
}

impl RefManager {
    // Read operations (from RefReader)

    pub fn has_branch(&self, name: &str) -> bool {
        let db = self.refs_db.read();
        matches!(db.get(name.as_bytes()), Ok(Some(_)))
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
        let db = self.refs_db.read();
        match db.get(name.as_bytes())? {
            Some(value) => Ok(Some(String::from(str::from_utf8(&value)?))),
            None => Ok(None),
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
        let db = self.refs_db.read();
        let mut branches = vec![];
        for item in db.iterator(IteratorMode::Start) {
            let (key, value) = item?;
            branches.push(Branch {
                name: str::from_utf8(&key)?.to_string(),
                commit_id: str::from_utf8(&value)?.to_string(),
            });
        }
        Ok(branches)
    }

    pub fn list_branches_with_commits(&self) -> Result<Vec<(Branch, Commit)>, OxenError> {
        let maybe_head_ref = self.read_head_ref()?;
        if maybe_head_ref.is_none() {
            return Ok(vec![]);
        }
        // Snapshot the branches under the read lock, then look up commits without holding it.
        // `get_commit_or_head` may itself need the refs DB; releasing the lock first avoids
        // taking it twice in nested scope (parking_lot RwLock is not re-entrant).
        let branches = self.list_branches()?;
        let mut out = Vec::with_capacity(branches.len());
        for branch in branches {
            let commit = repositories::commits::get_commit_or_head(
                &self.repository,
                Some(branch.name.clone()),
            )?;
            out.push((branch, commit));
        }
        Ok(out)
    }

    // Write operations (from RefWriter)

    pub fn set_head(&self, name: &str) -> Result<(), OxenError> {
        util::fs::atomic_write_to_path(&self.head_file, name.as_bytes())
    }

    pub fn create_branch(
        &self,
        name: impl AsRef<str>,
        commit_id: impl AsRef<str>,
    ) -> Result<Branch, OxenError> {
        let name = name.as_ref();
        let commit_id = commit_id.as_ref();

        if self.is_invalid_branch_name(name) {
            return Err(OxenError::InvalidBranchName(name.to_string()));
        }

        let db = self.refs_db.write();
        if db.get(name.as_bytes())?.is_some() {
            return Err(OxenError::BranchAlreadyExists(name.to_string()));
        }
        db.put(name.as_bytes(), commit_id.as_bytes())?;
        Ok(Branch {
            name: name.to_string(),
            commit_id: commit_id.to_string(),
        })
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
        if self.is_invalid_branch_name(new_name) {
            return Err(OxenError::InvalidBranchName(new_name.to_string()));
        }
        let db = self.refs_db.write();
        if db.get(new_name.as_bytes())?.is_some() {
            return Err(OxenError::BranchAlreadyExists(new_name.to_string()));
        }
        let Some(old_id) = db.get(old_name.as_bytes())? else {
            return Err(OxenError::local_branch_not_found(old_name));
        };
        db.delete(old_name.as_bytes())?;
        db.put(new_name.as_bytes(), &old_id)?;
        Ok(())
    }

    pub fn delete_branch(&self, name: &str) -> Result<Branch, OxenError> {
        let db = self.refs_db.write();
        let Some(value) = db.get(name.as_bytes())? else {
            return Err(OxenError::local_branch_not_found(name));
        };
        let commit_id = str::from_utf8(&value)?.to_string();
        db.delete(name.as_bytes())?;
        Ok(Branch {
            name: name.to_string(),
            commit_id,
        })
    }

    pub fn set_branch_commit_id(
        &self,
        name: impl AsRef<str>,
        commit_id: impl AsRef<str>,
    ) -> Result<(), OxenError> {
        let db = self.refs_db.write();
        db.put(name.as_ref().as_bytes(), commit_id.as_ref().as_bytes())?;
        Ok(())
    }

    /// Atomically replace the commit id for `branch` only if its current value matches
    /// `expected_old`. Pass `expected_old = None` to require the branch be absent.
    /// Returns [`OxenError::BranchHeadMismatch`] when the observed value diverges from
    /// `expected_old` (in which case the DB is left unchanged).
    pub fn compare_and_swap_branch_commit_id(
        &self,
        branch: &str,
        expected_old: Option<&str>,
        new_commit_id: &str,
    ) -> Result<(), OxenError> {
        let db = self.refs_db.write();
        let actual_bytes = db.get(branch.as_bytes())?;
        let actual = match &actual_bytes {
            Some(bytes) => Some(str::from_utf8(bytes)?),
            None => None,
        };
        if actual != expected_old {
            return Err(OxenError::BranchHeadMismatch {
                branch: branch.to_string(),
                expected: expected_old.map(str::to_string),
                actual: actual.map(str::to_string),
            });
        }
        db.put(branch.as_bytes(), new_commit_id.as_bytes())?;
        Ok(())
    }

    pub fn set_head_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        let head_val = self.read_head_ref()?; // could be branch name or commit ID
        if let Some(head_val) = head_val {
            if self.has_branch(&head_val) {
                self.set_head_branch_commit_id(commit_id)?;
            } else {
                self.set_head(commit_id)?;
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
                        manager.create_branch(format!("branch-{i}"), format!("commit-{i}"))?;
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
        })
        .await
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
        })
        .await
    }

    #[tokio::test]
    async fn test_default_head() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                assert_eq!(manager.read_head_ref()?.unwrap(), DEFAULT_BRANCH_NAME);
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn test_create_branch_set_head() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                let branch_name = "experiment/cat-dog";
                let commit_id = format!("{}", uuid::Uuid::new_v4());
                manager.create_branch(branch_name, &commit_id)?;
                manager.set_head(branch_name)?;
                assert_eq!(manager.head_commit_id()?, Some(commit_id));
                Ok(())
            })
        })
        .await
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
        })
        .await
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
        })
        .await
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
        })
        .await
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
        })
        .await
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
        })
        .await
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
        })
        .await
    }

    #[tokio::test]
    async fn test_invalid_branch_names() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                let result = manager.create_branch("my name", "1234");
                assert!(matches!(result, Err(OxenError::InvalidBranchName(_))));
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn test_concurrent_create_branch_same_name_exactly_one_wins() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // 10 threads race to create the same branch with different commit ids. Under the
            // refs RwLock, exactly one must succeed; the rest must get BranchAlreadyExists. The
            // pre-RwLock behavior had a check-then-act race where multiple creates could each
            // see has_branch==false and silently overwrite each other.
            let mut handles = vec![];
            for i in 0..10 {
                let repo_clone = repo.clone();
                let handle = thread::spawn(move || -> Result<bool, OxenError> {
                    with_ref_manager(&repo_clone, |manager| {
                        match manager.create_branch("contested", format!("commit-{i}")) {
                            Ok(_) => Ok(true),
                            Err(OxenError::BranchAlreadyExists(_)) => Ok(false),
                            Err(other) => Err(other),
                        }
                    })
                });
                handles.push(handle);
            }

            let outcomes: Vec<bool> = handles
                .into_iter()
                .map(|h| h.join().expect("thread panicked"))
                .collect::<Result<Vec<_>, _>>()?;
            let winners = outcomes.iter().filter(|&&won| won).count();
            assert_eq!(
                winners, 1,
                "expected exactly one create to win, got {winners}"
            );

            with_ref_manager(&repo, |manager| {
                assert!(manager.has_branch("contested"));
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn test_concurrent_rename_collisions() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Set up: branches src-a and src-b, both racing to rename to "dest".
            with_ref_manager(&repo, |manager| {
                manager.create_branch("src-a", "commit-a")?;
                manager.create_branch("src-b", "commit-b")?;
                Ok(())
            })?;

            let repo_a = repo.clone();
            let h_a = thread::spawn(move || -> Result<bool, OxenError> {
                with_ref_manager(&repo_a, |manager| {
                    match manager.rename_branch("src-a", "dest") {
                        Ok(_) => Ok(true),
                        Err(OxenError::BranchAlreadyExists(_)) => Ok(false),
                        Err(other) => Err(other),
                    }
                })
            });
            let repo_b = repo.clone();
            let h_b = thread::spawn(move || -> Result<bool, OxenError> {
                with_ref_manager(&repo_b, |manager| {
                    match manager.rename_branch("src-b", "dest") {
                        Ok(_) => Ok(true),
                        Err(OxenError::BranchAlreadyExists(_)) => Ok(false),
                        Err(other) => Err(other),
                    }
                })
            });

            let won_a = h_a.join().expect("thread a panicked")?;
            let won_b = h_b.join().expect("thread b panicked")?;
            assert_ne!(won_a, won_b, "exactly one rename must win");

            with_ref_manager(&repo, |manager| {
                let branches = manager.list_branches()?;
                let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
                assert!(names.contains(&"dest"), "dest must exist after one win");
                // Whichever source lost retains its original name; the winner's source is gone.
                let losing_src = if won_a { "src-b" } else { "src-a" };
                let winning_src = if won_a { "src-a" } else { "src-b" };
                assert!(names.contains(&losing_src));
                assert!(!names.contains(&winning_src));
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_and_swap_branch_commit_id_happy_path() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                manager.create_branch("feature", "c1")?;
                manager.compare_and_swap_branch_commit_id("feature", Some("c1"), "c2")?;
                assert_eq!(
                    manager.get_commit_id_for_branch("feature")?.as_deref(),
                    Some("c2")
                );
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_and_swap_branch_commit_id_mismatch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                manager.create_branch("feature", "c1")?;
                let result =
                    manager.compare_and_swap_branch_commit_id("feature", Some("wrong"), "c2");
                match result {
                    Err(OxenError::BranchHeadMismatch {
                        branch,
                        expected,
                        actual,
                    }) => {
                        assert_eq!(branch, "feature");
                        assert_eq!(expected.as_deref(), Some("wrong"));
                        assert_eq!(actual.as_deref(), Some("c1"));
                    }
                    other => panic!("expected BranchHeadMismatch, got {other:?}"),
                }
                // Value must be unchanged.
                assert_eq!(
                    manager.get_commit_id_for_branch("feature")?.as_deref(),
                    Some("c1")
                );
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_and_swap_branch_commit_id_absent_expected() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                // expected = None means "the branch must be absent" — succeeds and creates it.
                manager.compare_and_swap_branch_commit_id("new-branch", None, "c1")?;
                assert_eq!(
                    manager.get_commit_id_for_branch("new-branch")?.as_deref(),
                    Some("c1"),
                );

                // Once present, a second swap with expected = None must fail with the actual.
                let result = manager.compare_and_swap_branch_commit_id("new-branch", None, "c2");
                match result {
                    Err(OxenError::BranchHeadMismatch {
                        expected, actual, ..
                    }) => {
                        assert_eq!(expected, None);
                        assert_eq!(actual.as_deref(), Some("c1"));
                    }
                    other => panic!("expected BranchHeadMismatch, got {other:?}"),
                }
                Ok(())
            })
        })
        .await
    }

    #[tokio::test]
    async fn test_concurrent_set_branch_commit_id_serializes() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // 10 threads racing on set_branch_commit_id — every write must serialize under
            // the RwLock and the final value must equal one of the values we wrote (no torn
            // writes, no panics). Before Phase 2 wires CAS in, last-writer-wins is allowed;
            // this test asserts safety only, not no-loss.
            with_ref_manager(&repo, |manager| manager.create_branch("hot", "initial"))?;

            let mut handles = vec![];
            for i in 0..10 {
                let repo_clone = repo.clone();
                let handle = thread::spawn(move || -> Result<(), OxenError> {
                    with_ref_manager(&repo_clone, |manager| {
                        manager.set_branch_commit_id("hot", format!("commit-{i}"))
                    })
                });
                handles.push(handle);
            }
            for h in handles {
                h.join().expect("thread panicked")?;
            }

            with_ref_manager(&repo, |manager| {
                let final_value = manager
                    .get_commit_id_for_branch("hot")?
                    .expect("branch must exist");
                let valid: Vec<String> = (0..10).map(|i| format!("commit-{i}")).collect();
                assert!(
                    valid.contains(&final_value),
                    "final value {final_value} is not one of the writes"
                );
                Ok(())
            })
        })
        .await
    }
}
