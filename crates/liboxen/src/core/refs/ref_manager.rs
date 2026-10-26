//! All RefManager methods are synchronous and acquire `parking_lot::RwLock` guards on
//! `refs_db`. Do not introduce `.await` while holding a guard — `parking_lot` is not
//! re-entrant or await-aware, and a held guard across an await will deadlock the runtime.
//! Per-operation `spawn_blocking` callers are fine because the guard lifetime is bounded
//! by the closure.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;
use rocksdb::{DB, IteratorMode};

use crate::constants::{HEAD_FILE, REFS_DIR};
use crate::core::db;
use crate::core::db::weak_cache::WeakDbCache;
use crate::error::OxenError;
use crate::model::{Branch, Commit, LocalRepository};
use crate::repositories;
use crate::util;
use crate::util::fs::AtomicFile;

// How many repositories keep their refs database open after their last caller drops it. Each
// warm handle holds its database's block reservation, so this bounds what the registry ties up.
const WARM_REFS_DBS: NonZeroUsize = NonZeroUsize::new(64).unwrap();

// Registry of open refs DB handles, keyed by `.oxen/refs` dir. An in-use handle is never
// evicted, so the shared-Arc invariant that compound read-modify-write sequences
// (e.g. `create_branch`'s "check exists, then put") rely on holds unconditionally.
static REFS_DBS: LazyLock<WeakDbCache<RwLock<DB>>> =
    LazyLock::new(|| WeakDbCache::new(WARM_REFS_DBS));

/// Drops this repository's registry entry and its warm handle. A handle a caller still holds
/// stays open and closes on its last drop.
pub fn remove_from_cache(repository_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let refs_dir = util::fs::oxen_hidden_dir(repository_path).join(REFS_DIR);
    REFS_DBS.forget(&refs_dir);
    Ok(())
}

/// Drops the registry entries and warm handles under `repository_path`, with the same effect on
/// open handles as [`remove_from_cache`].
pub fn remove_from_cache_with_children(repository_path: impl AsRef<Path>) -> Result<(), OxenError> {
    REFS_DBS.forget_prefix(repository_path.as_ref());
    Ok(())
}

pub struct RefManager {
    refs_db: Arc<RwLock<DB>>,
    head_file: PathBuf,
    repository: LocalRepository,
}

/// Runs `operation` against a [`RefManager`] for `repository`.
///
/// Every concurrent caller for the same repo receives the same shared `Arc<RwLock<DB>>` for as
/// long as at least one `with_ref_manager` scope stays alive. A caller whose repo is mid-open
/// waits for that open; callers for other repos do not.
pub fn with_ref_manager<F, T>(repository: &LocalRepository, operation: F) -> Result<T, OxenError>
where
    F: FnOnce(&RefManager) -> Result<T, OxenError>,
{
    let hidden = util::fs::oxen_hidden_dir(&repository.path);
    let refs_dir = hidden.join(REFS_DIR);
    let refs_db = open_refs_db(&refs_dir)?;
    let manager = RefManager {
        refs_db,
        head_file: hidden.join(HEAD_FILE),
        repository: repository.clone(),
    };
    operation(&manager)
}

/// Return the shared refs-DB handle for `refs_dir`, opening it when no live handle exists.
/// A cache hit does no filesystem work.
fn open_refs_db(refs_dir: &Path) -> Result<Arc<RwLock<DB>>, OxenError> {
    REFS_DBS.get_or_open(refs_dir, || {
        util::fs::create_dir_all(refs_dir)?;
        let opts = db::key_val::opts::default();
        db::open_with_lock_retry(|| DB::open(&opts, dunce::simplified(refs_dir)))
            .map(RwLock::new)
            .map_err(|err| refs_db_open_failed(refs_dir, err))
    })
}

fn refs_db_open_failed(refs_dir: &Path, source: rocksdb::Error) -> OxenError {
    log::error!("Failed to open refs database at {refs_dir:?}: {source}");
    OxenError::RefsDbOpenFailed {
        path: refs_dir.to_path_buf(),
        source,
    }
}

impl RefManager {
    // Read operations (from RefReader)

    pub fn has_branch(&self, name: &str) -> bool {
        let db = self.refs_db.read();
        matches!(db.get(name.as_bytes()), Ok(Some(_)))
    }

    /// Whether the repo has no branches, reading only the first ref.
    pub fn is_empty(&self) -> Result<bool, OxenError> {
        let db = self.refs_db.read();
        Ok(db
            .iterator(IteratorMode::Start)
            .next()
            .transpose()?
            .is_none())
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
        // Releasing first avoids re-entrant refs-DB acquisition (parking_lot RwLock is not
        // re-entrant), and looking up by the snapshotted commit_id (not the branch name) keeps
        // each returned pair self-consistent: a concurrent writer can advance the branch after
        // we drop the lock, but `get_by_id` with the snapshot value returns the commit that
        // matches the snapshot branch.
        let branches = self.list_branches()?;
        let mut out = Vec::with_capacity(branches.len());
        for branch in branches {
            let commit = repositories::commits::get_commit_or_head(
                &self.repository,
                Some(branch.commit_id.clone()),
            )?;
            out.push((branch, commit));
        }
        Ok(out)
    }

    // Write operations (from RefWriter)

    pub fn set_head(&self, name: &str) -> Result<(), OxenError> {
        AtomicFile::new(&self.head_file).write(name.as_bytes())
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
    use std::thread;

    #[tokio::test]
    async fn test_branch_refs() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            with_ref_manager(&repo, |manager| {
                assert_eq!(
                    manager.read_head_ref()?.as_deref(),
                    Some(DEFAULT_BRANCH_NAME)
                );
                assert_eq!(
                    manager.list_branches()?.len(),
                    1,
                    "a repo starts with one branch"
                );

                manager.create_branch("my-branch", uuid::Uuid::new_v4().to_string())?;
                assert_eq!(manager.list_branches()?.len(), 2);
                manager.create_branch("name_1", "1")?;
                manager.create_branch("name_2", "2")?;
                manager.create_branch("name_3", "3")?;
                assert_eq!(manager.list_branches()?.len(), 5);

                assert!(manager.create_branch("my-branch", "2").is_err());
                assert_eq!(
                    manager.list_branches()?.len(),
                    5,
                    "a refused create adds nothing"
                );
                assert!(matches!(
                    manager.create_branch("my name", "1234"),
                    Err(OxenError::InvalidBranchName(_))
                ));

                manager.create_branch("delete-me", "1234")?;
                let count = manager.list_branches()?.len();
                manager.delete_branch("delete-me")?;
                assert_eq!(manager.list_branches()?.len(), count - 1);

                let og_name = "rename-me";
                manager.create_branch(og_name, "1234")?;
                let count = manager.list_branches()?.len();
                assert!(manager.rename_branch(og_name, "invalid~name").is_err());
                assert!(manager.rename_branch(og_name, og_name).is_err());
                manager.rename_branch(og_name, "new-name")?;
                let branches = manager.list_branches()?;
                assert_eq!(branches.len(), count);
                assert!(branches.iter().any(|b| b.name == "new-name"));

                manager.create_branch("feature", "c1")?;
                manager.compare_and_swap_branch_commit_id("feature", Some("c1"), "c2")?;
                assert_eq!(
                    manager.get_commit_id_for_branch("feature")?.as_deref(),
                    Some("c2")
                );
                match manager.compare_and_swap_branch_commit_id("feature", Some("wrong"), "c3") {
                    Err(OxenError::BranchHeadMismatch {
                        branch,
                        expected,
                        actual,
                    }) => {
                        assert_eq!(branch, "feature");
                        assert_eq!(expected.as_deref(), Some("wrong"));
                        assert_eq!(actual.as_deref(), Some("c2"));
                    }
                    other => panic!("expected BranchHeadMismatch, got {other:?}"),
                }
                assert_eq!(
                    manager.get_commit_id_for_branch("feature")?.as_deref(),
                    Some("c2"),
                    "a mismatched swap leaves the value unchanged"
                );

                // expected = None means "the branch must be absent" — succeeds and creates it.
                manager.compare_and_swap_branch_commit_id("new-branch", None, "c1")?;
                assert_eq!(
                    manager.get_commit_id_for_branch("new-branch")?.as_deref(),
                    Some("c1"),
                );
                // Once present, a second swap with expected = None must fail with the actual.
                match manager.compare_and_swap_branch_commit_id("new-branch", None, "c2") {
                    Err(OxenError::BranchHeadMismatch {
                        expected, actual, ..
                    }) => {
                        assert_eq!(expected, None);
                        assert_eq!(actual.as_deref(), Some("c1"));
                    }
                    other => panic!("expected BranchHeadMismatch, got {other:?}"),
                }
                Ok(())
            })?;

            repositories::branches::create_from_head(&repo, "feature/add-something")?;
            repositories::branches::create_from_head(&repo, "bug/something-is-broken")?;
            with_ref_manager(&repo, |manager| {
                let branches = manager.list_branches()?;
                assert!(branches.iter().any(|b| b.name == "feature/add-something"));
                assert!(branches.iter().any(|b| b.name == "bug/something-is-broken"));
                assert!(branches.iter().any(|b| b.name == DEFAULT_BRANCH_NAME));
                Ok(())
            })?;

            // Threads each create their own branch and read all branches.
            let count = with_ref_manager(&repo, |manager| manager.list_branches())?.len();
            let handles: Vec<_> = (0..5)
                .map(|i| {
                    let repo = repo.clone();
                    thread::spawn(move || {
                        with_ref_manager(&repo, |manager| {
                            manager.create_branch(format!("branch-{i}"), format!("commit-{i}"))?;
                            manager.list_branches()
                        })
                    })
                })
                .collect();
            for handle in handles {
                handle.join().expect("thread panicked")?;
            }
            let branches = with_ref_manager(&repo, |manager| manager.list_branches())?;
            assert_eq!(branches.len(), count + 5);

            // 10 threads race to create the same branch with different commit ids. Under the
            // refs RwLock, exactly one must succeed; the rest must get BranchAlreadyExists. The
            // pre-RwLock behavior had a check-then-act race where multiple creates could each
            // see has_branch==false and silently overwrite each other.
            let handles: Vec<_> = (0..10)
                .map(|i| {
                    let repo = repo.clone();
                    thread::spawn(move || -> Result<bool, OxenError> {
                        with_ref_manager(&repo, |manager| {
                            match manager.create_branch("contested", format!("commit-{i}")) {
                                Ok(_) => Ok(true),
                                Err(OxenError::BranchAlreadyExists(_)) => Ok(false),
                                Err(other) => Err(other),
                            }
                        })
                    })
                })
                .collect();
            let outcomes = handles
                .into_iter()
                .map(|h| h.join().expect("thread panicked"))
                .collect::<Result<Vec<_>, _>>()?;
            let winners = outcomes.iter().filter(|&&won| won).count();
            assert_eq!(
                winners, 1,
                "expected exactly one create to win, got {winners}"
            );
            assert!(with_ref_manager(&repo, |manager| Ok(
                manager.has_branch("contested")
            ))?);

            // Branches src-a and src-b race to rename to "dest".
            with_ref_manager(&repo, |manager| {
                manager.create_branch("src-a", "commit-a")?;
                manager.create_branch("src-b", "commit-b")
            })?;
            let rename_to_dest = |src: &'static str| {
                let repo = repo.clone();
                thread::spawn(move || -> Result<bool, OxenError> {
                    with_ref_manager(&repo, |manager| match manager.rename_branch(src, "dest") {
                        Ok(_) => Ok(true),
                        Err(OxenError::BranchAlreadyExists(_)) => Ok(false),
                        Err(other) => Err(other),
                    })
                })
            };
            let (h_a, h_b) = (rename_to_dest("src-a"), rename_to_dest("src-b"));
            let won_a = h_a.join().expect("thread a panicked")?;
            let won_b = h_b.join().expect("thread b panicked")?;
            assert_ne!(won_a, won_b, "exactly one rename must win");
            let branches = with_ref_manager(&repo, |manager| manager.list_branches())?;
            let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
            assert!(names.contains(&"dest"), "dest must exist after one win");
            // Whichever source lost retains its original name; the winner's source is gone.
            let (winning_src, losing_src) = if won_a {
                ("src-a", "src-b")
            } else {
                ("src-b", "src-a")
            };
            assert!(names.contains(&losing_src));
            assert!(!names.contains(&winning_src));

            // 10 threads race on set_branch_commit_id. Every write must serialize under the RwLock
            // and the final value must equal one of the values we wrote (no torn writes, no
            // panics). Last-writer-wins is allowed, so this asserts safety only, not no-loss.
            with_ref_manager(&repo, |manager| manager.create_branch("hot", "initial"))?;
            let handles: Vec<_> = (0..10)
                .map(|i| {
                    let repo = repo.clone();
                    thread::spawn(move || {
                        with_ref_manager(&repo, |manager| {
                            manager.set_branch_commit_id("hot", format!("commit-{i}"))
                        })
                    })
                })
                .collect();
            for handle in handles {
                handle.join().expect("thread panicked")?;
            }
            let final_value =
                with_ref_manager(&repo, |manager| manager.get_commit_id_for_branch("hot"))?
                    .expect("branch must exist");
            let valid: Vec<String> = (0..10).map(|i| format!("commit-{i}")).collect();
            assert!(
                valid.contains(&final_value),
                "final value {final_value} is not one of the writes"
            );

            // Moves HEAD onto a commit id that does not exist, so it runs last.
            with_ref_manager(&repo, |manager| {
                let branch_name = "experiment/cat-dog";
                let commit_id = uuid::Uuid::new_v4().to_string();
                manager.create_branch(branch_name, &commit_id)?;
                manager.set_head(branch_name)?;
                assert_eq!(manager.head_commit_id()?, Some(commit_id));
                Ok(())
            })
        })
        .await
    }
}
