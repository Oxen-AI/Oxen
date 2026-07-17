//! # Branches
//!
//! Interact with Oxen branches.
//!

use std::path::{Path, PathBuf};

use crate::core::db::dir_hashes::dir_hashes_db::dir_hash_db_path;
use crate::core::refs::with_ref_manager;
use crate::core::v_latest::branches::OnConflict;
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository};
use crate::repositories;
use crate::{core, util};

/// List all the local branches within a repo
pub async fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let repo = repo.clone();
    tokio::task::spawn_blocking(move || with_ref_manager(&repo, |manager| manager.list_branches()))
        .await?
}

/// List all the local branches within a repo along with their head commits
pub fn list_with_commits(repo: &LocalRepository) -> Result<Vec<(Branch, Commit)>, OxenError> {
    with_ref_manager(repo, |manager| manager.list_branches_with_commits())
}

/// Get a branch by name, returning an error if it doesn't exist
pub fn get_by_name(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    with_ref_manager(repo, |manager| manager.get_branch_by_name(name))?
        .ok_or_else(|| OxenError::local_branch_not_found(name))
}

/// Get commit id from a branch by name
pub fn get_commit_id(repo: &LocalRepository, name: &str) -> Result<Option<String>, OxenError> {
    with_ref_manager(repo, |manager| manager.get_commit_id_for_branch(name))
}

/// Check if a branch exists.
/// Returns Ok(false) if the branch doesn't exist.
/// Returns Err() if there was some other problem accessing the local repository.
pub fn exists(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    match get_by_name(repo, name) {
        Ok(_) => Ok(true),
        Err(OxenError::BranchNotFound(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Get the current branch
pub fn current_branch(repo: &LocalRepository) -> Result<Option<Branch>, OxenError> {
    with_ref_manager(repo, |manager| manager.get_current_branch())
}

/// # Create a new branch from the head commit
/// This creates a new pointer to the current commit with a name,
/// it does not switch you to this branch, you still must call `checkout_branch`
pub fn create_from_head(
    repo: &LocalRepository,
    name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    let head_commit = repositories::commits::head_commit(repo)?;
    with_ref_manager(repo, |manager| manager.create_branch(name, &head_commit.id))
}

/// # Create a local branch from a specific commit id
pub fn create(
    repo: &LocalRepository,
    name: impl AsRef<str>,
    commit_id: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    let commit_id = commit_id.as_ref();

    if repositories::commits::commit_id_exists(repo, commit_id)? {
        with_ref_manager(repo, |manager| manager.create_branch(name, commit_id))
    } else {
        Err(OxenError::commit_id_does_not_exist(commit_id))
    }
}

/// # Create a branch and check it out in one go
/// This creates a branch with name,
/// then switches HEAD to point to the branch
pub fn create_checkout(repo: &LocalRepository, name: impl AsRef<str>) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    let name = util::fs::linux_path_str(name);
    println!("Create and checkout branch: {name}");
    let head_commit = repositories::commits::head_commit(repo)?;

    with_ref_manager(repo, |manager| {
        let branch = manager.create_branch(&name, &head_commit.id)?;
        manager.set_head(&name)?;
        Ok(branch)
    })
}

/// Update the branch name to point to a commit id, creating the branch if it doesn't exist.
/// Validates that the commit exists before updating.
pub fn update(
    repo: &LocalRepository,
    name: impl AsRef<str>,
    commit_id: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    let commit_id = commit_id.as_ref();

    if !repositories::commits::commit_id_exists(repo, commit_id)? {
        return Err(OxenError::commit_id_does_not_exist(commit_id));
    }

    with_ref_manager(repo, |manager| {
        if let Some(mut branch) = manager.get_branch_by_name(name)? {
            // Set the branch to point to the commit
            manager.set_branch_commit_id(name, commit_id)?;
            branch.commit_id = commit_id.to_string();
            Ok(branch)
        } else {
            manager.create_branch(name, commit_id)
        }
    })
}

/// Reject the ref advance unless the server can fully serve `head`: every merkle node and version
/// blob it adds relative to `base` is present (else `ReachableObjectsMissing`), and its
/// directory-hash index — needed to resolve the tree by path — exists (else `DirHashIndexMissing`).
///
/// Server ref-advance paths that move onto a client-supplied commit call this first, so a ref can
/// never point at a commit the server can't serve. Local/CLI branch ops skip it — a local clone is
/// the source of truth.
pub async fn verify_reachable_objects(
    repo: &LocalRepository,
    base: Option<&Commit>,
    head: &Commit,
) -> Result<(), OxenError> {
    let missing = repositories::tree::find_missing_added_objects(repo, base, head).await?;
    if !missing.is_empty() {
        return Err(OxenError::ReachableObjectsMissing {
            missing_nodes: missing.nodes.len(),
            missing_versions: missing.versions.len(),
        });
    }

    // Path-based serving also needs the commit's dir-hashes index — require it even when every
    // referenced object is present.
    if !dir_hash_db_path(repo, head).exists() {
        return Err(OxenError::DirHashIndexMissing {
            commit: head.id.clone(),
        });
    }

    Ok(())
}

/// Verify that advancing a ref to `commit_id` wouldn't point at a commit missing reachable
/// objects, scoping the check to what it adds over `base_branch`'s head. A commit that doesn't
/// exist yet is left for the ref-advance op to reject; a missing base branch checks the whole tree.
pub async fn verify_advance_to(
    repo: &LocalRepository,
    commit_id: &str,
    base_branch: &str,
) -> Result<(), OxenError> {
    let Some(head_commit) = repositories::commits::get_by_id(repo, commit_id)? else {
        return Ok(());
    };
    // Only an absent base branch falls back to a whole-tree check; real lookup errors surface.
    let base_commit = match get_by_name(repo, base_branch) {
        Ok(branch) => repositories::commits::get_by_id(repo, &branch.commit_id)?,
        Err(OxenError::BranchNotFound(_)) => None,
        Err(e) => return Err(e),
    };
    verify_reachable_objects(repo, base_commit.as_ref(), &head_commit).await
}

/// Delete a local branch
pub fn delete(repo: &LocalRepository, name: impl AsRef<str>) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    // Make sure they don't delete the current checked out branch
    if let Ok(Some(branch)) = current_branch(repo)
        && branch.name == name
    {
        let err = format!("Err: Cannot delete current checked out branch '{name}'");
        return Err(OxenError::basic_str(err));
    }

    if branch_has_been_merged(repo, name)? {
        with_ref_manager(repo, |manager| manager.delete_branch(name))
    } else {
        let err = format!(
            "Err: The branch '{name}' is not fully merged.\nIf you are sure you want to delete it, run 'oxen branch -D {name}'."
        );
        Err(OxenError::basic_str(err))
    }
}

/// # Force delete a local branch
/// Caution! Will delete a local branch without checking if it has been merged or pushed.
pub fn force_delete(repo: &LocalRepository, name: impl AsRef<str>) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    if let Ok(Some(branch)) = current_branch(repo)
        && branch.name == name
    {
        let err = format!("Err: Cannot delete current checked out branch '{name}'");
        return Err(OxenError::basic_str(err));
    }

    with_ref_manager(repo, |manager| manager.delete_branch(name))
}

/// Check if a branch is checked out
pub fn is_checked_out(repo: &LocalRepository, name: &str) -> bool {
    if let Ok(Some(current_branch)) = with_ref_manager(repo, |manager| manager.get_current_branch())
    {
        // If we are already on the branch, do nothing
        if current_branch.name == name {
            return true;
        }
    }
    false
}

/// Checkout a branch
pub async fn checkout_branch_from_commit(
    repo: &LocalRepository,
    name: impl AsRef<str>,
    from_commit: &Option<Commit>,
) -> Result<(), OxenError> {
    let name = name.as_ref();
    log::debug!("checkout_branch {name}");
    core::v_latest::branches::checkout(repo, name, from_commit).await
}

/// Checkout a subtree from a commit
pub async fn checkout_subtrees_to_commit(
    repo: &LocalRepository,
    to_commit: &Commit,
    subtree_paths: &[PathBuf],
    depth: i32,
) -> Result<(), OxenError> {
    core::v_latest::branches::checkout_subtrees(repo, to_commit, subtree_paths, depth).await
}

/// Checkout a commit. `on_conflict` decides whether to abort or overwrite when the working tree
/// has diverged from both the source and target commits (see [`OnConflict`]).
pub async fn checkout_commit_from_commit(
    repo: &LocalRepository,
    commit: &Commit,
    from_commit: &Option<Commit>,
    on_conflict: OnConflict,
) -> Result<(), OxenError> {
    core::v_latest::branches::checkout_commit(repo, commit, from_commit, on_conflict).await
}

pub fn set_head(repo: &LocalRepository, value: impl AsRef<str>) -> Result<(), OxenError> {
    let value = value.as_ref();
    log::debug!("set_head {value}");
    with_ref_manager(repo, |manager| {
        manager.set_head(value)?;
        Ok(())
    })
}

fn branch_has_been_merged(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    with_ref_manager(repo, |manager| {
        if let Some(branch_commit_id) = manager.get_commit_id_for_branch(name)? {
            if let Some(commit_id) = manager.head_commit_id()? {
                let history = repositories::commits::list_from(repo, &commit_id)?;
                for commit in history.iter() {
                    if commit.id == branch_commit_id {
                        return Ok(true);
                    }
                }
                // We didn't find commit
                Ok(false)
            } else {
                // Cannot check if it has been merged if we are in a detached HEAD state
                Ok(false)
            }
        } else {
            let err = format!("Err: The branch '{name}' does not exist.");
            Err(OxenError::basic_str(err))
        }
    })
}

pub fn rename_current_branch(repo: &LocalRepository, new_name: &str) -> Result<(), OxenError> {
    if let Ok(Some(branch)) = current_branch(repo) {
        with_ref_manager(repo, |manager| {
            manager.rename_branch(&branch.name, new_name)?;
            manager.set_head(new_name)?;
            Ok(())
        })
    } else {
        log::error!("rename_current_branch No current branch found");
        Err(OxenError::must_be_on_valid_branch())
    }
}

// Traces through a branches history to list all unique versions of a file
pub fn list_entry_versions_on_branch(
    local_repo: &LocalRepository,
    branch_name: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    let branch = repositories::branches::get_by_name(local_repo, branch_name)?;
    log::debug!(
        "get branch commits for branch {:?} -> {}",
        branch.name,
        branch.commit_id
    );
    core::v_latest::branches::list_entry_versions_for_commit(local_repo, &branch.commit_id, path)
}

pub use crate::core::v_latest::branches::set_working_repo_to_commit;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::refs::with_ref_manager;
    use crate::error::OxenError;
    use crate::{repositories, test, util};

    #[tokio::test]
    async fn test_list_branch_versions_main() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Make a dir
            let dir_path = Path::new("test_dir");
            let dir_repo_path = repo.path.join(dir_path);
            util::fs::create_dir_all(dir_repo_path)?;

            // File in the dir
            let file_path = dir_path.join(Path::new("test_file.txt"));
            let file_repo_path = repo.path.join(&file_path);
            util::fs::write_to_path(&file_repo_path, "test")?;

            // Add the dir
            repositories::add(&repo, &repo.path).await?;
            let commit_1 = repositories::commit(&repo, "adding test dir")?;

            // New file in root
            let file_path_2 = Path::new("test_file_2.txt");
            let file_repo_path_2 = repo.path.join(file_path_2);
            util::fs::write_to_path(&file_repo_path_2, "test")?;

            // Add the file
            repositories::add(&repo, &file_repo_path_2).await?;
            let commit_2 = repositories::commit(&repo, "adding test file")?;

            // Now modify both files, add a third
            let file_path_3 = Path::new("test_file_3.txt");
            let file_repo_path_3 = repo.path.join(file_path_3);

            util::fs::write_to_path(file_repo_path_3, "test 3")?;
            util::fs::write_to_path(&file_repo_path_2, "something different now")?;
            util::fs::write_to_path(&file_repo_path, "something different now")?;

            // Add-commit all
            repositories::add(&repo, &repo.path).await?;

            let commit_3 = repositories::commit(&repo, "adding test file 2")?;

            let _branch = repositories::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME)?;

            let file_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, "main", &file_path)?;

            let file_2_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, "main", file_path_2)?;

            let file_3_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, "main", file_path_3)?;

            assert_eq!(file_versions.len(), 2);
            assert_eq!(file_versions[0].0.id, commit_3.id);
            assert_eq!(file_versions[1].0.id, commit_1.id);

            println!("commit_1: {commit_1}");
            println!("commit_2: {commit_2}");
            println!("commit_3: {commit_3}");
            for v in &file_2_versions {
                println!("file_2_versions: {:?} -> {:?}", v.0, v.1);
            }

            assert_eq!(file_2_versions.len(), 2);
            assert_eq!(file_2_versions[0].0.id, commit_3.id);
            assert_eq!(file_2_versions[1].0.id, commit_2.id);

            assert_eq!(file_3_versions.len(), 1);
            assert_eq!(file_3_versions[0].0.id, commit_3.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_branch_versions_branch_off_main() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let dir_path = Path::new("test_dir");
            util::fs::create_dir_all(repo.path.join(dir_path))?;

            let file_path = dir_path.join(Path::new("test_file.txt"));
            let file_repo_path = repo.path.join(&file_path);

            // STARTING ON MAIN

            // Write initial file
            util::fs::write_to_path(&file_repo_path, "test")?;
            repositories::add(&repo, &repo.path).await?;
            let commit_1 = repositories::commit(&repo, "adding test file")?;

            // Change it
            util::fs::write_to_path(&file_repo_path, "something different now")?;
            repositories::add(&repo, &repo.path).await?;
            let commit_2 = repositories::commit(&repo, "adding test file 2")?;

            // Add an irrelevant file - aka this isn't changing for commit 3
            let file_path_2 = Path::new("test_file_2.txt");
            let file_repo_path_2 = repo.path.join(file_path_2);
            util::fs::write_to_path(file_repo_path_2, "test")?;
            repositories::add(&repo, &repo.path).await?;
            let _commit_3 = repositories::commit(&repo, "adding test file 3")?;

            // Branch off of main
            repositories::branches::create_checkout(&repo, "test_branch")?;

            // Change the file again
            util::fs::write_to_path(&file_repo_path, "something different now again")?;
            repositories::add(&repo, &repo.path).await?;
            let commit_4 = repositories::commit(&repo, "adding test file 4")?;

            // One more time on branch
            util::fs::write_to_path(&file_repo_path, "something different now again again")?;
            repositories::add(&repo, &repo.path).await?;
            let commit_5 = repositories::commit(&repo, "adding test file 5")?;

            // Back to main - hacky to avoid async checkout
            with_ref_manager(&repo, |manager| {
                manager.set_head(DEFAULT_BRANCH_NAME)?;
                Ok(())
            })?;

            // Another commit
            util::fs::write_to_path(&file_repo_path, "something different now again again again")?;
            repositories::add(&repo, &repo.path).await?;
            let commit_6 = repositories::commit(&repo, "adding test file 6")?;

            let _main = repositories::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME)?;
            let _branch = repositories::branches::get_by_name(&repo, "test_branch")?;
            let main_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, "main", &file_path)?;

            let branch_versions = repositories::branches::list_entry_versions_on_branch(
                &repo,
                "test_branch",
                &file_path.to_path_buf(),
            )?;

            for v in &main_versions {
                println!("main: {:?} -> {:?}", v.0, v.1);
            }

            for v in &branch_versions {
                println!("branch: {:?} -> {:?}", v.0, v.1);
            }

            // Main should have commits 6, 2, and 1.
            assert_eq!(main_versions.len(), 3);
            assert_eq!(main_versions[0].0.id, commit_6.id);
            assert_eq!(main_versions[1].0.id, commit_2.id);
            assert_eq!(main_versions[2].0.id, commit_1.id);

            // Branch should have commits 5, 4, 2, and 1.
            assert_eq!(branch_versions.len(), 4);
            assert_eq!(branch_versions[0].0.id, commit_5.id);
            assert_eq!(branch_versions[1].0.id, commit_4.id);
            assert_eq!(branch_versions[2].0.id, commit_2.id);
            assert_eq!(branch_versions[3].0.id, commit_1.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_force_update_existing_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create two commits
            let file_path = repo.path.join("file.txt");
            util::fs::write_to_path(&file_path, "first")?;
            repositories::add(&repo, &file_path).await?;
            let commit_1 = repositories::commit(&repo, "first commit")?;

            util::fs::write_to_path(&file_path, "second")?;
            repositories::add(&repo, &file_path).await?;
            let _commit_2 = repositories::commit(&repo, "second commit")?;

            // Create a branch at current HEAD (commit_2)
            repositories::branches::create_checkout(&repo, "test-branch")?;

            // Force update it back to commit_1
            repositories::branches::update(&repo, "test-branch", &commit_1.id)?;

            // Verify the branch now points to commit_1
            let fetched = repositories::branches::get_by_name(&repo, "test-branch")?;
            assert_eq!(fetched.commit_id, commit_1.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_force_update_creates_new_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let head = repositories::commits::head_commit(&repo)?;

            // Force update a branch that doesn't exist yet
            let branch = repositories::branches::update(&repo, "new-branch", &head.id)?;
            assert_eq!(branch.commit_id, head.id);
            assert_eq!(branch.name, "new-branch");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_force_update_invalid_commit_fails() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let result =
                repositories::branches::update(&repo, "test-branch", "nonexistent_commit_id");
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_delete_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Get the original branches
            let og_branches = repositories::branches::list(&repo).await?;
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Must checkout main again before deleting
            repositories::checkout(&repo, og_branch.name).await?;

            // Now we can delete
            repositories::branches::delete(&repo, branch_name)?;

            // Should be same num as og_branches
            let leftover_branches = repositories::branches::list(&repo).await?;
            assert_eq!(og_branches.len(), leftover_branches.len());

            Ok(())
        })
        .await
    }

    /// Open the server-side repository for a pushed remote so a test can mutate its on-disk
    /// version store. `bin/test-rust` starts oxen-server against `$SYNC_DIR`, inherited by the
    /// test process; repos live under `<SYNC_DIR>/<namespace>/<name>`.
    fn server_repo(name: &str) -> Result<crate::model::LocalRepository, OxenError> {
        let sync_dir =
            std::path::PathBuf::from(std::env::var("SYNC_DIR").map_err(|_| {
                OxenError::basic_str("SYNC_DIR not set; run tests via bin/test-rust")
            })?);
        repositories::get_by_namespace_and_name(
            &sync_dir,
            crate::constants::DEFAULT_NAMESPACE,
            name,
            None,
        )?
        .ok_or_else(|| OxenError::basic_str(format!("server repo not found for {name}")))
    }

    // Every server ref-advance path — branch update, new-branch create, and push-time merge —
    // must refuse to point a ref at a commit whose newly-added blob is missing.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_ref_advance_gates_reject_incomplete_commit() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            // commit1 is complete; commit2 adds b.txt.
            let a_path = repo.path.join("a.txt");
            test::write_txt_file_to_path(&a_path, "alpha")?;
            repositories::add(&repo, &a_path).await?;
            let commit1 = repositories::commit(&repo, "add a.txt")?;

            let remote = test::repo_remote_url_from(&repo.dirname());
            crate::command::config::set_remote(
                &mut repo,
                crate::constants::DEFAULT_REMOTE_NAME,
                &remote,
            )?;
            let remote_repo = test::create_remote_repo(&repo).await?;
            repositories::push(&repo).await?;

            let b_path = repo.path.join("b.txt");
            test::write_txt_file_to_path(&b_path, "beta")?;
            repositories::add(&repo, &b_path).await?;
            let commit2 = repositories::commit(&repo, "add b.txt")?;
            repositories::push(&repo).await?;

            // The server loses b.txt's blob — referenced only by commit2.
            let b_hash = repositories::tree::get_node_by_path(&repo, &commit2, "b.txt")?
                .expect("b.txt in commit2")
                .file()?
                .hash()
                .to_string();
            let server = server_repo(&repo.dirname())?;
            server.version_store().delete_version(&b_hash).await?;

            // Resetting back to the complete commit1 is allowed (it adds nothing missing).
            crate::api::client::branches::update(&remote_repo, DEFAULT_BRANCH_NAME, &commit1)
                .await?;
            let head = crate::api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                .await?
                .expect("main exists");
            assert_eq!(head.commit_id, commit1.id);

            // update gate: re-advancing main to commit2 must be rejected.
            let update_result =
                crate::api::client::branches::update(&remote_repo, DEFAULT_BRANCH_NAME, &commit2)
                    .await;
            assert!(
                update_result.is_err(),
                "update onto a commit with a missing added blob must be rejected"
            );
            let head = crate::api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                .await?
                .expect("main exists");
            assert_eq!(
                head.commit_id, commit1.id,
                "update must not advance main onto an incomplete commit"
            );

            // create gate: creating a new branch at commit2 (main is at the complete commit1) must
            // be rejected, and the branch must not be created.
            let create_result = crate::api::client::branches::create_from_commit(
                &remote_repo,
                "at-commit2",
                &commit2,
            )
            .await;
            assert!(
                create_result.is_err(),
                "creating a branch at a commit with a missing added blob must be rejected"
            );
            assert!(
                crate::api::client::branches::get_by_name(&remote_repo, "at-commit2")
                    .await?
                    .is_none(),
                "a rejected create must not leave the branch behind"
            );

            // merge gate: a push-time merge of commit2 into main (at commit1) must be rejected.
            let merge_result = crate::api::client::branches::maybe_create_merge(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &commit2.id,
                &commit1.id,
            )
            .await;
            assert!(
                merge_result.is_err(),
                "merging in a commit with a missing added blob must be rejected"
            );

            crate::api::client::repositories::delete(&remote_repo).await?;
            Ok(())
        })
        .await
    }

    // The gate also refuses a commit whose objects are all present but whose dir-hashes index is
    // missing — the tree can't be served by path without it. The check lives in the shared
    // `verify_reachable_objects`, so exercising the update gate covers all three ref-advance paths.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_ref_advance_gate_requires_dir_hashes() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            let a_path = repo.path.join("a.txt");
            test::write_txt_file_to_path(&a_path, "alpha")?;
            repositories::add(&repo, &a_path).await?;
            let commit1 = repositories::commit(&repo, "add a.txt")?;

            let remote = test::repo_remote_url_from(&repo.dirname());
            crate::command::config::set_remote(
                &mut repo,
                crate::constants::DEFAULT_REMOTE_NAME,
                &remote,
            )?;
            let remote_repo = test::create_remote_repo(&repo).await?;
            repositories::push(&repo).await?;

            // commit2 is pushed complete: every node and blob it adds is on the server.
            let b_path = repo.path.join("b.txt");
            test::write_txt_file_to_path(&b_path, "beta")?;
            repositories::add(&repo, &b_path).await?;
            let commit2 = repositories::commit(&repo, "add b.txt")?;
            repositories::push(&repo).await?;

            // Reset main to commit1, then remove commit2's dir-hashes index on the server.
            crate::api::client::branches::update(&remote_repo, DEFAULT_BRANCH_NAME, &commit1)
                .await?;
            let server = server_repo(&repo.dirname())?;
            let dir_hashes =
                crate::core::db::dir_hashes::dir_hashes_db::dir_hash_db_path(&server, &commit2);
            util::fs::remove_dir_all(&dir_hashes)?;

            // Re-advancing main to commit2 must be rejected even though its objects are all
            // present, and main must stay at commit1.
            let result =
                crate::api::client::branches::update(&remote_repo, DEFAULT_BRANCH_NAME, &commit2)
                    .await;
            assert!(
                result.is_err(),
                "advance onto a commit missing its dir-hashes index must be rejected"
            );
            let head = crate::api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                .await?
                .expect("main exists");
            assert_eq!(
                head.commit_id, commit1.id,
                "a rejected advance must not move main"
            );

            crate::api::client::repositories::delete(&remote_repo).await?;
            Ok(())
        })
        .await
    }

    // Creating a branch *from another branch* is a ref advance too: it must refuse when the source
    // branch's head commit is missing an added blob, just like commit-based creation.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_ref_advance_gate_rejects_create_from_incomplete_branch() -> Result<(), OxenError>
    {
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            let a_path = repo.path.join("a.txt");
            test::write_txt_file_to_path(&a_path, "alpha")?;
            repositories::add(&repo, &a_path).await?;
            let commit1 = repositories::commit(&repo, "add a.txt")?;

            let remote = test::repo_remote_url_from(&repo.dirname());
            crate::command::config::set_remote(
                &mut repo,
                crate::constants::DEFAULT_REMOTE_NAME,
                &remote,
            )?;
            let remote_repo = test::create_remote_repo(&repo).await?;
            repositories::push(&repo).await?;

            let b_path = repo.path.join("b.txt");
            test::write_txt_file_to_path(&b_path, "beta")?;
            repositories::add(&repo, &b_path).await?;
            let commit2 = repositories::commit(&repo, "add b.txt")?;
            repositories::push(&repo).await?;

            // "src" points at the complete commit2; then main resets to commit1 so the gate below
            // diffs commit2 against commit1 and actually inspects b.txt.
            crate::api::client::branches::create_from_branch(&remote_repo, "src", DEFAULT_BRANCH_NAME)
                .await?;
            crate::api::client::branches::update(&remote_repo, DEFAULT_BRANCH_NAME, &commit1)
                .await?;

            // The server loses b.txt's blob — referenced only by commit2 (src's head).
            let b_hash = repositories::tree::get_node_by_path(&repo, &commit2, "b.txt")?
                .expect("b.txt in commit2")
                .file()?
                .hash()
                .to_string();
            let server = server_repo(&repo.dirname())?;
            server.version_store().delete_version(&b_hash).await?;

            // Creating a branch from "src" (head = the now-incomplete commit2) must be rejected, and
            // the new branch must not be created.
            let result =
                crate::api::client::branches::create_from_branch(&remote_repo, "derived", "src")
                    .await;
            assert!(
                result.is_err(),
                "create-from-branch off a source whose head is missing an added blob must be rejected"
            );
            assert!(
                crate::api::client::branches::get_by_name(&remote_repo, "derived")
                    .await?
                    .is_none(),
                "a rejected create-from-branch must not leave the branch behind"
            );

            crate::api::client::repositories::delete(&remote_repo).await?;
            Ok(())
        })
        .await
    }
}
