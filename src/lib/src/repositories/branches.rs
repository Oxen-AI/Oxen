//! # Branches
//!
//! Interact with Oxen branches.
//!

use std::path::Path;

use crate::constants::{BRANCH_LOCKS_DIR, OXEN_HIDDEN_DIR};
use crate::core::refs::{RefReader, RefWriter};
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository};
use crate::repositories;
use crate::{core, util};

/// List all the local branches within a repo
pub fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
    Ok(branches)
}

/// Get a branch by name
pub fn get_by_name(repo: &LocalRepository, name: &str) -> Result<Option<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    ref_reader.get_branch_by_name(name)
}

/// Get branch by name or fall back the current
pub fn get_by_name_or_current(
    repo: &LocalRepository,
    branch_name: Option<impl AsRef<str>>,
) -> Result<Branch, OxenError> {
    if let Some(branch_name) = branch_name {
        let branch_name = branch_name.as_ref();
        match repositories::branches::get_by_name(repo, branch_name)? {
            Some(branch) => Ok(branch),
            None => Err(OxenError::local_branch_not_found(branch_name)),
        }
    } else {
        match repositories::branches::current_branch(repo)? {
            Some(branch) => Ok(branch),
            None => {
                log::error!("get_by_name_or_current No current branch found");
                Err(OxenError::must_be_on_valid_branch())
            }
        }
    }
}

/// Get commit id from a branch by name
pub fn get_commit_id(repo: &LocalRepository, name: &str) -> Result<Option<String>, OxenError> {
    match RefReader::new(repo) {
        Ok(ref_reader) => ref_reader.get_commit_id_for_branch(name),
        _ => Err(OxenError::basic_str("Could not read reference for repo.")),
    }
}

/// Check if a branch exists
pub fn exists(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    match get_by_name(repo, name)? {
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

/// Get the current branch
pub fn current_branch(repo: &LocalRepository) -> Result<Option<Branch>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branch = ref_reader.get_current_branch()?;
    Ok(branch)
}

/// # Create a new branch from the head commit
/// This creates a new pointer to the current commit with a name,
/// it does not switch you to this branch, you still must call `checkout_branch`
pub fn create_from_head(
    repo: &LocalRepository,
    name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    let ref_writer = RefWriter::new(repo)?;
    let head_commit = repositories::commits::head_commit(repo)?;
    ref_writer.create_branch(name, &head_commit.id)
}

/// # Create a local branch from a specific commit id
pub fn create(
    repo: &LocalRepository,
    name: impl AsRef<str>,
    commit_id: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    let commit_id = commit_id.as_ref();
    let ref_writer = RefWriter::new(repo)?;
    if repositories::commits::commit_id_exists(repo, commit_id)? {
        ref_writer.create_branch(name, commit_id)
    } else {
        Err(OxenError::commit_id_does_not_exist(commit_id))
    }
}

/// # Create a branch and check it out in one go
/// This creates a branch with name,
/// then switches HEAD to point to the branch
pub fn create_checkout(repo: &LocalRepository, name: impl AsRef<str>) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    println!("Create and checkout branch: {name}");
    let head_commit = repositories::commits::head_commit(repo)?;
    let ref_writer = RefWriter::new(repo)?;

    let branch = ref_writer.create_branch(name, &head_commit.id)?;
    ref_writer.set_head(name);
    Ok(branch)
}

/// Update the branch name to point to a commit id
pub fn update(
    repo: &LocalRepository,
    name: impl AsRef<str>,
    commit_id: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    let commit_id = commit_id.as_ref();
    let ref_reader = RefReader::new(repo)?;
    match ref_reader.get_branch_by_name(name)? {
        Some(branch) => {
            // Set the branch to point to the commit
            let ref_writer = RefWriter::new(repo)?;
            match ref_writer.set_branch_commit_id(name, commit_id) {
                Ok(()) => Ok(branch),
                Err(err) => Err(err),
            }
        }
        None => create(repo, name, commit_id),
    }
}

/// Delete a local branch
pub fn delete(repo: &LocalRepository, name: impl AsRef<str>) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    // Make sure they don't delete the current checked out branch
    if let Ok(Some(branch)) = current_branch(repo) {
        if branch.name == name {
            let err = format!("Err: Cannot delete current checked out branch '{name}'");
            return Err(OxenError::basic_str(err));
        }
    }

    if branch_has_been_merged(repo, name)? {
        let ref_writer = RefWriter::new(repo)?;
        ref_writer.delete_branch(name)
    } else {
        let err = format!("Err: The branch '{name}' is not fully merged.\nIf you are sure you want to delete it, run 'oxen branch -D {name}'.");
        Err(OxenError::basic_str(err))
    }
}

/// # Force delete a local branch
/// Caution! Will delete a local branch without checking if it has been merged or pushed.
pub fn force_delete(repo: &LocalRepository, name: impl AsRef<str>) -> Result<Branch, OxenError> {
    let name = name.as_ref();
    if let Ok(Some(branch)) = current_branch(repo) {
        if branch.name == name {
            let err = format!("Err: Cannot delete current checked out branch '{name}'");
            return Err(OxenError::basic_str(err));
        }
    }

    let ref_writer = RefWriter::new(repo)?;
    ref_writer.delete_branch(name)
}

/// Check if a branch is checked out
pub fn is_checked_out(repo: &LocalRepository, name: &str) -> bool {
    match RefReader::new(repo) {
        Ok(ref_reader) => {
            if let Ok(Some(current_branch)) = ref_reader.get_current_branch() {
                // If we are already on the branch, do nothing
                if current_branch.name == name {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Lock a branch for pushing
pub fn lock(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    // Errors if lock exists - to avoid double-request ("is_locked" -> if false "lock")
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Locking branch: {} to path {}",
        name,
        branch_lock_file.display()
    );

    if branch_lock_file.exists() || repositories::is_locked(repo) {
        return Err(OxenError::remote_branch_locked());
    }

    // If the branch exists, get the current head commit and lock it as the current "latest commit"
    // during the lifetime of the push operation.
    let maybe_branch = repositories::branches::get_by_name(repo, name)?;

    let maybe_latest_commit;
    if let Some(branch) = maybe_branch {
        maybe_latest_commit = branch.commit_id;
    } else {
        maybe_latest_commit = "branch being created".to_string();
    }

    // Create locks dir if needed
    if !locks_dir.exists() {
        util::fs::create_dir_all(&locks_dir)?;
    }

    util::fs::write_to_path(&branch_lock_file, maybe_latest_commit)?;
    Ok(())
}

/// Check if a branch is locked
pub fn is_locked(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    // Get the oxen hidden dir
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    // Create locks dir if not exists
    if !locks_dir.exists() {
        util::fs::create_dir_all(&locks_dir)?;
    }

    // Add a file with the branch name to the locks dir
    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Checking if branch is locked: {} at path {}",
        name,
        branch_lock_file.display()
    );
    // Branch is locked if file exists
    Ok(branch_lock_file.exists())
}

/// Read the lock file for a branch
pub fn read_lock_file(repo: &LocalRepository, name: &str) -> Result<String, OxenError> {
    // Get the oxen hidden dir
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    // Add a file with the branch name to the locks dir
    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Reading lock file for branch: {} at path {}",
        name,
        branch_lock_file.display()
    );

    // Check if lock exists
    if !branch_lock_file.exists() {
        let err = format!("Err: Branch '{name}' is not locked.");
        return Err(OxenError::basic_str(err));
    }

    let contents = std::fs::read_to_string(branch_lock_file)?;
    Ok(contents)
}

/// Get the latest synced commit
pub fn latest_synced_commit(repo: &LocalRepository, name: &str) -> Result<Commit, OxenError> {
    // If branch is locked, we want to get the commit from the lockfile
    if is_locked(repo, name)? {
        let commit_id = read_lock_file(repo, name)?;
        let commit = repositories::commits::get_by_id(repo, &commit_id)?
            .ok_or(OxenError::commit_id_does_not_exist(&commit_id))?;
        return Ok(commit);
    }
    // If branch is not locked, we want to get the latest commit from the branch
    let branch = repositories::branches::get_by_name(repo, name)?
        .ok_or(OxenError::local_branch_not_found(name))?;
    let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&branch.commit_id))?;
    Ok(commit)
}

/// Unlock a branch for pushing
pub fn unlock(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    // Get the oxen hidden dir
    let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let locks_dir = oxen_dir.join(BRANCH_LOCKS_DIR);

    // Add a file with the branch name to the locks dir
    let clean_name = branch_name_no_slashes(name);
    let branch_lock_file = locks_dir.join(clean_name);
    log::debug!(
        "Unlocking branch: {} at path {}",
        name,
        branch_lock_file.display()
    );

    // Check if lock exists
    if !branch_lock_file.exists() {
        log::debug!("Branch is not locked, nothing to do");
        return Ok(());
    }

    util::fs::remove_file(&branch_lock_file)?;

    Ok(())
}

/// Checkout a branch
pub async fn checkout_branch(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    log::debug!("checkout_branch {}", name);
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::branches::checkout(repo, name).await,
        MinOxenVersion::V0_19_0 => core::v0_19_0::branches::checkout(repo, name).await,
    }
}

/// Checkout a commit id
pub async fn checkout_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::branches::checkout_commit_id(repo, commit_id).await
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::branches::checkout_commit_id(repo, commit_id).await
        }
    }
}

pub fn set_head(repo: &LocalRepository, value: &str) -> Result<(), OxenError> {
    log::debug!("set_head {}", value);
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_head(value);
    Ok(())
}

fn branch_has_been_merged(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    if let Some(branch_commit_id) = ref_reader.get_commit_id_for_branch(name)? {
        if let Some(commit_id) = ref_reader.head_commit_id()? {
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
}

pub fn rename_current_branch(repo: &LocalRepository, new_name: &str) -> Result<(), OxenError> {
    if let Ok(Some(branch)) = current_branch(repo) {
        let ref_writer = RefWriter::new(repo)?;
        ref_writer.rename_branch(&branch.name, new_name)?;
        ref_writer.set_head(new_name);
        Ok(())
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
    let branch = repositories::branches::get_by_name(local_repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;
    log::debug!(
        "get branch commits for branch {:?} -> {}",
        branch.name,
        branch.commit_id
    );
    match local_repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::branches::list_entry_versions_for_commit(
            local_repo,
            &branch.commit_id,
            path,
        ),
        MinOxenVersion::V0_19_0 => core::v0_19_0::branches::list_entry_versions_for_commit(
            local_repo,
            &branch.commit_id,
            path,
        ),
    }
}

pub async fn set_working_repo_to_commit(
    repo: &LocalRepository,
    commit: &Commit,
    force: bool,
) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            panic!("set_working_repo_to_commit not implemented for oxen v0.10.0")
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::branches::set_working_repo_to_commit(repo, commit, force).await
        }
    }
}

fn branch_name_no_slashes(name: &str) -> String {
    // Replace all slashes with dashes

    name.replace('/', "-")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_list_branch_versions_main() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Make a dir
            let dir_path = Path::new("test_dir");
            let dir_repo_path = repo.path.join(dir_path);
            util::fs::create_dir_all(dir_repo_path)?;

            // File in the dir
            let file_path = dir_path.join(Path::new("test_file.txt"));
            let file_repo_path = repo.path.join(&file_path);
            util::fs::write_to_path(&file_repo_path, "test")?;

            // Add the dir
            repositories::add(&repo, &repo.path)?;
            let commit_1 = repositories::commit(&repo, "adding test dir")?;

            // New file in root
            let file_path_2 = Path::new("test_file_2.txt");
            let file_repo_path_2 = repo.path.join(file_path_2);
            util::fs::write_to_path(&file_repo_path_2, "test")?;

            // Add the file
            repositories::add(&repo, &file_repo_path_2)?;
            let commit_2 = repositories::commit(&repo, "adding test file")?;

            // Now modify both files, add a third
            let file_path_3 = Path::new("test_file_3.txt");
            let file_repo_path_3 = repo.path.join(file_path_3);

            util::fs::write_to_path(file_repo_path_3, "test 3")?;
            util::fs::write_to_path(&file_repo_path_2, "something different now")?;
            util::fs::write_to_path(&file_repo_path, "something different now")?;

            // Add-commit all
            repositories::add(&repo, &repo.path)?;

            let commit_3 = repositories::commit(&repo, "adding test file 2")?;

            let _branch = repositories::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME)?.unwrap();

            let file_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, "main", &file_path)?;

            let file_2_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, "main", file_path_2)?;

            let file_3_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, "main", file_path_3)?;

            assert_eq!(file_versions.len(), 2);
            assert_eq!(file_versions[0].0.id, commit_3.id);
            assert_eq!(file_versions[1].0.id, commit_1.id);

            let tree = repositories::tree::get_by_commit(&repo, &commit_1)?;
            println!("AFTER commit_1: {}", commit_1);
            tree.print();

            let tree = repositories::tree::get_by_commit(&repo, &commit_2)?;
            println!("AFTER commit_2: {}", commit_2);
            tree.print();

            let tree = repositories::tree::get_by_commit(&repo, &commit_3)?;
            println!("AFTER commit_3: {}", commit_3);
            tree.print();

            println!("commit_1: {}", commit_1);
            println!("commit_2: {}", commit_2);
            println!("commit_3: {}", commit_3);
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
    }

    #[test]
    fn test_list_branch_versions_branch_off_main() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let dir_path = Path::new("test_dir");
            std::fs::create_dir_all(repo.path.join(dir_path))?;

            let file_path = dir_path.join(Path::new("test_file.txt"));
            let file_repo_path = repo.path.join(&file_path);

            // STARTING ON MAIN

            // Write initial file
            util::fs::write_to_path(&file_repo_path, "test")?;
            repositories::add(&repo, &repo.path)?;
            let commit_1 = repositories::commit(&repo, "adding test file")?;

            // Change it
            util::fs::write_to_path(&file_repo_path, "something different now")?;
            repositories::add(&repo, &repo.path)?;
            let commit_2 = repositories::commit(&repo, "adding test file 2")?;

            // Add an irrelevant file - aka this isn't changing for commit 3
            let file_path_2 = Path::new("test_file_2.txt");
            let file_repo_path_2 = repo.path.join(file_path_2);
            util::fs::write_to_path(file_repo_path_2, "test")?;
            repositories::add(&repo, &repo.path)?;
            let _commit_3 = repositories::commit(&repo, "adding test file 3")?;

            // Branch off of main
            repositories::branches::create_checkout(&repo, "test_branch")?;

            // Change the file again
            util::fs::write_to_path(&file_repo_path, "something different now again")?;
            repositories::add(&repo, &repo.path)?;
            let commit_4 = repositories::commit(&repo, "adding test file 4")?;

            // One more time on branch
            util::fs::write_to_path(&file_repo_path, "something different now again again")?;
            repositories::add(&repo, &repo.path)?;
            let commit_5 = repositories::commit(&repo, "adding test file 5")?;

            // Back to main - hacky to avoid async checkout
            {
                let ref_writer = core::refs::RefWriter::new(&repo)?;
                ref_writer.set_head(DEFAULT_BRANCH_NAME);
            }

            // Another commit
            util::fs::write_to_path(&file_repo_path, "something different now again again again")?;
            repositories::add(&repo, &repo.path)?;
            let commit_6 = repositories::commit(&repo, "adding test file 6")?;

            let _main = repositories::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME)?.unwrap();
            let _branch = repositories::branches::get_by_name(&repo, "test_branch")?.unwrap();
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
    }

    #[tokio::test]
    async fn test_local_delete_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Get the original branches
            let og_branches = repositories::branches::list(&repo)?;
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Must checkout main again before deleting
            repositories::checkout(&repo, og_branch.name).await?;

            // Now we can delete
            repositories::branches::delete(&repo, branch_name)?;

            // Should be same num as og_branches
            let leftover_branches = repositories::branches::list(&repo)?;
            assert_eq!(og_branches.len(), leftover_branches.len());

            Ok(())
        })
        .await
    }
}
