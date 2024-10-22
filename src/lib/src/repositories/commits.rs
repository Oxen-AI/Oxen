//! # Commits
//!
//! Create, read, and list commits
//!

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MerkleHash};
use crate::opts::PaginateOpts;
use crate::util;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{core, resource};

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// # Commit the staged files in the repo
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_commit");
/// let repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// repositories::add(&repo, &hello_file)?;
///
/// // Commit staged
/// repositories::commit(&repo, "My commit message")?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn commit(repo: &LocalRepository, message: &str) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::commit(repo, message),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::commit(repo, message),
    }
}

/// Iterate over all commits and get the one with the latest timestamp
pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::latest_commit(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::latest_commit(repo),
    }
}

/// The current HEAD commit of the branch you currently have checked out
pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::head_commit(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::head_commit(repo),
    }
}

/// Maybe get the head commit if it exists
/// Returns None if the head commit does not exist (empty repo)
pub fn head_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::head_commit_maybe(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::head_commit_maybe(repo),
    }
}

/// Get the root commit of a repository
pub fn root_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let root_commit = core::v0_10_0::commits::root_commit(repo)?;
            Ok(Some(root_commit))
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::root_commit_maybe(repo),
    }
}

/// Get a commit by it's MerkleHash
pub fn get_by_hash(repo: &LocalRepository, hash: &MerkleHash) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::get_by_id(repo, &hash.to_string()),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::get_by_hash(repo, hash),
    }
}

/// Get a commit by it's string hash
pub fn get_by_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_id = commit_id.as_ref();
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::get_by_id(repo, commit_id),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::get_by_id(repo, commit_id),
    }
}

/// Commit id exists
pub fn commit_id_exists(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<bool, OxenError> {
    get_by_id(repo, commit_id.as_ref()).map(|commit| commit.is_some())
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list(repo),
    }
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list_all(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_all(repo),
    }
}

/// List unsynced commits for the repository (ie they are missing their .version/ files)
pub fn list_unsynced(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("list_unsynced not supported in v0.10.0"),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_unsynced(repo),
    }
}

/// List unsynced commits from a specific revision
pub fn list_unsynced_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<HashSet<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("list_unsynced_from not supported in v0.10.0"),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_unsynced_from(repo, revision),
    }
}
// Source
pub fn get_commit_or_head<S: AsRef<str> + Clone>(
    repo: &LocalRepository,
    commit_id_or_branch_name: Option<S>,
) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => resource::get_commit_or_head(repo, commit_id_or_branch_name),
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::commits::get_commit_or_head(repo, commit_id_or_branch_name)
        }
    }
}

pub fn list_all_paginated(
    repo: &LocalRepository,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    log::info!("list_all_paginated: {:?} {:?}", repo.path, pagination);
    let commits = list_all(repo)?;
    let commits: Vec<Commit> = commits.into_iter().collect();
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

/// List the history for a specific branch or commit (revision)
pub fn list_from(repo: &LocalRepository, revision: &str) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list_from(repo, revision),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_from(repo, revision),
    }
}
pub fn list_from_with_depth(
    repo: &LocalRepository,
    revision: &str,
) -> Result<HashMap<Commit, usize>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => Err(OxenError::basic_str(
            "list_from_with_depth not supported in v0.10.0",
        )),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_from_with_depth(repo, revision),
    }
}

/// List the history between two commits
pub fn list_between(
    repo: &LocalRepository,
    start: &Commit,
    end: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::list_between(repo, start, end),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::list_between(repo, start, end),
    }
}

/// Get a list commits by the commit message
pub fn get_by_message(
    repo: &LocalRepository,
    msg: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let commits = list_all(repo)?;
    let filtered: Vec<Commit> = commits
        .into_iter()
        .filter(|commit| commit.message == msg.as_ref())
        .collect();
    Ok(filtered)
}

/// Get the most recent commit by the commit message, starting at the HEAD commit
pub fn first_by_message(
    repo: &LocalRepository,
    msg: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commits = list(repo)?;
    Ok(commits
        .into_iter()
        .find(|commit| commit.message == msg.as_ref()))
}

/// List all the commits that have missing entries
/// Useful for knowing which commits to resend
pub fn list_with_missing_entries(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::commits::list_with_missing_entries(repo, commit_id)
        }
        MinOxenVersion::V0_19_0 => {
            panic!("list_with_missing_entries not needed in v0.19.0");
        }
    }
}

/// Retrieve entries with filepaths matching a provided glob pattern
pub fn search_entries(
    repo: &LocalRepository,
    commit: &Commit,
    pattern: &str,
) -> Result<HashSet<PathBuf>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::commits::search_entries(repo, commit, pattern),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::search_entries(repo, commit, pattern),
    }
}

/// List paginated commits starting from the given revision
pub fn list_from_paginated(
    repo: &LocalRepository,
    revision: &str,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let commits = list_from(repo, revision)?;
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}

/// List paginated commits by resource
pub fn list_by_path_from_paginated(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    log::info!("list_by_path_from_paginated: {:?} {:?}", commit, path);
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::commits::list_by_path_from_paginated(repo, commit, path, pagination)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::commits::list_by_path_from_paginated(repo, commit, path, pagination)
        }
    }
}

// TODO: Temporary function until after v0.19.0, we shouldn't need this check
// once everything is working off the Merkle tree
pub fn get_commit_status_tmp(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<core::v0_10_0::cache::cacher_status::CacherStatusType>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::cache::commit_cacher::get_status(repo, commit),
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::get_commit_status_tmp(repo, commit),
    }
}

// TODO: Temporary function until after v0.19.0, we shouldn't need this check
// once everything is working off the Merkle tree
pub fn is_commit_valid_tmp(repo: &LocalRepository, commit: &Commit) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::cache::cachers::content_validator::is_valid(repo, commit)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::commits::is_commit_valid_tmp(repo, commit),
    }
}

pub fn commit_history_is_complete(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    // Get full commit history from this head backwards
    let history = list_from(repo, &commit.id)?;

    // Ensure traces back to base commit
    let maybe_initial_commit = history.last().unwrap();
    if !maybe_initial_commit.parent_ids.is_empty() {
        // If it has parents, it isn't an initial commit
        return Ok(false);
    }

    // Ensure all commits and their parents are synced
    // Initialize commit reader
    for c in &history {
        log::debug!(
            "commit_history_is_complete checking if commit is synced: {}",
            c
        );
        if !core::commit_sync_status::commit_is_synced(repo, c) {
            log::debug!("commit_history_is_complete ❌ commit is not synced: {}", c);
            return Ok(false);
        } else {
            log::debug!("commit_history_is_complete ✅ commit is synced: {}", c);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::str::FromStr;

    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::model::StagedEntryStatus;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use super::*;

    #[test]
    fn test_command_commit_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            repositories::add(&repo, &hello_file)?;
            // Commit the file
            let commit = repositories::commit(&repo, "My message")?;
            assert_eq!(commit.message, "My message");

            // Get status and make sure it is removed from the untracked and added
            let repo_status = repositories::status(&repo)?;
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_commit_removed_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            repositories::add(&repo, &hello_file)?;

            // Remove the file
            util::fs::remove_file(&hello_file)?;

            // Can still commit the file, since it is in the versions directory
            repositories::commit(&repo, "My message")?;

            // Get status and make sure the file was not committed
            let head = repositories::commits::head_commit(&repo)?;
            let commit_list = repositories::entries::list_for_commit(&repo, &head)?;
            assert_eq!(commit_list.len(), 1);

            // Add the removed file and commit
            repositories::add(&repo, &hello_file)?;
            repositories::commit(&repo, "Second Message")?;

            // We should now have no entries
            let head = repositories::commits::head_commit(&repo)?;
            let commit_list = repositories::entries::list_for_commit(&repo, &head)?;
            assert_eq!(commit_list.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_command_commit_train_data_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Track the file
            let train_dir = repo.path.join("train");
            repositories::add(&repo, train_dir)?;
            // Commit the file
            repositories::commit(&repo, "Adding training data")?;

            let repo_status = repositories::status(&repo)?;
            repo_status.print();
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 2);
            assert_eq!(repo_status.untracked_dirs.len(), 4);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_command_commit_dir_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Track the annotations dir, which has sub dirs
            let annotations_dir = repo.path.join("annotations");
            repositories::add(&repo, annotations_dir)?;
            repositories::commit(&repo, "Adding annotations data dir, which has two levels")?;

            let repo_status = repositories::status(&repo)?;
            repo_status.print();

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 2);
            assert_eq!(repo_status.untracked_dirs.len(), 4);

            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 1);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_command_commit_second_level_dir_then_revert() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Track & commit (dir already created in helper)
            let new_dir_path = repo.path.join("annotations").join("train");
            repositories::add(&repo, &new_dir_path)?;
            repositories::commit(&repo, "Adding train dir")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a branch to make the changes
            let branch_name = "feature/adding-annotations";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Track & commit (dir already created in helper)
            let test_dir_path = repo.path.join("annotations").join("test");
            let og_num_files = util::fs::rcount_files_in_dir(&test_dir_path);

            repositories::add(&repo, &test_dir_path)?;
            repositories::commit(&repo, "Adding test dir")?;

            // checkout OG and make sure it removes the train dir
            repositories::checkout(&repo, orig_branch.name).await?;
            assert!(!test_dir_path.exists());

            // checkout branch again and make sure it reverts
            repositories::checkout(&repo, branch_name).await?;
            assert!(test_dir_path.exists());
            assert_eq!(util::fs::rcount_files_in_dir(&test_dir_path), og_num_files);

            Ok(())
        })
        .await
    }

    #[test]
    fn test_command_commit_removed_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (dir already created in helper)
            let dir_to_remove = repo.path.join("train");
            let og_file_count = util::fs::rcount_files_in_dir(&dir_to_remove);

            repositories::add(&repo, &dir_to_remove)?;
            repositories::commit(&repo, "Adding train directory")?;

            // Delete the directory
            util::fs::remove_dir_all(&dir_to_remove)?;

            // Add the deleted dir, so that we can commit the deletion
            repositories::add(&repo, &dir_to_remove)?;

            // Make sure we have the correct amount of files tagged as removed
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), og_file_count);
            assert_eq!(
                status.staged_files.iter().next().unwrap().1.status,
                StagedEntryStatus::Removed
            );

            status.print();

            // Make sure they don't show up in the status
            assert_eq!(status.removed_files.len(), 0);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_commit_after_merge_conflict() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            let labels_path = repo.path.join("labels.txt");
            repositories::add(&repo, &labels_path)?;
            repositories::commit(&repo, "adding initial labels file")?;

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a "none" category on a branch
            let branch_name = "change-labels";
            repositories::branches::create_checkout(&repo, branch_name)?;

            test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
            repositories::add(&repo, &labels_path)?;
            repositories::commit(&repo, "adding none category")?;

            // Add a "person" category on a the main branch
            repositories::checkout(&repo, og_branch.name).await?;

            test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
            repositories::add(&repo, &labels_path)?;
            repositories::commit(&repo, "adding person category")?;

            // Try to merge in the changes
            repositories::merge::merge(&repo, branch_name)?;

            // We should have a conflict
            let status = repositories::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Assume that we fixed the conflict and added the file
            let path = status.merge_conflicts[0].base_entry.path.clone();
            let fullpath = repo.path.join(path);
            repositories::add(&repo, fullpath)?;

            // Should commit, and then see full commit history
            repositories::commit(&repo, "merging into main")?;

            // Should have commits:
            //  1) add labels
            //  2) change-labels branch modification
            //  3) main branch modification
            //  4) merge commit
            let history = repositories::commits::list(&repo)?;
            assert_eq!(history.len(), 4);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_with_no_staged_changes() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Add a text file
            let text_path = repo.path.join("text.txt");
            util::fs::write_to_path(&text_path, "Hello World")?;

            // Get the hash of the file at this timestamp
            repositories::add(&repo, &text_path)?;
            repositories::commit(&repo, "Committing hello world")?;

            // Modify the text file
            util::fs::write_to_path(&text_path, "Goodbye, world!")?;

            let status = repositories::status(&repo)?;
            status.print();

            // There should be nothing to commit since the file is untracked
            let commit_result = repositories::commit(&repo, "Committing goodbye world");
            assert!(commit_result.is_err());

            // Make sure the entry is still there
            let head = repositories::commits::head_commit(&repo)?;
            let tree = repositories::tree::get_by_commit(&repo, &head)?;
            let text_entry = tree.get_by_path(Path::new("text.txt"))?;
            assert!(text_entry.is_some());

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_commit_hash_on_modified_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Add a text file
            let text_path = repo.path.join("text.txt");
            util::fs::write_to_path(&text_path, "Hello World")?;

            // Get the hash of the file at this timestamp
            let hash_when_add =
                MerkleHash::from_str(&util::hasher::hash_file_contents(&text_path)?)?;
            repositories::add(&repo, &text_path)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Note v10 did not have this line, and we didn't copy to the versions dir on add
            repositories::commit(&repo, "Committing hello world")?;

            // Modify the text file
            util::fs::write_to_path(&text_path, "Goodbye, world!")?;

            // Get the new hash
            let hash_after_modification =
                MerkleHash::from_str(&util::hasher::hash_file_contents(&text_path)?)?;

            // Add and commit the file
            repositories::add(&repo, &text_path)?;
            repositories::commit(&repo, "Committing goodbye world")?;

            // Get the most recent commit - the new head commit
            let head = repositories::commits::head_commit(&repo)?;

            // get the merkle tree for the commit
            let tree = repositories::tree::get_by_commit(&repo, &head)?;
            println!("tree after second commit");
            tree.print();

            // Get the commit entry for the text file
            let text_entry = tree.get_by_path(Path::new("text.txt"))?.unwrap();

            // Hashes should be different
            assert_ne!(hash_when_add, hash_after_modification);

            // Hash should match new hash
            assert_eq!(text_entry.hash, hash_after_modification);

            Ok(())
        })
    }

    #[test]
    fn test_commit_file_and_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create committer with no commits
            let repo_path = &repo.path;
            let train_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&train_dir)?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 1")?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 2")?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 3")?;
            let annotation_file = test::add_txt_file_to_dir(repo_path, "some annotations...")?;

            let test_dir = repo_path.join("test_data");
            std::fs::create_dir_all(&test_dir)?;
            let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 1")?;
            let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 2")?;

            // Add a file and a directory
            repositories::add(&repo, &annotation_file)?;
            repositories::add(&repo, &train_dir)?;

            let message = "Adding training data to 🐂";
            repositories::commit(&repo, message)?;

            // should be one commit now
            let commit_history = repositories::commits::list(&repo)?;
            assert_eq!(commit_history.len(), 1);

            // Check that the files are no longer staged
            let status = repositories::status(&repo)?;
            let files = status.staged_files;
            let dirs = status.staged_dirs;
            assert_eq!(files.len(), 0);
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_commit_history_is_complete() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let deep_clone =
                    repositories::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                // Get head commit of deep_clone repo
                let head_commit = repositories::commits::head_commit(&deep_clone)?;
                assert!(commit_history_is_complete(&deep_clone, &head_commit)?);
                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_history_is_not_complete_standard_repo() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let clone = repositories::clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;
                // Get head commit of deep_clone repo
                let head_commit = repositories::commits::head_commit(&clone)?;
                assert!(!commit_history_is_complete(&clone, &head_commit)?);
                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[test]
    fn test_commit_history_order() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let train_dir = repo.path.join("train");
            repositories::add(&repo, train_dir)?;
            let initial_commit_message = "adding train dir";
            repositories::commit(&repo, initial_commit_message)?;

            // Write a text file
            let text_path = repo.path.join("newnewnew.txt");
            util::fs::write_to_path(&text_path, "Hello World")?;
            repositories::add(&repo, &text_path)?;
            repositories::commit(&repo, "adding text file")?;

            let test_dir = repo.path.join("test");
            repositories::add(&repo, test_dir)?;
            let most_recent_message = "adding test dir";
            repositories::commit(&repo, most_recent_message)?;

            let history = repositories::commits::list(&repo)?;
            assert_eq!(history.len(), 3);

            assert_eq!(history.first().unwrap().message, most_recent_message);
            assert_eq!(history.last().unwrap().message, initial_commit_message);

            Ok(())
        })
    }

    #[test]
    fn test_get_commit_history_list_between() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let new_file = repo.path.join("new_1.txt");
            test::write_txt_file_to_path(&new_file, "new 1")?;
            repositories::add(&repo, new_file)?;
            let base_commit = repositories::commit(&repo, "commit 1")?;

            let new_file = repo.path.join("new_2.txt");
            test::write_txt_file_to_path(&new_file, "new 2")?;
            repositories::add(&repo, new_file)?;
            repositories::commit(&repo, "commit 2")?;

            let new_file = repo.path.join("new_3.txt");
            test::write_txt_file_to_path(&new_file, "new 3")?;
            repositories::add(&repo, new_file)?;
            let head_commit = repositories::commit(&repo, "commit 3")?;

            let new_file = repo.path.join("new_4.txt");
            test::write_txt_file_to_path(&new_file, "new 4")?;
            repositories::add(&repo, new_file)?;
            repositories::commit(&repo, "commit 4")?;

            let history = repositories::commits::list_between(&repo, &head_commit, &base_commit)?;
            assert_eq!(history.len(), 3);

            assert_eq!(history.first().unwrap().message, head_commit.message);
            assert_eq!(history.last().unwrap().message, base_commit.message);

            Ok(())
        })
    }

    #[test]
    fn test_commit_subdir_then_root_file() -> Result<(), OxenError> {
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

            let tree_1 = repositories::tree::get_by_commit(&repo, &commit_1)?;
            println!("INITIAL commit_1: {}", commit_1);
            tree_1.print();

            // New file in root
            let file_path_2 = Path::new("test_file_2.txt");
            let file_repo_path_2 = repo.path.join(file_path_2);
            util::fs::write_to_path(&file_repo_path_2, "test")?;

            // Add the file
            repositories::add(&repo, &file_repo_path_2)?;
            let commit_2 = repositories::commit(&repo, "adding test file")?;

            let tree_1 = repositories::tree::get_by_commit(&repo, &commit_1)?;
            println!("AFTER commit_1: {}", commit_1);
            tree_1.print();

            let tree_2 = repositories::tree::get_by_commit(&repo, &commit_2)?;
            println!("AFTER commit_2: {}", commit_2);
            tree_2.print();

            // Make sure the file is not in the first commit
            // This was biting us in an initial implementation
            // BECAUSE the file contents was the same, the hash was not updated
            let node_from_tree_1 = tree_1.get_by_path(file_path_2)?;
            assert!(node_from_tree_1.is_none());

            // Make sure the file is in the second commit
            let node_from_tree_2 = tree_2.get_by_path(file_path_2)?;
            assert!(node_from_tree_2.is_some());

            Ok(())
        })
    }

    #[test]
    fn test_add_and_commit_empty_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Make an empty dir
            let empty_dir = repo.path.join("empty_dir");
            util::fs::create_dir_all(&empty_dir)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should find the untracked dir
            assert!(status
                .untracked_dirs
                .iter()
                .any(|(path, _)| *path == PathBuf::from("empty_dir")));

            // Add the empty dir
            repositories::add(&repo, &empty_dir)?;

            let status = repositories::status(&repo)?;
            status.print();

            let commit = repositories::commit(&repo, "adding empty dir")?;

            let tree = repositories::tree::get_by_commit(&repo, &commit)?;
            println!("tree after commit: {}", commit);
            tree.print();

            assert!(tree.get_by_path(PathBuf::from("empty_dir"))?.is_some());

            Ok(())
        })
    }
}
