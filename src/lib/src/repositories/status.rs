//! # oxen status
//!
//! Check which files have been modified, added, or removed,
//! and which files are staged for commit.
//!

use std::path::Path;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{LocalRepository, StagedData};

/// # oxen status
///
/// Get status of files in repository, returns what files are tracked,
/// added, untracked, etc
///
/// Empty Repository:
///
/// ```
/// use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("repo_dir_status_1");
/// // Initialize empty repo
/// let repo = repositories::init(&base_dir)?;
/// // Get status on repo
/// let status = repositories::status(&repo)?;
/// assert!(status.is_clean());
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
///
/// Repository with files
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// let base_dir = Path::new("repo_dir_status_2");
/// // Initialize empty repo
/// let repo = repositories::init(&base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Get status on repo
/// let status = repositories::status(&repo)?;
/// assert_eq!(status.untracked_files.len(), 1);
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::status::status(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::status::status(repo),
    }
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::status::status_from_dir(repo, dir),
        MinOxenVersion::V0_19_0 => core::v0_19_0::status::status_from_dir(repo, dir),
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::opts::RestoreOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use std::path::Path;
    use std::path::PathBuf;

    #[test]
    fn test_command_status_empty() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let repo_status = repositories::status(&repo)?;

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_command_status_nothing_staged_full_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let repo_status = repositories::status(&repo)?;

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            // README.md
            // labels.txt
            // prompts.jsonl
            assert_eq!(repo_status.untracked_files.len(), 3);
            // train/
            // test/
            // nlp/
            // large_files/
            // annotations/
            assert_eq!(repo_status.untracked_dirs.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_command_add_one_file_top_level() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            repositories::add(&repo, repo.path.join(Path::new("labels.txt")))?;

            let repo_status = repositories::status(&repo)?;
            repo_status.print();

            // TODO: v0_10_0 logic should have no dirs staged
            // root dir should be staged
            assert_eq!(repo_status.staged_dirs.len(), 1);
            // labels.txt
            assert_eq!(repo_status.staged_files.len(), 1);
            // README.md
            // prompts.jsonl
            assert_eq!(repo_status.untracked_files.len(), 2);
            // train/
            // test/
            // nlp/
            // large_files/
            // annotations/
            assert_eq!(repo_status.untracked_dirs.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_command_status_shows_intermediate_directory_if_file_added() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Add a deep file
            repositories::add(
                &repo,
                repo.path.join(Path::new("annotations/train/one_shot.csv")),
            )?;

            // Make sure that we now see the full annotations/train/ directory
            let repo_status = repositories::status(&repo)?;
            repo_status.print();

            // annotations/
            assert_eq!(repo_status.staged_dirs.len(), 1);
            // annotations/train/one_shot.csv
            assert_eq!(repo_status.staged_files.len(), 1);
            // annotations/test/
            // train/
            // large_files/
            // test/
            // nlp/
            assert_eq!(repo_status.untracked_dirs.len(), 5);
            // README.md
            // labels.txt
            // prompts.jsonl
            // annotations/README.md
            // annotations/train/two_shot.csv
            // annotations/train/annotations.txt
            // annotations/train/bounding_box.csv
            assert_eq!(repo_status.untracked_files.len(), 7);

            Ok(())
        })
    }

    #[test]
    fn test_command_commit_nothing_staged() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let initial_len = commits.len();
            let result = repositories::commit(&repo, "Should not work");
            assert!(result.is_err());
            let commits = repositories::commits::list(&repo)?;
            // We should not have added any commits
            assert_eq!(commits.len(), initial_len);
            Ok(())
        })
    }

    #[test]
    fn test_command_commit_nothing_staged_but_file_modified() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let initial_len = commits.len();

            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(labels_path, "changing this guy, but not committing")?;

            let result = repositories::commit(&repo, "Should not work");
            assert!(result.is_err());
            let commits = repositories::commits::list(&repo)?;
            // We should not have added any commits
            assert_eq!(commits.len(), initial_len);
            Ok(())
        })
    }

    #[test]
    fn test_command_status_has_txt_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(hello_file, "Hello World")?;

            // Get status
            let repo_status = repositories::status(&repo)?;
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 1);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merge_conflict_shows_in_status() -> Result<(), OxenError> {
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
            let commit = repositories::merge::merge(&repo, branch_name)?;

            // Make sure we didn't get a commit out of it
            assert!(commit.is_none());

            // Make sure we can access the conflicts in the status command
            let status = repositories::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_rm_regular_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // Move the file to a new name
            let og_basename = PathBuf::from("README.md");
            let og_file = repo.path.join(&og_basename);
            util::fs::remove_file(og_file)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.removed_files.len(), 1);

            let opts = RmOpts::from_path(&og_basename);
            repositories::rm(&repo, &opts)?;
            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), 1);
            assert_eq!(
                status.staged_files[&og_basename].status,
                StagedEntryStatus::Removed
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_rm_directory_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // Move the file to a new name
            let og_basename = PathBuf::from("README.md");
            let og_file = repo.path.join(&og_basename);
            util::fs::remove_file(og_file)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.removed_files.len(), 1);

            let opts = RmOpts::from_path(&og_basename);
            repositories::rm(&repo, &opts)?;
            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), 1);
            assert_eq!(
                status.staged_files[&og_basename].status,
                StagedEntryStatus::Removed
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_move_regular_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // Move `README.md` to `README2.md`
            let og_basename = PathBuf::from("README.md");
            let og_file = repo.path.join(&og_basename);
            let new_basename = PathBuf::from("README2.md");
            let new_file = repo.path.join(new_basename);

            util::fs::rename(&og_file, &new_file)?;

            // Status before
            let status = repositories::status(&repo)?;

            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.removed_files.len(), 1);
            assert_eq!(status.untracked_files.len(), 1);

            // Add one file...
            repositories::add(&repo, &og_file)?;
            let status = repositories::status(&repo)?;
            // No notion of movement until the pair are added
            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.staged_files.len(), 1);

            // Complete the pair
            repositories::add(&repo, &new_file)?;
            let status = repositories::status(&repo)?;
            assert_eq!(status.moved_files.len(), 1);
            assert_eq!(status.staged_files.len(), 2); // Staged files still operates on the addition + removal

            // Restore one file and break the pair
            repositories::restore(&repo, RestoreOpts::from_staged_path(og_basename))?;

            // Pair is broken; no more "moved"
            let status = repositories::status(&repo)?;
            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_move_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // Move train to to new_train/train2
            let og_basename = PathBuf::from("train");
            let og_dir = repo.path.join(og_basename);
            let new_basename = PathBuf::from("new_train").join("train2");
            let new_dir = repo.path.join(new_basename);

            // Create the dir before move
            util::fs::create_dir_all(&new_dir)?;
            util::fs::rename(&og_dir, &new_dir)?;

            let status = repositories::status(&repo)?;
            println!("status after rename: {status:?}");
            status.print();
            assert_eq!(status.moved_files.len(), 0);
            // TODO: v0_10_0 logic should have root and new_train/train2
            assert_eq!(status.untracked_dirs.len(), 1);
            // TODO: v0_10_0 test had 5 removed files here, but when the entire
            // directory was moved it doesn't make sense to show individual files
            assert_eq!(status.removed_files.len(), 1);

            // Add the removals
            repositories::add(&repo, &og_dir)?;
            // repositories::add(&repo, &new_dir)?;

            let status = repositories::status(&repo)?;
            // No moved files, 5 staged (the removals)
            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.staged_files.len(), 5);
            assert_eq!(status.staged_dirs.len(), 1);

            // Complete the pairs
            repositories::add(&repo, &new_dir)?;
            let status = repositories::status(&repo)?;
            assert_eq!(status.moved_files.len(), 5);
            assert_eq!(status.staged_files.len(), 10);
            assert_eq!(status.staged_dirs.len(), 2);
            Ok(())
        })
        .await
    }

    #[test]
    fn test_status_list_added_directories() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write two files to a sub directory
            let repo_path = &repo.path;
            let training_data_dir = PathBuf::from("training_data");
            let sub_dir = repo_path.join(&training_data_dir);
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            repositories::add(&repo, &sub_dir)?;

            // List files
            let status = repositories::status(&repo)?;
            println!("status: {status:?}");
            status.print();
            let dirs = status.staged_dirs;

            // TODO: v0_10_0 logic should have root and training_data
            // We should just have training_data staged
            assert_eq!(dirs.len(), 1);
            let added_dir = dirs.get(&training_data_dir).unwrap();
            assert_eq!(added_dir.path, training_data_dir);

            Ok(())
        })
    }

    #[test]
    fn test_status_remove_file_top_level() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Get head commit
            // List all entries in that commit
            let repo_path = &repo.path;
            let file_to_rm = repo_path.join("labels.txt");

            let status = repositories::status(&repo)?;
            status.print();

            // Remove a committed file
            util::fs::remove_file(&file_to_rm)?;

            // List removed
            let status = repositories::status(&repo)?;
            status.print();
            let files = status.removed_files;

            // There is one removed file, and nothing else
            assert_eq!(files.len(), 1);
            assert_eq!(status.staged_dirs.len(), 0);
            assert_eq!(status.staged_files.len(), 0);
            assert_eq!(status.untracked_dirs.len(), 0);
            assert_eq!(status.untracked_files.len(), 0);
            assert_eq!(status.modified_files.len(), 0);

            // And it is
            let relative_path = util::fs::path_relative_to_dir(&file_to_rm, repo_path)?;
            assert!(files.contains(&relative_path));

            Ok(())
        })
    }

    #[test]
    fn test_status_remove_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let repo_path = &repo.path;
            let one_shot_file = repo_path
                .join("annotations")
                .join("train")
                .join("one_shot.csv");

            // Remove a committed file
            util::fs::remove_file(&one_shot_file)?;

            // List removed
            let status = repositories::status(&repo)?;
            status.print();
            let files = status.removed_files;

            // There is one removed file
            assert_eq!(files.len(), 1);

            // And it is
            let relative_path = util::fs::path_relative_to_dir(&one_shot_file, repo_path)?;
            assert!(files.contains(&relative_path));

            Ok(())
        })
    }

    #[test]
    fn test_status_modify_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let repo_path = &repo.path;
            let one_shot_file = repo_path
                .join("annotations")
                .join("train")
                .join("one_shot.csv");

            // Modify the committed file
            let one_shot_file = test::modify_txt_file(one_shot_file, "new content coming in hot")?;

            // List modified
            let status = repositories::status(&repo)?;
            status.print();
            let files = status.modified_files;

            // There is one modified file
            assert_eq!(files.len(), 1);

            // And it is
            let relative_path = util::fs::path_relative_to_dir(one_shot_file, repo_path)?;
            assert!(files.contains(&relative_path));

            Ok(())
        })
    }

    #[test]
    fn test_status_list_untracked_directories_after_add() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 2 sub directories, one with  Write two files to a sub directory
            let repo_path = &repo.path;
            let train_dir = repo_path.join("train");
            std::fs::create_dir_all(&train_dir)?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_1.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_1.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_2.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_2.jpg"))?;

            let test_dir = repo_path.join("test");
            std::fs::create_dir_all(&test_dir)?;
            let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/cat_3.jpg"))?;
            let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/dog_3.jpg"))?;

            let valid_dir = repo_path.join("valid");
            std::fs::create_dir_all(&valid_dir)?;
            let _ = test::add_img_file_to_dir(&valid_dir, Path::new("data/test/images/dog_4.jpg"))?;

            let base_file_1 = test::add_txt_file_to_dir(repo_path, "Hello 1")?;
            let _base_file_2 = test::add_txt_file_to_dir(repo_path, "Hello 2")?;
            let _base_file_3 = test::add_txt_file_to_dir(repo_path, "Hello 3")?;

            // At first there should be 3 untracked
            let untracked_dirs = repositories::status(&repo)?.untracked_dirs;
            assert_eq!(untracked_dirs.len(), 3);

            // Add the directory
            repositories::add(&repo, &train_dir)?;
            // Add one file
            repositories::add(&repo, &base_file_1)?;

            // List the files
            let status = repositories::status(&repo)?;
            println!("status: {status:?}");
            status.print();
            let staged_files = status.staged_files;
            let staged_dirs = status.staged_dirs;
            let untracked_files = status.untracked_files;
            let untracked_dirs = status.untracked_dirs;

            // There is 5 added file and 2 added dirs (root + train)
            assert_eq!(staged_files.len(), 5);
            assert_eq!(staged_dirs.len(), 2);

            // There are 2 untracked files
            assert_eq!(untracked_files.len(), 2);
            // There are 2 untracked dirs at the top level
            assert_eq!(untracked_dirs.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_status_list_modified_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create entry_reader with no commits
            let repo_path = &repo.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // add the file
            repositories::add(&repo, &hello_file)?;

            // commit the file
            repositories::commit(&repo, "added hello 1")?;

            let status = repositories::status(&repo)?;
            let mod_files = status.modified_files;
            assert_eq!(mod_files.len(), 0);

            // modify the file
            let hello_file = test::modify_txt_file(hello_file, "Hello 2")?;

            // List files
            let status = repositories::status(&repo)?;
            status.print();
            let mod_files = status.modified_files;
            assert_eq!(mod_files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(hello_file, repo_path)?;
            assert!(mod_files.contains(&relative_path));

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_command_status_modified_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Track & commit all the data
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            repositories::add(&repo, &repo.path)?;
            repositories::commit(&repo, "Adding one shot")?;

            let branch_name = "feature/modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
            test::modify_txt_file(one_shot_path, file_contents)?;
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.modified_files.len(), 1);
            assert!(status
                .modified_files
                .contains(&PathBuf::from("annotations/train/one_shot.csv")));

            Ok(())
        })
        .await
    }
}
