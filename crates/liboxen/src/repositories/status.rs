//! # oxen status
//!
//! Check which files have been modified, added, or removed,
//! and which files are staged for commit.
//!

/// # oxen status
///
/// Get status of files in repository, returns what files are tracked,
/// added, untracked, etc
///
/// Empty Repository:
///
/// ```ignore
/// use liboxen::repositories;
///
/// let base_dir = Path::new("repo_dir_status_1");
/// // Initialize empty repo
/// let repo = repositories::init(&base_dir)?;
/// // Get status on repo
/// let status = repositories::status(&repo).await?;
/// assert!(status.is_clean());
/// ```
///
/// Repository with files
/// ```ignore
/// use liboxen::repositories;
/// use liboxen::util;
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
/// let status = repositories::status(&repo).await?;
/// assert_eq!(status.untracked_files.len(), 1);
/// ```
pub use crate::core::v_latest::status::status;

pub use crate::core::v_latest::status::status_from_opts;

pub use crate::core::v_latest::status::status_from_dir;

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::model::staged_data::StagedDataOpts;
    use crate::opts::RestoreOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;

    use crate::util;

    use std::collections::HashSet;
    use std::path::Path;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_command_status_empty() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let repo_status = repositories::status(&repo).await?;

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    /// Regression: a file whose working copy differs from HEAD AND whose HEAD-expected
    /// blob is missing from the local version store should be reported as "unrestorable",
    /// not "modified". The two have different remediation paths (`oxen restore` vs.
    /// `oxen fetch --missing-files`); status used to lump them together, leaving users
    /// (Kaga, in the Nex stuck-pull saga) running restore in a loop while it silently
    /// no-op'd.
    async fn test_status_distinguishes_unrestorable_from_modified() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("hello.txt");
            util::fs::write_to_path(&path, "Hello World")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            // Drift the working copy.
            util::fs::write_to_path(&path, "drifted")?;

            // Sanity: status flags it as plain `modified` while the blob is still in
            // the local version store.
            let status_before = repositories::status(&repo).await?;
            assert!(
                status_before
                    .modified_files
                    .iter()
                    .any(|p| p.ends_with("hello.txt"))
            );
            assert!(status_before.unrestorable_files.is_empty());

            // Wipe the blob HEAD expects for hello.txt. After this, `oxen restore` would
            // hit `RestoreFailed` / `VersionStoreDataMissing`.
            let head_node = repositories::tree::get_node_by_path(&repo, &commit, "hello.txt")?
                .expect("hello.txt must be in HEAD's tree");
            let blob_hash = head_node.hash.to_string();
            let version_store = repo.version_store();
            assert!(version_store.version_exists(&blob_hash).await?);
            version_store.delete_version(&blob_hash).await?;

            let status_after = repositories::status(&repo).await?;
            assert!(
                status_after
                    .unrestorable_files
                    .iter()
                    .any(|p| p.ends_with("hello.txt")),
                "expected hello.txt under unrestorable_files, got: {:?}",
                status_after.unrestorable_files
            );
            assert!(
                !status_after
                    .modified_files
                    .iter()
                    .any(|p| p.ends_with("hello.txt")),
                "expected hello.txt NOT under modified_files (it's unrestorable), got: {:?}",
                status_after.modified_files
            );
            // is_clean and has_modified_entries should account for unrestorable files
            // the same as modified ones.
            assert!(!status_after.is_clean());
            assert!(status_after.has_modified_entries());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_status_nothing_staged_full_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let repo_status = repositories::status(&repo).await?;

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            // README.md
            // labels.txt
            // prompts.jsonl
            // LICENSE
            assert_eq!(repo_status.untracked_files.len(), 4);
            // train/
            // test/
            // nlp/
            // large_files/
            // annotations/
            assert_eq!(repo_status.untracked_dirs.len(), 5);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_one_file_top_level() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            repositories::add(&repo, repo.path.join(Path::new("labels.txt"))).await?;

            let repo_status = repositories::status(&repo).await?;
            repo_status.print();

            // root dir should be staged
            assert_eq!(repo_status.staged_dirs.len(), 1);
            // labels.txt
            assert_eq!(repo_status.staged_files.len(), 1);
            // README.md
            // prompts.jsonl
            // LICENSE
            assert_eq!(repo_status.untracked_files.len(), 3);
            // train/
            // test/
            // nlp/
            // large_files/
            // annotations/
            assert_eq!(repo_status.untracked_dirs.len(), 5);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_status_shows_intermediate_directory_if_file_added()
    -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Add a deep file
            repositories::add(
                &repo,
                repo.path.join(Path::new("annotations/train/one_shot.csv")),
            )
            .await?;

            // Make sure that we now see the full annotations/train/ directory
            let repo_status = repositories::status(&repo).await?;
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
            // LICENSE
            // annotations/README.md
            // annotations/train/two_shot.csv
            // annotations/train/annotations.txt
            // annotations/train/bounding_box.csv
            assert_eq!(repo_status.untracked_files.len(), 8);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_modified_files_status_with_search_paths() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a deep file and a shallow file.
            let train_dir = repo.path.join("annotations").join("train");
            util::fs::create_dir_all(&train_dir)?;
            let one_shot_relative_path = Path::new("annotations/train/one_shot.csv");
            let one_shot_path = repo.path.join(one_shot_relative_path);
            util::fs::write_to_path(&one_shot_path, "one shot")?;
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "labels")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "add files")?;

            test::modify_txt_file(&one_shot_path, "new one shot coming in hot")?;
            test::modify_txt_file(&labels_path, "new labels coming in hot")?;

            // Search path scoped to the deep directory.
            let opts = StagedDataOpts::from_paths(&[train_dir]);

            let repo_status = repositories::status::status_from_opts(&repo, &opts).await?;

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            // We should only see the modified file in the annotations/train/ directory
            assert_eq!(repo_status.modified_files.len(), 1);
            assert!(
                repo_status
                    .modified_files
                    .contains(&one_shot_relative_path.to_path_buf())
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_modified_files_status_with_file_search_paths() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a deep file and a shallow file.
            let train_dir = repo.path.join("annotations").join("train");
            util::fs::create_dir_all(&train_dir)?;
            let one_shot_relative_path = Path::new("annotations/train/one_shot.csv");
            let one_shot_path = repo.path.join(one_shot_relative_path);
            util::fs::write_to_path(&one_shot_path, "one shot")?;
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "labels")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "add files")?;

            test::modify_txt_file(&one_shot_path, "new one shot coming in hot")?;
            test::modify_txt_file(&labels_path, "new labels coming in hot")?;

            // Search path scoped to the deep file itself.
            let opts = StagedDataOpts::from_paths(std::slice::from_ref(&one_shot_path));

            let repo_status = repositories::status::status_from_opts(&repo, &opts).await?;

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            // We should only see the modified file in the annotations/train/ directory
            assert_eq!(repo_status.modified_files.len(), 1);
            assert!(
                repo_status
                    .modified_files
                    .contains(&one_shot_relative_path.to_path_buf())
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_ignore_directory_with_modified_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a deep file (under annotations/) and a shallow file.
            let train_dir = repo.path.join("annotations").join("train");
            util::fs::create_dir_all(&train_dir)?;
            let one_shot_path = train_dir.join("one_shot.csv");
            util::fs::write_to_path(&one_shot_path, "one shot")?;
            let labels_relative_path = Path::new("labels.txt");
            let labels_path = repo.path.join(labels_relative_path);
            util::fs::write_to_path(&labels_path, "labels")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "add files")?;

            test::modify_txt_file(&one_shot_path, "new one shot coming in hot")?;
            test::modify_txt_file(&labels_path, "new labels coming in hot")?;

            // Ignore the annotations/ directory — only the shallow file should show.
            let opts = StagedDataOpts {
                ignore: Some(HashSet::from([repo.path.join("annotations")])),
                ..Default::default()
            };

            let repo_status = repositories::status::status_from_opts(&repo, &opts).await?;

            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            assert_eq!(repo_status.modified_files.len(), 1);
            assert!(
                repo_status
                    .modified_files
                    .contains(&labels_relative_path.to_path_buf())
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_added_files_status_with_search_paths() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit two deep files.
            let train_dir = repo.path.join("annotations").join("train");
            util::fs::create_dir_all(&train_dir)?;
            let one_shot_relative_path = Path::new("annotations/train/one_shot.csv");
            let one_shot_path = repo.path.join(one_shot_relative_path);
            let two_shot_relative_path = Path::new("annotations/train/two_shot.csv");
            let two_shot_path = repo.path.join(two_shot_relative_path);
            util::fs::write_to_path(&one_shot_path, "one shot")?;
            util::fs::write_to_path(&two_shot_path, "two shot")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "add train files")?;

            // Stage a modification to one deep file
            test::modify_txt_file(&one_shot_path, "new one shot coming in hot")?;
            repositories::add(&repo, &one_shot_path).await?;

            // Modify another deep file without staging
            test::modify_txt_file(&two_shot_path, "new two shot coming in hot")?;

            // Write an untracked file at the repo root (outside the search path)
            let untracked_path = repo.path.join("untracked.txt");
            util::fs::write_to_path(&untracked_path, "I'm sneaking in there untracked")?;

            let opts = StagedDataOpts::from_paths(&[train_dir]);

            let repo_status = repositories::status::status_from_opts(&repo, &opts).await?;

            // Make sure that we only see the modified files
            assert_eq!(repo_status.staged_dirs.len(), 1);
            assert_eq!(repo_status.staged_files.len(), 1);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            // We should only see the modified file in the annotations/train/ directory
            assert_eq!(repo_status.modified_files.len(), 1);
            assert!(
                repo_status
                    .modified_files
                    .contains(&two_shot_relative_path.to_path_buf())
            );

            // Make sure we can see the staged file
            assert_eq!(repo_status.staged_files.len(), 1);
            assert!(
                repo_status
                    .staged_files
                    .contains_key(&one_shot_relative_path.to_path_buf())
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_commit_nothing_staged() -> Result<(), OxenError> {
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

    #[tokio::test]
    async fn test_command_commit_nothing_staged_but_file_modified() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "original")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "add labels")?;

            let initial_len = repositories::commits::list(&repo)?.len();

            // Modify a committed file without staging it.
            util::fs::write_to_path(&labels_path, "changing this guy, but not committing")?;

            let result = repositories::commit(&repo, "Should not work");
            assert!(result.is_err());
            // We should not have added any commits
            assert_eq!(repositories::commits::list(&repo)?.len(), initial_len);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_status_has_txt_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(hello_file, "Hello World")?;

            // Get status
            let repo_status = repositories::status(&repo).await?;
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 1);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_conflict_shows_in_status() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            let labels_path = repo.path.join("labels.txt");
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "adding initial labels file")?;

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a "none" category on a branch
            let branch_name = "change-labels";
            repositories::branches::create_checkout(&repo, branch_name)?;

            test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "adding none category")?;

            // Add a "person" category on a the main branch
            repositories::checkout(&repo, og_branch.name).await?;

            test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "adding person category")?;

            // Try to merge in the changes
            let commit = repositories::merge::merge(&repo, branch_name).await?;

            // Make sure we didn't get a commit out of it
            assert!(commit.is_none());

            // Make sure we can access the conflicts in the status command
            let status = repositories::status(&repo).await?;
            assert_eq!(status.merge_conflicts.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_rm_regular_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a single file, then delete it from disk.
            let og_basename = PathBuf::from("README.md");
            let og_file = repo.path.join(&og_basename);
            util::fs::write_to_path(&og_file, "hello")?;
            repositories::add(&repo, &og_file).await?;
            repositories::commit(&repo, "add README")?;

            util::fs::remove_file(&og_file)?;

            let status = repositories::status(&repo).await?;
            assert_eq!(status.removed_files.len(), 1);

            let opts = RmOpts::from_path(&og_basename);
            repositories::rm(&repo, &opts).await?;
            let status = repositories::status(&repo).await?;

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
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a single file, then move `README.md` to `README2.md`
            let og_basename = PathBuf::from("README.md");
            let og_file = repo.path.join(&og_basename);
            util::fs::write_to_path(&og_file, "readme")?;
            repositories::add(&repo, &og_file).await?;
            repositories::commit(&repo, "add README")?;

            let new_basename = PathBuf::from("README2.md");
            let new_file = repo.path.join(new_basename);

            util::fs::rename(&og_file, &new_file)?;

            // Status before
            let status = repositories::status(&repo).await?;

            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.removed_files.len(), 1);
            assert_eq!(status.untracked_files.len(), 1);

            // Add one file...
            repositories::add(&repo, &og_file).await?;
            let status = repositories::status(&repo).await?;
            // No notion of movement until the pair are added
            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.staged_files.len(), 1);

            // Complete the pair
            repositories::add(&repo, &new_file).await?;
            let status = repositories::status(&repo).await?;
            assert_eq!(status.moved_files.len(), 1);
            assert_eq!(status.staged_files.len(), 2); // Staged files still operates on the addition + removal

            // Restore one file and break the pair
            repositories::restore(&repo, RestoreOpts::from_staged_path(og_basename)).await?;

            // Pair is broken; no more "moved"
            let status = repositories::status(&repo).await?;
            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_move_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a directory of 7 files so the move-pair counts below line up.
            let og_basename = PathBuf::from("train");
            let og_dir = repo.path.join(og_basename);
            util::fs::create_dir_all(&og_dir)?;
            test::populate_dir_with_txt_files(&og_dir, "img", 7)?;
            repositories::add(&repo, &og_dir).await?;
            repositories::commit(&repo, "add train dir")?;

            // Move train/ to new_train/train2
            let new_basename = PathBuf::from("new_train").join("train2");
            let new_dir = repo.path.join(new_basename);

            // Create the dir before move
            util::fs::create_dir_all(&new_dir)?;
            util::fs::rename(&og_dir, &new_dir)?;

            let status = repositories::status(&repo).await?;
            status.print();
            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.untracked_dirs.len(), 1);
            // When the whole directory was moved, individual files aren't listed as
            // removed — only the directory move is reported.
            assert_eq!(status.removed_files.len(), 1);

            // Add the removals
            repositories::add(&repo, &og_dir).await?;
            // repositories::add(&repo, &new_dir)?;

            let status = repositories::status(&repo).await?;
            // No moved files, 7 staged (the removals)
            assert_eq!(status.moved_files.len(), 0);
            assert_eq!(status.staged_files.len(), 7);
            assert_eq!(status.staged_dirs.len(), 1);

            // Complete the pairs
            repositories::add(&repo, &new_dir).await?;
            let status = repositories::status(&repo).await?;
            assert_eq!(status.moved_files.len(), 7);
            assert_eq!(status.staged_files.len(), 14);
            assert_eq!(status.staged_dirs.len(), 2);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_list_added_directories() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write two files to a sub directory
            let repo_path = &repo.path;
            let training_data_dir = PathBuf::from("training_data");
            let sub_dir = repo_path.join(&training_data_dir);
            util::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            repositories::add(&repo, &sub_dir).await?;

            // List files
            let status = repositories::status(&repo).await?;
            println!("status: {status:?}");
            status.print();
            let dirs = status.staged_dirs;

            // We should just have training_data staged
            assert_eq!(dirs.len(), 1);
            let added_dir = dirs.get(&training_data_dir).unwrap();
            assert_eq!(added_dir.path, training_data_dir);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_remove_file_top_level() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let repo_path = &repo.path;
            let file_to_rm = repo_path.join("labels.txt");
            util::fs::write_to_path(&file_to_rm, "labels")?;
            repositories::add(&repo, &file_to_rm).await?;
            repositories::commit(&repo, "add labels")?;

            // Remove the only committed file
            util::fs::remove_file(&file_to_rm)?;

            let status = repositories::status(&repo).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_status_remove_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let repo_path = &repo.path;
            let train_dir = repo_path.join("annotations").join("train");
            util::fs::create_dir_all(&train_dir)?;
            let one_shot_file = train_dir.join("one_shot.csv");
            util::fs::write_to_path(&one_shot_file, "a,b\n1,2\n")?;
            util::fs::write_to_path(train_dir.join("two_shot.csv"), "a,b\n3,4\n")?;
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "add train dir")?;

            // Remove a committed file (leaving a sibling so the dir stays tracked)
            util::fs::remove_file(&one_shot_file)?;

            let status = repositories::status(&repo).await?;
            let files = status.removed_files;
            assert_eq!(files.len(), 1);

            let relative_path = util::fs::path_relative_to_dir(&one_shot_file, repo_path)?;
            assert!(files.contains(&relative_path));

            Ok(())
        })
        .await
    }

    // Regression test: passing a deleted, tracked single-file path in opts.paths
    // should classify it as removed. The dir-based tree-side check in walk_status
    // only fires for directories in dir_hashes, so a missing file path was being
    // silently dropped.
    #[tokio::test]
    async fn test_status_remove_file_with_explicit_file_path() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let repo_path = &repo.path;
            let train_dir = repo_path.join("annotations").join("train");
            util::fs::create_dir_all(&train_dir)?;
            let target_file = train_dir.join("one_shot.csv");
            util::fs::write_to_path(&target_file, "a,b\n1,2\n")?;
            util::fs::write_to_path(train_dir.join("two_shot.csv"), "a,b\n3,4\n")?;
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "add train dir")?;

            util::fs::remove_file(&target_file)?;

            let opts = StagedDataOpts::from_paths(std::slice::from_ref(&target_file));
            let status = repositories::status::status_from_opts(&repo, &opts).await?;

            let relative_path = util::fs::path_relative_to_dir(&target_file, repo_path)?;
            assert_eq!(status.removed_files.len(), 1);
            assert!(status.removed_files.contains(&relative_path));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_modify_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let repo_path = &repo.path;
            let train_dir = repo_path.join("annotations").join("train");
            util::fs::create_dir_all(&train_dir)?;
            let one_shot_file = train_dir.join("one_shot.csv");
            util::fs::write_to_path(&one_shot_file, "a,b\n1,2\n")?;
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "add train dir")?;

            // Modify the committed file
            let one_shot_file = test::modify_txt_file(one_shot_file, "new content coming in hot")?;

            let status = repositories::status(&repo).await?;
            let files = status.modified_files;
            assert_eq!(files.len(), 1);

            let relative_path = util::fs::path_relative_to_dir(one_shot_file, repo_path)?;
            assert!(files.contains(&relative_path));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_list_untracked_directories_after_add() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 2 sub directories, one with  Write two files to a sub directory
            let repo_path = &repo.path;
            let train_dir = repo_path.join("train");
            util::fs::create_dir_all(&train_dir)?;
            let _ = test::add_img_file_to_dir(
                &train_dir,
                test::REPO_ROOT.join("data/test/images/cat_1.jpg").as_path(),
            )?;
            let _ = test::add_img_file_to_dir(
                &train_dir,
                test::REPO_ROOT.join("data/test/images/dog_1.jpg").as_path(),
            )?;
            let _ = test::add_img_file_to_dir(
                &train_dir,
                test::REPO_ROOT.join("data/test/images/cat_2.jpg").as_path(),
            )?;
            let _ = test::add_img_file_to_dir(
                &train_dir,
                test::REPO_ROOT.join("data/test/images/dog_2.jpg").as_path(),
            )?;

            let test_dir = repo_path.join("test");
            util::fs::create_dir_all(&test_dir)?;
            let _ = test::add_img_file_to_dir(
                &test_dir,
                test::REPO_ROOT.join("data/test/images/cat_3.jpg").as_path(),
            )?;
            let _ = test::add_img_file_to_dir(
                &test_dir,
                test::REPO_ROOT.join("data/test/images/dog_3.jpg").as_path(),
            )?;

            let valid_dir = repo_path.join("valid");
            util::fs::create_dir_all(&valid_dir)?;
            let _ = test::add_img_file_to_dir(
                &valid_dir,
                test::REPO_ROOT.join("data/test/images/dog_4.jpg").as_path(),
            )?;

            let base_file_1 = test::add_txt_file_to_dir(repo_path, "Hello 1")?;
            let _base_file_2 = test::add_txt_file_to_dir(repo_path, "Hello 2")?;
            let _base_file_3 = test::add_txt_file_to_dir(repo_path, "Hello 3")?;

            // At first there should be 3 untracked
            let status = repositories::status(&repo).await?;
            status.print();
            let untracked_dirs = status.untracked_dirs;
            assert_eq!(untracked_dirs.len(), 3);

            // Add the directory
            repositories::add(&repo, &train_dir).await?;
            // Add one file
            repositories::add(&repo, &base_file_1).await?;

            // List the files
            let status = repositories::status(&repo).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_status_list_modified_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create entry_reader with no commits
            let repo_path = &repo.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // add the file
            repositories::add(&repo, &hello_file).await?;

            // commit the file
            repositories::commit(&repo, "added hello 1")?;

            let status = repositories::status(&repo).await?;
            let mod_files = status.modified_files;
            assert_eq!(mod_files.len(), 0);

            // modify the file
            let hello_file = test::modify_txt_file(hello_file, "Hello 2")?;

            // List files
            let status = repositories::status(&repo).await?;
            status.print();
            let mod_files = status.modified_files;
            assert_eq!(mod_files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(hello_file, repo_path)?;
            assert!(mod_files.contains(&relative_path));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_status_modified_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Track & commit all the data
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Adding one shot")?;

            let branch_name = "feature/modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
            test::modify_txt_file(one_shot_path, file_contents)?;
            let status = repositories::status(&repo).await?;
            status.print();
            assert_eq!(status.modified_files.len(), 1);
            assert!(
                status
                    .modified_files
                    .contains(&PathBuf::from("annotations/train/one_shot.csv"))
            );

            Ok(())
        })
        .await
    }
}
