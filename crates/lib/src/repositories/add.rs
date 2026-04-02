//! # oxen add
//!
//! Stage data for commit
//!

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use std::path::Path;

/// # Stage files into repository
///
/// ```ignore
/// use liboxen::repositories;
/// use liboxen::util;
///
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_add");
/// let repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// repositories::add(&repo, &hello_file).await?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// ```
pub async fn add(repo: &LocalRepository, path: impl AsRef<Path>) -> Result<(), OxenError> {
    add_all_with_version(repo, vec![path], repo.min_version()).await
}

pub async fn add_all<T: AsRef<Path>>(
    repo: &LocalRepository,
    paths: impl IntoIterator<Item = T>,
) -> Result<(), OxenError> {
    add_all_with_version(repo, paths, repo.min_version()).await
}

pub async fn add_all_with_version<T: AsRef<Path>>(
    repo: &LocalRepository,
    paths: impl IntoIterator<Item = T>,
    version: MinOxenVersion,
) -> Result<(), OxenError> {
    match version {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::add::add(repo, paths).await,
    }
}

#[cfg(test)]
mod tests {
    use crate::model::LocalRepository;
    use crate::model::StagedData;
    use crate::model::StagedEntryStatus;
    use crate::test;

    use std::path::Path;
    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::opts::clone_opts::CloneOpts;
    use crate::repositories;

    use crate::util;

    #[tokio::test]
    async fn test_clone_root_subtree_depth_1_add_file() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from("")]);
                opts.fetch_opts.depth = Some(1);
                let local_repo = repositories::clone::clone(&opts).await?;

                // Add a new file
                let hello_file = local_repo.path.join("clone_depth_1_add.txt");
                util::fs::write_to_path(
                    &hello_file,
                    "Oxen.ai is the best data version control system.",
                )?;
                repositories::add(&local_repo, &hello_file).await?;

                // Get the status and make sure the file is staged
                let status = repositories::status(&local_repo)?;
                assert_eq!(status.staged_files.len(), 1);
                assert!(
                    status
                        .staged_files
                        .contains_key(&PathBuf::from("clone_depth_1_add.txt"))
                );

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_annotations_test_subtree_add_file() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("annotations").join("test")]);
                let local_repo = repositories::clone::clone(&opts).await?;

                let annotations_test_dir = local_repo.path.join("annotations").join("test");

                // Add a new file
                let readme_file = annotations_test_dir.join("README.md");
                util::fs::write_to_path(
                    &readme_file,
                    r"
Q: What is the best data version control system?
A: Oxen.ai
",
                )?;
                repositories::add(&local_repo, &readme_file).await?;

                // Get the status and make sure the file is staged
                let status = repositories::status(&local_repo)?;
                assert_eq!(status.staged_files.len(), 1);
                assert!(
                    status
                        .staged_files
                        .contains_key(&PathBuf::from("annotations").join("test").join("README.md"))
                );

                // Make sure no files are marked as removed, because they are just not downloaded in the subtree
                assert_eq!(status.removed_files.len(), 0);

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            repositories::add(&repo, &hello_file).await?;
            // Get status and make sure it is removed from the untracked, and added to the tracked
            let repo_status = repositories::status(&repo)?;
            // TODO: v0_10_0 logic should have 0 staged_dirs
            // We stage the parent dir, so should have 1 staged_dir
            assert_eq!(repo_status.staged_dirs.len(), 1);
            assert_eq!(repo_status.staged_files.len(), 1);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_modified_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // Modify and add the file deep in a sub dir
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            let file_contents = "file,label\ntrain/cat_1.jpg,0";
            test::modify_txt_file(one_shot_path, file_contents)?;
            let status = repositories::status(&repo)?;
            println!("status: {status:?}");
            status.print();
            assert_eq!(status.modified_files.len(), 1);
            // Add the top level directory, and make sure the modified file gets added
            let annotation_dir_path = repo.path.join("annotations");
            repositories::add(&repo, annotation_dir_path).await?;
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.staged_files.len(), 1);
            repositories::commit(&repo, "Changing one shot")?;
            let status = repositories::status(&repo)?;
            assert!(status.is_clean());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_removed_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // (file already created in helper)
            let file_to_remove = repo.path.join("labels.txt");

            // Commit the file
            repositories::add(&repo, &file_to_remove).await?;
            repositories::commit(&repo, "Adding labels file")?;

            // Delete the file
            util::fs::remove_file(&file_to_remove)?;

            // We should recognize it as missing now
            let status = repositories::status(&repo)?;
            assert_eq!(status.removed_files.len(), 1);

            Ok(())
        })
        .await
    }

    // At some point we were adding rocksdb inside the working dir...def should not do that
    #[tokio::test]
    async fn test_command_add_dot_should_not_add_new_files() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let num_files = util::fs::count_files_in_dir(&repo.path);

            repositories::add(&repo, &repo.path).await?;

            // Add shouldn't add any new files in the working dir
            let num_files_after_add = util::fs::count_files_in_dir(&repo.path);

            assert_eq!(num_files, num_files_after_add);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_dot_can_add_removed_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Track a new file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Adding a file")?;

            // Remove the file and add with dot
            util::fs::remove_file(&hello_file)?;
            repositories::add(&repo, &repo.path).await?;

            // The new file should be staged as removed
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.staged_files.len(), 1);
            assert!(!hello_file.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_dot_only_adds_changed_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Track 3 new files
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            let third_file = repo.path.join("third.txt");
            util::fs::write_to_path(&third_file, "!")?;

            // Add all files with dot
            repositories::add(&repo, &repo.path).await?;

            // All files should be staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 3);

            repositories::commit(&repo, "Adding 3 files")?;

            // Alter 2 files
            util::fs::remove_file(&hello_file)?;
            test::modify_txt_file(&world_file, "MODIFIED")?;

            // Add all files again
            repositories::add(&repo, &repo.path).await?;

            // The modified and removed files should be staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_can_add_merge_conflict() -> Result<(), OxenError> {
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
            repositories::merge::merge(&repo, branch_name).await?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Assume that we fixed the conflict and added the file
            let path = status.merge_conflicts[0].base_entry.path.clone();
            let fullpath = repo.path.join(path);
            repositories::add(&repo, fullpath).await?;

            // Adding should add to added files
            let status = repositories::status(&repo)?;

            assert_eq!(status.staged_files.len(), 1);

            // Adding should get rid of the merge conflict
            assert_eq!(status.merge_conflicts.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let dir = Path::new("nlp");
            let repo_dir = repo.path.join(dir);
            repositories::add(&repo, repo_dir).await?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should add all the sub dirs
            // nlp/classification/annotations/
            assert_eq!(status.staged_dirs.len(), 1);

            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_stage_with_wildcard() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let _objects_dir = repo.path.join(".oxen/objects");

            // Modify and add the file deep in a sub dir
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            let file_contents = "file,label\ntrain/cat_1.jpg,0";
            test::modify_txt_file(one_shot_path, file_contents)?;
            let status = repositories::status(&repo)?;
            assert_eq!(status.modified_files.len(), 1);
            // Add the top level directory, and make sure the modified file gets added
            let annotation_dir_path = repo.path.join("annotations/*");
            repositories::add(&repo, annotation_dir_path).await?;
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.staged_files.len(), 1);
            repositories::commit(&repo, "Changing one shot")?;
            let status = repositories::status(&repo)?;
            assert!(status.is_clean());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_wildcard_add_remove_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let dir = Path::new("nlp");
            let repo_dir = repo.path.join(dir);
            repositories::add(&repo, repo_dir).await?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should add all the sub dirs
            // nlp/classification/annotations/
            assert_eq!(status.staged_dirs.len(), 1);
            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            repositories::commit(&repo, "Adding nlp dir")?;

            // Remove the nlp dir
            let dir = Path::new("nlp");
            let repo_nlp_dir = repo.path.join(dir);
            std::fs::remove_dir_all(repo_nlp_dir)?;

            let status = repositories::status(&repo)?;
            status.print();

            // status.removed_files currently is files and dirs,
            // we roll up the dirs into the parent dir, so len should be 1
            // TODO: https://app.asana.com/0/1204211285259102/1208493904390183/f
            assert_eq!(status.removed_files.len(), 1);
            assert_eq!(status.staged_files.len(), 0);

            // Add the removed nlp dir with a wildcard
            repositories::add(&repo, "nlp/*").await?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_wildcard_add_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let dir = Path::new("nlp/*");
            let repo_dir = repo.path.join(dir);
            repositories::add(&repo, repo_dir).await?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should add all the sub dirs
            // nlp/classification/annotations/
            assert_eq!(status.staged_dirs.len(), 1);

            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_empty_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Make an empty dir
            let empty_dir = repo.path.join("empty_dir");
            util::fs::create_dir_all(&empty_dir)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should find the untracked dir
            assert!(
                status
                    .untracked_dirs
                    .iter()
                    .any(|(path, _)| *path == Path::new("empty_dir"))
            );

            // Add the empty dir
            repositories::add(&repo, &empty_dir).await?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should find the untracked dir
            assert_eq!(
                status
                    .staged_dirs
                    .paths
                    .get(&PathBuf::from("empty_dir"))
                    .unwrap()
                    .len(),
                1
            );

            assert!(!status.is_clean());

            // Empty dir should not be untracked
            assert_eq!(status.untracked_dirs.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_all_files_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write two files to a sub directory
            let repo_path = &repo.path;
            let training_data_dir = PathBuf::from("training_data");
            let sub_dir = repo_path.join(&training_data_dir);
            util::fs::create_dir_all(&sub_dir)?;

            let sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
            let sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

            let status = repositories::status(&repo)?;
            let dirs = status.untracked_dirs;

            // There is one directory
            assert_eq!(dirs.len(), 1);

            // Then we add all three
            repositories::add(&repo, &sub_file_1).await?;
            repositories::add(&repo, &sub_file_2).await?;
            repositories::add(&repo, &sub_file_3).await?;

            // There now there are no untracked directories
            let status = repositories::status(&repo)?;
            println!("status after add: {status:?}");
            status.print();
            let dirs = status.untracked_dirs;
            assert_eq!(dirs.len(), 0);

            // And there is one tracked directory
            let staged_dirs = status.staged_dirs;

            assert_eq!(staged_dirs.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_stager_add_dir_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Write two files to a sub directory
            let annotations_dir = repo.path.join("annotations");

            // Add the directory which has the structure
            // annotations/
            //   README.md
            //   train/
            //     bounding_box.csv
            //     annotations.txt
            //     two_shot.txt
            //     one_shot.csv
            //   test/
            //     annotations.txt
            repositories::add(&repo, &annotations_dir).await?;

            // List dirs
            let status = repositories::status(&repo)?;
            status.print();
            let dirs = status.staged_dirs;

            // There are 3 staged directories
            assert_eq!(dirs.len(), 3);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_add_if_not_modified() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Make sure we have a valid file
            let repo_path = &repo.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it
            repositories::add(&repo, &hello_file).await?;

            // Commit it
            repositories::commit(&repo, "Add Hello World")?;

            // try to add it again
            repositories::add(&repo, &hello_file).await?;

            // make sure we don't have it added again, because the hash hadn't changed since last commit
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_add_after_modified_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Track & commit all the data
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Adding one shot")?;

            let branch_name = "feature/modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
            let one_shot_path = test::modify_txt_file(one_shot_path, file_contents)?;
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.modified_files.len(), 1);
            assert!(
                status
                    .modified_files
                    .contains(&PathBuf::from("annotations/train/one_shot.csv"))
            );

            repositories::add(&repo, &one_shot_path).await?;
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.staged_files.len(), 1);
            assert_eq!(status.modified_files.len(), 0);
            assert!(
                status
                    .staged_files
                    .contains_key(&PathBuf::from("annotations/train/one_shot.csv"))
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_wildcard_prefix_match() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Data from populate_train_dir:
            // train/cat_1.jpg, train/cat_2.jpg, train/cat_3.jpg
            // train/dog_1.jpg, train/dog_2.jpg, train/dog_3.jpg, train/dog_4.jpg

            // Add only the cats
            repositories::add(&repo, "train/cat_*.jpg").await?;
            let status = repositories::status(&repo)?;

            assert_eq!(status.staged_files.len(), 3);

            // Add the dogs
            repositories::add(&repo, "train/dog_*.jpg").await?;
            let status = repositories::status(&repo)?;

            // Should stage all 7 files now
            assert_eq!(status.staged_files.len(), 7);

            Ok(())
        })
        .await
    }

    /// Given the relative path (posibly nested) to a file and contents it should have,
    /// this function creates the file and stages it to the repository using `oxen add`.
    async fn create_and_stage(repo: &LocalRepository, file_relative: &Path, content: &str) {
        let file_full = repo.path.join(file_relative);
        let dir = file_full.parent().expect("parent dir should exist");
        tokio::fs::create_dir_all(dir)
            .await
            .expect(&format!("could not create directory: {}", dir.display()));
        tokio::fs::write(&file_full, content)
            .await
            .expect(&format!("could not write file: {}", file_full.display()));
        repositories::add(&repo, file_relative)
            .await
            .expect(&format!("could not oxen add file: {}", file_full.display()));
    }

    /// Given a relative path to a file or directory in the repository,
    /// this function removes it from the filesystem and stages the removal using `oxen add`.
    async fn remove_and_stage(repo: &LocalRepository, relative: &Path) {
        let full = repo.path.join(relative);
        if full.is_file() {
            tokio::fs::remove_file(&full)
                .await
                .expect(&format!("could not remove file: {}", full.display()));
        } else if full.is_dir() {
            tokio::fs::remove_dir_all(&full)
                .await
                .expect(&format!("could not remove directory: {}", full.display()));
        } else {
            panic!("path is neither a file nor a directory: {}", full.display())
        }
        repositories::add(repo, relative)
            .await
            .expect(&format!("could not oxen add: {}", relative.display()));
    }

    /// Commit a staged file (either added, removed, or modified) to the repository.
    /// Use the message_prefix as 'added', 'removed', or 'modified'.
    async fn commit_staged(repo: &LocalRepository, message_prefix: &str, file_relative: &Path) {
        repositories::commit(
            repo,
            &format!("{message_prefix} {}", file_relative.display()),
        )
        .expect("could not oxen commit");
    }

    /// Asserts that there are the expected number of staged files for addition, removal, and modification.
    fn expect_staged(
        status: &StagedData,
        n_expect_add: usize,
        n_expect_rm: usize,
        n_expect_mod: usize,
    ) {
        let n_expected = n_expect_add + n_expect_mod + n_expect_rm;
        assert!(
            status.staged_files.len() == n_expected,
            "Expecting there to be {n_expected} file(s), but found ({}): {:?}",
            status.staged_files.len(),
            status.staged_files
        );

        let count_type = |s: StagedEntryStatus| -> usize {
            status
                .staged_files
                .iter()
                .filter(|(_, entry)| entry.status == s)
                .count()
        };

        let n_rm = count_type(StagedEntryStatus::Removed);
        assert_eq!(
            n_rm, n_expect_rm,
            "Expecting {n_expect_rm} staged file(s) for removal but found ({n_rm}): {:?}",
            status.staged_files
        );

        let n_add = count_type(StagedEntryStatus::Added);
        assert_eq!(
            n_add, n_expect_add,
            "Expecting {n_expect_add} staged file(s) for addition but found ({n_add}): {:?}",
            status.staged_files
        );

        let n_mod = count_type(StagedEntryStatus::Modified);
        assert_eq!(
            n_mod, n_expect_mod,
            "Expecting {n_expect_mod} staged file(s) for modification but found ({n_mod}): {:?}",
            status.staged_files
        );
    }

    /// Test removal of a single file at the root.
    #[tokio::test]
    async fn test_remove_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = Path::new("file.txt");

            create_and_stage(&repo, &file, "hello").await;
            let status = repositories::status(&repo).expect("oxen status failed");
            expect_staged(&status, 1, 0, 0);
            commit_staged(&repo, "added", &file);

            remove_and_stage(&repo, &file).await;
            let status = repositories::status(&repo).expect("oxen status failed");
            expect_staged(&status, 0, 1, 0);
            commit_staged(&repo, "removed", &file);

            Ok(())
        })
        .await
    }

    /// Test removal of a single directory at the root containing a single file.
    #[tokio::test]
    async fn test_remove_file_in_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let dir = repo.path.join("1");
            tokio::fs::create_dir_all(&dir)
                .await
                .expect("failed to create dir 1");

            let file_path = dir.join("file.txt");
            tokio::fs::write(&file_path, "hello")
                .await
                .expect("failed to write text file");

            // Add and commit
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "add 1/file.txt").expect("intial oxen commit failed");

            // Remove the directory and file from the filesystem
            tokio::fs::remove_file(repo.path.join("1"))
                .await
                .expect("failed to remove dir + file");

            // `oxen add .` should stage the removed directory
            repositories::add(&repo, &repo.path)
                .await
                .expect("Failed to oxen add");

            let status = repositories::status(&repo).expect("oxen status failed");

            println!("\n\nSTATUS:\n{:?}", status);

            assert!(
                status.staged_files.len() >= 1,
                "Expecting there to only be one staged file, but found ({}): {:?}",
                status.staged_files.len(),
                status.staged_files
            );
            assert!(
                status
                    .staged_files
                    .iter()
                    .any(|(_, entry)| entry.status == StagedEntryStatus::Removed),
                "Expecting file to be staged as removed. Instead found {} staged files: {:?}",
                status.staged_files.len(),
                status.staged_files
            );

            // No remaining unstaged removed files
            // assert_eq!(status.removed_files.len(), 1, "Expecting 1 removed files but found ({}): {:?}", status.removed_files.len(), status.removed_files);

            // Commit and verify the merkle tree no longer contains dir "3"
            repositories::commit(&repo, "rm file in dir").expect("failed to oxen commit");
            let head =
                repositories::commits::head_commit(&repo).expect("failed to get head commit");
            let dir_2 =
                repositories::tree::get_dir_without_children(&repo, &head, Path::new("1/2"), None)
                    .expect("failed to get dir 1/2 w/o children");

            // Dir "2" should exist but have 0 entries (dir "3" removed)
            assert!(dir_2.is_some(), "Expecting 1/2 to exist but it does not");
            let dir_3 = repositories::tree::get_dir_without_children(
                &repo,
                &head,
                Path::new("1/2/3"),
                None,
            )
            .expect("failed to get dir 1/2/3 w/o children");
            assert!(
                dir_3.is_none(),
                "Directory 1/2/3 should not exist in merkle tree after removal"
            );

            Ok(())
        })
        .await
    }

    /// Tests removal of nested directories and their contents.
    #[tokio::test]
    async fn test_add_dot_stages_removed_nested_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create nested directory structure: 1/2/3/file.txt
            let nested_dir = repo.path.join("1").join("2").join("3");
            util::fs::create_dir_all(&nested_dir).expect("failed to create dir 1/2/3");
            let file_path = nested_dir.join("file.txt");
            test::write_txt_file_to_path(&file_path, "hello").expect("failed to write text file");

            // Add and commit
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "add nested dir").expect("intial oxen commit failed");

            // Remove the nested directory from filesystem
            util::fs::remove_dir_all(repo.path.join("1").join("2").join("3"))
                .expect("failed to remove dir 1/2/3");

            // `oxen add .` should stage the removed directory
            repositories::add(&repo, &repo.path)
                .await
                .expect("Failed to oxen add");

            let status = repositories::status(&repo).expect("oxen status failed");

            println!("\n\nSTATUS:\n{:?}", status);

            // // The file inside 1/2/3 should be staged as removed
            // assert!(status.staged_files.len() >= 1, "Expecting there to only be one staged file, but found ({}): {:?}", status.staged_files.len(), status.staged_files);
            // assert!(status
            //     .staged_files
            //     .iter()
            //     .any(|(_, entry)| entry.status == StagedEntryStatus::Removed),
            // "Expecting file inside 1/2/3 to be staged as removed. Instead found {} staged files: {:?}",
            //     status.staged_files.len(), status.staged_files);

            // No remaining unstaged removed files
            assert_eq!(
                status.removed_files.len(),
                1,
                "Expecting 1 removed files but found ({}): {:?}",
                status.removed_files.len(),
                status.removed_files
            );

            // Commit and verify the merkle tree no longer contains dir "3"
            repositories::commit(&repo, "rm nested dir").expect("failed to oxen commit");
            let head =
                repositories::commits::head_commit(&repo).expect("failed to get head commit");
            let dir_2 =
                repositories::tree::get_dir_without_children(&repo, &head, Path::new("1/2"), None)
                    .expect("failed to get dir 1/2 w/o children");

            // Dir "2" should exist but have 0 entries (dir "3" removed)
            assert!(dir_2.is_some(), "Expecting 1/2 to exist but it does not");
            let dir_3 = repositories::tree::get_dir_without_children(
                &repo,
                &head,
                Path::new("1/2/3"),
                None,
            )
            .expect("failed to get dir 1/2/3 w/o children");
            assert!(
                dir_3.is_none(),
                "Directory 1/2/3 should not exist in merkle tree after removal"
            );

            Ok(())
        })
        .await
    }
}
