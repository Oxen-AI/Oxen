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
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_add");
/// let repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// repositories::add(&repo, &hello_file)?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
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

    use std::path::Path;
    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::opts::clone_opts::CloneOpts;
    use crate::model::staged_data::StagedDataOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::api;

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
                assert!(status
                    .staged_files
                    .contains_key(&PathBuf::from("clone_depth_1_add.txt")));

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
                assert!(status
                    .staged_files
                    .contains_key(&PathBuf::from("annotations").join("test").join("README.md")));

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
            println!("status: {:?}", status);
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
            assert!(status
                .untracked_dirs
                .iter()
                .any(|(path, _)| *path == PathBuf::from("empty_dir")));

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
            println!("status after add: {:?}", status);
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
            assert!(status
                .modified_files
                .contains(&PathBuf::from("annotations/train/one_shot.csv")));

            repositories::add(&repo, &one_shot_path).await?;
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.staged_files.len(), 1);
            assert_eq!(status.modified_files.len(), 0);
            assert!(status
                .staged_files
                .contains_key(&PathBuf::from("annotations/train/one_shot.csv")));

            Ok(())
        })
        .await
    
    
    }

    #[tokio::test]
    async fn test_remote_mode_add_after_modified_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;

                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let file_path = PathBuf::from("annotations").join("train").join("new_file.csv");
                let full_path = cloned_repo.path.join(&file_path);

                // Create new file locally 
                let file_contents = "New file contents!";
                util::fs::write_to_path(&full_path, file_contents)?;

                // Status displays only the untracked file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[file_path.clone()]);
                let directory = String::from(".");

                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_id, &PathBuf::from("."), &status_opts).await?;

                status.print_with_params(&status_opts);

                assert_eq!(status.staged_files.len(), 0);
                assert_eq!(status.modified_files.len(), 0);
                assert_eq!(status.untracked_files.len(), 1);
                assert_eq!(status.removed_files.len(), 0);

                // Add the new file 
                api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![full_path.clone()]).await?;

                // Status displays only the staged file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[file_path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_id, &directory, &status_opts).await?;
                
                 status.print_with_params(&status_opts);
                 
                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.modified_files.len(), 0);
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.removed_files.len(), 0);
                assert!(status.staged_files.contains_key(&file_path));

                // Modify the file
                let file_contents = "file,label\ntrain/cat_1.jpg,1000";
                test::modify_txt_file(&full_path, file_contents)?;

                // Add the modified file
                api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![full_path.clone()]).await?;

                // Status now displays only the modified file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[file_path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_id, &directory, &status_opts).await?;

                assert_eq!(status.staged_files.len(), 0);
                assert_eq!(status.modified_files.len(), 1);
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.removed_files.len(), 0);
                assert!(status.staged_files.contains_key(&file_path));

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
