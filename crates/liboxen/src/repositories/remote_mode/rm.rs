// Placeholder file, for unit test organization only
// `oxen rm` currently does not have any unique logic in remote-mode

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::staged_data::StagedDataOpts;
    use crate::opts::clone_opts::CloneOpts;
    use crate::repositories;
    use crate::repositories::remote_mode::test_helpers::{add, clone, status};
    use crate::test;
    use crate::util;

    use crate::config::UserConfig;
    use crate::model::NewCommitBody;
    use crate::model::StagedEntryStatus;

    // Remote-mode clones of one remote each remove a file from their own workspace their own way
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_rm_file() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Removing a committed file stages its removal, whether the clone restored it to
                // disk first or never did, and whether the path is relative or under the clone
                let readme = PathBuf::from("README.md");
                let synced_repo = clone(&remote_repo, &dir, "synced_repo").await?;
                let head_commit = repositories::commits::head_commit(&synced_repo)?;
                repositories::remote_mode::restore(
                    &synced_repo,
                    std::slice::from_ref(&readme),
                    &head_commit.id,
                )
                .await?;
                let unsynced_repo = clone(&remote_repo, &dir, "unsynced_repo").await?;
                let full_path_repo = clone(&remote_repo, &dir, "full_path_repo").await?;
                let full_path = full_path_repo.path.join(&readme);
                for (repo, path) in [
                    (&synced_repo, readme.clone()),
                    (&unsynced_repo, readme.clone()),
                    (&full_path_repo, full_path),
                ] {
                    let workspace_id = repo.workspace_name.as_deref().unwrap();
                    api::client::workspaces::files::rm_files(
                        repo,
                        &remote_repo,
                        workspace_id,
                        vec![path],
                    )
                    .await?;
                    let removed_status = status(repo, &remote_repo).await?;
                    removed_status.print();
                    let case = repo.path.display();
                    assert_eq!(removed_status.untracked_files.len(), 0, "{case}");
                    assert_eq!(removed_status.staged_files.len(), 1, "{case}");
                }

                // Removing a staged file from the staged area leaves it untracked. Its path
                // doesn't collide with the committed README, which would show as modified
                // rather than untracked.
                let staged_repo = clone(&remote_repo, &dir, "staged_repo").await?;
                let workspace_id = staged_repo.workspace_name.as_deref().unwrap();
                let file_path = PathBuf::from("hello.txt");
                test::write_txt_file_to_path(staged_repo.path.join(&file_path), "Test content")?;
                add(&staged_repo, &remote_repo, vec![file_path.clone()]).await?;
                let staged_status = status(&staged_repo, &remote_repo).await?;
                assert_eq!(staged_status.staged_files.len(), 1);
                api::client::workspaces::files::rm_files_from_staged(
                    &staged_repo,
                    &remote_repo,
                    workspace_id,
                    vec![file_path],
                )
                .await?;
                let unstaged_status = status(&staged_repo, &remote_repo).await?;
                assert_eq!(unstaged_status.staged_files.len(), 0);
                assert_eq!(unstaged_status.untracked_files.len(), 1);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // One remote-mode clone of the bounding box remote fails to remove a directory it never
    // committed, then commits the removal of one file from a directory it did commit
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_rm_one_file_in_dir() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let cloned_repo = clone(&remote_repo, &dir, "new_repo").await?;
                let workspace_id = cloned_repo.workspace_name.as_deref().unwrap();
                let cfg = UserConfig::get()?;

                // Removing a directory that is not a committed part of the remote repo fails
                let untracked_dir = PathBuf::from("new_dir");
                let full_path = cloned_repo.path.join(&untracked_dir);
                util::fs::create_dir_all(&full_path)?;
                test::add_txt_file_to_dir(&full_path, "new_file.txt")?;
                let result = api::client::workspaces::files::rm_files(
                    &cloned_repo,
                    &remote_repo,
                    workspace_id,
                    vec![untracked_dir],
                )
                .await;
                assert!(result.is_err());

                // Removing one file of a committed directory stages only that file, and committing
                // the removal takes it off disk and out of the tree
                let images_dir = PathBuf::from("images");
                let full_images_dir = cloned_repo.path.join(&images_dir);
                util::fs::create_dir_all(&full_images_dir)?;
                let file1 = images_dir.join("dog_1.txt");
                let file2 = images_dir.join("dog_2.txt");
                let file3 = images_dir.join("cat_1.txt");

                test::write_txt_file_to_path(cloned_repo.path.join(&file1), "dog")?;
                test::write_txt_file_to_path(cloned_repo.path.join(&file2), "dog")?;
                test::write_txt_file_to_path(cloned_repo.path.join(&file3), "cat")?;

                println!(
                    "File 1: {file1:?}; exists? {:?}; clone repo path: {:?}; exist? {:?}",
                    file1.exists(),
                    cloned_repo.path.join(&file1),
                    cloned_repo.path.join(&file1).exists(),
                );

                add(
                    &cloned_repo,
                    &remote_repo,
                    vec![file1.clone(), file2, file3],
                )
                .await?;
                let commit_body = NewCommitBody::from_config(&cfg, "Adding images");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;

                let head = repositories::commits::head_commit(&cloned_repo)?;
                let tree =
                    repositories::tree::get_root_with_children(&cloned_repo, &head)?.unwrap();
                let (files, _) = repositories::tree::list_files_and_dirs(&tree)?;

                // The three created files and bounding_box.csv
                assert_eq!(files.len(), 4);

                // Remove just one file
                api::client::workspaces::files::rm_files(
                    &cloned_repo,
                    &remote_repo,
                    workspace_id,
                    vec![file1.clone()],
                )
                .await?;

                // Check status: only one file should be staged for removal
                let removed_status = status(&cloned_repo, &remote_repo).await?;
                println!("status: {removed_status:?}");
                assert_eq!(removed_status.staged_files.len(), 1);
                assert_eq!(
                    removed_status.staged_files.get(&file1).unwrap().status,
                    StagedEntryStatus::Removed
                );

                // Commit removal
                let commit_body = NewCommitBody::from_config(&cfg, "Removing file");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;

                // Confirm file was removed locally and from the tree
                assert!(!cloned_repo.path.join(&file1).exists());
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let tree =
                    repositories::tree::get_root_with_children(&cloned_repo, &head)?.unwrap();
                let (files, _) = repositories::tree::list_files_and_dirs(&tree)?;
                println!("files: {files:?}");
                assert_eq!(files.len(), 3);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // TODO: 'status.print()' has weird behavior when staging subdirs for removal
    //       Resolve this at some point
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_rm_dir_with_wildcards() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("annotations", |_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let directory = ".".to_string();
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());
                let status_opts =
                    StagedDataOpts::from_paths_remote_mode(std::slice::from_ref(&cloned_repo.path));

                // The glob case works in a workspace of its own, so the asterisk case commits
                // from a clean one
                let glob_workspace_id = uuid::Uuid::new_v4().to_string();
                api::client::workspaces::create(
                    &remote_repo,
                    DEFAULT_BRANCH_NAME,
                    &glob_workspace_id,
                )
                .await?;

                // Remove the subdirectories' files with a glob
                api::client::workspaces::files::rm_files(
                    &cloned_repo,
                    &remote_repo,
                    &glob_workspace_id,
                    vec![PathBuf::from("annotations").join("t*")],
                )
                .await?;

                let status = repositories::remote_mode::status(
                    &cloned_repo,
                    &remote_repo,
                    &glob_workspace_id,
                    &directory,
                    &status_opts,
                )
                .await?;
                status.print();

                // Does not remove the README file
                assert_eq!(status.staged_files.len(), 5);

                // Count files and dirs for comparison
                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                let commit_root =
                    repositories::tree::get_root_with_children(&cloned_repo, &head_commit)?
                        .unwrap();
                let files_in_tree =
                    repositories::tree::list_all_files(&commit_root, &PathBuf::from("."))?;
                let prev_files = files_in_tree.len();
                let (_, dirs_in_tree) = repositories::tree::list_files_and_dirs(&commit_root)?;
                let prev_dirs = dirs_in_tree.len();

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let path_with_asterisk = PathBuf::from("annotations").join("*");

                // Remove everything under the directory with an asterisk
                let paths_to_remove = vec![path_with_asterisk.clone()];
                api::client::workspaces::files::rm_files(
                    &cloned_repo,
                    &remote_repo,
                    &workspace_id,
                    paths_to_remove,
                )
                .await?;

                // Check status to verify files are staged for removal
                let status = repositories::remote_mode::status(
                    &cloned_repo,
                    &remote_repo,
                    &workspace_id,
                    &directory,
                    &status_opts,
                )
                .await?;
                status.print();

                // assert_eq!(status.staged_dirs.paths.len(), 2);
                assert_eq!(status.staged_files.len(), 6);

                // Commit the removals
                let commit_message = "Removing subdirectories".to_string();
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, &commit_message);
                let new_commit =
                    repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;

                // Check that the files and dirs were removed
                let commit_root =
                    repositories::tree::get_root_with_children(&cloned_repo, &new_commit)?.unwrap();

                let files_in_tree =
                    repositories::tree::list_all_files(&commit_root, &PathBuf::from("."))?;
                let (_, dirs_in_tree) = repositories::tree::list_files_and_dirs(&commit_root)?;

                // 6 files were removed
                assert_eq!(files_in_tree.len(), prev_files - 6);

                // 2 dirs were removed
                assert_eq!(dirs_in_tree.len(), prev_dirs - 2);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
