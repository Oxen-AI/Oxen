// Placeholder file, for unit test organization only
// `oxen rm` currently does not have any unique logic in remote-mode


#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::opts::clone_opts::CloneOpts;
    use crate::model::staged_data::StagedDataOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::api;

    use crate::model::NewCommitBody;
    use crate::config::UserConfig;
    use crate::model::StagedEntryStatus;

    // TODO: Real bugs uncovered:
    // 1. Handling errors with rm; need to return err_files
        
    #[tokio::test]
    async fn test_remote_mode_rm_dir_that_is_not_committed_should_throw_error() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let untracked_dir = PathBuf::from("new_dir");
                let full_path = cloned_repo.path.join(&untracked_dir);
                util::fs::create_dir_all(&full_path)?;
                let _ = test::add_txt_file_to_dir(&full_path, "new_file.txt")?;

                // Try to remove it. This should fail because the directory is not a committed part of the remote repo.
                let paths_to_remove = vec![untracked_dir.clone()];
                let result = api::client::workspaces::files::rm_files(&cloned_repo, &remote_repo, &workspace_id, paths_to_remove).await;
                assert!(result.is_err());
                
                Ok(())
            }).await?;
            
            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_rm_staged_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());
                
                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let file_path = PathBuf::from("README.md");
                let full_path = cloned_repo.path.join(&file_path);

                // Create and stage a file
                test::write_txt_file_to_path(&full_path, "Test content")?;
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_id, &directory, vec![file_path.clone()]).await?;
                
                // Check status to confirm it's staged
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_id, &directory, &status_opts).await?;
                assert_eq!(status.staged_files.len(), 1);

                // Remove it from staged area
                let paths_to_remove = vec![file_path.clone()];
                api::client::workspaces::files::rm_files_from_staged(&remote_repo, &workspace_id, paths_to_remove).await?;

                // Check status again; it should be unstaged
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_id, &directory, &status_opts).await?;
                assert_eq!(status.staged_files.len(), 0);
                assert_eq!(status.untracked_files.len(), 1);
                
                Ok(())
            }).await?;
            
            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_rm_one_file_in_dir() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());
                
                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                // Create a directory with multiple files and commit it
                let images_dir = PathBuf::from("images");
                let full_images_dir = cloned_repo.path.join(&images_dir);
                util::fs::create_dir_all(&full_images_dir)?;
                let file1 = images_dir.join("dog_1.txt");
                let file2 = images_dir.join("dog_2.txt");
                let file3 = images_dir.join("cat_1.txt");
                
                test::write_txt_file_to_path(cloned_repo.path.join(&file1), "dog")?;
                test::write_txt_file_to_path(cloned_repo.path.join(&file2), "dog")?;
                test::write_txt_file_to_path(cloned_repo.path.join(&file3), "cat")?;

                println!("File 1: {file1:?}; exists? {:?}; clone repo path: {:?}; exist? {:?}",
                    file1.exists(),
                    cloned_repo.path.join(&file1),
                    cloned_repo.path.join(&file1).exists(),
                );

                let files_to_add = vec![file1.clone(), file2.clone(), file3.clone()];

                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_id, &directory, files_to_add).await?;
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Adding images");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let tree = repositories::tree::get_root_with_children(&cloned_repo, &head)?.unwrap();
                let (files, _) = repositories::tree::list_files_and_dirs(&tree)?;

                // The three created files and bounding_box.csv
                assert_eq!(files.len(), 4);
                
                // Remove just one file
                let paths_to_remove = vec![file1.clone()];
                api::client::workspaces::files::rm_files(&cloned_repo, &remote_repo, &workspace_id, paths_to_remove).await?;
                
                // Check status: only one file should be staged for removal
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_id, &directory, &status_opts).await?;
                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.staged_files.get(&file1).unwrap().status, StagedEntryStatus::Removed);

                // Commit removal
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Removing file");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;

                // Confirm file was removed locally and from the tree
                assert!(!cloned_repo.path.join(file1.clone()).exists());
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let tree = repositories::tree::get_root_with_children(&cloned_repo, &head)?.unwrap();
                let (files, _) = repositories::tree::list_files_and_dirs(&tree)?;
                println!("files: {files:?}");
                assert_eq!(files.len(), 3);

                
                Ok(())
            }).await?;
            
            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_rm_dir_with_glob_path() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let path_with_slash = PathBuf::from("annotations").join("*");

                // Remove the directory using a path with a trailing slash
                let paths_to_remove = vec![path_with_slash.clone()];
                api::client::workspaces::files::rm_files(&cloned_repo, &remote_repo, &workspace_id, paths_to_remove).await?;

                // Check status to verify files are staged for removal
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_id, &directory, &status_opts).await?;
                
                assert_eq!(status.staged_dirs.paths.len(), 1);
                assert_eq!(status.staged_files.len(), 4);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }
}