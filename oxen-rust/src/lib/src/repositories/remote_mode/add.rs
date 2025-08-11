// Placeholder file, for unit test organization only
// `oxen add` currently does not have any unique logic in remote-mode

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::opts::clone_opts::CloneOpts;
    use crate::model::staged_data::StagedDataOpts;
    use crate::{api, repositories, test, util};
    use crate::config::UserConfig;
    use crate::model::NewCommitBody;

    #[tokio::test]
    async fn test_remote_mode_add_file() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            println!("Capture");

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Create new file in repo
                let file_path = PathBuf::from("new_file.csv");

                // Get status, should show untracked file
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;

                assert_eq!(status.untracked_files.len(), 1);

                // Add file 
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![file_path]).await?;

                // Get status, should show staged file
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.staged_files.len(), 1);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_add_file_with_full_path() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Create new file in repo
                let file_path = PathBuf::from("new_file.csv");
                let full_path = cloned_repo.path.join(&file_path);

                // Add file with full path
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![full_path]).await?;

                // Get status, should show staged file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.staged_files.len(), 1);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_add_and_modify_file_in_new_subdir() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;

                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let subdir_path = PathBuf::from("annotations").join("train");
                let file_path = subdir_path.join("new_file.csv");
                let full_path = cloned_repo.path.join(&file_path);

                // Create new file locally 
                let file_contents = "New file contents!";
                util::fs::create_dir_all(&subdir_path)?;
                util::fs::write_to_path(&full_path, file_contents)?;

                // Status displays only the untracked file and dirs
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[file_path.clone()]);
                let directory = String::from(".");
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &PathBuf::from("."), &status_opts).await?;

                status.print_with_params(&status_opts);

                assert_eq!(status.staged_files.len(), 0);
                assert_eq!(status.staged_dirs.len(), 0);
                assert_eq!(status.modified_files.len(), 0);
                assert_eq!(status.untracked_files.len(), 1);
                assert_eq!(status.untracked_dirs.len(), 2);
                assert_eq!(status.removed_files.len(), 0);

                // Add the new file 
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![full_path.clone()]).await?;

                // Status displays only the staged file and dirs
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[file_path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                
                status.print_with_params(&status_opts);

                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.staged_dirs.len(), 2);
                assert_eq!(status.modified_files.len(), 0);
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert_eq!(status.removed_files.len(), 0);
                assert!(status.staged_files.contains_key(&file_path));

                // Modify the file
                let file_contents = "file,label\ntrain/cat_1.jpg,1000";
                test::modify_txt_file(&full_path, file_contents)?;

                // Add the modified file
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![full_path.clone()]).await?;

                // Status now displays the modified as well as the staged entries
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[file_path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;

                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.staged_files.len(), 2);
                assert_eq!(status.modified_files.len(), 1);
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert_eq!(status.removed_files.len(), 0);
                assert!(status.staged_files.contains_key(&file_path));

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    
    #[tokio::test]
    async fn test_remote_mode_add_multiple_files_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                
                // Write three files to a subdirectory
                let training_data_dir = PathBuf::from("training_data");
                let sub_dir = cloned_repo.path.join(&training_data_dir);
                util::fs::create_dir_all(&sub_dir)?;

                let sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
                let sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
                let sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

                // Get status, should show untracked files
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[training_data_dir.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                assert_eq!(status.untracked_files.len(), 3);
                assert_eq!(status.staged_files.len(), 0);

                // Add all three files
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![sub_file_1, sub_file_2, sub_file_3]).await?;

                // Status should now show the files as staged
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.staged_files.len(), 3);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_add_dir_recursive() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                // Create a directory with a nested structure
                let annotations_dir = cloned_repo.path.join("annotations");
                let train_dir = annotations_dir.join("train");
                let test_dir = annotations_dir.join("test");
                util::fs::create_dir_all(&train_dir)?;
                util::fs::create_dir_all(&test_dir)?;
                let _ = test::add_txt_file_to_dir(&annotations_dir, "README.md")?;
                let _ = test::add_txt_file_to_dir(&train_dir, "bounding_box.csv")?;
                let _ = test::add_txt_file_to_dir(&train_dir, "one_shot.csv")?;
                let _ = test::add_txt_file_to_dir(&test_dir, "annotations.txt")?;

                // Add the top-level directory
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![annotations_dir.clone()]).await?;

                // Check status for all staged files
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[annotations_dir.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                assert_eq!(status.staged_dirs.len(), 3); // annotations, annotations/train, annotations/test
                assert_eq!(status.staged_files.len(), 4);
                
                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_cannot_add_if_not_modified() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                
                // Create a file, add it, and commit it
                let dir_path = cloned_repo.path.join("dir");
                let hello_file_path = dir_path.join("hello.txt");
                let _ = test::add_txt_file_to_dir(&hello_file_path, "Hello World")?;
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![hello_file_path.clone()]).await?;
                
                let cfg = UserConfig::get()?;
                let body = NewCommitBody {
                    message: "Add Hello World".to_string(),
                    author: cfg.name,
                    email: cfg.email,
                };
                repositories::remote_mode::commit(&cloned_repo, &body).await?;

                // Add again without modification
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![hello_file_path.clone()]).await?;

                // Verify neither the file nor its parent dir were staged
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[PathBuf::from("hello.txt")]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                assert_eq!(status.staged_files.len(), 0);
                assert_eq!(status.staged_dirs.len(), 0);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }
    
}