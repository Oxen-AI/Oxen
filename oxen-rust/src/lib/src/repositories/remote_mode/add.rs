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


    // TODO: Actual bugs unconvered:
    // 1: Err_files
    // 2: Adding modified files

    #[tokio::test]
    async fn test_remote_mode_add_file_with_full_path() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Create new file in repo
                let file_path = test::add_txt_file_to_dir(&cloned_repo.path, "new file contents")?;

                // Get status, should show untracked file
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
            
                assert_eq!(status.untracked_files.len(), 1);

                // Add file 
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![file_path]).await?;

                // Get status, should show staged file
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.staged_files.len(), 1);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_add_file_with_relative_path() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Create new file in repo
                let file_path = test::add_txt_file_to_dir(&cloned_repo.path, "new file contents")?;

                // Get relative path
                let relative_path = util::fs::path_relative_to_dir(&file_path, &cloned_repo.path.clone())?;
        
                // Add file with full path
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![relative_path]).await?;

                // Get status, should show staged file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.staged_files.len(), 1);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    
    #[tokio::test]
    async fn test_remote_mode_add_file_with_canon_path() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Create new file in repo
                let file_path = test::add_txt_file_to_dir(&cloned_repo.path, "new file contents")?;

                // Get canon path
                let canon_path = util::fs::canonicalize(&file_path)?;
        
                // Add file with full path
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![canon_path]).await?;

                // Get status, should show staged file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
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


                // Create new file locally 
                let subdir_path = PathBuf::from("new").join("dir");
                util::fs::create_dir_all(&subdir_path)?;

                let file_path = subdir_path.join("new_file.csv");
                let full_path = cloned_repo.path.join(&file_path);
                let file_contents = "cateory_1,category_2,category_3\nnew,file,contents";
                util::fs::write_to_path(&full_path, file_contents)?;

                // Status displays only the untracked file and dirs
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let directory = String::from(".");
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &PathBuf::from("."), &status_opts).await?;

                status.print_with_params(&status_opts);

                assert_eq!(status.staged_files.len(), 0);
                assert_eq!(status.staged_dirs.len(), 0);
                assert_eq!(status.modified_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 1);

                // Add the new file 
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![full_path.clone()]).await?;

                // Status displays only the staged file and dirs
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                
                status.print_with_params(&status_opts);

                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.staged_dirs.len(), 1);
                assert_eq!(status.modified_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert!(status.staged_files.contains_key(&file_path));

                // Modify the file
                let file_contents = "file,label\ntrain/cat_1.jpg,1000";
                test::modify_txt_file(&full_path, file_contents)?;

                // Add the modified file
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![full_path.clone()]).await?;

                // Status now displays the modified as well as the staged entries
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print_with_params(&status_opts);

                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.staged_dirs.len(), 1);
                assert_eq!(status.modified_files.len(), 1);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert!(status.modified_files.contains(&file_path));

                // Add the modified file
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![full_path.clone()]).await?;

                // Status again displays only the staged file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print_with_params(&status_opts);

                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.staged_dirs.len(), 1);
                assert_eq!(status.modified_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 0);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Note: The current behavior with untracked dirs is that we display just the dir with their 'item count'
    // This means we're looking for untracked dirs, as opposed to files, in the test below
    #[tokio::test]
    async fn test_remote_mode_add_multiple_files_in_sub_dir() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
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

                let sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello")?;
                let sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "World")?;
                let sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "!")?;

                // Get status, should show untracked files
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;

                assert_eq!(status.untracked_dirs.len(), 1);
                assert_eq!(status.staged_files.len(), 0);

                // Add all three files
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![sub_file_1, sub_file_2, sub_file_3]).await?;

                // Status should now show the files as staged
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert_eq!(status.untracked_dirs.len(), 0);
                assert_eq!(status.staged_files.len(), 3);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_add_files_with_glob_path() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                // Write three files to the root dir
                let repo_dir = cloned_repo.path.clone();
                let _file_1 = test::add_txt_file_to_dir(&repo_dir, "Hello")?;
                let _file_2 = test::add_txt_file_to_dir(&repo_dir, "World")?;
                let _file_3 = test::add_txt_file_to_dir(&repo_dir, "!")?;
                
                // Write three files to a subdirectory
                let training_data_dir = PathBuf::from("training_data");
                let sub_dir = cloned_repo.path.join(&training_data_dir);
                util::fs::create_dir_all(&sub_dir)?;

                let _sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello")?;
                let _sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "World")?;
                let _sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "!")?;


                // Add all files with glob path
                let glob_path = PathBuf::from("*");
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![glob_path]).await?;

                // Status should now show the files as staged
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                
                status.print();

                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert_eq!(status.staged_files.len(), 6);
                // This includes the root dir
                assert_eq!(status.staged_dirs.len(), 2);

                // Write more files
                let file_path = PathBuf::from("new_file.txt");
                let full_path = cloned_repo.path.join(&file_path);
                let file_content = format!("new file");
                test::write_txt_file_to_path(&full_path, &file_content)?;

                let excluded_path = PathBuf::from("excluded.txt");
                let excluded_full_path = cloned_repo.path.join(&excluded_path);
                let excluded_content = format!("excluded");
                test::write_txt_file_to_path(&excluded_full_path, &excluded_content)?;

                let dir_path = PathBuf::from("new_dir");
                let full_dir_path = cloned_repo.path.join(&dir_path);
                util::fs::create_dir_all(&full_dir_path)?;

                let embedded_file = PathBuf::from("embedded.txt");
                let embedded_full_path = full_dir_path.join(embedded_file); 
                let embedded_content = format!("embedded file");
                test::write_txt_file_to_path(&embedded_full_path, &embedded_content)?;

                // Adding `n*` should add both the new file and new dir
                let glob_path2 = PathBuf::from("n*");
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![glob_path2]).await?;

                // Verify new paths were added 
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                
                status.print();

                assert_eq!(status.untracked_files.len(), 1);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert_eq!(status.staged_files.len(), 8);
                // This includes the root dir
                assert_eq!(status.staged_dirs.len(), 3);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_add_dir_recursive() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
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
                let new_dir = cloned_repo.path.join("new");
                let train_dir = new_dir.join("train");
                let test_dir = new_dir.join("test");

                util::fs::create_dir_all(&train_dir)?;
                util::fs::create_dir_all(&test_dir)?;

                let _ = test::add_txt_file_to_dir(&new_dir, "text 1")?;
                let _ = test::add_txt_file_to_dir(&train_dir, "text 2")?;
                let _ = test::add_txt_file_to_dir(&train_dir, "text 3")?;
                let _ = test::add_txt_file_to_dir(&test_dir, "text 4")?;

                // Add the top-level directory
                // TODO: Check how many files were added
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![new_dir.clone()]).await?;

                // Check status for all staged files
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert_eq!(status.staged_dirs.len(), 3); 
                assert_eq!(status.staged_files.len(), 4);
                
                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_cannot_add_if_not_modified() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
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
                util::fs::create_dir_all(&dir_path)?;
                let hello_file_path = test::add_txt_file_to_dir(&dir_path, "hello.txt")?;

                println!("1");
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![hello_file_path.clone()]).await?;
                    println!("2");
                let cfg = UserConfig::get()?;
                let body = NewCommitBody {
                    message: "Add text file".to_string(),
                    author: cfg.name,
                    email: cfg.email,
                };
                    println!("3");
                repositories::remote_mode::commit(&cloned_repo, &body).await?;
                    println!("4");

                // Add again without modification
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![hello_file_path.clone()]).await?;
                    println!("5");

                // Verify repo is still clean 
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert!(status.is_clean());

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }
    
}