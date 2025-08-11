use crate::api;
use crate::error::OxenError;
use crate::model::{LocalRepository, NewCommitBody};
use crate::opts::FetchOpts;
use crate::repositories;

use crate::repositories::Commit;

pub async fn commit(local_repo: &LocalRepository, commit_body: &NewCommitBody) -> Result<Commit, OxenError> {
    
    let workspace_identifier = if local_repo.is_remote_mode() {
        &local_repo.workspace_name.clone().unwrap()
    } else {
        return Err(OxenError::basic_str(
            "Error: Cannot run remote mode commands outside remote mode repo",
        ));
    };


    println!("Committing to remote with message: {}", commit_body.message);
    let Some(branch) = repositories::branches::current_branch(local_repo)? else {
        log::error!("Remote-mode commit No current branch found");
        return Err(OxenError::must_be_on_valid_branch());
    };

    let remote_repo = api::client::repositories::get_default_remote(local_repo).await?;

    // TODO: Do we print successful commit already?
    let commit = api::client::workspaces::commit(&remote_repo, &branch.name, workspace_identifier, commit_body).await?;

    // Update local tree
    let fetch_opts = FetchOpts::from_branch(&branch.name);
    repositories::fetch::fetch_branch(local_repo, &fetch_opts).await?;

    Ok(commit)
} 

#[cfg(test)]
mod tests {

    use crate::{api, repositories, util, test};
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::CloneOpts;

    use crate::model::staged_data::StagedDataOpts;
    use crate::model::EntryDataType;
    use crate::config::UserConfig;
    use std::path::{Path, PathBuf};


    #[tokio::test]
    async fn test_remote_mode_commit_file() -> Result<(), OxenError> {
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

                // Add file 
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![file_path]).await?;

                // Commit
                let cfg = UserConfig::get()?;
                let body = NewCommitBody {
                    message: "Adding new file".to_string(),
                    author: cfg.name,
                    email: cfg.email,
                };

                repositories::remote_mode::commit(&cloned_repo, &body).await?;
                
                // Verify repo is clean
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                assert!(status.is_clean());

                // Verify commit exists locally
                assert!(repositories::commits::head_commit_maybe(&cloned_repo)?.is_some());


                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_commit_several_times() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                let commit_root = repositories::tree::get_root_with_children(&cloned_repo, &head_commit)?.unwrap();
                let mut files_in_tree = repositories::tree::list_all_files(&commit_root, &PathBuf::from("."))?;   
                let mut previous_head_commit = head_commit;

                assert_eq!(files_in_tree.len(), 0);

                // Perform several sequential commits and store the commit objects
                let mut commits = vec![];
                for i in 1..=4 {
                    let filename = format!("file_{}.txt", i);
                    let file_path = PathBuf::from(&filename);
                    let full_path = cloned_repo.path.join(&file_path);
                    let file_content = format!("This is the content for file {}", i);

                    test::write_txt_file_to_path(&full_path, &file_content)?;
                    api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![file_path.clone()]).await?;

                    let commit_message = format!("Adding {}", &filename);
                    let commit_body = NewCommitBody::from_config(&UserConfig::get()?, &commit_message);
                    let new_commit = repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                    commits.push(new_commit.clone());

                    let new_head_commit = repositories::commits::head_commit(&cloned_repo)?;
                    assert_eq!(new_commit.id, new_head_commit.id);
                    assert_ne!(previous_head_commit.id, new_head_commit.id);
                    
                    let commit_root = repositories::tree::get_root_with_children(&cloned_repo, &new_commit)?.unwrap();
                    let new_files_in_tree = repositories::tree::list_all_files(&commit_root, &PathBuf::from("."))?;
                    assert_eq!(new_files_in_tree.len(), files_in_tree.len() + 1);
                    assert!(repositories::tree::has_path(&cloned_repo, &new_head_commit, file_path)?);

                    previous_head_commit = new_head_commit;
                    files_in_tree = new_files_in_tree;
                }

                // Test commit history list between two points
                let base_commit = commits[0].clone();
                let head_commit = commits[2].clone();
                
                let history = repositories::commits::list_between(&cloned_repo, &base_commit, &head_commit)?;
                assert_eq!(history.len(), 3);
                
                assert_eq!(history.first().unwrap().message, head_commit.message);
                assert_eq!(history.last().unwrap().message, base_commit.message);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_add_and_commit_downloaded_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone an empty repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Download file from remote: 
                // TODO: Download file

                // Verify bounding_box.csv and its parent dirs are no longer unsynced
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[cloned_repo.path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;

                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert_eq!(status.unsynced_files.len(), 0);
                assert_eq!(status.unsynced_dirs.len(), 0);

                // Modify bounding box
                let subdir_path = PathBuf::from("annotations").join("train");
                let file_path = subdir_path.join("bounding_box.csv");
                let full_path = cloned_repo.path.join(&file_path);

                let new_contents = "file,label\ntrain/cat_1.jpg,1000";
                test::modify_txt_file(&full_path, new_contents)?;

                // Add and commit modified file
                api::client::workspaces::files::add(&remote_repo, &workspace_identifier, &directory, vec![full_path]).await?;
                let cfg = UserConfig::get()?;
                let body = NewCommitBody {
                    message: "Modifying bounding_box.csv".to_string(),
                    author: cfg.name,
                    email: cfg.email,
                };

                repositories::remote_mode::commit(&cloned_repo, &body).await?;

                // Verify the file is synced
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;

                assert_eq!(status.untracked_files.len(), 0);
                assert_eq!(status.untracked_dirs.len(), 0);
                assert_eq!(status.unsynced_files.len(), 0);
                assert_eq!(status.unsynced_dirs.len(), 0);

                // TODO: Download file again to different name
                // TODO: Show that the file contents match new_contents

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_cannot_commit_without_staged_files() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                // Clone repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Get the current number of commits
                let commits = repositories::commits::list(&cloned_repo)?;
                let initial_len = commits.len();

                // Modify a file, but do not add it
                let labels_path = cloned_repo.path.join(Path::new("labels.txt"));
                util::fs::write_to_path(&labels_path, "changing this guy, but not committing")?;

                // Try to commit, which should fail because nothing is staged
                let cfg = UserConfig::get()?;
                let body = NewCommitBody {
                    message: "Should not work".to_string(),
                    author: cfg.name,
                    email: cfg.email,
                };

                let result = repositories::remote_mode::commit(&cloned_repo, &body).await;
                assert!(result.is_err());
                
                // The number of commits should not have changed
                let commits = repositories::commits::list(&cloned_repo)?;
                assert_eq!(commits.len(), initial_len);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_commit_removed_file() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                
                // Create a file locally
                let hello_file_path = PathBuf::from("hello.txt");
                let full_path = cloned_repo.path.join(&hello_file_path);
                test::write_txt_file_to_path(&full_path, "Hello World")?;
                
                // Add the file, which uploads its content to the remote workspace
                api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![hello_file_path.clone()]).await?;
                
                // Remove the file locally from the working directory
                util::fs::remove_file(&full_path)?;
                
                // Commit the file, verifying the commit succeeds even though the file is no longer on disk
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "My message");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                // Verify the head commit has one entry
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let commit_list = repositories::entries::list_for_commit(&cloned_repo, &head)?;
                assert_eq!(commit_list.len(), 1);

                // Add the removed file to stage the deletion
                api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![hello_file_path.clone()]).await?;
                
                // Commit the deletion
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Second Message");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                // Verify no entries remain in the head commit
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let commit_list = repositories::entries::list_for_commit(&cloned_repo, &head)?;
                assert_eq!(commit_list.len(), 0);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_commit_removed_dir() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string(); 

                // Create a directory with files
                let dir_to_remove = PathBuf::from("train");
                let full_dir_path = cloned_repo.path.join(&dir_to_remove);
                util::fs::create_dir_all(&full_dir_path)?;
                let _ = test::add_txt_file_to_dir(&full_dir_path, "file1.txt")?;
                let _ = test::add_txt_file_to_dir(&full_dir_path, "file2.txt")?;
                let og_file_count = util::fs::rcount_files_in_dir(&full_dir_path);
                
                // Add the directory, which stages its contents remotely
                api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![dir_to_remove.clone()]).await?;
                
                // Commit the new directory and its contents
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Adding train directory");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;

                // Verify head has entries from the new directory
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let commit_list = repositories::entries::list_for_commit(&cloned_repo, &head)?;
                assert_eq!(commit_list.len(), og_file_count);
                
                // Delete the directory locally
                util::fs::remove_dir_all(&full_dir_path)?;
                
                // Add the deletion to stage the removal
                api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![dir_to_remove.clone()]).await?;

                // Commit the deletion
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Removing train directory");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                // Verify no entries remain in the head commit
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let commit_list = repositories::entries::list_for_commit(&cloned_repo, &head)?;
                assert_eq!(commit_list.len(), 0);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_commit_invalid_parquet_file() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                // Create an invalid parquet file locally
                let invalid_parquet_file = test::test_invalid_parquet_file();
                let full_path = cloned_repo.path.join("invalid.parquet");
                util::fs::copy(&invalid_parquet_file, &full_path)?;
                
                let file_path = PathBuf::from("invalid.parquet");
                api::client::workspaces::files::add(&remote_repo, &workspace_id, &directory, vec![file_path.clone()]).await?;
                
                // Commit the file
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Adding invalid parquet file");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                // Verify the file's data type in the commit tree
                let head = repositories::commits::head_commit(&cloned_repo)?;
                let tree = repositories::tree::get_root_with_children(&cloned_repo, &head)?.unwrap();
                let file_node = tree.get_by_path(&file_path)?.unwrap();
                
                assert_eq!(*file_node.file()?.data_type(), EntryDataType::Binary);
                
                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }
}