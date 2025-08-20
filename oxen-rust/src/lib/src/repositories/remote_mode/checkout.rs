use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::{api, repositories};
use crate::core::v_latest::index::CommitMerkleTree;
use crate::model::MerkleHash;

use colored::Colorize;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use uuid::Uuid;

pub async fn checkout(
    repo: &mut LocalRepository,
    name: &str,
) -> Result<(), OxenError> {
    match repositories::checkout(repo, name).await {
        Ok(Some(branch)) => {

            // Change current workspace name
            repo.set_workspace(branch.name.clone())?;
            repo.save()?;

        }
        // TODO: This should create a workspace on this commit
        Ok(None) => {
            //println!("Checked out commit: {}", name);
            
        }
        Err(OxenError::RevisionNotFound(name)) => {
            println!("Revision not found: {}\n\nIf the branch exists on the remote, run\n\n  oxen fetch -b {}\n\nto update the local copy, then try again.", name, name);
            return Err(OxenError::RevisionNotFound(name));
        }
        Err(e) => {
            return Err(e);
        }
    }

    Ok(())
}

pub async fn create_checkout(
    repo: &mut LocalRepository,
    branch_name: &str,
) -> Result<(), OxenError> {
   
    // Save files in working directory to version store
    let head_commit = repositories::commits::head_commit(repo)?;
    let mut paths_to_store: HashMap<PathBuf, MerkleHash> = HashMap::new();
    let _from_root = CommitMerkleTree::root_with_present_children(&repo, &head_commit, &mut paths_to_store)?.unwrap();

    let version_store = repo.version_store()?;
    for (path, hash) in paths_to_store {
        println!("HERE: Path: {path:?}, hash: {hash:?}");
        version_store.store_version_from_path(&hash.to_string(), &path).await?;
    }

    // Create the new branch
    let workspace_name = create_checkout_branch(repo, branch_name).await?;

    // Update repo to new workspace and branch
    repositories::checkout(repo, branch_name).await?;
    repo.set_workspace(&workspace_name)?;
    repo.save()?;

    Ok(())
}

// Creates the new branch, but does not check it out
pub async fn create_checkout_branch(
    repo: &mut LocalRepository,
    branch_name: &str,
) -> Result<String, OxenError> {

    // Create the new branch
    repositories::branches::create_from_head(repo, branch_name)?;

    // Generate a random workspace id
    let workspace_id = Uuid::new_v4().to_string();

    // Use the branch name as the workspace name
    let workspace_name = format!("{}: {workspace_id}", branch_name);
    let Some(remote) = repo.remote() else {
        return Err(OxenError::basic_str(
            "Error: local repository has no remote",
        ));
    };
    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or_else(|| OxenError::remote_repo_not_found(branch_name))?;

    // Create the remote branch from the commit
    let head_commit = repositories::commits::head_commit(repo)?;
    api::client::branches::create_from_commit(&remote_repo, &branch_name, &head_commit).await?;

    let workspace = api::client::workspaces::create_with_path(
        &remote_repo,
        &branch_name,
        &workspace_id,
        Path::new("/"),
        Some(workspace_name.clone()),
    )
    .await?;

    match workspace.status.as_str() {
        "resource_created" => {
            println!(
                "{}",
                "Remote-mode repository initialized successfully!"
                    .green()
                    .bold()
            );
        }
        "resource_found" => {
            let err_msg = format!(
                "Remote-mode repo for workspace {} already exists",
                workspace_id.clone()
            );
            println!("{}", err_msg.yellow().bold());
            return Err(OxenError::basic_str(format!(
                "Error: Remote-mode repo already exists for workspace {}",
                workspace_id
            )));
        }
        other => {
            println!(
                "{}",
                format!("Unexpected workspace status: {}", other).red()
            );
        }
    }
    println!("{} {}", "Workspace ID:".green().bold(), workspace.id.bold());

    // Add the new branch name to workspaces
    repo.add_workspace(&workspace_name);

    Ok(workspace_name)
}

#[cfg(test)]
mod tests {
    use crate::{api, repositories, test, util};
    use crate::error::OxenError;

    use crate::model::NewCommitBody;
    use crate::repositories::remote_mode;
    
    use crate::opts::CloneOpts;
    use crate::config::UserConfig;

    
    #[tokio::test]
    async fn test_remote_mode_checkout_non_existant_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // This shouldn't work
            let checkout_result = repositories::remote_mode::checkout(&mut repo, "non-existant").await;
            assert!(checkout_result.is_err());

            Ok(())
        })
        .await
    }


    #[tokio::test]
    async fn test_remote_mode_checkout_current_branch_name_does_nothing() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let mut cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let branch_name = "feature".to_string();
                repositories::remote_mode::create_checkout(&mut cloned_repo, &branch_name).await?;

                // Call repositories::checkout to get the outputted branch name
                let checkout_branch = repositories::checkout(&mut cloned_repo, &branch_name).await?.unwrap();
                assert_eq!(checkout_branch.name, branch_name);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_checkout_changes_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let mut cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let orig_branch_name = repositories::branches::current_branch(&cloned_repo)?.unwrap().name.clone();
                let orig_workspace_name = cloned_repo.workspace_name.clone().unwrap();

                // Create and checkout a new branch
                let new_branch_name = "feature/workspace-change";
                remote_mode::create_checkout(&mut cloned_repo, new_branch_name).await?;

                // Verify the workspace name has changed
                let new_workspace_name = cloned_repo.workspace_name.clone().unwrap();
                assert_ne!(orig_workspace_name, new_workspace_name);

                // Checkout the original branch
                repositories::remote_mode::checkout(&mut cloned_repo, &orig_branch_name).await?;

                // Verify the workspace name has reverted to the original
                assert_eq!(cloned_repo.workspace_name.clone().unwrap(), orig_workspace_name);

                // Verify the workspace name remains the same after the commit
                assert_eq!(cloned_repo.workspace_name.unwrap(), orig_workspace_name);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_mode_checkout_updates_branch() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let mut cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let orig_branch_name = repositories::branches::current_branch(&cloned_repo)?.unwrap().name.clone();

                // Create and checkout a new branch
                let new_branch_name = "feature/workspace-change";
                remote_mode::create_checkout(&mut cloned_repo, new_branch_name).await?;

                // Verify the branch has been updated
                let current_branch = repositories::branches::current_branch(&cloned_repo)?.unwrap();
                assert_ne!(current_branch.name, orig_branch_name);

                // Checkout the original branch
                repositories::remote_mode::checkout(&mut cloned_repo, &orig_branch_name).await?;

                // Verify the branch has been reverted to the original
                let current_branch = repositories::branches::current_branch(&cloned_repo)?.unwrap();
                assert_eq!(current_branch.name, orig_branch_name);


                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_mode_checkout_added_file_and_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let mut cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let main_branch = repositories::branches::current_branch(&cloned_repo)?.unwrap();

                // Write the first file and commit to the main branch
                let hello_file = cloned_repo.path.join("hello.txt");
                let file_contents = "Hello";

                util::fs::write_to_path(&hello_file, file_contents)?;
                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_id, &directory, vec![hello_file.clone()]).await?;
                
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Added hello.txt");
                let _initial_commit = repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                // Create a new branch and checkout
                let branch_name = "feature";

                repositories::remote_mode::create_checkout(&mut cloned_repo, branch_name).await?;
                let branch_workspace = cloned_repo.workspace_name.clone();

                // Add a new file to the new branch and commit
                let world_file = cloned_repo.path.join("world.txt");
                util::fs::write_to_path(&world_file, "World")?;
                let current_workspace_id = cloned_repo.workspace_name.clone().unwrap();
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &current_workspace_id, &directory, vec![world_file.clone()]).await?;
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Added world.txt");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                // Go back to the main branch
                repositories::remote_mode::checkout(&mut cloned_repo, &main_branch.name).await?;

                // Assert the workspace name changed
                assert_ne!(cloned_repo.workspace_name, branch_workspace);
                
                // The world file should no longer be on disk after checkout
                assert!(hello_file.exists());
                assert!(!world_file.exists());

                // Go back to the world branch
                repositories::remote_mode::checkout(&mut cloned_repo, &branch_name).await?;
                assert_eq!(cloned_repo.workspace_name, branch_workspace);
                assert!(hello_file.exists());
                assert!(world_file.exists());

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_checkout_added_file_keep_untracked() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let mut cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());
                
                let main_branch = repositories::branches::current_branch(&cloned_repo)?.unwrap();

                // Write the first file and commit to the main branch
                let hello_file = cloned_repo.path.join("hello.txt");
                let file_contents = "Hello";
                util::fs::write_to_path(&hello_file, file_contents)?;

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_id, &directory, vec![hello_file.clone()]).await?;
                
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Added hello.txt");
                let _initial_commit = repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;

                // Create an untracked file that should persist
                let keep_file = cloned_repo.path.join("keep_me.txt");
                util::fs::write_to_path(&keep_file, "I am untracked, don't remove me")?;

                // Create a new branch and checkout
                let branch_name = "feature";
                repositories::remote_mode::create_checkout(&mut cloned_repo, branch_name).await?;

                // Add a second file to the new branch and commit
                let world_file = cloned_repo.path.join("world.txt");
                util::fs::write_to_path(&world_file, "World")?;
                let current_workspace_id = cloned_repo.workspace_name.clone().unwrap();

                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &current_workspace_id, &directory, vec![world_file.clone()]).await?;
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Added world.txt");

                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                
                // Go back to the main branch
                repositories::remote_mode::checkout(&mut cloned_repo, &main_branch.name).await?;

                // Assert that the untracked file still exists
                assert!(keep_file.exists());
                assert!(hello_file.exists());
                assert!(!world_file.exists());

                // Go back to the new branch
                repositories::remote_mode::checkout(&mut cloned_repo, &branch_name).await?;
                assert!(keep_file.exists());
                assert!(hello_file.exists());
                assert!(world_file.exists());

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_checkout_modified_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let mut cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                // Get main branch
                let main_branch = repositories::branches::current_branch(&cloned_repo)?.unwrap();

                // Write and commit the first file to the main branch
                let hello_file = cloned_repo.path.join("hello.txt");
                let initial_content = "Hello";
                util::fs::write_to_path(&hello_file, initial_content)?;

                let workspace_id = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_id, &directory, vec![hello_file.clone()]).await?;
                
                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Added hello.txt");
                let _initial_commit = repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                assert_eq!(util::fs::read_from_path(&hello_file)?, initial_content);

                // Create a new branch and checkout
                let branch_name = "feature";
                repositories::remote_mode::create_checkout(&mut cloned_repo, branch_name).await?;
                
                // Modify the file content on the new branch and commit
                let modified_content = "World";
                test::modify_txt_file(&hello_file, modified_content)?;

                let current_workspace_id = cloned_repo.workspace_name.clone().unwrap();
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &current_workspace_id, &directory, vec![hello_file.clone()]).await?;

                let commit_body = NewCommitBody::from_config(&UserConfig::get()?, "Changed file to world");
                repositories::remote_mode::commit(&cloned_repo, &commit_body).await?;
                assert_eq!(util::fs::read_from_path(&hello_file)?, modified_content);

                // Go back to the main branch
                repositories::remote_mode::checkout(&mut cloned_repo, &main_branch.name).await?;
                assert_eq!(util::fs::read_from_path(&hello_file)?, initial_content);
                
                // Checkout the new branch
                repositories::remote_mode::checkout(&mut cloned_repo, &branch_name).await?;
                assert_eq!(util::fs::read_from_path(&hello_file)?, modified_content);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

}