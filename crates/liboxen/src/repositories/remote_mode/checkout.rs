use crate::error::OxenError;
use crate::model::{LocalRepository, PartialNode};
use crate::{api, repositories};

use colored::Colorize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub async fn checkout(repo: &mut LocalRepository, name: &str) -> Result<(), OxenError> {
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
            println!(
                "Revision not found: {name}\n\nIf the branch exists on the remote, run\n\n  oxen fetch -b {name}\n\nto update the local copy, then try again."
            );
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
    let mut partial_nodes: HashMap<PathBuf, PartialNode> = HashMap::new();

    let _from_root = repositories::tree::get_root_with_children_and_partial_nodes(
        repo,
        &head_commit,
        None,
        None,
        None,
        &mut partial_nodes,
    )?
    .unwrap();

    let version_store = repo.version_store();
    for (path, node) in partial_nodes {
        let full_path = repo.path.join(&path);

        if full_path.exists() {
            let file = tokio::fs::File::open(&full_path).await?;
            let size = file.metadata().await?.len();
            let reader = tokio::io::BufReader::new(file);
            version_store
                .store_version_from_reader(&node.hash.to_string(), Box::new(reader), size)
                .await?;
        }
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
    let workspace_name = format!("{branch_name}: {workspace_id}");
    let Some(remote) = repo.remote() else {
        return Err(OxenError::basic_str(
            "Error: local repository has no remote",
        ));
    };
    let remote_repo = api::client::repositories::get_by_remote(&remote).await?;

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
                "Error: Remote-mode repo already exists for workspace {workspace_id}"
            )));
        }
        other => {
            println!("{}", format!("Unexpected workspace status: {other}").red());
        }
    }
    println!("{} {}", "Workspace ID:".green().bold(), workspace.id.bold());

    // Add the new branch name to workspaces
    repo.add_workspace(&workspace_name);

    Ok(workspace_name)
}

#[cfg(test)]
mod tests {
    use crate::config::UserConfig;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::repositories::remote_mode;
    use crate::repositories::remote_mode::test_helpers::{add, clone};
    use crate::{repositories, test, util};

    #[tokio::test]
    async fn test_remote_mode_checkout_non_existant_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // This shouldn't work
            let checkout_result =
                repositories::remote_mode::checkout(&mut repo, "non-existent").await;
            assert!(checkout_result.is_err());

            Ok(())
        })
        .await
    }

    // Remote-mode clones of one remote each check out branches their own way: one moves to a new
    // branch and back, and the other commits files on two branches and moves between them
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_checkout() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Creating a branch moves the clone to it and to a new workspace, checking out the
                // branch it is on returns that branch, and checking out the original branch brings
                // back the original workspace
                let mut branch_repo = clone(&remote_repo, &dir, "branch_repo").await?;
                let orig_branch_name = repositories::branches::current_branch(&branch_repo)?
                    .unwrap()
                    .name;
                let orig_workspace_name = branch_repo.workspace_name.clone();
                let new_branch_name = "feature/workspace-change";
                remote_mode::create_checkout(&mut branch_repo, new_branch_name).await?;
                assert_ne!(branch_repo.workspace_name, orig_workspace_name);
                let current_branch = repositories::branches::current_branch(&branch_repo)?.unwrap();
                assert_eq!(current_branch.name, new_branch_name);
                let checkout_branch = repositories::checkout(&branch_repo, new_branch_name)
                    .await?
                    .unwrap();
                assert_eq!(checkout_branch.name, new_branch_name);
                remote_mode::checkout(&mut branch_repo, &orig_branch_name).await?;
                assert_eq!(branch_repo.workspace_name, orig_workspace_name);
                let current_branch = repositories::branches::current_branch(&branch_repo)?.unwrap();
                assert_eq!(current_branch.name, orig_branch_name);

                // A file committed on main and left alone on the new branch, and an untracked file,
                // stay through every checkout. A file added on the new branch leaves the working
                // tree when main is checked out and comes back with the branch, and a file the
                // branch changes to content of the same size changes content with it.
                let mut files_repo = clone(&remote_repo, &dir, "files_repo").await?;
                let main_branch = repositories::branches::current_branch(&files_repo)?.unwrap();
                let hello_file = files_repo.path.join("hello.txt");
                util::fs::write_to_path(&hello_file, "Hello")?;
                let modified_file = files_repo.path.join("modified.txt");
                util::fs::write_to_path(&modified_file, "Hello")?;
                add(
                    &files_repo,
                    &remote_repo,
                    vec![hello_file.clone(), modified_file.clone()],
                )
                .await?;
                let commit_body = NewCommitBody::from_config(
                    &UserConfig::get()?,
                    "Added hello.txt and modified.txt",
                );
                remote_mode::commit(&files_repo, &commit_body).await?;
                let keep_file = files_repo.path.join("keep_me.txt");
                let keep_contents = "I am untracked, don't remove me";
                util::fs::write_to_path(&keep_file, keep_contents)?;

                let branch_name = "feature";
                remote_mode::create_checkout(&mut files_repo, branch_name).await?;
                let branch_workspace = files_repo.workspace_name.clone();
                let world_file = files_repo.path.join("world.txt");
                util::fs::write_to_path(&world_file, "World")?;
                test::modify_txt_file(&modified_file, "World")?;
                add(
                    &files_repo,
                    &remote_repo,
                    vec![world_file.clone(), modified_file.clone()],
                )
                .await?;
                let commit_body = NewCommitBody::from_config(
                    &UserConfig::get()?,
                    "Added world.txt and changed modified.txt",
                );
                remote_mode::commit(&files_repo, &commit_body).await?;

                remote_mode::checkout(&mut files_repo, &main_branch.name).await?;
                assert_ne!(files_repo.workspace_name, branch_workspace);
                assert_eq!(util::fs::read_from_path(&hello_file)?, "Hello");
                assert_eq!(util::fs::read_from_path(&modified_file)?, "Hello");
                assert!(!world_file.exists());
                assert_eq!(util::fs::read_from_path(&keep_file)?, keep_contents);

                remote_mode::checkout(&mut files_repo, branch_name).await?;
                assert_eq!(files_repo.workspace_name, branch_workspace);
                assert_eq!(util::fs::read_from_path(&hello_file)?, "Hello");
                assert_eq!(util::fs::read_from_path(&modified_file)?, "World");
                assert_eq!(util::fs::read_from_path(&world_file)?, "World");
                assert_eq!(util::fs::read_from_path(&keep_file)?, keep_contents);

                // Regression test: checkout in remote mode should not fail when a file exists in
                // the source branch's tree but is not materialized on disk. Previously,
                // r_remove_if_not_in_target would push non-existent paths into files_to_store,
                // causing store_version_from_reader to fail with NotFound.
                util::fs::remove_file(&world_file)?;
                remote_mode::checkout(&mut files_repo, &main_branch.name).await?;
                assert_eq!(util::fs::read_from_path(&hello_file)?, "Hello");
                assert_eq!(util::fs::read_from_path(&modified_file)?, "Hello");
                assert!(!world_file.exists());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
