use crate::constants::DEFAULT_REMOTE_NAME;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};
use crate::opts::CloneOpts;
use crate::{api, repositories, util};
use colored::Colorize;
use std::path::Path;
use uuid::Uuid;

pub async fn clone_repo(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    // Notify the server that we are starting a clone
    api::client::repositories::pre_clone(&remote_repo).await?;

    // if directory already exists -> return Err
    let repo_path = &opts.dst;
    if repo_path.exists() {
        let err = format!("Directory already exists: {}", remote_repo.name);
        return Err(OxenError::basic_str(err));
    }

    // if directory does not exist, create it
    util::fs::create_dir_all(repo_path)?;

    // if create successful, create .oxen directory
    let oxen_hidden_path = util::fs::oxen_hidden_dir(repo_path);
    util::fs::create_dir_all(&oxen_hidden_path)?;

    // save LocalRepository in .oxen directory
    let mut local_repo = LocalRepository::from_remote(remote_repo.clone(), repo_path)?;
    repo_path.clone_into(&mut local_repo.path);
    local_repo.set_remote(DEFAULT_REMOTE_NAME, &remote_repo.remote.url);
    local_repo.set_min_version(remote_repo.min_version());
    local_repo.set_subtree_paths(opts.fetch_opts.subtree_paths.clone());
    local_repo.set_depth(opts.fetch_opts.depth);

    local_repo.save()?;

    if remote_repo.is_empty {
        println!("The remote repository is empty. Oxen has configured the local repository, but there are no files yet.");
        return Ok(local_repo);
    }

    repositories::fetch::fetch_branch(&local_repo, &opts.fetch_opts).await?;
    repositories::checkout::checkout(&local_repo, opts.fetch_opts.branch.as_str()).await?;

    // Notify the server that we are done cloning
    api::client::repositories::post_clone(&remote_repo).await?;

    Ok(local_repo)
}

pub async fn clone_repo_remote_mode(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    // Notify the server that we are done cloning
    // TODO: Convert to 'remote-mode clone' notif
    api::client::repositories::pre_clone(&remote_repo).await?;

    // if directory already exists -> return Err
    let repo_path = &opts.dst;
    if repo_path.exists() {
        let err = format!("Directory already exists: {}", remote_repo.name);
        return Err(OxenError::basic_str(err));
    }

    // if directory does not exist, create it
    util::fs::create_dir_all(repo_path)?;

    // if create successful, create .oxen directory
    let oxen_hidden_path = util::fs::oxen_hidden_dir(repo_path);
    util::fs::create_dir_all(&oxen_hidden_path)?;

    let branch_name = &opts.fetch_opts.branch;

    // Generate a random workspace id
    let workspace_id = Uuid::new_v4().to_string();

    // Use the branch name as the workspace name
    let name = format!("{}: {workspace_id}", branch_name.clone());

    // Save LocalRepository in .oxen directory
    let mut local_repo = LocalRepository::from_remote(remote_repo.clone(), repo_path)?;
    repo_path.clone_into(&mut local_repo.path);
    local_repo.set_remote(DEFAULT_REMOTE_NAME, &remote_repo.remote.url);
    local_repo.set_min_version(remote_repo.min_version());
    local_repo.set_remote_mode(Some(true));

    println!("More");

    let workspace = api::client::workspaces::create_with_new_branch(
        &remote_repo,
        &branch_name,
        &workspace_id,
        Path::new("/"),
        Some(name.clone()),
    )
    .await?;

        println!("workspace");

    // TODO: Different messages here? Still colorize?
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
            // TODO: When would this ever occur?
            let err_msg = format!(
                "Remote-mode repo for workspace {} already exists",
                workspace_id.clone()
            );
            println!("{}", err_msg.yellow().bold());
            return Err(OxenError::basic_str("Err: Cannot "));
        }
        other => {
            println!(
                "{}",
                format!("Unexpected workspace status: {}", other).red()
            );
        }
    }
    println!("{} {}", "Workspace ID:".green().bold(), workspace.id.bold());

    local_repo.add_workspace(name.clone());
    local_repo.set_workspace(name.clone())?;
    local_repo.save()?;

    if remote_repo.is_empty {
        println!("The remote repository is empty. Oxen has configured the local repository, but there are no files yet.");
        return Ok(local_repo);
    }

    println!("End");

    // Fetch Merkle tree for commit
    repositories::fetch::fetch_branch(&local_repo, &opts.fetch_opts).await?;

    // Set head branch
    repositories::branches::set_head(&local_repo, branch_name)?;

    // Notify the server that we are done cloning
    // TODO: Convert to 'remote-mode clone' notif
    api::client::repositories::post_clone(&remote_repo).await?;

    Ok(local_repo)
}
