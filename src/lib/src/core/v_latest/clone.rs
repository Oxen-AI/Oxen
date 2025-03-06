use crate::constants::DEFAULT_REMOTE_NAME;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};
use crate::opts::CloneOpts;
use crate::util;
use crate::{api, repositories};

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

    repositories::pull::pull_remote_branch(&local_repo, &opts.fetch_opts).await?;

    // Notify the server that we are done cloning
    api::client::repositories::post_clone(&remote_repo).await?;

    Ok(local_repo)
}
