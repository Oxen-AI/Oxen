use crate::config::RepositoryConfig;
use crate::constants::{DEFAULT_REMOTE_NAME, DEFAULT_VNODE_SIZE, REPO_CONFIG_FILENAME};
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};
use crate::opts::CloneOpts;
use crate::util;
use crate::{api, repositories};

use std::path::Path;

pub async fn clone_repo(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
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
    let repo_config_file = oxen_hidden_path.join(Path::new(REPO_CONFIG_FILENAME));
    let mut local_repo = LocalRepository::from_remote(remote_repo.clone(), repo_path)?;
    repo_path.clone_into(&mut local_repo.path);
    local_repo.set_remote(DEFAULT_REMOTE_NAME, &remote_repo.remote.url);
    local_repo.set_min_version(remote_repo.min_version());

    // Save remote config in .oxen/config.toml
    let remote_cfg = RepositoryConfig {
        remote_name: Some(DEFAULT_REMOTE_NAME.to_string()),
        remotes: vec![remote_repo.remote.clone()],
        min_version: Some(remote_repo.min_version().to_string()),
        vnode_size: Some(DEFAULT_VNODE_SIZE),
    };

    let toml = toml::to_string(&remote_cfg)?;
    util::fs::write_to_path(&repo_config_file, &toml)?;

    if remote_repo.is_empty {
        println!("The remote repository is empty. Oxen has configured the local repository, but there are no files yet.");
        return Ok(local_repo);
    }

    if opts.shallow {
        repositories::pull::pull_remote_branch_shallow(
            &local_repo,
            DEFAULT_REMOTE_NAME,
            &opts.branch,
        )
        .await?;
    } else {
        repositories::pull::pull_remote_branch(
            &local_repo,
            DEFAULT_REMOTE_NAME,
            &opts.branch,
            opts.all,
        )
        .await?;
    }

    Ok(local_repo)
}
