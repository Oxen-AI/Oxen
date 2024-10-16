use crate::config::repository_config::RepositoryConfig;
use crate::constants::{DEFAULT_REMOTE_NAME, REPO_CONFIG_FILENAME};

use crate::core::v0_10_0::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch, RemoteRepository};
use crate::opts::{CloneOpts, PullOpts};
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{api, util};

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

    // Save remote config in .oxen/config.toml
    let remote_cfg = RepositoryConfig {
        remote_name: Some(DEFAULT_REMOTE_NAME.to_string()),
        remotes: vec![remote_repo.remote.clone()],
        min_version: Some(remote_repo.min_version().to_string()),
        vnode_size: None,
    };

    let toml = toml::to_string(&remote_cfg)?;
    util::fs::write_to_path(&repo_config_file, &toml)?;

    // Pull all commit objects, but not entries
    let rb = RemoteBranch::from_branch(&opts.branch);
    let indexer = EntryIndexer::new(&local_repo)?;
    maybe_pull_entries(&local_repo, &remote_repo, &indexer, &rb, opts).await?;

    if opts.all {
        log::debug!("pulling all entries");
        let remote_branches = api::client::branches::list(&remote_repo).await?;
        if remote_branches.len() > 1 {
            println!(
                "🐂 Pre-fetching {} additional remote branches...",
                remote_branches.len() - 1
            );
        }

        let n_other_branches: u64 = if remote_branches.len() > 1 {
            (remote_branches.len() - 1) as u64
        } else {
            0
        };

        let bar = oxen_progress_bar(n_other_branches as u64, ProgressBarType::Counter);

        for branch in remote_branches {
            // We've already pulled the target branch in full
            if branch.name == rb.branch {
                continue;
            }

            let remote_branch = RemoteBranch::from_branch(&branch.name);
            indexer
                .pull_most_recent_commit_object(&remote_repo, &remote_branch, false)
                .await?;
            bar.inc(1);
        }
        bar.finish_and_clear();
    }

    println!(
        "\n🎉 cloned {} to {}/\n",
        remote_repo.remote.url, remote_repo.name
    );
    api::client::repositories::post_clone(&remote_repo).await?;

    Ok(local_repo)
}

async fn maybe_pull_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    indexer: &EntryIndexer,
    rb: &RemoteBranch,
    opts: &CloneOpts,
) -> Result<(), OxenError> {
    // Shallow means we will not pull the actual data until a user tells us to
    if opts.shallow {
        indexer
            .pull_most_recent_commit_object(remote_repo, rb, true)
            .await?;
        local_repo.write_is_shallow(true)?;
    } else {
        // Pull all entries
        indexer
            .pull(
                rb,
                PullOpts {
                    should_pull_all: opts.all,
                    should_update_head: true,
                },
            )
            .await?;
    }
    Ok(())
}
