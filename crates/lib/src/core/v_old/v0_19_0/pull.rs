use crate::api;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::repositories;

use crate::core::v_latest::fetch;
use crate::opts::fetch_opts::FetchOpts;

pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let mut fetch_opts = FetchOpts::new();
    fetch_opts.depth = repo.depth();
    fetch_opts.subtree_paths = repo.subtree_paths();
    pull_remote_branch(repo, &fetch_opts).await
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    let fetch_opts = FetchOpts {
        all: true,
        depth: repo.depth(),
        subtree_paths: repo.subtree_paths(),
        ..FetchOpts::new()
    };
    repositories::pull_remote_branch(repo, &fetch_opts).await
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    fetch_opts: &FetchOpts,
) -> Result<(), OxenError> {
    let remote = &fetch_opts.remote;
    let branch = &fetch_opts.branch;
    println!("🐂 oxen pull {} {}", remote, branch);

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let previous_head_commit = repositories::commits::head_commit_maybe(repo)?;

    // Fetch all the tree nodes and the entries
    fetch::fetch_remote_branch(repo, &remote_repo, fetch_opts).await?;

    let new_head_commit = repositories::revisions::get(repo, branch)?
        .ok_or(OxenError::revision_not_found(branch.to_owned().into()))?;

    // Merge if there are changes
    if let Some(previous_head_commit) = &previous_head_commit {
        log::debug!(
            "checking if we need to merge previous {} new {}",
            previous_head_commit.id,
            new_head_commit.id
        );
        if previous_head_commit.id != new_head_commit.id {
            repositories::merge::merge_commit_into_base(
                repo,
                &new_head_commit,
                previous_head_commit,
            )?;
        }
    }

    if let Some(subtree_paths) = &fetch_opts.subtree_paths {
        let depth = fetch_opts.depth.unwrap_or(-1);
        repositories::branches::checkout_subtrees_from_commit(
            repo,
            &new_head_commit,
            subtree_paths,
            depth,
        )
        .await?;
    } else {
        repositories::branches::checkout_branch_from_commit(repo, branch, &previous_head_commit)
            .await?;
    }

    repositories::branches::set_head(repo, branch)?;

    Ok(())
}
