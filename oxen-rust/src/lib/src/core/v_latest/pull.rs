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
    let mut fetch_opts = fetch_opts.clone();
    println!("🐂 oxen pull {remote} {branch}");

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    api::client::repositories::pre_pull(&remote_repo).await?;

    let previous_head_commit = repositories::commits::head_commit_maybe(repo)?;

    // Fetch all the tree nodes and the entries
    fetch_opts.should_update_branch_head = false;
    let remote_branch = fetch::fetch_remote_branch(repo, &remote_repo, &fetch_opts).await?;

    let new_head_commit = repositories::revisions::get(repo, &remote_branch.commit_id)?.ok_or(
        OxenError::revision_not_found(remote_branch.commit_id.to_owned().into()),
    )?;

    match previous_head_commit {
        Some(previous_head_commit) => {
            log::debug!(
                "checking if we need to merge previous {} new {}",
                previous_head_commit.id,
                new_head_commit.id
            );
            if previous_head_commit.id != new_head_commit.id {
                match repositories::merge::merge_commit_into_base(
                    repo,
                    &new_head_commit,
                    &previous_head_commit,
                )
                .await
                {
                    Ok(Some(commit)) => {
                        // Merge successful, update branch head
                        repositories::branches::update(repo, branch, commit.id)?;
                    }
                    Ok(None) => {
                        // Merge conflict, keep the previous commit
                        return Err(OxenError::merge_conflict(
                            "There was a merge conflict, please resolve it before pulling",
                        ));
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        None => {
            repositories::branches::update(repo, branch, new_head_commit.id)?;
            repositories::checkout::checkout(repo, branch).await?;
        }
    }

    api::client::repositories::post_pull(&remote_repo).await?;

    Ok(())
}
