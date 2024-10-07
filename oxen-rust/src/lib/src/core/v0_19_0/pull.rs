use crate::api;
use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch};
use crate::repositories;

use crate::core::v0_19_0::fetch;

pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let rb = RemoteBranch::default();
    pull_remote_branch(repo, &rb.remote, &rb.branch, false).await
}

pub async fn pull_shallow(repo: &LocalRepository) -> Result<(), OxenError> {
    let pull_all = false;
    repositories::pull_remote_branch(repo, DEFAULT_REMOTE_NAME, DEFAULT_BRANCH_NAME, pull_all).await
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    let pull_all = true;
    repositories::pull_remote_branch(repo, DEFAULT_REMOTE_NAME, DEFAULT_BRANCH_NAME, pull_all).await
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch: impl AsRef<str>,
    all: bool,
) -> Result<(), OxenError> {
    let remote = remote.as_ref();
    let branch = branch.as_ref();
    println!("🐂 oxen pull {} {}", remote, branch);

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let rb = RemoteBranch {
        remote: remote.to_string(),
        branch: branch.to_string(),
    };

    let previous_head_commit = repositories::commits::head_commit_maybe(repo)?;

    // Fetch all the tree nodes and the entries
    fetch::fetch_remote_branch(repo, &remote_repo, &rb, all).await?;

    // TODO: this should ideally be in the repositories::pull module,
    // but I'm not sure how that will interact with the v0_10_0 code
    repositories::branches::checkout_branch(repo, branch).await?;

    let new_head_commit = repositories::revisions::get(repo, branch)?
        .ok_or(OxenError::revision_not_found(branch.into()))?;

    // Merge if there are changes
    if let Some(previous_head_commit) = previous_head_commit {
        if previous_head_commit.id != new_head_commit.id {
            repositories::merge::merge_commit_into_base(
                repo,
                &new_head_commit,
                &previous_head_commit,
            )?;
        }
    }

    repositories::branches::set_head(repo, branch)?;

    Ok(())
}
