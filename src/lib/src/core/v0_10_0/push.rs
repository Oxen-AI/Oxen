use crate::constants::DEFAULT_BRANCH_NAME;
use crate::core::v0_10_0::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, RemoteBranch};
use crate::repositories;

pub async fn push(repo: &LocalRepository) -> Result<Branch, OxenError> {
    let mut remote_branch = RemoteBranch::default();

    // Push the currently checked out branch
    let Some(local_branch) = repositories::branches::current_branch(repo)? else {
        return Err(OxenError::local_branch_not_found(DEFAULT_BRANCH_NAME));
    };

    let local_branch_cpy = local_branch.clone();
    remote_branch.branch = local_branch_cpy.clone().name;
    let indexer = EntryIndexer::new(repo)?;
    indexer.push(local_branch_cpy, remote_branch).await?;
    Ok(local_branch)
}

pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch_name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let remote = remote.as_ref();
    let branch_name = branch_name.as_ref();

    let Some(local_branch) = repositories::branches::get_by_name(repo, branch_name)? else {
        return Err(OxenError::local_branch_not_found(branch_name));
    };

    let indexer = EntryIndexer::new(repo)?;
    let remote_branch = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch_name),
    };
    indexer.push(local_branch.clone(), remote_branch).await?;
    Ok(local_branch)
}
