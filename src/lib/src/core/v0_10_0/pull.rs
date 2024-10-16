//! Legacy pull logic for oxen v0.10.0 and above

use crate::core::v0_10_0::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch, RemoteRepository};
use crate::opts::PullOpts;

/// Pull a repository's data from default branches origin/main
/// Defaults defined in
/// `constants::DEFAULT_REMOTE_NAME` and `constants::DEFAULT_BRANCH_NAME`
pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: false,
                should_update_head: true,
            },
        )
        .await
}

pub async fn pull_shallow(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: false,
                should_update_head: true,
            },
        )
        .await
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: true,
                should_update_head: true,
            },
        )
        .await
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
    all: bool,
) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer
        .pull(
            &rb,
            PullOpts {
                should_pull_all: all,
                should_update_head: true,
            },
        )
        .await
}

pub async fn pull_remote_repo(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    rb: &RemoteBranch,
    opts: &PullOpts,
) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    indexer.pull_remote_repo(remote_repo, rb, opts).await
}
