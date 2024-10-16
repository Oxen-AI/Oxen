//! # oxen fetch
//!
//! Download objects and refs from the remote repository
//!

use crate::api;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, RemoteBranch, RemoteRepository};
use crate::repositories;

/// # Fetch the remote branches and objects
pub async fn fetch(repo: &LocalRepository, all: bool) -> Result<Vec<Branch>, OxenError> {
    for remote in repo.remotes().iter() {
        fetch_remote(repo, &remote.name, all).await?;
    }

    Ok(vec![])
}

pub async fn fetch_remote(
    repo: &LocalRepository,
    remote_name: &str,
    all: bool,
) -> Result<Vec<Branch>, OxenError> {
    let remote = repo
        .get_remote(remote_name)
        .ok_or(OxenError::remote_not_set(remote_name))?;
    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let remote_branches = api::client::branches::list(&remote_repo).await?;
    let local_branches = repositories::branches::list(repo)?;

    // Find branches that are on the remote but not on the local
    let mut branches_to_create = vec![];
    let mut branches_to_fetch = vec![];
    for remote_branch in remote_branches {
        if !local_branches.iter().any(|b| b.name == remote_branch.name) {
            branches_to_create.push(remote_branch);
        } else {
            branches_to_fetch.push(remote_branch);
        }
    }

    // TODO: handle branches_to_fetch?
    // Fetch the new branches
    for branch in branches_to_create {
        let rb = RemoteBranch {
            remote: remote.name.to_owned(),
            branch: branch.name.to_owned(),
        };

        fetch_remote_branch(repo, &remote_repo, &rb, all).await?;
    }

    Ok(vec![])
}

pub async fn fetch_remote_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    rb: &RemoteBranch,
    all: bool,
) -> Result<(), OxenError> {
    println!("Fetch remote branch: {}/{}", remote_repo.name, rb.branch);

    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let indexer = core::v0_10_0::index::EntryIndexer::new(repo)?;
            indexer
                .pull_most_recent_commit_object(remote_repo, rb, false)
                .await?;
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::fetch::fetch_remote_branch(repo, remote_repo, rb, all).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_fetch_branches() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|mut repo| async move {
            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push the main branch
            repositories::push(&repo).await?;

            // Create a few local branches and push them
            let branches = ["test_moo", "test_moo_2"];
            for branch in branches.iter() {
                repositories::branches::create_checkout(&repo, branch)?;
                let filepath = repo.path.join(format!("file_{}.txt", branch));
                test::write_txt_file_to_path(&filepath, format!("a file on {}", branch))?;
                repositories::add(&repo, &filepath)?;
                repositories::commit(&repo, &format!("Adding file on {}", branch))?;
                repositories::push(&repo).await?;
            }

            // Clone the main branch, then fetch the others
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;
                let branches = repositories::branches::list(&cloned_repo)?;

                assert_eq!(1, branches.len());

                repositories::fetch(&cloned_repo, false).await?;

                let branches = repositories::branches::list(&cloned_repo)?;
                assert_eq!(3, branches.len());

                let current_branch = repositories::branches::current_branch(&cloned_repo)?.unwrap();
                assert_eq!(current_branch.name, DEFAULT_BRANCH_NAME);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }
}
