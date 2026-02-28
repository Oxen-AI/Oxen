//! # oxen fetch
//!
//! Download objects and refs from the remote repository
//!

use crate::api;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, RemoteBranch, RemoteRepository};
use crate::opts::fetch_opts::FetchOpts;
use crate::repositories;
use futures::{stream, StreamExt};

/// # Fetch the remote branches and objects
pub async fn fetch_all(
    repo: &LocalRepository,
    fetch_opts: &FetchOpts,
) -> Result<Vec<Branch>, OxenError> {
    let remote = repo
        .get_remote(&fetch_opts.remote)
        .ok_or(OxenError::remote_not_set(fetch_opts.remote.clone()))?;
    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    api::client::repositories::pre_fetch(&remote_repo).await?;
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

    log::debug!("Branches to create: {branches_to_create:?}");
    log::debug!("Branches to fetch: {branches_to_fetch:?}");

    let branches_to_process = branches_to_fetch
        .into_iter()
        .map(|branch| (branch, false))
        .chain(branches_to_create.into_iter().map(|branch| (branch, true)));

    // Build a stream of fetch futures
    let stream = stream::iter(branches_to_process.map(|(branch, should_update_head)| {
        let rb = RemoteBranch {
            remote: remote.name.to_owned(),
            branch: branch.name.to_owned(),
        };

        let opts = FetchOpts {
            should_update_branch_head: should_update_head,
            branch: branch.name.to_owned(),
            remote: remote.name.to_owned(),
            ..fetch_opts.clone()
        };

        log::debug!(
            "Fetching remote branch: {}/{} (update_head: {})",
            remote_repo.name,
            rb.branch,
            should_update_head
        );
        let repo = repo.clone();
        let remote_repo = remote_repo.clone();
        async move { fetch_remote_branch(&repo, &remote_repo, &opts).await }
    }))
    // We were having concurrency issues unpacking the merkle tree tarballs
    // setting to 1 for now, then we can revisit
    .buffer_unordered(1)
    .collect::<Vec<_>>();

    let branches: Result<Vec<Branch>, OxenError> = stream.await.into_iter().collect();

    api::client::repositories::post_fetch(&remote_repo).await?;

    branches
}

pub async fn fetch_branch(
    repo: &LocalRepository,
    fetch_opts: &FetchOpts,
) -> Result<Branch, OxenError> {
    let remote = repo
        .get_remote(&fetch_opts.remote)
        .ok_or(OxenError::remote_not_set(fetch_opts.remote.clone()))?;
    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    api::client::repositories::pre_fetch(&remote_repo).await?;
    let branch = fetch_remote_branch(repo, &remote_repo, fetch_opts).await?;
    api::client::repositories::post_fetch(&remote_repo).await?;
    Ok(branch)
}

pub async fn fetch_remote_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    fetch_opts: &FetchOpts,
) -> Result<Branch, OxenError> {
    println!(
        "Fetch remote branch: {}/{}",
        remote_repo.name, fetch_opts.branch
    );

    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::fetch::fetch_remote_branch(repo, remote_repo, fetch_opts).await,
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::opts::fetch_opts::FetchOpts;
    use crate::repositories;

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
                let filepath = repo.path.join(format!("file_{branch}.txt"));
                test::write_txt_file_to_path(&filepath, format!("a file on {branch}"))?;
                repositories::add(&repo, &filepath).await?;
                repositories::commit(&repo, &format!("Adding file on {branch}"))?;
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

                repositories::fetch_all(&cloned_repo, &FetchOpts::new()).await?;

                let branches = repositories::branches::list(&cloned_repo)?;
                assert_eq!(3, branches.len());

                let current_branch = repositories::branches::current_branch(&cloned_repo)?.unwrap();
                assert_eq!(current_branch.name, DEFAULT_BRANCH_NAME);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(())
            })
            .await
        })
        .await
    }
}
