//! # oxen fetch
//!
//! Download objects and refs from the remote repository
//!

use crate::api;
use crate::core::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, RemoteBranch};

/// # Fetch the remote branches and objects
pub async fn fetch(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    for remote in repo.remotes.iter() {
        fetch_remote(repo, &remote.name).await?;
    }

    Ok(vec![])
}

pub async fn fetch_remote(
    repo: &LocalRepository,
    remote_name: &str,
) -> Result<Vec<Branch>, OxenError> {
    let remote = repo
        .get_remote(remote_name)
        .ok_or(OxenError::remote_not_set(remote_name))?;
    let remote_repo = api::remote::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let remote_branches = api::remote::branches::list(&remote_repo).await?;
    let local_branches = api::local::branches::list(repo)?;

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

    // Pull the new branches
    let indexer = EntryIndexer::new(repo)?;
    for branch in branches_to_create {
        println!("Fetch remote branch: {}/{}", remote_name, branch.name);
        let rb = RemoteBranch {
            remote: remote.name.to_owned(),
            branch: branch.name.to_owned(),
        };
        indexer
            .pull_most_recent_commit_object(&remote_repo, &rb, false)
            .await?;
    }

    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::test;

    #[tokio::test]
    async fn test_fetch_branches() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Create a few local branches and push them
            let branches = ["test_moo", "test_moo_2"];
            for branch in branches.iter() {
                command::create_checkout(&repo, branch)?;
                let filepath = repo.path.join(format!("file_{}.txt", branch));
                test::write_txt_file_to_path(&filepath, format!("a file on {}", branch))?;
                command::add(&repo, &filepath)?;
                command::commit(&repo, &format!("Adding file on {}", branch))?;
                command::push(&repo).await?;
            }

            // Clone the main branch, then fetch the others
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &new_repo_dir.join("new_repo"))
                        .await?;
                let branches = api::local::branches::list(&cloned_repo)?;

                assert_eq!(1, branches.len());

                command::fetch(&cloned_repo).await?;

                let branches = api::local::branches::list(&cloned_repo)?;
                assert_eq!(3, branches.len());

                let current_branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
                assert_eq!(current_branch.name, DEFAULT_BRANCH_NAME);

                api::remote::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }
}
