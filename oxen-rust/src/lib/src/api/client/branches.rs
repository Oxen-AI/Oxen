use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{Branch, Commit, LocalRepository, RemoteRepository};
use crate::opts::PaginateOpts;
use crate::view::{
    BranchLockResponse, BranchNewFromBranchName, BranchNewFromCommitId, BranchRemoteMerge,
    BranchResponse, CommitResponse, ListBranchesResponse, PaginatedEntryVersions,
    PaginatedEntryVersionsResponse, StatusMessage,
};
use serde_json::json;
use std::path::Path;

pub async fn get_by_name(
    repository: &RemoteRepository,
    branch_name: impl AsRef<str>,
) -> Result<Option<Branch>, OxenError> {
    let branch_name = branch_name.as_ref();
    let uri = format!("/branches/{branch_name}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let status = res.status();
    if 404 == status {
        return Ok(None);
    }

    let body = client::parse_json_body(&url, res).await?;
    let response: BranchResponse = serde_json::from_str(&body)?;
    Ok(Some(response.branch))
}

/// Create a new branch from an existing branch
pub async fn create_from_branch(
    repository: &RemoteRepository,
    new_name: impl AsRef<str>,
    from_name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let new_name = new_name.as_ref();
    let from_name = from_name.as_ref();

    let url = api::endpoint::url_from_repo(repository, "/branches")?;
    log::debug!("branches::create_from_branch {}", url);

    let params = serde_json::to_string(&BranchNewFromBranchName {
        new_name: new_name.to_string(),
        from_name: from_name.to_string(),
    })?;

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: BranchResponse = serde_json::from_str(&body)?;
    Ok(response.branch)
}

/// Create a new remote branch from a commit
/// The commit must already exist on the remote
pub async fn create_from_commit(
    repository: &RemoteRepository,
    new_name: impl AsRef<str>,
    commit: &Commit,
) -> Result<Branch, OxenError> {
    let new_name = new_name.as_ref();

    let url = api::endpoint::url_from_repo(repository, "/branches")?;
    log::debug!("branches::create_from_commit {}", url);

    let params = serde_json::to_string(&BranchNewFromCommitId {
        new_name: new_name.to_string(),
        commit_id: commit.id.to_string(),
    })?;

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: BranchResponse = serde_json::from_str(&body)?;
    Ok(response.branch)
}

pub async fn create_from_commit_id(
    repository: &RemoteRepository,
    new_name: impl AsRef<str>,
    commit_id: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let new_name = new_name.as_ref();
    let commit_id = commit_id.as_ref();

    let url = api::endpoint::url_from_repo(repository, "/branches")?;
    log::debug!("branches::create_from_commit_id {}", url);

    let params = serde_json::to_string(&BranchNewFromCommitId {
        new_name: new_name.to_string(),
        commit_id: commit_id.to_string(),
    })?;

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: BranchResponse = serde_json::from_str(&body)?;
    Ok(response.branch)
}

/// List all branches on the remote
pub async fn list(repository: &RemoteRepository) -> Result<Vec<Branch>, OxenError> {
    let url = api::endpoint::url_from_repo(repository, "/branches")?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: ListBranchesResponse = serde_json::from_str(&body)?;
    Ok(response.branches)
}

/// Update a remote branch to point to a new commit
pub async fn update(
    repository: &RemoteRepository,
    branch_name: impl AsRef<str>,
    commit: &Commit,
) -> Result<Branch, OxenError> {
    let branch_name = branch_name.as_ref();
    let uri = format!("/branches/{branch_name}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("api::client::branches::update url: {}", url);

    let params = serde_json::to_string(&json!({ "commit_id": commit.id }))?;
    let client = client::new_for_url(&url)?;
    let res = client.put(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: BranchResponse = serde_json::from_str(&body)?;
    Ok(response.branch)
}

// Creates a merge commit between two commits on the server if possible, returning the commit
pub async fn maybe_create_merge(
    repository: &RemoteRepository,
    branch_name: &str,
    local_head_id: &str,
    remote_head_id: &str, // Remote head pre-push - merge target
) -> Result<Commit, OxenError> {
    let uri = format!("/branches/{branch_name}/merge");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("api::client::branches::maybe_create_merge url: {}", url);

    let commits = BranchRemoteMerge {
        client_commit_id: local_head_id.to_string(),
        server_commit_id: remote_head_id.to_string(),
    };
    let params = serde_json::to_string(&commits)?;

    let client = client::new_for_url(&url)?;
    let res = client.put(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response.commit)
}

/// # Delete a remote branch
pub async fn delete_remote(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch_name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    if let Some(remote) = repo.get_remote(&remote) {
        if let Some(remote_repo) = api::client::repositories::get_by_remote(&remote).await? {
            if let Some(branch) =
                api::client::branches::get_by_name(&remote_repo, &branch_name).await?
            {
                api::client::branches::delete(&remote_repo, &branch.name).await?;
                Ok(branch)
            } else {
                Err(OxenError::remote_branch_not_found(branch_name))
            }
        } else {
            Err(OxenError::remote_repo_not_found(&remote.url))
        }
    } else {
        Err(OxenError::remote_not_set(remote))
    }
}

pub async fn delete(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<StatusMessage, OxenError> {
    let uri = format!("/branches/{branch_name}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("Deleting branch: {}", url);
    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: StatusMessage = serde_json::from_str(&body)?;
    Ok(response)
}

pub async fn lock(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<StatusMessage, OxenError> {
    let uri = format!("/branches/{branch_name}/lock");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("Locking branch: {}", url);
    let client = client::new_for_url(&url)?;
    let res = client.post(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: StatusMessage = serde_json::from_str(&body)?;
    Ok(response)
}

pub async fn unlock(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<StatusMessage, OxenError> {
    let uri = format!("/branches/{branch_name}/unlock");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("Unlocking branch: {}", url);
    let client = client::new_for_url(&url)?;
    let res = client.post(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: StatusMessage = serde_json::from_str(&body)?;
    Ok(response)
}

pub async fn is_locked(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<bool, OxenError> {
    let uri = format!("/branches/{branch_name}/lock");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("Checking if branch is locked: {}", url);
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: BranchLockResponse = serde_json::from_str(&body)?;
    Ok(response.is_locked)
}

pub async fn latest_synced_commit(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<Commit, OxenError> {
    let uri = format!("/branches/{branch_name}/latest_synced_commit");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("Retrieving latest synced commit for branch...");
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response.commit)
}

pub async fn list_entry_versions(
    repository: &RemoteRepository,
    branch_name: &str,
    path: &Path,
    page_opts: &PaginateOpts,
) -> Result<PaginatedEntryVersions, OxenError> {
    let path_str = path.to_string_lossy();
    let uri = format!(
        "/branches/{branch_name}/versions/{path_str}?page={}&page_size={}",
        page_opts.page_num, page_opts.page_size
    );
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("Listing entry versions for branch: {}", url);
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: PaginatedEntryVersionsResponse = serde_json::from_str(&body)?;
    Ok(response.versions)
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::config::UserConfig;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::PaginateOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_create_remote_branch() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // add and commit a file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file)?;
            repositories::commit(&local_repo, "Added a new file")?;

            // set proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // push it
            repositories::push(&local_repo).await?;

            let name = "my-branch";
            let branch =
                api::client::branches::create_from_branch(&remote_repo, name, "main").await?;
            assert_eq!(branch.name, name);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_create_remote_branch_from_existing() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // add and commit a file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file)?;
            repositories::commit(&local_repo, "Added a new file")?;

            // set proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // push it
            repositories::push(&local_repo).await?;

            let name = "my-branch";
            let from = "old-branch";
            api::client::branches::create_from_branch(&remote_repo, from, DEFAULT_BRANCH_NAME)
                .await?;
            let branch =
                api::client::branches::create_from_branch(&remote_repo, name, from).await?;
            assert_eq!(branch.name, name);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_branch_by_name() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // add and commit a file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file)?;
            repositories::commit(&local_repo, "Added a new file")?;

            // set proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // push it
            repositories::push(&local_repo).await?;

            let branch_name = "my-branch";
            api::client::branches::create_from_branch(&remote_repo, branch_name, "main").await?;

            let branch = api::client::branches::get_by_name(&remote_repo, branch_name).await?;
            assert!(branch.is_some());
            assert_eq!(branch.unwrap().name, branch_name);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_remote_branches() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // Create and push the main branch
            // add a file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file)?;
            repositories::commit(&local_repo, "Added a new file")?;

            // Set proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push it
            repositories::push(&local_repo).await?;

            api::client::branches::create_from_branch(
                &remote_repo,
                "branch-1",
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            api::client::branches::create_from_branch(
                &remote_repo,
                "branch-2",
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let branches = api::client::branches::list(&remote_repo).await?;
            assert_eq!(branches.len(), 3);

            assert!(branches.iter().any(|b| b.name == "branch-1"));
            assert!(branches.iter().any(|b| b.name == "branch-2"));
            assert!(branches.iter().any(|b| b.name == DEFAULT_BRANCH_NAME));

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_branch() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // add and commit a file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file)?;
            repositories::commit(&local_repo, "Added a new file")?;

            // set proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // push it
            repositories::push(&local_repo).await?;

            let branch_name = "my-branch";
            api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let branch = api::client::branches::get_by_name(&remote_repo, branch_name).await?;
            assert!(branch.is_some());
            let branch = branch.unwrap();
            assert_eq!(branch.name, branch_name);

            api::client::branches::delete(&remote_repo, branch_name).await?;

            let deleted_branch =
                api::client::branches::get_by_name(&remote_repo, branch_name).await?;
            assert!(deleted_branch.is_none());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_latest_synced_commit_no_lock() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // add and commit a file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file)?;
            repositories::commit(&local_repo, "Added a new file")?;

            // set proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // push it
            repositories::push(&local_repo).await?;

            let branch_name = "my-branch";
            api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let branch = api::client::branches::get_by_name(&remote_repo, branch_name)
                .await?
                .unwrap();
            let commit =
                api::client::branches::latest_synced_commit(&remote_repo, branch_name).await?;
            assert_eq!(commit.id, branch.commit_id);
            Ok(remote_repo)
        })
        .await
    }

    #[test]
    fn test_rename_current_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // add and commit a file
            let new_file = repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&repo, new_file)?;
            repositories::commit(&repo, "Added a new file")?;

            // Create and checkout branch
            let og_branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, og_branch_name)?;

            // Rename branch
            let new_branch_name = "feature/brave-new-world";
            repositories::branches::rename_current_branch(&repo, new_branch_name)?;

            // Check that the branch name has changed
            let current_branch = repositories::branches::current_branch(&repo)?.unwrap();
            assert_eq!(current_branch.name, new_branch_name);

            // Check that old branch no longer exists
            repositories::branches::list(&repo)?
                .iter()
                .for_each(|branch| {
                    assert_ne!(branch.name, og_branch_name);
                });

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_delete_remote_branch() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // Create new branch
            let new_branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, new_branch_name)?;

            // Push new branch
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                new_branch_name,
            )
            .await?;

            // Delete the branch
            api::client::branches::delete(&remote_repo, new_branch_name).await?;

            let remote_branches = api::client::branches::list(&remote_repo).await?;
            assert_eq!(1, remote_branches.len());

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_should_not_push_branch_that_does_not_exist() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push main branch first
            if repositories::push::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, "main")
                .await
                .is_err()
            {
                panic!("Pushing main branch should work");
            }

            // Then try to push branch that doesn't exist
            if repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                "branch-does-not-exist",
            )
            .await
            .is_ok()
            {
                panic!("Should not be able to push branch that does not exist");
            }

            let remote_branches = api::client::branches::list(&remote_repo).await?;
            assert_eq!(1, remote_branches.len());

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_delete_branch_you_are_on() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            // add and commit a file
            let new_file = repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&repo, new_file)?;
            repositories::commit(&repo, "Added a new file")?;

            let branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add another commit on this branch that moves us ahead of main
            if repositories::branches::delete(&repo, branch_name).is_ok() {
                panic!("Should not be able to delete the branch you are on");
            }

            Ok(())
        })
        .await
    }

    #[test]
    fn test_cannot_force_delete_branch_you_are_on() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // add and commit a file
            let new_file = repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&repo, new_file)?;
            repositories::commit(&repo, "Added a new file")?;

            let branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add another commit on this branch that moves us ahead of main
            if repositories::branches::force_delete(&repo, branch_name).is_ok() {
                panic!("Should not be able to force delete the branch you are on");
            }

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_cannot_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            // add and commit a file
            let new_file = repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&repo, new_file)?;
            repositories::commit(&repo, "Added a new file")?;

            let og_branches = repositories::branches::list(&repo)?;
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add another commit on this branch
            let labels_path = repo.path.join("labels.txt");
            repositories::add(&repo, labels_path)?;
            repositories::commit(&repo, "adding initial labels file")?;

            // Checkout main again
            repositories::checkout(&repo, og_branch.name).await?;

            // Should not be able to delete `my-branch` because it is ahead of `main`
            if repositories::branches::delete(&repo, branch_name).is_ok() {
                panic!(
                    "Should not be able to delete the branch that is ahead of the one you are on"
                );
            }

            // Should be one less branch
            let leftover_branches = repositories::branches::list(&repo)?;
            assert_eq!(og_branches.len(), leftover_branches.len() - 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_force_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            // add and commit a file
            let new_file = repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&repo, new_file)?;
            repositories::commit(&repo, "Added a new file")?;

            let og_branches = repositories::branches::list(&repo)?;
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add another commit on this branch
            let labels_path = repo.path.join("labels.txt");
            repositories::add(&repo, labels_path)?;
            repositories::commit(&repo, "adding initial labels file")?;

            // Checkout main again
            repositories::checkout(&repo, og_branch.name).await?;

            // Force delete
            repositories::branches::force_delete(&repo, branch_name)?;

            // Should be one less branch
            let leftover_branches = repositories::branches::list(&repo)?;
            assert_eq!(og_branches.len(), leftover_branches.len());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_branch_latest_synced_commit_no_lock() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;
            let remote_main = api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                .await?
                .unwrap();

            // Save commit
            let main_head_before = remote_main.commit_id.clone();
            // Check latest synced
            let latest_synced =
                api::client::branches::latest_synced_commit(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(latest_synced.id, main_head_before);

            // Now push a new commit
            let labels_path = repo.path.join("labels.txt");
            test::write_txt_file_to_path(&labels_path, "I am the labels file")?;
            repositories::add(&repo, labels_path)?;
            repositories::commit(&repo, "adding labels file")?;
            repositories::push(&repo).await?;

            // Get main again, latest should have moved
            let remote_main = api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                .await?
                .unwrap();
            let main_head_after = remote_main.commit_id.clone();
            let latest_synced =
                api::client::branches::latest_synced_commit(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(latest_synced.id, main_head_after);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_branch_latest_synced_commit_with_lock() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;
            let remote_main = api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                .await?
                .unwrap();

            // Save commit
            let main_head_before = remote_main.commit_id.clone();
            // Check latest synced
            let latest_synced =
                api::client::branches::latest_synced_commit(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(latest_synced.id, main_head_before);

            // Lock up the branch
            api::client::branches::lock(&remote_repo, DEFAULT_BRANCH_NAME).await?;

            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            // Use remote staging to commit without releasing lock (push releases lock)
            let labels_path = repo.path.join("labels.txt");
            test::write_txt_file_to_path(&labels_path, "I am the labels file")?;
            api::client::workspaces::files::add_many(
                &remote_repo,
                &workspace_id,
                "./",
                vec![labels_path],
            )
            .await?;
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace_id,
                &NewCommitBody {
                    message: "adding labels file".to_string(),
                    author: "me".to_string(),
                    email: "me&aol.gov".to_string(),
                },
            )
            .await?;

            // Get main again, latest should still be behind
            let remote_main = api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                .await?
                .unwrap();
            let main_head_after = remote_main.commit_id.clone();
            let latest_synced =
                api::client::branches::latest_synced_commit(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert!(latest_synced.id != main_head_after);
            assert_eq!(latest_synced.id, main_head_before);

            // Release the lock (as if push is complete)
            api::client::branches::unlock(&remote_repo, DEFAULT_BRANCH_NAME).await?;
            let latest_synced_updated =
                api::client::branches::latest_synced_commit(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(latest_synced_updated.id, main_head_after);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_entry_versions() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("annotations", |mut repo| async move {
            // Set up remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Get path to existing annotations file
            let file_path = Path::new("annotations")
                .join("test")
                .join("annotations.csv");
            let file_repo_path = repo.path.join(&file_path);

            // Store initial commit id
            let commit_1 = repositories::commits::head_commit(&repo)?;

            // Modify annotations file with new line
            test::append_line_txt_file(&file_repo_path, "test/new_image.jpg,unknown,1.0,1.0,1,1")?;
            repositories::add(&repo, &file_repo_path)?;
            let commit_2 = repositories::commit(&repo, "adding new annotation")?;

            // Push to remote
            repositories::push(&repo).await?;

            // Get paginated versions from API
            let result = api::client::branches::list_entry_versions(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &file_path,
                &PaginateOpts::default(),
            )
            .await?;

            // Verify results
            assert_eq!(result.versions.len(), 2);

            // Most recent version should be first
            assert_eq!(result.versions[0].commit.id, commit_2.id);
            assert_eq!(result.versions[1].commit.id, commit_1.id);

            Ok(())
        })
        .await
    }
}
