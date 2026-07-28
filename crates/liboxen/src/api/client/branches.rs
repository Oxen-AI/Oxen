use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{Branch, Commit, LocalRepository, RemoteRepository};
use crate::opts::PaginateOpts;
use crate::view::{
    BranchNewFromBranchName, BranchNewFromCommitId, BranchRemoteMerge, BranchResponse,
    CommitResponse, ListBranchesResponse, PaginatedEntryVersions, PaginatedEntryVersionsResponse,
    StatusMessage,
};
use serde_json::json;
use std::path::Path;

#[tracing::instrument(skip_all)]
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
    log::debug!("branches::create_from_branch {url}");

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
    log::debug!("branches::create_from_commit {url}");

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
    log::debug!("branches::create_from_commit_id {url}");

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
#[tracing::instrument(skip_all)]
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
    log::debug!("api::client::branches::update url: {url}");

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
    log::debug!("api::client::branches::maybe_create_merge url: {url}");

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
    let remote = repo
        .get_remote(&remote)
        .ok_or_else(|| OxenError::RemoteNotSet(remote.as_ref().to_string()))?;
    let remote_repo = api::client::repositories::get_by_remote(&remote).await?;
    let branch = api::client::branches::get_by_name(&remote_repo, &branch_name)
        .await?
        .ok_or_else(|| OxenError::remote_branch_not_found(branch_name))?;
    api::client::branches::delete(&remote_repo, &branch.name).await?;
    Ok(branch)
}

pub async fn delete(
    repository: &RemoteRepository,
    branch_name: &str,
) -> Result<StatusMessage, OxenError> {
    let uri = format!("/branches/{branch_name}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("Deleting branch: {url}");
    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: StatusMessage = serde_json::from_str(&body)?;
    Ok(response)
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
    log::debug!("Listing entry versions for branch: {url}");
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: PaginatedEntryVersionsResponse = serde_json::from_str(&body)?;
    Ok(response.versions)
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use super::{create_from_branch, delete, get_by_name, list, list_entry_versions};
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
    use crate::error::OxenError;
    use crate::opts::{PaginateOpts, PushOpts};
    use crate::repositories;
    use crate::repositories::branches as local_branches;
    use crate::repositories::push::push_remote_branch;
    use crate::test;
    use crate::util;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_branches() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            let new_file = local_repo.path.join("new_file.csv");
            util::fs::write(&new_file, "a,b\n1,2\n")?;
            // A data frame stored as a JSON array, which only its content shows to be tabular
            util::fs::write(local_repo.path.join("labels.json"), r#"[{"a": 1}]"#)?;
            repositories::add(&local_repo, &local_repo.path).await?;
            let commit_1 = repositories::commit(&local_repo, "Added a new file")?;
            test::append_line_txt_file(&new_file, "3,4")?;
            repositories::add(&local_repo, &new_file).await?;
            let commit_2 = repositories::commit(&local_repo, "Changed the new file")?;

            test::attach_remote_repo(&mut local_repo, &remote_repo)?;
            repositories::push(&local_repo).await?;

            let result = list_entry_versions(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                Path::new("new_file.csv"),
                &PaginateOpts::default(),
            )
            .await?;
            assert_eq!(result.versions.len(), 2);
            assert_eq!(
                result.versions[0].commit.id, commit_2.id,
                "the newest version comes first"
            );
            assert_eq!(result.versions[1].commit.id, commit_1.id);
            let json_result = list_entry_versions(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                Path::new("labels.json"),
                &PaginateOpts::default(),
            )
            .await?;
            assert_eq!(json_result.versions.len(), 1, "labels.json has one version");
            for versions in [&result.versions, &json_result.versions] {
                assert!(
                    versions.iter().all(|v| v.schema_hash.is_some()),
                    "every version of a data frame has a schema hash, got {versions:?}"
                );
            }

            let branch = create_from_branch(&remote_repo, "my-branch", DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, "my-branch");
            let found = get_by_name(&remote_repo, "my-branch").await?;
            assert_eq!(
                found.map(|branch| branch.name).as_deref(),
                Some("my-branch")
            );

            create_from_branch(&remote_repo, "old-branch", DEFAULT_BRANCH_NAME).await?;
            let branch = create_from_branch(&remote_repo, "from-old-branch", "old-branch").await?;
            assert_eq!(
                branch.name, "from-old-branch",
                "a branch can start from another branch"
            );

            let names: Vec<String> = list(&remote_repo)
                .await?
                .into_iter()
                .map(|branch| branch.name)
                .collect();
            assert_eq!(names.len(), 4, "{names:?}");
            for name in [
                DEFAULT_BRANCH_NAME,
                "my-branch",
                "old-branch",
                "from-old-branch",
            ] {
                assert!(
                    names.iter().any(|n| n == name),
                    "{name} is listed: {names:?}"
                );
            }

            delete(&remote_repo, "my-branch").await?;
            assert!(get_by_name(&remote_repo, "my-branch").await?.is_none());

            // A branch pushed from a local repo deletes the same way.
            local_branches::create_checkout(&local_repo, "pushed-branch")?;
            let opts = PushOpts {
                remote: DEFAULT_REMOTE_NAME.to_string(),
                branch: "pushed-branch".to_string(),
                ..Default::default()
            };
            push_remote_branch(&local_repo, &opts).await?;
            delete(&remote_repo, "pushed-branch").await?;

            let opts = PushOpts {
                branch: "branch-does-not-exist".to_string(),
                ..opts
            };
            assert!(
                push_remote_branch(&local_repo, &opts).await.is_err(),
                "a branch that does not exist cannot be pushed"
            );

            assert_eq!(
                list(&remote_repo).await?.len(),
                3,
                "the deleted branches are gone from the list"
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_local_branch_delete_and_rename() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            let new_file = repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&repo, new_file).await?;
            repositories::commit(&repo, "Added a new file")?;
            let og_branch_count = local_branches::count(&repo).await?;
            let og_branch = local_branches::current_branch(&repo)?.unwrap();

            let branch_name = "my-branch";
            local_branches::create_checkout(&repo, branch_name)?;
            assert!(
                local_branches::delete(&repo, branch_name).is_err(),
                "the branch you are on cannot be deleted"
            );
            assert!(
                local_branches::force_delete(&repo, branch_name).is_err(),
                "the branch you are on cannot be force deleted"
            );

            // A commit on my-branch moves it ahead of the original branch.
            repositories::add(&repo, repo.path.join("labels.txt")).await?;
            repositories::commit(&repo, "adding initial labels file")?;
            repositories::checkout(&repo, og_branch.name).await?;
            assert!(
                local_branches::delete(&repo, branch_name).is_err(),
                "a branch ahead of the current one cannot be deleted"
            );
            assert_eq!(local_branches::count(&repo).await?, og_branch_count + 1);
            local_branches::force_delete(&repo, branch_name)?;
            assert_eq!(
                local_branches::count(&repo).await?,
                og_branch_count,
                "a force delete removes a branch ahead of the current one"
            );

            let og_branch_name = "feature/world-explorer";
            local_branches::create_checkout(&repo, og_branch_name)?;
            let new_branch_name = "feature/brave-new-world";
            local_branches::rename_current_branch(&repo, new_branch_name)?;
            let current = local_branches::current_branch(&repo)?.unwrap();
            assert_eq!(current.name, new_branch_name);
            assert!(
                local_branches::list(&repo)
                    .await?
                    .iter()
                    .all(|branch| branch.name != og_branch_name),
                "the old name is gone"
            );

            Ok(())
        })
        .await
    }
}
