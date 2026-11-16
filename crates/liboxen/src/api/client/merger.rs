//! Interact with the remote repository to get information about mergeability of branches
//!

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{RemoteRepository, User};
use crate::view::merge::{MergeResult, MergeSuccessResponse, Mergeable, MergeableResponse};

/// Can check the mergeability of head into base
/// base or head are strings that can be branch names or commit ids
pub async fn mergeable(
    remote_repo: &RemoteRepository,
    base: &str,
    head: &str,
) -> Result<Mergeable, OxenError> {
    let uri = format!("/merge/{base}..{head}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("api::client::merger::mergeability url: {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: MergeableResponse = serde_json::from_str(&body)?;
    Ok(response.mergeable)
}

/// Merge the head branch into the base branch. `author` attributes the merge commit.
pub async fn merge(
    remote_repo: &RemoteRepository,
    base: &str,
    head: &str,
    author: &User,
) -> Result<MergeResult, OxenError> {
    let uri = format!("/merge/{base}..{head}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("api::client::merger::merge url: {url}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(author).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: MergeSuccessResponse = serde_json::from_str(&body)?;
    Ok(response.commits)
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{LocalRepository, RemoteRepository};
    use crate::opts::FetchOpts;
    use crate::repositories;
    use crate::test;

    /// Starts a case on branches of its own, both pushed: `base` at a commit of its own on top of
    /// the fixture's, and `head` forked from it and checked out. The commit gives the case a parent
    /// no other case shares, so two cases making the same commit in the same second get distinct
    /// ids.
    async fn start_case(repo: &LocalRepository, base: &str, head: &str) -> Result<(), OxenError> {
        repositories::checkout(repo, DEFAULT_BRANCH_NAME).await?;
        repositories::branches::create_checkout(repo, base)?;
        let marker = repo.path.join(format!("{base}.txt"));
        test::write_txt_file_to_path(&marker, base)?;
        repositories::add(repo, &marker).await?;
        repositories::commit(repo, &format!("Start {base}"))?;
        repositories::push(repo).await?;

        repositories::branches::create_checkout(repo, head)?;
        repositories::push(repo).await?;
        Ok(())
    }

    /// Commits `contents` to each of `files` on the checked-out branch, as one commit.
    async fn commit_files(
        repo: &LocalRepository,
        files: &[(&str, &str)],
        message: &str,
    ) -> Result<(), OxenError> {
        for (file, contents) in files {
            let path = repo.path.join(file);
            test::write_txt_file_to_path(&path, contents)?;
            repositories::add(repo, &path).await?;
        }
        repositories::commit(repo, message)?;
        Ok(())
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_merger_cases_on_readme_remote() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            case_multiple_commits_until_the_base_conflicts(&local_repo, &remote_repo).await?;
            case_merge_unique(&local_repo, &remote_repo).await?;
            // Last, since it commits to the fixture's main, which the other cases fork from
            case_head_with_no_commits_stays_mergeable(&local_repo, &remote_repo).await?;
            Ok(remote_repo)
        })
        .await
    }

    async fn case_head_with_no_commits_stays_mergeable(
        local_repo: &LocalRepository,
        remote_repo: &RemoteRepository,
    ) -> Result<(), OxenError> {
        // Forks from main itself rather than a commit of its own. The server checks a new branch's
        // commit against main's, and only a head at main's own commit has a tree identical to it.
        let (base, head) = (DEFAULT_BRANCH_NAME, "no-commits-head");
        repositories::checkout(local_repo, base).await?;
        repositories::branches::create_checkout(local_repo, head)?;
        repositories::push(local_repo).await?;

        let mergeability = api::client::merger::mergeable(remote_repo, base, head).await?;
        assert!(mergeability.is_mergeable);
        assert_eq!(
            mergeability.commits.len(),
            1,
            "a head that added nothing carries only the commit it forked from"
        );

        // Put the base ahead of the head
        repositories::checkout(local_repo, base).await?;
        commit_files(local_repo, &[("file_1.txt", "hello")], "adding file 1").await?;
        repositories::push(local_repo).await?;

        let mergeability = api::client::merger::mergeable(remote_repo, base, head).await?;
        assert!(mergeability.is_mergeable);
        assert_eq!(mergeability.commits.len(), 1);
        Ok(())
    }

    async fn case_multiple_commits_until_the_base_conflicts(
        local_repo: &LocalRepository,
        remote_repo: &RemoteRepository,
    ) -> Result<(), OxenError> {
        let (base, head) = ("conflicts-base", "conflicts-head");
        start_case(local_repo, base, head).await?;

        // Two commits on the head branch, the first of which rewrites README.md
        commit_files(
            local_repo,
            &[
                ("README.md", "I am the README now"),
                ("file_1.txt", "hello"),
            ],
            "adding file 1",
        )
        .await?;
        commit_files(local_repo, &[("file_2.txt", "world")], "adding file 2").await?;
        repositories::push(local_repo).await?;

        let mergeability = api::client::merger::mergeable(remote_repo, base, head).await?;
        assert!(mergeability.is_mergeable);
        assert_eq!(mergeability.commits.len(), 3);
        assert_eq!(mergeability.conflicts.len(), 0);

        // Rewrite the same file on the base branch
        repositories::checkout(local_repo, base).await?;
        commit_files(
            local_repo,
            &[("README.md", "I am on the base conflicting the README")],
            "modifying readme on the base",
        )
        .await?;
        repositories::push(local_repo).await?;

        let mergeability = api::client::merger::mergeable(remote_repo, base, head).await?;
        assert!(!mergeability.is_mergeable);
        assert_eq!(mergeability.commits.len(), 3);
        assert_eq!(
            mergeability.conflicts.len(),
            1,
            "both branches rewrote README.md"
        );
        Ok(())
    }

    async fn case_merge_unique(
        local_repo: &LocalRepository,
        remote_repo: &RemoteRepository,
    ) -> Result<(), OxenError> {
        let (base, head) = ("merge-base", "merge-head");
        start_case(local_repo, base, head).await?;

        // Add a file on the head branch
        let new_file_name = "merge_file.txt";
        commit_files(local_repo, &[(new_file_name, "hello")], "adding file").await?;
        repositories::push(local_repo).await?;

        // Merge the head branch into base
        let author = UserConfig::get()?.to_user();
        let merge_result = api::client::merger::merge(remote_repo, base, head, &author).await?;

        repositories::checkout(local_repo, base).await?;
        let commits_before = repositories::commits::list(local_repo)?;
        repositories::pull::pull_remote_branch(local_repo, &FetchOpts::from_branch(base)).await?;

        let commits_after = repositories::commits::list(local_repo)?;
        assert!(commits_after.len() > commits_before.len());

        let path = local_repo.path.join(new_file_name);
        assert!(path.exists());

        // Check that the branch was updated
        let new_head = repositories::branches::current_branch(local_repo)?.unwrap();
        assert_eq!(new_head.commit_id, merge_result.merge.id);
        Ok(())
    }
}
