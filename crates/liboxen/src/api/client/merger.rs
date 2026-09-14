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
    use crate::error::OxenError;
    use crate::opts::FetchOpts;
    use crate::repositories;
    use crate::test;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_merger_head_with_no_commits_stays_mergeable() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push(&local_repo).await?;

            let mergeability = api::client::merger::mergeable(&remote_repo, base, head).await?;
            assert!(mergeability.is_mergeable);
            assert_eq!(
                mergeability.commits.len(),
                1,
                "a head that added nothing carries only the commit it forked from"
            );

            // Put the base ahead of the head
            repositories::checkout(&local_repo, base).await?;
            let path = local_repo.path.join("file_1.txt");
            test::write_txt_file_to_path(&path, "hello")?;
            repositories::add(&local_repo, &path).await?;
            repositories::commit(&local_repo, "adding file 1")?;
            repositories::push(&local_repo).await?;

            let mergeability = api::client::merger::mergeable(&remote_repo, base, head).await?;
            assert!(mergeability.is_mergeable);
            assert_eq!(mergeability.commits.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_merger_multiple_commits_until_the_base_conflicts() -> Result<(), OxenError>
    {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push(&local_repo).await?;

            // Two commits on the head branch, the first of which rewrites README.md
            let path = local_repo.path.join("README.md");
            test::write_txt_file_to_path(&path, "I am the README now")?;
            repositories::add(&local_repo, &path).await?;

            let path = local_repo.path.join("file_1.txt");
            test::write_txt_file_to_path(&path, "hello")?;
            repositories::add(&local_repo, &path).await?;
            repositories::commit(&local_repo, "adding file 1")?;

            let path = local_repo.path.join("file_2.txt");
            test::write_txt_file_to_path(&path, "world")?;
            repositories::add(&local_repo, &path).await?;
            repositories::commit(&local_repo, "adding file 2")?;

            repositories::push(&local_repo).await?;

            let mergeability = api::client::merger::mergeable(&remote_repo, base, head).await?;
            assert!(mergeability.is_mergeable);
            assert_eq!(mergeability.commits.len(), 3);
            assert_eq!(mergeability.conflicts.len(), 0);

            // Rewrite the same file on the base branch
            repositories::checkout(&local_repo, base).await?;
            let path = local_repo.path.join("README.md");
            test::write_txt_file_to_path(&path, "I am on main conflicting the README")?;
            repositories::add(&local_repo, &path).await?;
            repositories::commit(&local_repo, "modifying readme on main")?;
            repositories::push(&local_repo).await?;

            let mergeability = api::client::merger::mergeable(&remote_repo, base, head).await?;
            assert!(!mergeability.is_mergeable);
            assert_eq!(mergeability.commits.len(), 3);
            assert_eq!(
                mergeability.conflicts.len(),
                1,
                "both branches rewrote README.md"
            );

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_merger_merge_unique() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push(&local_repo).await?;

            // Modify a file on the head branch
            let new_file_name = "merge_file.txt";
            let path = local_repo.path.join(new_file_name);
            test::write_txt_file_to_path(&path, "hello")?;
            repositories::add(&local_repo, &path).await?;
            repositories::commit(&local_repo, "adding file")?;
            repositories::push(&local_repo).await?;

            // Merge the head branch into base
            let author = UserConfig::get()?.to_user();
            let merge_result =
                api::client::merger::merge(&remote_repo, base, head, &author).await?;

            repositories::checkout(&local_repo, base).await?;
            let commits_before = repositories::commits::list(&local_repo)?;
            let fetch_opts = FetchOpts::new();
            repositories::pull::pull_remote_branch(&local_repo, &fetch_opts).await?;

            let commits_after = repositories::commits::list(&local_repo)?;
            assert!(commits_after.len() > commits_before.len());

            let path = local_repo.path.join(new_file_name);
            assert!(path.exists());

            // Check that the branch was updated
            let new_head = repositories::branches::current_branch(&local_repo)?.unwrap();
            assert_eq!(new_head.commit_id, merge_result.merge.id);

            Ok(remote_repo)
        })
        .await
    }
}
