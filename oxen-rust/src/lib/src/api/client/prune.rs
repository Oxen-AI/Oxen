//! # Remote Prune
//!
//! Trigger prune operations on a remote repository.
//!

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::oxen_response::ErrorResponse;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct PruneRequest {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PruneStats {
    pub nodes_scanned: usize,
    pub nodes_kept: usize,
    pub nodes_removed: usize,
    pub versions_scanned: usize,
    pub versions_kept: usize,
    pub versions_removed: usize,
    pub bytes_freed: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PruneResponse {
    pub status: String,
    pub status_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorResponse>,
    pub stats: PruneStats,
}

/// Trigger a prune operation on a remote repository
///
/// # Arguments
/// * `remote_repo` - The remote repository to prune
/// * `dry_run` - If true, only report what would be removed without actually removing it
///
/// # Returns
/// Statistics about the prune operation
pub async fn prune(remote_repo: &RemoteRepository, dry_run: bool) -> Result<PruneStats, OxenError> {
    let uri = "/prune".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let request_body = PruneRequest { dry_run };

    match client.post(&url).json(&request_body).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<PruneResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got PruneResponse: {:?}", val);
                    Ok(val.stats)
                }
                Err(err) => {
                    log::error!("Failed to parse PruneResponse from body: {}", body);
                    Err(OxenError::basic_str(format!(
                        "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    )))
                }
            }
        }
        Err(err) => {
            let err = format!("Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_remote_prune_dry_run() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let train_dir = repo_dir.join("train");
            util::fs::create_dir_all(&train_dir)?;
            let file1 = train_dir.join("file1.txt");
            test::write_txt_file_to_path(&file1, "file1")?;

            repositories::add(&local_repo, &file1).await?;
            repositories::commit(&local_repo, "add file1.txt")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            repositories::push(&local_repo).await?;

            // Run prune dry-run
            let stats = api::client::prune::prune(&remote_repo, true).await?;

            // Should not remove anything since all files are referenced
            assert_eq!(stats.nodes_removed, 0);
            assert_eq!(stats.versions_removed, 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_prune_removes_dangling_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let train_dir = repo_dir.join("train");
            util::fs::create_dir_all(&train_dir)?;

            // Create initial commit on main
            let file1 = train_dir.join("file1.txt");
            test::write_txt_file_to_path(&file1, "file1")?;
            repositories::add(&local_repo, &file1).await?;
            let initial_commit = repositories::commit(&local_repo, "add file1.txt")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            repositories::push(&local_repo).await?;

            // Create a feature branch locally
            let branch_name = "feature-branch";
            repositories::branches::create(&local_repo, branch_name, &initial_commit.id)?;
            repositories::checkout(&local_repo, branch_name).await?;

            // Add a commit to the feature branch
            let file2 = train_dir.join("file2.txt");
            test::write_txt_file_to_path(&file2, "file2")?;
            repositories::add(&local_repo, &file2).await?;
            repositories::commit(&local_repo, "add file2.txt")?;

            // Push the feature branch to remote
            repositories::push(&local_repo).await?;

            // Checkout main and delete the feature branch on remote
            repositories::checkout(&local_repo, "main").await?;
            api::client::branches::delete(&remote_repo, branch_name).await?;

            // Run prune (not dry-run) - should remove orphaned data from deleted branch
            let stats = api::client::prune::prune(&remote_repo, false).await?;

            // Should have removed some nodes and versions from the deleted branch
            assert!(stats.nodes_removed > 0 || stats.versions_removed > 0);

            Ok(())
        })
        .await
    }
}
