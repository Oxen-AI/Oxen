use std::path::Path;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::entries::PaginatedDirEntriesResponse;
use crate::view::PaginatedDirEntries;

pub async fn list(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path_str = directory.as_ref().to_str().unwrap();
    let uri = if path_str.is_empty() || path_str == "." {
        format!("/workspaces/{workspace_id}/ls?page={page}&page_size={page_size}")
    } else {
        format!("/workspaces/{workspace_id}/ls/{path_str}?page={page}&page_size={page_size}")
    };
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("workspaces::ls url: {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: PaginatedDirEntriesResponse = serde_json::from_str(&body).map_err(|err| {
        OxenError::basic_str(format!(
            "api::workspaces::ls error parsing response from {url}\n\nErr {err:?}\n\n{body}"
        ))
    })?;
    Ok(response.entries)
}

#[cfg(test)]
mod tests {
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::test;
    use crate::view::entries::EMetadataEntry;
    use crate::{api, constants};

    #[tokio::test]
    async fn test_workspace_ls_root() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let _workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;

            let result = api::client::workspaces::ls::list(
                &remote_repo,
                &workspace_id,
                "",
                constants::DEFAULT_PAGE_NUM,
                constants::DEFAULT_PAGE_SIZE,
            )
            .await?;

            assert!(!result.entries.is_empty(), "Should return entries at root");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_ls_with_additions() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let _workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;

            // Upload a file to the workspace
            let test_file = local_repo.path.join("new_file.txt");
            crate::util::fs::write_to_path(&test_file, "new content")?;
            api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                "",
                test_file,
            )
            .await?;

            let result = api::client::workspaces::ls::list(
                &remote_repo,
                &workspace_id,
                "",
                constants::DEFAULT_PAGE_NUM,
                constants::DEFAULT_PAGE_SIZE,
            )
            .await?;

            // Find the added file
            let added = result
                .entries
                .iter()
                .find(|e| e.filename() == "new_file.txt");
            assert!(added.is_some(), "Should find newly added file");

            if let Some(EMetadataEntry::WorkspaceMetadataEntry(ws_entry)) = added {
                assert!(ws_entry.changes.is_some(), "Added file should have changes");
            }

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_workspace_ls_with_removals() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let _workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;

            // Remove a file from the workspace
            let rm_path = std::path::Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            api::client::workspaces::changes::rm(&remote_repo, &workspace_id, &rm_path).await?;

            let result = api::client::workspaces::ls::list(
                &remote_repo,
                &workspace_id,
                "annotations/train",
                constants::DEFAULT_PAGE_NUM,
                constants::DEFAULT_PAGE_SIZE,
            )
            .await?;

            // The removed file should NOT appear in the listing
            let removed = result
                .entries
                .iter()
                .find(|e| e.filename() == "bounding_box.csv");
            assert!(
                removed.is_none(),
                "Removed file should not appear in workspace ls"
            );

            Ok(remote_repo)
        })
        .await
    }
}
