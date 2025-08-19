use serde::{Deserialize, Serialize};

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::opts::DFOpts;
use crate::util;
use crate::view::entries::PaginatedMetadataEntriesResponse;
use crate::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use futures_util::StreamExt;
use std::io::Write;
use std::path::Path;

use crate::model::RemoteRepository;
use crate::view::{JsonDataFrameViewResponse, JsonDataFrameViews, StatusMessage};

pub mod columns;
pub mod embeddings;
pub mod rows;

#[derive(Serialize, Deserialize)]
struct PutParam {
    is_indexed: bool,
}

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<WorkspaceJsonDataFrameViewResponse, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();
    let query_str = opts.to_http_query_params();
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<WorkspaceJsonDataFrameViewResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err = format!(
                "workspaces::data_frames::get error parsing from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn download(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: &DFOpts, // opts holds output path
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();
    let query_str = opts.to_http_query_params();
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/download/{file_path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    // Download the file and save it to the output path
    let Some(output_path_str) = &opts.output else {
        return Err(OxenError::basic_str("output path is required"));
    };
    let output_path = Path::new(&output_path_str);
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;

    // Create the output file
    log::debug!("Download creating output file {:?}", output_path);
    let mut file = util::fs::file_create(output_path)?;

    // Stream the response body to the file
    let mut stream = res.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk)?;
    }

    Ok(())
}

pub async fn is_indexed(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<bool, OxenError> {
    let res = get(remote_repo, workspace_id, path, &DFOpts::empty()).await?;
    Ok(res.is_indexed)
}

pub async fn list(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    workspace_id: &str,
) -> Result<PaginatedMetadataEntriesResponse, OxenError> {
    let uri = format!("/workspaces/{workspace_id}/data_frames/branch/{branch_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<PaginatedMetadataEntriesResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err = format!(
                "api::workspaces::get_by_branch error parsing from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn index(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<StatusMessage, OxenError> {
    let path = util::fs::linux_path(path.as_ref());
    put(
        remote_repo,
        workspace_id,
        path,
        &serde_json::json!({"is_indexed": true}),
    )
    .await
}

pub async fn unindex(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<StatusMessage, OxenError> {
    put(
        remote_repo,
        workspace_id,
        path,
        &serde_json::json!({"is_indexed": false}),
    )
    .await
}

pub async fn put(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<StatusMessage, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();

    let uri = format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(data)?;

    let client = client::new_for_url(&url)?;
    let res = client.put(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err =
                format!("api::workspaces::put error parsing from {url}\n\nErr {err:?} \n\n{body}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn restore(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/workspaces/{workspace_id}/data_frames/resource/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("workspaces::data_frames::restore {}", url);
    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("workspaces::data_frames::restore got body: {}", body);
    Ok(())
}

pub async fn diff(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    page_num: usize,
    page_size: usize,
) -> Result<JsonDataFrameViews, OxenError> {
    let file_path_str = path.to_str().unwrap();

    let uri = format!("/workspaces/{workspace_id}/data_frames/diff/{file_path_str}?page={page_num}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("diff got body: {}", body);
            let response: Result<JsonDataFrameViewResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(data) => Ok(data.data_frame),

                Err(err) => Err(OxenError::basic_str(format!(
                    "api::staging::diff error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::staging::diff Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn rename_data_frame(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
) -> Result<StatusMessage, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();

    let uri = format!("/workspaces/{workspace_id}/data_frames/rename/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(&serde_json::json!({
        "new_path": new_path.as_ref().to_string_lossy()
    }))?;

    let client = client::new_for_url(&url)?;
    let res = client.put(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err =
                format!("api::workspaces::put error parsing from {url}\n\nErr {err:?} \n\n{body}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::config::UserConfig;
    use crate::constants::{
        DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE, DEFAULT_REMOTE_NAME,
    };
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::{api, repositories, util};
    use crate::{command, test};

    #[tokio::test]
    async fn test_get_by_resource() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");

            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::put(
                &remote_repo,
                workspace_id,
                path,
                &serde_json::json!({"is_indexed": true}),
            )
            .await?;

            let res = api::client::workspaces::data_frames::get(
                &remote_repo,
                workspace_id,
                path,
                &DFOpts::empty(),
            )
            .await?;

            assert_eq!(res.status.status_message, "resource_found");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

            let res = api::client::workspaces::data_frames::list(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                workspace_id,
            )
            .await?;

            assert_eq!(res.entries.entries.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_rename_data_frame() -> Result<(), OxenError> {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            // Define the original and new paths for the data frame
            let original_path = Path::new("annotations/train/bounding_box.csv");
            let new_path = Path::new("new/dir/bounding_box_renamed.csv");

            // Index the original data frame
            api::client::workspaces::data_frames::index(
                &remote_repo,
                &workspace.id,
                &original_path,
            )
            .await?;

            // Rename the data frame
            let rename_response = api::client::workspaces::data_frames::rename_data_frame(
                &remote_repo,
                &workspace.id,
                &original_path,
                &new_path,
            )
            .await?;
            assert_eq!(rename_response.status, "success");
            let user = UserConfig::get()?.to_user();
            // Commit the changes
            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email.to_owned(),
                message: "renamed data frame".to_string(),
            };

            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace.id,
                &new_commit,
            )
            .await?;

            // Verify that the data frame has been renamed
            let renamed_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &new_path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(renamed_df.status.status_message, "resource_found");

            let original_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &original_path,
                DFOpts::empty(),
            )
            .await?;

            assert_eq!(original_df.status.status_message, "resource_found");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_edit_rename_and_commit_data_frame_to_nonexistent_folder() -> Result<(), OxenError>
    {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            // Define the original path for the data frame

            let original_path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let new_path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("nonexistent_folder"))
                .join(Path::new("bounding_box_edited.csv"));

            // Index the original data frame
            api::client::workspaces::data_frames::index(
                &remote_repo,
                &workspace.id,
                &original_path,
            )
            .await?;

            let original_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &original_path,
                DFOpts::empty(),
            )
            .await?;

            let og_row_count = original_df.data_frame.view.to_df().await.height();
            let new_row = r#"{"file": "train/dog_4.jpg", "label": "dog", "min_x": 15.0, "min_y": 20.0, "width": 300, "height": 400}"#;


            // Edit the data frame (this is a placeholder for your actual edit logic)
            let edit_response = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace.id,
                &original_path,
                new_row.to_string(), // Assuming this function takes the new path as a parameter
            )
            .await;
            assert!(edit_response.is_ok());
            api::client::workspaces::data_frames::rename_data_frame(
                &remote_repo,
                &workspace.id,
                &original_path,
                &new_path,
            )
            .await?;

            // Commit the changes
            let user = UserConfig::get()?.to_user();
            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email.to_owned(),
                message: "edited data frame".to_string(),
            };

            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace.id,
                &new_commit,
            )
            .await?;

            // Verify that the edited data frame has the new name
            let edited_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &new_path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(edited_df.status.status_message, "resource_found");
            let new_row_count = edited_df.data_frame.view.to_df().await.height();
            assert_eq!(new_row_count, og_row_count + 1);

            // Verify that the original data frame still exists
            let original_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &original_path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(original_df.data_frame.view.to_df().await.height(), og_row_count);
            assert_eq!(original_df.status.status_message, "resource_found");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_edit_rename_and_commit_data_frame() -> Result<(), OxenError> {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            // Define the original path for the data frame

            let original_path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let new_path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box_edited.csv"));

            // Index the original data frame
            api::client::workspaces::data_frames::index(
                &remote_repo,
                &workspace.id,
                &original_path,
            )
            .await?;

            let original_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &original_path,
                DFOpts::empty(),
            )
            .await?;

            let og_row_count = original_df.data_frame.view.to_df().await.height();
            let new_row = r#"{"file": "train/dog_4.jpg", "label": "dog", "min_x": 15.0, "min_y": 20.0, "width": 300, "height": 400}"#;


            // Edit the data frame (this is a placeholder for your actual edit logic)
            let edit_response = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace.id,
                &original_path,
                new_row.to_string(), // Assuming this function takes the new path as a parameter
            )
            .await;
            assert!(edit_response.is_ok());

            api::client::workspaces::data_frames::rename_data_frame(
                &remote_repo,
                &workspace.id,
                &original_path,
                &new_path,
            )
            .await?;

            // Commit the changes
            let user = UserConfig::get()?.to_user();
            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email.to_owned(),
                message: "edited data frame".to_string(),
            };

            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace.id,
                &new_commit,
            )
            .await?;

            // Verify that the edited data frame has the new name
            let edited_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &new_path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(edited_df.status.status_message, "resource_found");
            let new_row_count = edited_df.data_frame.view.to_df().await.height();
            assert_eq!(new_row_count, og_row_count + 1);

            // Verify that the original data frame still exists
            let original_df = api::client::data_frames::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &original_path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(original_df.data_frame.view.to_df().await.height(), og_row_count);
            assert_eq!(original_df.status.status_message, "resource_found");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_query_workspace_data_frames_with_sql() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

            test::run_empty_dir_test_async(|sync_dir| async move {
                let output_path = sync_dir.join("test_download.csv");
                let mut opts = DFOpts::empty();
                opts.sql = Some("SELECT * FROM df WHERE label = 'dog'".to_string());
                opts.output = Some(output_path.clone());
                let df = api::client::workspaces::data_frames::get(
                    &remote_repo,
                    workspace_id,
                    &path,
                    &opts,
                )
                .await?;

                // There should be 4 rows with label = dog
                let df = df.data_frame.unwrap();
                let view_df = df.view.to_df().await;
                assert_eq!(view_df.height(), 4);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

            test::run_empty_dir_test_async(|sync_dir| async move {
                let output_path = sync_dir.join("test_download.csv");
                let mut opts = DFOpts::empty();
                opts.output = Some(output_path.clone());
                api::client::workspaces::data_frames::download(
                    &remote_repo,
                    workspace_id,
                    &path,
                    &opts,
                )
                .await?;

                assert!(output_path.exists());

                // Check the file contents are the same
                let file_contents = std::fs::read_to_string(output_path)?;
                let expected_contents = std::fs::read_to_string(local_repo.path.join(path))?;
                assert_eq!(file_contents, expected_contents);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_workspace_data_frames_to_different_format() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

            test::run_empty_dir_test_async(|sync_dir| async move {
                let output_path = sync_dir.join("test_download.jsonl");
                let mut opts = DFOpts::empty();
                opts.output = Some(output_path.clone());
                api::client::workspaces::data_frames::download(
                    &remote_repo,
                    workspace_id,
                    &path,
                    &opts,
                )
                .await?;

                assert!(output_path.exists());

                // Check the file contents are the same
                let og_df = tabular::read_df(local_repo.path.join(path), DFOpts::empty()).await?;
                let download_df = tabular::read_df(&output_path, DFOpts::empty()).await?;
                assert_eq!(og_df.height(), download_df.height());
                assert_eq!(og_df.width(), download_df.width());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_workspace_data_frames_with_sql() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

            test::run_empty_dir_test_async(|sync_dir| async move {
                let output_path = sync_dir.join("test_download.csv");
                let mut opts = DFOpts::empty();
                opts.sql = Some("SELECT * FROM df WHERE label = 'dog'".to_string());
                opts.output = Some(output_path.clone());
                api::client::workspaces::data_frames::download(
                    &remote_repo,
                    workspace_id,
                    &path,
                    &opts,
                )
                .await?;

                assert!(output_path.exists());

                // There should be 4 rows with label = dog
                let df = tabular::read_df(&output_path, DFOpts::empty()).await?;
                assert_eq!(df.height(), 4);
                assert_eq!(df.width(), 6);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_workspace_data_frames_with_aggregation_sql() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

            test::run_empty_dir_test_async(|sync_dir| async move {
                let output_path = sync_dir.join("test_download.csv");
                let mut opts = DFOpts::empty();
                opts.sql = Some("SELECT label, COUNT(*) FROM df GROUP BY label".to_string());
                opts.output = Some(output_path.clone());
                api::client::workspaces::data_frames::download(
                    &remote_repo,
                    workspace_id,
                    &path,
                    &opts,
                )
                .await?;

                assert!(output_path.exists());

                // There should be 2 rows by 2 columns
                let df = tabular::read_df(&output_path, DFOpts::empty()).await?;
                println!("{df}");
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 2);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_index_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            let res = api::client::workspaces::data_frames::index(&remote_repo, workspace_id, path)
                .await?;

            assert_eq!(res.status, "success");

            let res =
                api::client::workspaces::data_frames::unindex(&remote_repo, workspace_id, path)
                    .await?;

            assert_eq!(res.status, "success");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_index_workspace_data_frame_with_binary_column() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            let path = test::test_binary_column_parquet_file();

            let file_name = "binary_col.parquet";
            let dst_path = local_repo.path.join(file_name);
            util::fs::copy(&path, &dst_path)?;

            repositories::add(&local_repo, &dst_path).await?;
            repositories::commit(&local_repo, "add binary column parquet file")?;
            command::config::set_remote(
                &mut local_repo,
                DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;
            repositories::push(&local_repo).await?;

            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            let res =
                api::client::workspaces::data_frames::index(&remote_repo, workspace_id, file_name)
                    .await?;

            assert_eq!(res.status, "success");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_data_frame_diff() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = "some_workspace";
            let path = Path::new("annotations/train/bounding_box.csv");

            let res =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(res.is_ok());

            let res = api::client::workspaces::data_frames::index(&remote_repo, workspace_id, path)
                .await?;

            assert_eq!(res.status, "success");

            let res = api::client::workspaces::data_frames::diff(
                &remote_repo,
                workspace_id,
                path,
                1,
                100,
            )
            .await;

            assert!(res.is_ok());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_restore_modified_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await;
            assert!(workspace.is_ok());

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let result_1 = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await;
            assert!(result_1.is_ok());

            let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let result_2 = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string(),
                ).await;
            assert!(result_2.is_ok());


            // Make sure both got staged
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            log::debug!("Got this diff {:?}", diff);
            assert_eq!(diff.view.size.height, 2);

            // Delete result_2
            let result_delete = api::client::workspaces::data_frames::restore(
                &remote_repo,
                &workspace_id,
                &path,
            ).await;
            assert!(result_delete.is_ok());

            // Should be cleared
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;
            assert_eq!(diff.view.size.height, 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_modified_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id)
                    .await;
            assert!(workspace.is_ok());

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::index(
                &remote_repo,
                &workspace_id,
                &path
            ).await?;

            api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string()
            ).await?;

            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            assert_eq!(diff.view.size.height, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_delete_row_from_modified_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id)
                    .await;
            assert!(workspace.is_ok());

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await?;

            let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let (_df_2, row_id_2) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string(),
                ).await?;

            // Make sure both got staged
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            assert_eq!(diff.view.size.height, 2);

            let uuid_2 = row_id_2.unwrap();
            // Delete result_2
            let result_delete = api::client::workspaces::data_frames::rows::delete(
                &remote_repo,
                &workspace_id,
                &path,
                &uuid_2
            ).await;
            assert!(result_delete.is_ok());

            // Make there is only one left
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;
            assert_eq!(diff.view.size.height, 1);

            Ok(remote_repo)
        })
        .await
    }

    // Test fast forward merge on pull
    /*
    oxen init
    oxen add .
    oxen commit -m "add data"
    oxen push
    # update data frame file on server
    oxen pull repo_a (should be fast forward)
    # update data frame file on server
    oxen pull repo_a (should be fast forward)
    */
    #[tokio::test]
    async fn test_update_df_on_server_fast_forward_pull() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            // Skip server side duckdb tests on windows
            return Ok(());
        }

        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|empty_dir| async move {
                let cloned_repo_dir = empty_dir.join("repo_b");
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &cloned_repo_dir).await?;

                // Read the initial data
                let bbox_filename = Path::new("annotations")
                    .join("train")
                    .join("bounding_box.csv");
                let bbox_file = cloned_repo.path.join(&bbox_filename);
                let og_df = tabular::read_df(&bbox_file, DFOpts::empty()).await?;

                // Update the file on the remote repo
                let user = UserConfig::get()?.to_user();
                let workspace_id = "workspace_a";
                let workspace =
                    api::client::workspaces::create(&remote_repo, &DEFAULT_BRANCH_NAME, &workspace_id)
                        .await;
                assert!(workspace.is_ok());

                // train/d-o-double-g.jpg,dog,101.5,32.0,385,330
                let directory = Path::new("annotations").join("train");
                let path = directory.join("bounding_box.csv");
                let data = "{\"file\":\"d-o-double-g.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

                api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                        &remote_repo,
                        workspace_id,
                        &path,
                        data.to_string()
                    ).await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending d-o-double-g data".to_string(),
                };
                api::client::workspaces::commit(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id, &new_commit).await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let bbox_file = cloned_repo.path.join(&bbox_filename);
                let df = tabular::read_df(&bbox_file, DFOpts::empty()).await?;
                assert_eq!(df.height(), og_df.height() + 1);

                // Add a more rows on this branch
                let workspace_id = "workspace_b";
                let workspace =
                    api::client::workspaces::create(&remote_repo, &DEFAULT_BRANCH_NAME, &workspace_id)
                        .await;
                assert!(workspace.is_ok());

                // train/d-o-triple-g.jpg,dog,101.5,32.0,385,330
                let directory = Path::new("annotations").join("train");
                let path = directory.join("bounding_box.csv");
                let data = "{\"file\":\"d-o-triple-g.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

                api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                        &remote_repo,
                        workspace_id,
                        &path,
                        data.to_string()
                    ).await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending d-o-triple-g data".to_string(),
                };
                api::client::workspaces::commit(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id, &new_commit).await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let bbox_file = cloned_repo.path.join(&bbox_filename);
                let df = tabular::read_df(&bbox_file, DFOpts::empty()).await?;
                assert_eq!(df.height(), og_df.height() + 2);

                Ok(())
            })
            .await?;
            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_update_root_df_on_server_fast_forward_pull() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|empty_dir| async move {
                let cloned_repo_dir = empty_dir.join("repo_b");
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &cloned_repo_dir).await?;

                // Read the initial data
                let prompts_filename = Path::new("prompts.jsonl");
                let prompts_file = cloned_repo.path.join(prompts_filename);
                let og_df = tabular::read_df(&prompts_file, DFOpts::empty()).await?;

                // Update the file on the remote repo
                let user = UserConfig::get()?.to_user();
                let workspace_id = "workspace_a";
                let workspace = api::client::workspaces::create(
                    &remote_repo,
                    &DEFAULT_BRANCH_NAME,
                    &workspace_id,
                )
                .await;
                assert!(workspace.is_ok());

                // Add a row to the prompts file
                let data = "{\"prompt\": \"What is another meaning of life?\", \"label\": \"43\"}";

                api::client::workspaces::data_frames::index(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                )
                .await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                    data.to_string(),
                )
                .await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending 43 data".to_string(),
                };
                api::client::workspaces::commit(
                    &remote_repo,
                    DEFAULT_BRANCH_NAME,
                    workspace_id,
                    &new_commit,
                )
                .await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let prompts_file = cloned_repo.path.join(prompts_filename);
                let df = tabular::read_df(&prompts_file, DFOpts::empty()).await?;
                assert_eq!(df.height(), og_df.height() + 1);

                // Add a more rows on this branch
                let workspace_id = "workspace_b";
                let workspace = api::client::workspaces::create(
                    &remote_repo,
                    &DEFAULT_BRANCH_NAME,
                    &workspace_id,
                )
                .await;
                assert!(workspace.is_ok());

                // Add a row to the prompts file
                let data =
                    "{\"prompt\": \"What is another another meaning of life?\", \"label\": \"44\"}";

                api::client::workspaces::data_frames::index(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                )
                .await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                    data.to_string(),
                )
                .await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending 44 data".to_string(),
                };
                api::client::workspaces::commit(
                    &remote_repo,
                    DEFAULT_BRANCH_NAME,
                    workspace_id,
                    &new_commit,
                )
                .await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let prompts_file = cloned_repo.path.join(prompts_filename);
                let df = tabular::read_df(&prompts_file, DFOpts::empty()).await?;
                assert_eq!(df.height(), og_df.height() + 2);

                Ok(())
            })
            .await?;
            Ok(remote_repo_copy)
        })
        .await
    }
}
