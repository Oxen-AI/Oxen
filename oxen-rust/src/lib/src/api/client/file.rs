use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::commit::NewCommitBody;
use crate::model::RemoteRepository;
use crate::view::CommitResponse;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::multipart::{Form, Part};
use std::path::Path;

pub async fn put_file(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    directory: impl AsRef<str>,
    file_path: impl AsRef<Path>,
    file_name: Option<impl AsRef<str>>,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let branch = branch.as_ref();
    let directory = directory.as_ref();
    let file_path = file_path.as_ref();
    let uri = format!("/file/{branch}/{directory}");
    log::debug!("put_file {uri:?}, file_path {file_path:?}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let file_part = Part::file(file_path).await?;
    let file_part = if let Some(file_name) = file_name {
        file_part.file_name(file_name.as_ref().to_string())
    } else {
        file_part
    };
    let mut form = Form::new().part("file", file_part);

    if let Some(body) = commit_body {
        form = form.text("name", body.author);
        form = form.text("email", body.email);
        form = form.text("message", body.message);
    }

    let req = client.put(&url).multipart(form);

    let res = req.send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response)
}

pub async fn get_file(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    file_path: impl AsRef<Path>,
) -> Result<Bytes, OxenError> {
    get_file_with_params(remote_repo, branch, file_path, None, None, None, None).await
}

/// Get a file with optional query parameters (for thumbnails, image resizing, etc.)
pub async fn get_file_with_params(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    file_path: impl AsRef<Path>,
    thumbnail: Option<bool>,
    width: Option<u32>,
    height: Option<u32>,
    timestamp: Option<f64>,
) -> Result<Bytes, OxenError> {
    let branch = branch.as_ref();
    let path_ref = file_path.as_ref();
    let file_path = path_ref
        .to_str()
        .ok_or_else(|| OxenError::basic_str(format!("Invalid UTF-8 in file path: {path_ref:?}")))?;
    let uri = format!("/file/{branch}/{file_path}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    // Build query parameters only for Some(...) values
    let mut query_params: Vec<(&str, String)> = Vec::new();
    if let Some(thumb) = thumbnail {
        query_params.push(("thumbnail", thumb.to_string()));
    }
    if let Some(w) = width {
        query_params.push(("width", w.to_string()));
    }
    if let Some(h) = height {
        query_params.push(("height", h.to_string()));
    }
    if let Some(ts) = timestamp {
        query_params.push(("timestamp", ts.to_string()));
    }

    let req = client.get(&url).query(&query_params);

    let res = req.send().await?;

    let res = res.error_for_status()?;
    let mut stream = res.bytes_stream();
    let mut buffer = BytesMut::new();
    while let Some(chunk_result) = stream.next().await {
        let chunk =
            chunk_result.map_err(|e| OxenError::basic_str(format!("Failed to read chunk: {e}")))?;
        buffer.extend_from_slice(&chunk);
    }

    Ok(buffer.freeze())
}

/// Get a video thumbnail
pub async fn get_file_thumbnail(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    file_path: impl AsRef<Path>,
    width: Option<u32>,
    height: Option<u32>,
    timestamp: Option<f64>,
) -> Result<Bytes, OxenError> {
    get_file_with_params(
        remote_repo,
        branch,
        file_path,
        Some(true),
        width,
        height,
        timestamp,
    )
    .await
}

/// Move/rename a file in place (mv in a temp workspace and commit)
pub async fn mv_file(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    source_path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let branch = branch.as_ref();
    let source_path = source_path.as_ref();
    let new_path = new_path.as_ref();

    let source_path_str = source_path.to_string_lossy().to_string();
    let new_path_str = new_path.to_string_lossy().to_string();

    let uri = format!("/file/{branch}/{source_path_str}");
    log::debug!("mv_file {uri:?}, new_path {new_path_str:?}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    // Build JSON body
    let mut body = serde_json::json!({
        "new_path": new_path_str
    });

    if let Some(commit) = commit_body {
        body["name"] = serde_json::Value::String(commit.author);
        body["email"] = serde_json::Value::String(commit.email);
        body["message"] = serde_json::Value::String(commit.message);
    }

    let req = client
        .patch(&url)
        .header("Content-Type", "application/json")
        .body(body.to_string());

    let res = req.send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response)
}

/// Delete a file in place (rm from a temp workspace and commit)
pub async fn delete_file(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    file_path: impl AsRef<Path>,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let branch = branch.as_ref();
    let file_path = file_path.as_ref();

    let file_path = file_path.to_string_lossy().to_string();

    let uri = format!("/file/{branch}/{file_path}");
    log::debug!("delete_file {uri:?}, file_path {file_path:?}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let mut form = Form::new();

    if let Some(body) = commit_body {
        form = form.text("name", body.author);
        form = form.text("email", body.email);
        form = form.text("message", body.message);
    }

    let req = client.delete(&url).multipart(form);

    let res = req.send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response)
}

#[cfg(test)]
mod tests {

    use actix_web::web::Bytes;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::{api, repositories, test, util};
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_update_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let branch_name = "main";
            let directory_name = "test_data";
            let file_path = test::test_img_file();
            let commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Update file test".to_string(),
            };

            let response = api::client::file::put_file(
                &remote_repo,
                branch_name,
                directory_name,
                &file_path,
                Some("test.jpeg"),
                Some(commit_body),
            )
            .await?;

            assert_eq!(response.status.status_message, "resource_created");

            // Pull changes from remote to local repo
            repositories::pull(&local_repo).await?;

            // Check that the file exists in the local repo after pulling
            let file_path_in_repo = local_repo.path.join(directory_name).join("test.jpeg");
            assert!(file_path_in_repo.exists());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_update_file_on_empty_repo() -> Result<(), OxenError> {
        test::run_empty_configured_remote_repo_test(|local_repo, remote_repo| async move {
            let branch_name = "main";
            let directory_name = "test_data";
            let file_path = test::test_img_file();
            let commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Update file test".to_string(),
            };

            let response = api::client::file::put_file(
                &remote_repo,
                branch_name,
                directory_name,
                &file_path,
                Some("test.jpeg"),
                Some(commit_body),
            )
            .await?;
            assert_eq!(response.status.status_message, "resource_created");

            // Pull changes from remote to local repo
            repositories::pull(&local_repo).await?;
            repositories::checkout(&local_repo, branch_name).await?;

            // // Check that the file exists in the local repo after pulling
            let file_path_in_repo = local_repo.path.join(directory_name).join("test.jpeg");
            assert!(file_path_in_repo.exists());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let file_path = test::test_bounding_box_csv();
            let bytes = api::client::file::get_file(&remote_repo, branch_name, file_path).await;

            assert!(bytes.is_ok());
            assert!(!bytes.unwrap().is_empty());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let prev_commits = repositories::commits::list_all(&local_repo)?;

            let branch_name = "main";
            let file_path = test::test_bounding_box_csv();

            let commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "remove file".to_string(),
            };

            // Delete the file on the remote repo
            let _commit_response = api::client::file::delete_file(
                &remote_repo,
                &branch_name,
                &file_path,
                Some(commit_body),
            )
            .await?;

            // Pull the change
            repositories::pull(&local_repo).await?;

            // Assert the commit was made and the file is removed
            assert!(!local_repo.path.join(&file_path).exists());

            /*
            let commit = commit_response.commit;
            let deleted_file_node =
                repositories::tree::get_node_by_path(&local_repo, &commit, &file_path)?;
            assert!(deleted_file_node.is_none());
            */

            let new_commits = repositories::commits::list_all(&local_repo)?;
            assert_eq!(new_commits.len(), prev_commits.len() + 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_file_after_upload() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let branch_name = "main";

            // Find a file (not a directory) to delete
            // Files in train directory: dog_1.jpg, dog_2.jpg, dog_3.jpg, dog_4.jpg, cat_1.jpg, cat_2.jpg, cat_3.jpg
            let file_to_delete = "train/dog_1.jpg";

            // Verify the file exists before deletion
            let file_path = local_repo.path.join(file_to_delete);
            assert!(file_path.exists(), "File should exist before deletion");

            // Delete the file
            let delete_commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Delete existing file from training data".to_string(),
            };

            let delete_response = api::client::file::delete_file(
                &remote_repo,
                branch_name,
                &file_to_delete,
                Some(delete_commit_body),
            )
            .await?;

            assert_eq!(delete_response.status.status_message, "resource_deleted");
            assert!(delete_response
                .commit
                .message
                .contains("Delete existing file from training data"));

            // Pull the deletion
            repositories::pull(&local_repo).await?;

            // Verify the file is deleted
            assert!(!file_path.exists(), "File should be deleted after deletion");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_mv_file() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let branch_name = "main";

            // File to move
            let source_path = "train/dog_1.jpg";
            let new_path = "renamed/images/dog_moved.jpg";

            // Verify the file exists before moving
            let file_path = local_repo.path.join(source_path);
            assert!(file_path.exists(), "Source file should exist before move");

            // Move the file
            let mv_commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Move file to new location".to_string(),
            };

            let mv_response = api::client::file::mv_file(
                &remote_repo,
                branch_name,
                source_path,
                new_path,
                Some(mv_commit_body),
            )
            .await?;

            assert_eq!(mv_response.status.status_message, "resource_updated");
            assert!(mv_response.commit.message.contains("Move file to new location"));

            // Pull the changes
            repositories::pull(&local_repo).await?;

            // Verify the file is at the new location
            let new_file_path = local_repo.path.join(new_path);
            assert!(
                new_file_path.exists(),
                "File should exist at new location after move"
            );

            // Verify the file is no longer at the original location
            assert!(
                !file_path.exists(),
                "File should not exist at original location after move"
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_file_with_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let base_dir = "annotations";
            let data_set = "train";
            let file_name = "file.txt";
            let workspace_id = "test_workspace_id";

            let file_path = PathBuf::from(base_dir)
                .join(data_set)
                .join(file_name)
                .to_string_lossy()
                .into_owned();

            let directory_name = PathBuf::from(base_dir)
                .join(data_set)
                .to_string_lossy()
                .into_owned();

            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            let full_path = local_repo.path.join(&file_path);
            util::fs::file_create(&full_path)?;
            util::fs::write(&full_path, b"test content")?;

            let _result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace.id,
                directory_name,
                &full_path,
            )
            .await;

            let bytes = api::client::file::get_file(&remote_repo, workspace_id, file_path).await;

            assert!(bytes.is_ok());
            assert!(!bytes.as_ref().unwrap().is_empty());
            assert_eq!(bytes.unwrap(), Bytes::from_static(b"test content"));

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    #[cfg(feature = "ffmpeg")]
    async fn test_upload_video_and_get_thumbnail() -> Result<(), OxenError> {
        test::run_empty_configured_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = DEFAULT_BRANCH_NAME;
            let directory_name = "videos";
            let video_file = test::test_video_file_with_name("basketball.mp4");

            // Verify the test video file exists
            assert!(
                video_file.exists(),
                "Test video file should exist at {video_file:?}"
            );

            let commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Upload test video".to_string(),
            };

            // Upload the video file
            let response = api::client::file::put_file(
                &remote_repo,
                branch_name,
                directory_name,
                &video_file,
                Some("basketball.mp4"),
                Some(commit_body),
            )
            .await?;

            assert_eq!(response.status.status_message, "resource_created");

            // Download the thumbnail with default settings
            let thumbnail_path = format!("{directory_name}/basketball.mp4");
            let thumbnail_bytes = api::client::file::get_file_thumbnail(
                &remote_repo,
                branch_name,
                thumbnail_path.as_str(),
                None,
                None,
                None,
            )
            .await?;

            // Verify thumbnail is not empty
            assert!(!thumbnail_bytes.is_empty(), "Thumbnail should not be empty");

            // Verify it's a JPEG (JPEG files start with FF D8 FF)
            assert!(
                thumbnail_bytes.len() >= 3,
                "Thumbnail should be at least 3 bytes"
            );
            assert_eq!(
                thumbnail_bytes[0], 0xFF,
                "Thumbnail should start with JPEG magic bytes"
            );
            assert_eq!(
                thumbnail_bytes[1], 0xD8,
                "Thumbnail should start with JPEG magic bytes"
            );
            assert_eq!(
                thumbnail_bytes[2], 0xFF,
                "Thumbnail should start with JPEG magic bytes"
            );

            // Test with custom dimensions
            let thumbnail_bytes_custom = api::client::file::get_file_thumbnail(
                &remote_repo,
                branch_name,
                thumbnail_path.as_str(),
                Some(640),
                Some(480),
                Some(0.5),
            )
            .await?;

            assert!(
                !thumbnail_bytes_custom.is_empty(),
                "Custom thumbnail should not be empty"
            );
            assert_eq!(
                thumbnail_bytes_custom[0], 0xFF,
                "Custom thumbnail should be a JPEG"
            );
            assert_eq!(
                thumbnail_bytes_custom[1], 0xD8,
                "Custom thumbnail should be a JPEG"
            );

            Ok(remote_repo)
        })
        .await
    }
}
