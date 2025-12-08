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
    let file_path = file_path.as_ref().to_str().unwrap();
    let uri = format!("/file/{branch}/{file_path}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    // Build query string if parameters are provided
    let url_with_params =
        if thumbnail.is_some() || width.is_some() || height.is_some() || timestamp.is_some() {
            let mut query_parts = Vec::new();
            if let Some(thumb) = thumbnail {
                query_parts.push(format!("thumbnail={thumb}"));
            }
            if let Some(w) = width {
                query_parts.push(format!("width={w}"));
            }
            if let Some(h) = height {
                query_parts.push(format!("height={h}"));
            }
            if let Some(ts) = timestamp {
                query_parts.push(format!("timestamp={ts}"));
            }
            format!("{}?{}", url, query_parts.join("&"))
        } else {
            url.to_string()
        };

    let req = client.get(&url_with_params);

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

/// Upload a ZIP file that gets extracted into the workspace directory
pub async fn upload_zip(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    directory: impl AsRef<str>,
    zip_path: impl AsRef<Path>,
    name: impl AsRef<str>,
    email: impl AsRef<str>,
    commit_message: Option<impl AsRef<str>>,
) -> Result<crate::model::Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let directory = directory.as_ref();
    let zip_path = zip_path.as_ref();
    let name = name.as_ref();
    let email = email.as_ref();

    // Read the ZIP file
    let zip_data = std::fs::read(zip_path)?;
    let file_name = zip_path
        .file_name()
        .ok_or_else(|| OxenError::basic_str("Invalid ZIP file path"))?
        .to_string_lossy();

    // Create the URL for workspace ZIP upload endpoint
    let uri = format!("/file/upload_zip/{branch_name}/{directory}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    // Create multipart form
    let file_part = reqwest::multipart::Part::bytes(zip_data).file_name(file_name.to_string());
    let mut form = reqwest::multipart::Form::new()
        .part("file", file_part)
        .text("name", name.to_string())
        .text("email", email.to_string())
        .text("resource_path", directory.to_string());

    if let Some(msg) = commit_message {
        form = form.text("commit_message", msg.as_ref().to_string());
    }

    // Send the request
    let client = client::new_for_url(&url)?;
    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;

    // Parse the response
    let response: crate::view::CommitResponse = serde_json::from_str(&body)
        .map_err(|e| OxenError::basic_str(format!("Failed to parse response: {e}")))?;

    Ok(response.commit)
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

    use std::io::Write;
    #[tokio::test]
    async fn test_upload_zip_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "upload-zip-test";
            api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            // Create a test ZIP file
            let temp_dir = tempfile::tempdir()?;
            let zip_path = temp_dir.path().join("test.zip");
            let zip_file = std::fs::File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(&zip_file);

            let options: zip::write::FileOptions<()> = zip::write::FileOptions::default();
            zip.start_file("image1.png", options).unwrap();
            zip.write_all(b"fake png data 1")?;
            zip.start_file("image2.png", options).unwrap();
            zip.write_all(b"fake png data 2")?;
            zip.finish().unwrap();
            drop(zip_file);

            // Upload the ZIP
            let result = api::client::file::upload_zip(
                &remote_repo,
                branch_name,
                "images",
                &zip_path,
                "Test User",
                "test@oxen.ai",
                Some("Upload test ZIP"),
            )
            .await;

            assert!(result.is_ok());
            let commit = result.unwrap();
            assert!(commit.message.contains("Upload test ZIP"));

            let bytes =
                api::client::file::get_file(&remote_repo, branch_name, "images/image1.png").await;

            assert!(bytes.is_ok());
            assert!(!bytes.as_ref().unwrap().is_empty());
            assert_eq!(
                bytes.as_ref().unwrap(),
                &Bytes::from_static(b"fake png data 1")
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
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
