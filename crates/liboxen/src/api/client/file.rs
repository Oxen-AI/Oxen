use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::model::commit::NewCommitBody;
use crate::storage::BoxedByteStream;
use crate::view::CommitResponse;

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
    put_multipart_file(
        remote_repo,
        format!("/file/{branch}/{directory}"),
        "files[]",
        file_path,
        file_name,
        commit_body,
    )
    .await
}

pub async fn put_file_to_path(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    file_path_on_repo: impl AsRef<str>,
    file_path: impl AsRef<Path>,
    file_name: Option<impl AsRef<str>>,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let branch = branch.as_ref();
    let file_path_on_repo = file_path_on_repo.as_ref();
    put_multipart_file(
        remote_repo,
        format!("/file/{branch}/{file_path_on_repo}"),
        "file",
        file_path,
        file_name,
        commit_body,
    )
    .await
}

async fn put_multipart_file(
    remote_repo: &RemoteRepository,
    uri: String,
    field_name: &'static str,
    file_path: impl AsRef<Path>,
    file_name: Option<impl AsRef<str>>,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let file_path = file_path.as_ref();
    log::debug!("put_multipart_file {uri:?}, file_path {file_path:?}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;

    let file_part = make_file_part(file_path, file_name).await?;
    let form = apply_commit_body(Form::new().part(field_name, file_part), commit_body);
    let res = client.put(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    Ok(serde_json::from_str(&body)?)
}

async fn make_file_part(
    file_path: &Path,
    file_name: Option<impl AsRef<str>>,
) -> Result<Part, OxenError> {
    let file_part = Part::file(file_path).await?;
    Ok(match file_name {
        Some(file_name) => file_part.file_name(file_name.as_ref().to_string()),
        None => file_part,
    })
}

fn apply_commit_body(mut form: Form, commit_body: Option<NewCommitBody>) -> Form {
    if let Some(body) = commit_body {
        form = form.text("name", body.author);
        form = form.text("email", body.email);
        form = form.text("message", body.message);
    }
    form
}

/// Optional query parameters for [`get_file`]: thumbnail generation and image resizing.
#[derive(Debug, Clone, Copy, Default)]
pub struct GetFileOpts {
    pub thumbnail: bool,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub timestamp: Option<f64>,
}

/// Streams a file's raw bytes from the remote at `revision`, without buffering the whole file in
/// memory. `revision` may be a branch name or a commit id. `opts` carry optional thumbnail/resize
/// query parameters. Callers that need the whole file in memory must drain the stream into a
/// buffer at the call site.
pub async fn get_file(
    remote_repo: &RemoteRepository,
    revision: &str,
    file_path: &Path,
    opts: GetFileOpts,
) -> Result<BoxedByteStream, OxenError> {
    let file_path = file_path.to_str().ok_or_else(|| {
        OxenError::basic_str(format!("Invalid UTF-8 in file path: {file_path:?}"))
    })?;
    let uri = format!("/file/{revision}/{file_path}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;

    let mut query_params: Vec<(&str, String)> = Vec::new();
    if opts.thumbnail {
        query_params.push(("thumbnail", "true".to_string()));
    }
    if let Some(width) = opts.width {
        query_params.push(("width", width.to_string()));
    }
    if let Some(height) = opts.height {
        query_params.push(("height", height.to_string()));
    }
    if let Some(timestamp) = opts.timestamp {
        query_params.push(("timestamp", timestamp.to_string()));
    }

    let res = client
        .get(&url)
        .query(&query_params)
        .send()
        .await?
        .error_for_status()?;

    let stream = res
        .bytes_stream()
        .map(|chunk| chunk.map_err(std::io::Error::other));
    // Pin on the heap so the boxed reqwest stream satisfies the `Unpin` bound.
    Ok(Box::new(Box::pin(stream)))
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
    use crate::api::client::workspaces;
    use crate::api::client::workspaces::files::upload_single_file;
    use crate::repositories::commits::list_all;
    use crate::util::fs;

    use bytes::Bytes;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::{api, repositories, test};
    use std::path::Path;

    use super::{GetFileOpts, delete_file, get_file, mv_file, put_file, put_file_to_path};

    fn commit_body(message: &str) -> NewCommitBody {
        NewCommitBody {
            author: "Test Author".to_string(),
            email: "test@example.com".to_string(),
            message: message.to_string(),
        }
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_put_get_move_and_delete_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let bbox_path = test::test_bounding_box_csv();
            assert!(
                !test::file_bytes(&remote_repo, DEFAULT_BRANCH_NAME, &bbox_path)
                    .await?
                    .is_empty()
            );

            // A file staged in a workspace is readable through the workspace id
            let workspace_id = "test_workspace_id";
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id).await?;
            let staged = Path::new("annotations").join("train").join("file.txt");
            let full_path = local_repo.path.join(&staged);
            fs::write(&full_path, b"test content")?;
            upload_single_file(
                &remote_repo,
                workspace_id,
                staged.parent().expect("a parent"),
                &full_path,
            )
            .await?;
            let bytes = test::file_bytes(&remote_repo, workspace_id, &staged).await?;
            assert_eq!(bytes, Bytes::from_static(b"test content"));
            fs::remove_file(&full_path)?;

            // Put a file into a directory, and at a full path
            let image = test::test_img_file();
            let response = put_file(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "test_data",
                &image,
                Some("test.jpeg"),
                Some(commit_body("put")),
            )
            .await?;
            assert_eq!(response.status.status_message, "resource_created");
            let full_path_on_repo = "test_data/test_full_path.jpeg";
            let response = put_file_to_path(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                full_path_on_repo,
                &image,
                Some("ignored-name.jpeg"),
                Some(commit_body("put to path")),
            )
            .await?;
            assert_eq!(response.status.status_message, "resource_created");
            repositories::pull(&local_repo).await?;
            let put_path = local_repo.path.join("test_data").join("test.jpeg");
            assert!(put_path.exists());
            assert!(local_repo.path.join(full_path_on_repo).exists());

            // Move a file, then delete it at its new path
            let moved = "renamed/images/test_moved.jpeg";
            let response = mv_file(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "test_data/test.jpeg",
                moved,
                Some(commit_body("Move file")),
            )
            .await?;
            assert_eq!(response.status.status_message, "resource_updated");
            assert!(response.commit.message.contains("Move file"));
            repositories::pull(&local_repo).await?;
            assert!(
                local_repo.path.join(moved).exists(),
                "moved to its new path"
            );
            assert!(!put_path.exists(), "gone from its old path");

            let response = delete_file(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &moved,
                Some(commit_body("Delete moved file")),
            )
            .await?;
            assert_eq!(response.status.status_message, "resource_deleted");
            assert!(response.commit.message.contains("Delete moved file"));
            repositories::pull(&local_repo).await?;
            assert!(!local_repo.path.join(moved).exists());

            // Deleting a file makes one commit
            let prev_commits = list_all(&local_repo)?;
            delete_file(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &bbox_path,
                Some(commit_body("remove file")),
            )
            .await?;
            repositories::pull(&local_repo).await?;
            assert!(!local_repo.path.join(&bbox_path).exists());
            assert_eq!(list_all(&local_repo)?.len(), prev_commits.len() + 1);

            // A file in a deleted directory can no longer be fetched
            delete_file(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "test_data",
                Some(commit_body("delete test_data dir")),
            )
            .await?;
            let result = get_file(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                Path::new(full_path_on_repo),
                GetFileOpts::default(),
            )
            .await;
            assert!(
                result.is_err(),
                "Fetching a file from a deleted directory should be an error"
            );

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_update_file_to_full_path_on_empty_repo() -> Result<(), OxenError> {
        test::run_empty_configured_remote_repo_test(|local_repo, remote_repo| async move {
            let branch_name = "main";
            let file_path_on_repo = "test_data/test_full_path.jpeg";
            let file_path = test::test_img_file();
            let commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Update file test full path".to_string(),
            };

            let response = api::client::file::put_file_to_path(
                &remote_repo,
                branch_name,
                file_path_on_repo,
                &file_path,
                Some("ignored-name.jpeg"),
                Some(commit_body),
            )
            .await?;
            assert_eq!(response.status.status_message, "resource_created");

            repositories::pull(&local_repo).await?;
            repositories::checkout(&local_repo, branch_name).await?;
            let file_path_in_repo = local_repo.path.join(file_path_on_repo);
            assert!(file_path_in_repo.exists());

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
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
            let mut thumbnail_stream = api::client::file::get_file(
                &remote_repo,
                branch_name,
                Path::new(&thumbnail_path),
                api::client::file::GetFileOpts {
                    thumbnail: true,
                    ..Default::default()
                },
            )
            .await?;
            let mut thumbnail_bytes = Vec::new();
            while let Some(chunk) = thumbnail_stream.next().await {
                thumbnail_bytes.extend_from_slice(&chunk?);
            }

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
            let mut custom_stream = api::client::file::get_file(
                &remote_repo,
                branch_name,
                Path::new(&thumbnail_path),
                api::client::file::GetFileOpts {
                    thumbnail: true,
                    width: Some(640),
                    height: Some(480),
                    timestamp: Some(0.5),
                },
            )
            .await?;
            let mut thumbnail_bytes_custom = Vec::new();
            while let Some(chunk) = custom_stream.next().await {
                thumbnail_bytes_custom.extend_from_slice(&chunk?);
            }

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
