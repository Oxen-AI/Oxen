use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{NewCommitBody, RemoteRepository};
use crate::view::CommitResponse;

use std::path::Path;

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
    let uri = format!("/import/upload/{branch_name}/{directory}");
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

/// Import a file from a URL into a repository branch
pub async fn import_url(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    directory: impl AsRef<str>,
    download_url: impl AsRef<str>,
    commit: &NewCommitBody,
) -> Result<crate::model::Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let directory = directory.as_ref();

    let uri = format!("/import/{branch_name}/{directory}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let body = serde_json::json!({
        "download_url": download_url.as_ref(),
        "name": commit.author,
        "email": commit.email,
        "message": commit.message,
    });

    let client = client::new_for_url(&url)?;
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;
    let body = client::parse_json_body(&url, response).await?;
    let response: CommitResponse = serde_json::from_str(&body)
        .map_err(|e| OxenError::basic_str(format!("Failed to parse response: {e}\n\n{body}")))?;

    Ok(response.commit)
}

#[cfg(test)]
mod tests {
    use crate::test;
    use bytes::Bytes;
    use std::path::Path;
    use tokio_stream::StreamExt;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;

    use crate::api;

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
            let result = api::client::import::upload_zip(
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

            let mut stream = api::client::file::get_file(
                &remote_repo,
                branch_name,
                Path::new("images/image1.png"),
                api::client::file::GetFileOpts::default(),
            )
            .await?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend_from_slice(&chunk?);
            }

            assert!(!bytes.is_empty());
            assert_eq!(bytes, Bytes::from_static(b"fake png data 1"));

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_upload_zip_file_empty_repo() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "upload-zip-test";

            // Create a test ZIP file
            let temp_dir = tempfile::tempdir()?;
            let zip_path = temp_dir.path().join("test.zip");

            {
                let zip_file = std::fs::File::create(&zip_path)?;
                let mut zip = zip::ZipWriter::new(&zip_file);

                let options: zip::write::FileOptions<()> = zip::write::FileOptions::default();
                zip.start_file("image1.png", options).unwrap();
                zip.write_all(b"fake png data 1")?;
                zip.start_file("image2.png", options).unwrap();
                zip.write_all(b"fake png data 2")?;
                zip.finish().unwrap();
            }

            // Upload the ZIP
            let result = api::client::import::upload_zip(
                &remote_repo,
                branch_name,
                "images",
                &zip_path,
                "Test User",
                "test@oxen.ai",
                Some("Upload test ZIP in empty repo"),
            )
            .await;

            assert!(result.is_ok(), "{result:?}");
            let commit = result.unwrap();
            assert!(commit.message.contains("Upload test ZIP in empty repo"));

            let mut stream = api::client::file::get_file(
                &remote_repo,
                branch_name,
                Path::new("images/image1.png"),
                api::client::file::GetFileOpts::default(),
            )
            .await?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend_from_slice(&chunk?);
            }

            assert_eq!(bytes, Bytes::from_static(b"fake png data 1"));

            let mut stream_2 = api::client::file::get_file(
                &remote_repo,
                branch_name,
                Path::new("images/image2.png"),
                api::client::file::GetFileOpts::default(),
            )
            .await?;
            let mut bytes_2 = Vec::new();
            while let Some(chunk) = stream_2.next().await {
                bytes_2.extend_from_slice(&chunk?);
            }

            assert_eq!(bytes_2, Bytes::from_static(b"fake png data 2"));

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_import_url_reupload_unchanged_is_noop() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = DEFAULT_BRANCH_NAME;

            // Serve stable, identical bytes from a local mock server so both imports below hash to
            // the same version, exercising re-import dedup. The oxen-server process performs the
            // fetch, so it must run in test mode to permit the loopback target. The mock and server
            // stay in scope for the whole closure so every GET is served.
            let mut server = mockito::Server::new_async().await;
            let _mock = server
                .mock("GET", "/images/bloxy_white_background.png")
                .with_status(200)
                .with_header("content-type", "image/png")
                .with_body("fake png bytes for re-import dedup testing")
                .create_async()
                .await;
            let download_url = format!("{}/images/bloxy_white_background.png", server.url());
            let download_url = download_url.as_str();

            let commit_body = NewCommitBody {
                message: "First import".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };

            // First import
            let first_commit = api::client::import::import_url(
                &remote_repo,
                branch_name,
                "imported",
                download_url,
                &commit_body,
            )
            .await?;
            assert!(!first_commit.id.is_empty());

            // Second import of the same file should fail (no changes to commit)
            let result = api::client::import::import_url(
                &remote_repo,
                branch_name,
                "imported",
                download_url,
                &NewCommitBody {
                    message: "Second import".to_string(),
                    ..commit_body.clone()
                },
            )
            .await;
            assert!(
                result.is_err(),
                "Expected import to fail with no changes: {result:?}"
            );

            Ok(remote_repo)
        })
        .await
    }
}
