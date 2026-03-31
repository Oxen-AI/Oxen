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
    update_timestamp: bool,
) -> Result<crate::model::Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let directory = directory.as_ref();

    let uri = format!("/import/{branch_name}/{directory}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let mut body = serde_json::json!({
        "download_url": download_url.as_ref(),
        "name": commit.author,
        "email": commit.email,
        "message": commit.message,
    });
    if update_timestamp {
        body["update_timestamp"] = serde_json::Value::Bool(true);
    }

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

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;

    use crate::api;

    use std::io::Write;
    use std::path::Path;

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

            let bytes =
                api::client::file::get_file(&remote_repo, branch_name, "images/image1.png").await;

            assert!(bytes.is_ok());
            assert_eq!(
                bytes.as_ref().unwrap(),
                &Bytes::from_static(b"fake png data 1")
            );

            let bytes_2 =
                api::client::file::get_file(&remote_repo, branch_name, "images/image2.png").await;

            assert!(bytes_2.is_ok());
            assert_eq!(
                bytes_2.as_ref().unwrap(),
                &Bytes::from_static(b"fake png data 2")
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_import_url_with_update_timestamp() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = DEFAULT_BRANCH_NAME;
            let download_url =
                "https://hub.oxen.ai/api/repos/ox/Oxen-AI-Assets/file/main/images/bloxy_white_background.png";

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
                false,
            )
            .await?;
            assert!(!first_commit.id.is_empty());

            // Second import of same file WITHOUT update_timestamp should fail (no changes)
            let result = api::client::import::import_url(
                &remote_repo,
                branch_name,
                "imported",
                download_url,
                &NewCommitBody {
                    message: "Second import no force".to_string(),
                    ..commit_body.clone()
                },
                false,
            )
            .await;
            assert!(
                result.is_err(),
                "Expected import to fail with no changes: {result:?}"
            );

            // Third import of same file WITH update_timestamp should succeed
            let third_commit = api::client::import::import_url(
                &remote_repo,
                branch_name,
                "imported",
                download_url,
                &NewCommitBody {
                    message: "Force update import".to_string(),
                    ..commit_body.clone()
                },
                true,
            )
            .await?;
            assert_ne!(first_commit.id, third_commit.id);

            // Verify latest_commit on the entry matches the update_timestamp commit
            let entries = api::client::dir::list(
                &remote_repo,
                branch_name,
                Path::new("imported"),
                1,
                100,
            )
            .await?;
            let file_entry = entries
                .entries
                .iter()
                .find(|e| e.filename() == "bloxy_white_background.png")
                .expect("Should find the imported file");
            let latest_commit = file_entry
                .latest_commit()
                .expect("Entry should have latest_commit");
            assert_eq!(
                latest_commit.id, third_commit.id,
                "latest_commit should match the update_timestamp commit"
            );

            Ok(remote_repo)
        })
        .await
    }
}
