use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;

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

#[cfg(test)]
mod tests {
    use actix_web::web::Bytes;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;

    use crate::{api, test};

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
}
