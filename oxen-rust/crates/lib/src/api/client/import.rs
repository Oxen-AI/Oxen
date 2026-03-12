use crate::api;
use crate::api::client;
use crate::api::client::retry;
use crate::error::OxenError;
use crate::model::RemoteRepository;

use std::path::Path;

/// Upload a ZIP file that gets extracted into the workspace directory
#[tracing::instrument(skip(
    remote_repo,
    branch_name,
    directory,
    zip_path,
    name,
    email,
    commit_message
))]
pub async fn upload_zip(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    directory: impl AsRef<str>,
    zip_path: impl AsRef<Path>,
    name: impl AsRef<str>,
    email: impl AsRef<str>,
    commit_message: Option<impl AsRef<str>>,
) -> Result<crate::model::Commit, OxenError> {
    metrics::counter!("oxen_client_import_upload_zip_total").increment(1);
    let timer = std::time::Instant::now();
    let branch_name = branch_name.as_ref();
    let directory = directory.as_ref();
    let zip_path = zip_path.as_ref();

    let name = name.as_ref();
    let email = email.as_ref();

    // Read the ZIP file into memory once for potential retries
    // NOTE: bytes::Bytes wraps the Vec<u8> into an Arc internally.
    //       We need to move into `reqwest.multipart::Part::bytes`, but with retries, we don't want
    //       to actually clone the data. This wrapper makes it cheap to clone as we're just
    //       incrementing the arc's internal reference count.
    let zip_data = bytes::Bytes::from(tokio::fs::read(zip_path).await?);
    let len_zip_data = zip_data.len() as u64;
    let file_name = zip_path
        .file_name()
        .ok_or_else(|| OxenError::basic_str("Invalid ZIP file path"))?
        .to_string_lossy()
        .to_string();

    let uri = format!("/import/upload/{branch_name}/{directory}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let commit_msg = commit_message.map(|m| m.as_ref().to_string());

    let config = retry::RetryConfig::default();
    retry::with_retry(&config, |_attempt| {
        let url = url.clone();
        let zip_data = zip_data.clone();
        let file_name = file_name.clone();
        let name = name.to_string();
        let email = email.to_string();
        let directory = directory.to_string();
        let commit_msg = commit_msg.clone();
        async move {
            let form = make_multipart_form(
                zip_data,
                len_zip_data,
                file_name,
                name,
                email,
                directory,
                commit_msg,
            );

            let client = client::new_for_url_transfer(&url)?;
            let response = client.post(&url).multipart(form).send().await?;
            let body = client::parse_json_body(&url, response).await?;
            let response: crate::view::CommitResponse = serde_json::from_str(&body)
                .map_err(|e| OxenError::basic_str(format!("Failed to parse response: {e}")))?;
            metrics::histogram!("oxen_client_import_upload_zip_duration_seconds")
                .record(timer.elapsed().as_secs_f64());
            Ok(response.commit)
        }
    })
    .await
}

fn make_multipart_form<T: Into<reqwest::Body>>(
    zip_data: T,
    len_zip_data: u64,
    file_name: String,
    name: String,
    email: String,
    directory: String,
    commit_msg: Option<String>,
) -> reqwest::multipart::Form {
    let file_part =
        reqwest::multipart::Part::stream_with_length(zip_data, len_zip_data).file_name(file_name);

    let mut form = reqwest::multipart::Form::new()
        .part("file", file_part)
        .text("name", name)
        .text("email", email)
        .text("resource_path", directory);

    if let Some(msg) = commit_msg {
        form = form.text("commit_message", msg);
    }
    form
}

#[cfg(test)]
mod tests {
    use crate::test;
    use bytes::Bytes;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;

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
}
