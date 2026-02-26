use std::path::Path;
use tokio::fs::File;
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;

pub async fn download_dir_as_zip(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    directory: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
) -> Result<u64, OxenError> {
    let revision = revision.as_ref().to_string();
    let directory = directory.as_ref().to_string_lossy().to_string();
    let local_path = local_path.as_ref();
    let uri = format!("/export/download/{revision}/{directory}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    if let Ok(res) = client.get(&url).send().await {
        let status = res.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::UNAUTHORIZED {
                let e = "Err: unauthorized request to download data".to_string();
                log::error!("{e}");
                return Err(OxenError::authentication(e));
            }

            return Err(OxenError::basic_str(format!(
                "download_dir_as_zip failed with status {status} for {url}"
            )));
        }

        let stream = res.bytes_stream();

        let mut reader =
            StreamReader::new(stream.map(|result| result.map_err(std::io::Error::other)));

        let mut file = File::create(local_path).await?;
        let size = match tokio::io::copy(&mut reader, &mut file).await {
            Ok(s) => s,
            Err(e) => {
                let _ = tokio::fs::remove_file(local_path).await;
                return Err(OxenError::basic_str(format!(
                    "Failed to download ZIP to {local_path:?}: {e}"
                )));
            }
        };

        log::debug!("Successfully downloaded {size} bytes to: {local_path:?}");

        Ok(size)
    } else {
        let err = format!("try_download_dir_as_zip failed to send request {url}");
        Err(OxenError::basic_str(err))
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::test;

    use crate::util;

    use std::io::Read;

    use std::path::PathBuf;
    use zip::ZipArchive;

    #[tokio::test]
    async fn test_download_dir_as_zip() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let remote_dir = "annotations";
            let output_dir = local_repo.path.clone().join("zip_download");

            util::fs::create_dir_all(&output_dir)?;
            let zip_local_path = output_dir.join("annotations.zip");

            // Download the zip file
            let _size = api::client::export::download_dir_as_zip(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                remote_dir,
                &zip_local_path,
            )
            .await?;

            assert!(
                zip_local_path.exists(),
                "Downloaded zip file does not exist."
            );

            let readme_path = PathBuf::from("README.md");
            let bounding_box_path = PathBuf::from("train").join("bounding_box.csv");
            let one_shot_path = PathBuf::from("train").join("one_shot.csv");
            let two_shot_path = PathBuf::from("train").join("two_shot.csv");
            let annotations_txt = PathBuf::from("train").join("annotations.txt");
            let annotations_csv = PathBuf::from("test").join("annotations.csv");

            let file_paths = vec![
                readme_path,
                bounding_box_path,
                one_shot_path,
                two_shot_path,
                annotations_txt,
                annotations_csv,
            ];

            let file = std::fs::File::open(&zip_local_path)?;
            let mut archive = ZipArchive::new(file).expect("Zip archive failed to open");

            assert_eq!(
                archive.len(),
                file_paths.len(),
                "Zip archive has incorrect number of files"
            );

            // Compare the files in the archive to the ones in the directory
            for path in file_paths {
                let local_content =
                    std::fs::read(local_repo.path.clone().join("annotations").join(&path))?;

                let mut zip_file = archive
                    .by_name(path.to_str().unwrap())
                    .unwrap_or_else(|_| panic!("File {path:?} not found in zip archive"));
                let mut zip_content: Vec<u8> = vec![];
                zip_file.read_to_end(&mut zip_content)?;

                assert_eq!(
                    zip_content, local_content,
                    "Content mismatch for file: {path:?}"
                );
            }

            Ok(remote_repo)
        })
        .await
    }
}
