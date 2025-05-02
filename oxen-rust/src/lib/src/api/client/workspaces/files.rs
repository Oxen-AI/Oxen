use crate::api;
use crate::api::client;
use crate::constants::AVG_CHUNK_SIZE;
use crate::error::OxenError;
use crate::model::RemoteRepository;

use crate::view::FilePathsResponse;

use bytesize::ByteSize;
use pluralizer::pluralize;
use std::path::{Path, PathBuf};

use crate::core::oxenignore;
use crate::model::LocalRepository;
use crate::util;

pub async fn add_from_local_repo(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<str>,
    paths: Vec<PathBuf>,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();

    // If no paths provided, return early
    if paths.is_empty() {
        return Ok(());
    }

    let ignore = oxenignore::create(local_repo);

    for path in &paths {
        let path = path.as_path();

        // * make sure file is not in .oxenignore
        if let Some(ignore) = &ignore {
            if ignore.matched(path, path.is_dir()).is_ignore() {
                continue;
            }
        }

        let (remote_directory, resolved_path) =
            resolve_remote_add_file_path(local_repo, path, directory)?;
        let directory_name = remote_directory.to_string_lossy().to_string();

        log::debug!(
            "repositories::workspaces::add Resolved path: {:?}",
            resolved_path
        );
        log::debug!(
            "repositories::workspaces::add Remote directory: {:?}",
            remote_directory
        );
        log::debug!(
            "repositories::workspaces::add Directory name: {:?}",
            directory_name
        );

        let result = upload_single_file(remote_repo, workspace_id, directory, path).await?;

        println!("{}", result.to_string_lossy());
    }

    Ok(())
}

pub async fn add(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<str>,
    paths: Vec<PathBuf>,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();

    // If no paths provided, return early
    if paths.is_empty() {
        return Ok(());
    }

    // TODO: add a progress bar
    upload_multiple_files(remote_repo, workspace_id, directory, paths).await?;

    Ok(())
}

/// Returns (remote_directory, resolved_path)
fn resolve_remote_add_file_path(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    directory: impl AsRef<str>,
) -> Result<(PathBuf, PathBuf), OxenError> {
    let path = path.as_ref();
    match util::fs::canonicalize(path) {
        Ok(path) => {
            if util::fs::file_exists_in_directory(&repo.path, &path) {
                // Path is in the repo, so we get the remote directory from the repo path
                let relative_to_repo = util::fs::path_relative_to_dir(&path, &repo.path)?;
                let remote_directory = relative_to_repo
                    .parent()
                    .ok_or_else(|| OxenError::file_has_no_parent(&path))?;
                Ok((remote_directory.to_path_buf(), path))
            } else {
                // Use the provided directory
                let dir_path = PathBuf::from(directory.as_ref());
                Ok((dir_path, path))
            }
        }
        Err(err) => {
            log::error!("Err: {err:?}");
            Err(OxenError::entry_does_not_exist(path))
        }
    }
}

pub async fn upload_single_file(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    let Ok(metadata) = path.metadata() else {
        return Err(OxenError::path_does_not_exist(path));
    };

    log::debug!("Uploading file with size: {}", metadata.len());
    // If the file is larger than AVG_CHUNK_SIZE, use the parallel upload strategy
    if metadata.len() > AVG_CHUNK_SIZE {
        let directory = directory.as_ref();
        match api::client::versions::parallel_large_file_upload(
            remote_repo,
            path,
            Some(directory),
            Some(workspace_id.as_ref().to_string()),
        )
        .await
        {
            Ok(upload) => Ok(upload.local_path),
            Err(err) => Err(err),
        }
    } else {
        // Single multipart request
        p_upload_single_file(remote_repo, workspace_id, directory, path).await
    }
}

async fn upload_multiple_files(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    paths: Vec<PathBuf>,
) -> Result<(), OxenError> {
    if paths.is_empty() {
        return Ok(());
    }

    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();

    // Separate files by size, storing the file size with each path
    let mut large_files = Vec::new();
    let mut small_files = Vec::new();
    let mut small_files_size = 0;

    // Group files by size
    for path in paths {
        if !path.exists() {
            log::warn!("File does not exist: {:?}", path);
            continue;
        }

        match path.metadata() {
            Ok(metadata) => {
                let file_size = metadata.len();
                if file_size > AVG_CHUNK_SIZE {
                    // Large file goes directly to parallel upload
                    large_files.push((path, file_size));
                } else {
                    // Small file goes to batch
                    small_files.push((path, file_size));
                    small_files_size += file_size;
                }
            }
            Err(err) => {
                log::warn!("Failed to get metadata for file {:?}: {}", path, err);
                continue;
            }
        }
    }

    // Process large files individually with parallel upload
    for (path, size) in large_files {
        log::info!("Uploading large file: {:?} ({} bytes)", path, size);
        match api::client::versions::parallel_large_file_upload(
            remote_repo,
            &path,
            Some(directory),
            Some(workspace_id.to_string()),
        )
        .await
        {
            Ok(_) => log::debug!("Successfully uploaded large file: {:?}", path),
            Err(err) => log::error!("Failed to upload large file {:?}: {}", path, err),
        }
    }

    // Upload small files in batches
    sequential_batched_small_file_upload(
        remote_repo,
        workspace_id,
        directory,
        small_files,
        small_files_size,
    )
    .await?;

    Ok(())
}

async fn sequential_batched_small_file_upload(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    small_files: Vec<(PathBuf, u64)>,
    small_files_size: u64,
) -> Result<(), OxenError> {
    // Batch small files in chunks of ~AVG_CHUNK_SIZE
    log::info!(
        "Uploading {} small files (total {} bytes)",
        small_files.len(),
        small_files_size
    );

    let workspace_id = workspace_id.as_ref();
    let directory_str = directory.as_ref().to_string_lossy();

    let mut current_batch = Vec::new();
    let mut current_batch_size = 0;

    for (idx, (path, file_size)) in small_files.iter().enumerate() {
        // Add the file to the current batch
        current_batch.push(path.clone());
        current_batch_size += file_size;

        // If the current batch is larger than our target size or we're at the end of the list, upload it
        if current_batch_size > AVG_CHUNK_SIZE || idx == small_files.len() - 1 {
            log::debug!(
                "Uploading batch of {} files ({} bytes)",
                current_batch.len(),
                current_batch_size
            );
            match multipart_upload(remote_repo, workspace_id, &directory_str, current_batch).await {
                Ok(_) => log::debug!("Successfully uploaded batch of files"),
                Err(err) => log::error!("Failed to upload batch of files: {}", err),
            }
            current_batch = Vec::new();
            current_batch_size = 0;
        }
    }

    Ok(())
}

async fn p_upload_single_file(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();
    let directory_name = directory.to_string_lossy();
    let path = path.as_ref();
    log::debug!("multipart_file_upload path: {:?}", path);
    let Ok(file) = std::fs::read(path) else {
        let err = format!("Error reading file at path: {path:?}");
        return Err(OxenError::basic_str(err));
    };

    let uri = format!("/workspaces/{workspace_id}/files/{directory_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let file_name: String = path.file_name().unwrap().to_string_lossy().into();
    log::info!(
        "api::client::workspaces::files::add sending file_name: {:?}",
        file_name
    );

    let file_part = reqwest::multipart::Part::bytes(file).file_name(file_name);
    let form = reqwest::multipart::Form::new().part("file", file_part);
    let client = client::new_for_url(&url)?;
    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let response: Result<FilePathsResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => {
            log::debug!("File path response: {:?}", val);
            if let Some(path) = val.paths.first() {
                Ok(path.clone())
            } else {
                Err(OxenError::basic_str("No file path returned from server"))
            }
        }
        Err(err) => {
            let err = format!("api::staging::add_file error parsing response from {url}\n\nErr {err:?} \n\n{body}");
            Err(OxenError::basic_str(err))
        }
    }
}

async fn multipart_upload(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory_name: impl AsRef<str>,
    paths: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory_name = directory_name.as_ref();

    log::debug!("Uploading {} files to {}", paths.len(), directory_name);

    let uri = format!("/workspaces/{workspace_id}/files/{directory_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let mut form = reqwest::multipart::Form::new();
    for path in paths {
        let file_name = path
            .file_name()
            .unwrap()
            .to_os_string()
            .into_string()
            .ok()
            .unwrap();
        let file = std::fs::read(&path).unwrap();
        let file_part = reqwest::multipart::Part::bytes(file).file_name(file_name);
        form = form.part("file[]", file_part);
    }

    let client = client::new_for_url(&url)?;
    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let response: FilePathsResponse = serde_json::from_str(&body)?;
    Ok(response.paths)
}

pub async fn add_many(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory_name: impl AsRef<str>,
    paths: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory_name = directory_name.as_ref();
    // Check if the total size of the files is too large (over 100mb for now)
    let limit = 100_000_000;
    let total_size: u64 = paths.iter().map(|p| p.metadata().unwrap().len()).sum();
    if total_size > limit {
        let error_msg = format!("Total size of files to upload is too large. {} > {} Consider using `oxen push` instead for now until upload supports bulk push.", ByteSize::b(total_size), ByteSize::b(limit));
        return Err(OxenError::basic_str(error_msg));
    }

    println!(
        "Uploading {} from {} {}",
        ByteSize(total_size),
        paths.len(),
        pluralize("file", paths.len() as isize, true)
    );

    let uri = format!("/workspaces/{workspace_id}/files/{directory_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let mut form = reqwest::multipart::Form::new();
    for path in paths {
        let file_name = path
            .file_name()
            .unwrap()
            .to_os_string()
            .into_string()
            .ok()
            .unwrap();
        let file = std::fs::read(&path).unwrap();
        let file_part = reqwest::multipart::Part::bytes(file).file_name(file_name);
        form = form.part("file[]", file_part);
    }

    let client = client::new_for_url(&url)?;
    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let response: Result<FilePathsResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.paths),
        Err(err) => {
            let err = format!("api::staging::add_files error parsing response from {url}\n\nErr {err:?} \n\n{body}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn rm(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/workspaces/{workspace_id}/files/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_file {}", url);
    let client = client::new_for_url(&url)?;
    let response = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    log::debug!("rm_file got body: {}", body);
    Ok(())
}

pub async fn download(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &str,
    output_path: Option<&Path>,
) -> Result<(), OxenError> {
    log::debug!("Downloading file from {workspace_id}/{path} to {output_path:?}");
    let uri = format!("/workspaces/{workspace_id}/files/{path}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("Downloading file from {url}");
    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;
    // Save the raw file contents from the response
    let file_contents = response.bytes().await?;
    let output_path = output_path.unwrap_or_else(|| Path::new(path));
    util::fs::write(output_path, file_contents)?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{EntryDataType, NewCommitBody};
    use crate::opts::fetch_opts::FetchOpts;
    use crate::opts::CloneOpts;
    use crate::{api, constants};
    use crate::{repositories, test};

    use std::path::Path;
    use uuid;

    #[tokio::test]
    async fn test_stage_single_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let directory_name = "images";
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = test::test_img_file();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 1);
            assert_eq!(entries.added_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_stage_large_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-large-file";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let directory_name = "my_large_file";
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = test::test_30k_parquet();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 1);
            assert_eq!(entries.added_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_stage_multiple_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-data";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let directory_name = "data";
            let paths = vec![
                test::test_img_file(),
                test::test_img_file_with_name("cole_anthony.jpeg"),
            ];
            let result = api::client::workspaces::files::add_many(
                &remote_repo,
                &workspace_id,
                directory_name,
                paths,
            )
            .await;
            assert!(result.is_ok());

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 2);
            assert_eq!(entries.added_files.total_entries, 2);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_create_remote_readme_repo_and_commit_multiple_data_frames(
    ) -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            let file_to_post = test::test_1k_parquet();
            let directory_name = "";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            println!("result: {:?}", result);
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add another data frame".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace_id,
                &body,
            )
            .await?;

            // List the entries
            let entries = api::client::entries::list_entries_with_type(
                &remote_repo,
                "",
                DEFAULT_BRANCH_NAME,
                &EntryDataType::Tabular,
            )
            .await?;
            assert_eq!(entries.len(), 1);

            // Upload a new data frame
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);
            let file_to_post = test::test_csv_file_with_name("emojis.csv");
            let directory_name = "moare_data";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            println!("result: {:?}", result);
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add emojis data frame".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace_id,
                &body,
            )
            .await?;

            // List the entries
            let entries = api::client::entries::list_entries_with_type(
                &remote_repo,
                "",
                DEFAULT_BRANCH_NAME,
                &EntryDataType::Tabular,
            )
            .await?;
            assert_eq!(entries.len(), 2);
            println!("entries: {:?}", entries);

            // Upload a new broken data frame
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);
            let file_to_post = test::test_invalid_parquet_file();
            let directory_name = "broken_data";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            println!("result: {:?}", result);
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add broken data frame".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace_id,
                &body,
            )
            .await?;

            // List the entries
            let entries = api::client::entries::list_entries_with_type(
                &remote_repo,
                "",
                DEFAULT_BRANCH_NAME,
                &EntryDataType::Tabular,
            )
            .await?;
            assert_eq!(entries.len(), 2);
            println!("entries: {:?}", entries);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_multiple_data_frames() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            let file_to_post = test::test_1k_parquet();
            let directory_name = "";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            println!("result: {:?}", result);
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add another data frame".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace_id,
                &body,
            )
            .await?;

            // List the entries
            let entries = api::client::entries::list_entries_with_type(
                &remote_repo,
                "",
                DEFAULT_BRANCH_NAME,
                &EntryDataType::Tabular,
            )
            .await?;
            assert_eq!(entries.len(), 1);

            // Upload a new data frame
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);
            let file_to_post = test::test_csv_file_with_name("emojis.csv");
            let directory_name = "moare_data";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            println!("result: {:?}", result);
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add emojis data frame".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace_id,
                &body,
            )
            .await?;

            // List the entries
            let entries = api::client::entries::list_entries_with_type(
                &remote_repo,
                "",
                DEFAULT_BRANCH_NAME,
                &EntryDataType::Tabular,
            )
            .await?;
            assert_eq!(entries.len(), 2);
            println!("entries: {:?}", entries);

            // Upload a new broken data frame
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);
            let file_to_post = test::test_invalid_parquet_file();
            let directory_name = "broken_data";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            println!("result: {:?}", result);
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add broken data frame".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &workspace_id,
                &body,
            )
            .await?;

            // List the entries
            let entries = api::client::entries::list_entries_with_type(
                &remote_repo,
                "",
                DEFAULT_BRANCH_NAME,
                &EntryDataType::Tabular,
            )
            .await?;
            assert_eq!(entries.len(), 2);
            println!("entries: {:?}", entries);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_staged_single_file_and_pull() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-data";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let file_to_post = test::test_img_file();
            let directory_name = "data";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add one image".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::client::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await?;

            let remote_commit = api::client::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit.is_some());
            assert_eq!(commit.id, remote_commit.unwrap().id);

            let remote_repo_cloned = remote_repo.clone();
            test::run_empty_dir_test_async(|cloned_repo_dir| async move {
                // Clone repo
                let opts = CloneOpts::new(remote_repo.remote.url, cloned_repo_dir.join("new_repo"));
                let cloned_repo = repositories::clone(&opts).await?;

                // Make sure that image is not on main branch
                let path = cloned_repo
                    .path
                    .join(directory_name)
                    .join(test::test_img_file().file_name().unwrap());
                assert!(!path.exists());

                // Pull the branch with new data
                let mut fetch_opts = FetchOpts::new();
                fetch_opts.branch = "add-data".to_string();
                repositories::pull_remote_branch(&cloned_repo, &fetch_opts).await?;

                // We should have the commit locally
                let local_commit = repositories::commits::head_commit(&cloned_repo)?;
                assert_eq!(local_commit.id, commit.id);

                // The file should exist locally
                println!("Looking for file at path: {:?}", path);
                assert!(path.exists());

                Ok(cloned_repo_dir)
            })
            .await?;

            Ok(remote_repo_cloned)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_schema_on_branch() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "test-schema-issues";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let original_schemas = api::client::schemas::list(&remote_repo, branch_name).await?;

            let directory_name = "tabular";
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // Post a parquet file
            let path = test::test_1k_parquet();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            // Post an image file
            let path = test::test_img_file();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add one data frame and one image".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::client::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await?;
            assert!(commit.message.contains("Add one data frame and one image"));

            // List the schemas on that branch
            let schemas = api::client::schemas::list(&remote_repo, branch_name).await?;
            assert_eq!(schemas.len(), original_schemas.len() + 1);

            // List the file counts on that branch in that directory
            let file_counts =
                api::client::dir::file_counts(&remote_repo, branch_name, directory_name).await?;
            assert_eq!(file_counts.dir.data_types.len(), 2);
            assert_eq!(
                file_counts
                    .dir
                    .data_types
                    .iter()
                    .find(|dt| dt.data_type == "image")
                    .unwrap()
                    .count,
                1
            );
            assert_eq!(
                file_counts
                    .dir
                    .data_types
                    .iter()
                    .find(|dt| dt.data_type == "tabular")
                    .unwrap()
                    .count,
                1
            );

            // List the file counts on that branch in the root directory
            let file_counts = api::client::dir::file_counts(&remote_repo, branch_name, "").await?;
            assert_eq!(file_counts.dir.data_types.len(), 2);
            assert_eq!(
                file_counts
                    .dir
                    .data_types
                    .iter()
                    .find(|dt| dt.data_type == "image")
                    .unwrap()
                    .count,
                1
            );
            assert_eq!(
                file_counts
                    .dir
                    .data_types
                    .iter()
                    .find(|dt| dt.data_type == "tabular")
                    .unwrap()
                    .count,
                2
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let directory_name = "images";
            let path = test::test_img_file();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            // Remove the file
            let result =
                api::client::workspaces::files::rm(&remote_repo, &workspace_id, result.unwrap())
                    .await;
            assert!(result.is_ok());

            // Make sure we have 0 files staged
            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 0);
            assert_eq!(entries.added_files.total_entries, 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_stage_file_in_multiple_subdirectories() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let directory_name = "my/images/dir/is/long";
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = test::test_img_file();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 1);
            assert_eq!(entries.added_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_add_from_local_repo_multiple_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let branch_name = "add-multiple-files";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = format!("test-workspace-{}", uuid::Uuid::new_v4());
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // Prepare paths and directory
            let paths = vec![
                test::test_img_file(),
                test::test_img_file_with_name("cole_anthony.jpeg"),
            ];
            let directory = "test_data";

            // Call the add function with multiple files
            let result = api::client::workspaces::files::add_from_local_repo(
                &local_repo,
                &remote_repo,
                &workspace_id,
                directory,
                paths,
            )
            .await;
            assert!(result.is_ok());

            // Verify that both files were added
            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory);
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 2);
            assert_eq!(entries.added_files.total_entries, 2);

            Ok(remote_repo)
        })
        .await
    }
}
