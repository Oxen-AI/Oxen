use crate::api::client;
use crate::constants::AVG_CHUNK_SIZE;
use crate::constants::{
    MAX_CONCURRENT_UPLOADS as DEFAULT_MAX_CONCURRENT_UPLOADS, MAX_RETRIES as DEFAULT_MAX_RETRIES,
};
use crate::core::progress::push_progress::PushProgress;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};
use crate::opts::GlobOpts;
use crate::util::{self, concurrency};
use crate::view::{ErrorFileInfo, ErrorFilesResponse, FilePathsResponse, FileWithHash};
use crate::{api, repositories, view::workspaces::ValidateUploadFeasibilityRequest};

use bytesize::ByteSize;
use futures_util::StreamExt;
use glob_match::glob_match;
use parking_lot::Mutex;
use pluralizer::pluralize;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, Duration};

use crate::util::hasher;
use flate2::write::GzEncoder;
use flate2::Compression;

const BASE_WAIT_TIME: usize = 300;
const MAX_WAIT_TIME: usize = 10_000;
const WORKSPACE_ADD_LIMIT: u64 = 100_000_000;

#[derive(Debug)]
pub struct UploadResult {
    pub files_to_add: Vec<FileWithHash>,
    pub err_files: Vec<ErrorFileInfo>,
}

// TODO: Test adding removed files
pub async fn add(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<str>,
    paths: Vec<PathBuf>,
    local_repo: &Option<LocalRepository>,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();

    // If no paths provided, return early
    if paths.is_empty() {
        return Ok(());
    }

    // Parse glob paths
    let glob_opts = GlobOpts {
        paths,
        staged_db: false,
        merkle_tree: false,
        working_dir: true,
        walk_dirs: true,
    };

    let expanded_paths = util::glob::parse_glob_paths(&glob_opts, local_repo.as_ref())?;
    let expanded_paths: Vec<PathBuf> = expanded_paths.iter().cloned().collect();
    // TODO: add a progress bar
    // TODO: need to handle error files and not display the `oxen added` message if files weren't added
    match upload_multiple_files(
        remote_repo,
        workspace_id,
        directory,
        expanded_paths.clone(),
        local_repo,
    )
    .await
    {
        Ok(()) => {
            println!(
                "🐂 oxen added {} entries to workspace {}",
                expanded_paths.len(),
                workspace_id
            );
        }
        Err(e) => {
            return Err(e);
        }
    }

    Ok(())
}

pub async fn add_bytes(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<str>,
    path: PathBuf,
    buf: &[u8],
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();

    match upload_bytes_as_file(remote_repo, workspace_id, directory, &path, buf).await {
        Ok(path) => {
            println!("🐂 oxen added entry {path:?} to workspace {}", workspace_id);
        }
        Err(e) => {
            return Err(e);
        }
    }

    Ok(())
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
            None,
            None,
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

pub async fn upload_bytes_as_file(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    path: impl AsRef<Path>,
    buf: &[u8],
) -> Result<PathBuf, OxenError> {
    p_upload_bytes_as_file(remote_repo, workspace_id, directory, path, buf).await
}

async fn upload_multiple_files(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    paths: Vec<PathBuf>,
    local_repo: &Option<LocalRepository>,
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

    let repo_path = if let Some(local_repo) = local_repo {
        local_repo.path.clone()
    } else {
        PathBuf::new()
    };

    // Group files by size
    for path in paths {
        // Adjustment for remote-mode repos
        let path = if local_repo.is_some() {
            let relative_path = util::fs::path_relative_to_dir(&path, &repo_path)?;
            repo_path.join(&relative_path)
        } else {
            path
        };

        if !path.exists() {
            log::warn!("File does not exist: {path:?}");
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
                log::warn!("Failed to get metadata for file {path:?}: {err}");
                continue;
            }
        }
    }

    let large_files_size = large_files.iter().map(|(_, size)| size).sum::<u64>();
    let total_size = large_files_size + small_files_size;

    validate_upload_feasibility(remote_repo, workspace_id, total_size).await?;
    // Process large files individually with parallel upload
    for (path, _size) in large_files {
        match api::client::versions::parallel_large_file_upload(
            remote_repo,
            &path,
            Some(directory),
            Some(workspace_id.to_string()),
            None,
            None,
        )
        .await
        {
            Ok(_) => log::debug!("Successfully uploaded large file: {path:?}"),
            Err(err) => log::error!("Failed to upload large file {path:?}: {err}"),
        }
    }

    // Upload small files in batches
    parallel_batched_small_file_upload(
        remote_repo,
        workspace_id,
        directory,
        small_files,
        small_files_size,
        local_repo,
    )
    .await?;

    Ok(())
}

async fn parallel_batched_small_file_upload(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    small_files: Vec<(PathBuf, u64)>,
    small_files_size: u64,
    local_repo: &Option<LocalRepository>,
) -> Result<(), OxenError> {
    if small_files.is_empty() {
        return Ok(());
    }

    // Batch small files in chunks of ~AVG_CHUNK_SIZE
    log::debug!(
        "Uploading {} small files (total {} bytes)",
        small_files.len(),
        small_files_size
    );

    let workspace_id = workspace_id.as_ref().to_string();
    let workspace_id_clone = workspace_id.clone();
    let directory = directory.as_ref().to_str().unwrap().to_string();
    let directory_clone = directory.clone();

    let worker_count = concurrency::default_num_threads();
    let (tx, mut rx) = mpsc::channel(worker_count);

    // Use semaphore to limit the number of concurrent uploads
    let max_concurrent_uploads = max_concurrent_uploads();
    let upload_limiter = Arc::new(Semaphore::new(max_concurrent_uploads));

    // Create a client for uploading batches
    // let client = Arc::new(api::client::builder_for_remote_repo(&remote_repo_clone)?.build()?);
    let progress = Arc::new(PushProgress::new_with_totals(small_files.len() as u64, 0));
    let err_files: Arc<Mutex<Vec<ErrorFileInfo>>> = Arc::new(Mutex::new(vec![]));
    let local_repo_clone = local_repo.clone();
    let producer_lock = Arc::clone(&err_files);
    let producer_handle = tokio::spawn(async move {
        let head_commit_maybe = if let Some(ref local_repo_clone) = local_repo_clone {
            repositories::commits::head_commit_maybe(local_repo_clone)
                .expect("Remote-mode repos must always have a head commit")
        } else {
            None
        };

        // Get repo path if provided
        let repo_path = if let Some(ref local_repo_clone) = local_repo_clone {
            local_repo_clone.path.clone()
        } else {
            PathBuf::new()
        };
        let mut producer_handles = Vec::new();
        for (path, size) in small_files {
            let tx_clone = tx.clone();
            let repo_path_clone = repo_path.clone();
            let head_commit_maybe_clone = head_commit_maybe.clone();
            let local_repo_clone = local_repo_clone.clone();
            let producer_lock = Arc::clone(&producer_lock);
            let task_handle = tokio::task::spawn_blocking(move || {
                let relative_path =
                    util::fs::path_relative_to_dir(&path, &repo_path_clone).unwrap();

                // In remote-mode repos, skip adding files already present in tree
                if let Some(ref head_commit) = head_commit_maybe_clone {
                    if let Some(file_node) = repositories::tree::get_file_by_path(
                        &local_repo_clone.unwrap(),
                        head_commit,
                        &relative_path,
                    )? {
                        if !util::fs::is_modified_from_node(&path, &file_node)? {
                            log::debug!("Skipping add on unmodified path {path:?}");
                            return Ok(());
                        }
                    }
                }

                // Note: Remote-mode repos expect the relative path in staging, while regular repos expect the file name
                let staging_path = if head_commit_maybe_clone.is_some() {
                    relative_path
                } else {
                    PathBuf::from(relative_path.file_name().unwrap())
                };

                let file = std::fs::read(&path).map_err(|e| {
                    OxenError::basic_str(format!("Failed to read file '{:?}': {e}", path))
                })?;

                let hash = hasher::hash_buffer(&file);

                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                std::io::copy(&mut file.as_slice(), &mut encoder).expect("");

                {
                    let compressed_bytes = match encoder.finish() {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            log::error!("Failed to finish gzip for file {}: {}", &hash, e);

                            let mut producer_err_files = producer_lock.lock();
                            producer_err_files.push(ErrorFileInfo {
                                hash: hash.clone(),
                                path: Some(path.clone()),
                                error: format!(
                                    "Failed to finish gzip for hash {:?}: {:?}",
                                    &path, e
                                ),
                            });

                            return Ok(());

                            // TODO: Should we return immediately on this error?
                            // It seems unlikely that retrying would fix a gzip error

                            /*return Err(OxenError::basic_str(format!(
                                "Failed to finish gzip for hash {:?}: {:?}",
                                &path, e
                            )));*/
                        }
                    };

                    let file_part = reqwest::multipart::Part::bytes(compressed_bytes)
                        .file_name(hash.clone())
                        .mime_str("application/gzip")?;

                    match tx_clone.blocking_send((file_part, hash.clone(), staging_path, size)) {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!("Error with blocking send for file {}: {}", &hash, e);

                            let mut producer_err_files = producer_lock.lock();
                            producer_err_files.push(ErrorFileInfo {
                                hash: hash.clone(),
                                path: Some(path.clone()),
                                error: format!(
                                    "Error with blocking send for file {:?}: {:?}",
                                    &path, e
                                ),
                            });

                            return Ok(());

                            // TODO: Should we return immediately on this error?

                            /*return Err(OxenError::basic_str(format!(
                                "Error with blocking send for file {:?}: {:?}",
                                &path, e
                            )));*/
                        }
                    }
                }

                Ok::<(), OxenError>(())
            });

            producer_handles.push(task_handle);
        }

        // Join task handles, cancelling the operation on error
        futures::future::try_join_all(producer_handles)
            .await
            .map_err(|e| {
                log::error!("producer task failed: {:?}", e);
                OxenError::basic_str(format!("producer task failed: {}", e))
            })?;

        Ok::<(), OxenError>(())
    });

    let consumer_lock = Arc::clone(&err_files);

    let client = Arc::new(api::client::builder_for_remote_repo(remote_repo)?.build()?);
    let client_clone = client.clone();
    let remote_repo_clone = remote_repo.clone();
    let progress_clone = progress.clone();

    let consumer_handle = tokio::spawn(async move {
        let mut upload_handles = Vec::new();
        let mut files_to_stage = Vec::new();
        let mut current_batch_size = 0;
        let mut current_batch_parts = Vec::new();
        let directory_clone = directory.clone();
        while let Some((file_part, file_hash, file_path, part_size)) = rx.recv().await {
            files_to_stage.push(FileWithHash {
                hash: file_hash,
                path: file_path,
            });

            current_batch_parts.push(file_part);
            current_batch_size += part_size;

            if current_batch_size >= AVG_CHUNK_SIZE || rx.is_closed() {
                let mut form = reqwest::multipart::Form::new();

                let num_entries = current_batch_parts.len();
                for part in current_batch_parts {
                    form = form.part("file[]", part);
                }

                let client_clone = client.clone();
                let remote_repo_clone = remote_repo_clone.clone();
                let consumer_lock_clone = Arc::clone(&consumer_lock.clone());
                let workspace_id_clone = workspace_id.clone();
                let directory_str = directory_clone.clone();

                let batch_size = current_batch_size;
                let bar = Arc::clone(&progress_clone);

                // TODO: Find way not to have to clone files_to_stage
                let files_to_stage_clone = files_to_stage.clone();
                let upload_limiter_clone = upload_limiter.clone();
                let upload_permit = upload_limiter_clone
                    .acquire_owned()
                    .await
                    .expect("failed to acquire semaphore permit");
                let upload_handle = tokio::spawn(async move {
                    log::debug!("upload files: {}", files_to_stage_clone.len());

                    // TODO: Use retry version of multipart batch upload
                    match api::client::versions::workspace_multipart_batch_upload_parts(
                        &remote_repo_clone,
                        Arc::clone(&client_clone),
                        form,
                    )
                    .await
                    {
                        Ok(err_files) => {
                            if !err_files.is_empty() {
                                let mut consumer_err_files = consumer_lock_clone.lock();
                                consumer_err_files.extend(err_files.clone());
                            }

                            log::debug!(
                                "Version file upload successful. Beginning staging for {:?} files",
                                files_to_stage_clone.len()
                            );
                            match stage_files_to_workspace_with_retry(
                                &remote_repo_clone,
                                client_clone,
                                &workspace_id_clone,
                                Arc::new(files_to_stage_clone),
                                &directory_str,
                                err_files,
                            )
                            .await
                            {
                                // If the staging operation returned successfully, record the err_files for re-upload
                                Ok(err_files) => {
                                    log::debug!("Successfully staged files to workspace with errs {err_files:?}");

                                    bar.add_bytes(batch_size);
                                    bar.add_files(num_entries as u64);

                                    let mut consumer_err_files = consumer_lock_clone.lock();
                                    consumer_err_files.extend(err_files);
                                }
                                // If staging failed, cancel the operation
                                Err(e) => {
                                    log::error!("failed to stage files to workspace: {}", e);
                                    return Err(OxenError::basic_str(format!(
                                        "failed to stage to workspace: {}",
                                        e
                                    )));
                                }
                            }

                            files_to_stage.clear();
                        }
                        // If uploading the version files fails, cancel the operation
                        Err(e) => {
                            log::error!("failed to upload version files to workspace: {}", e);
                            return Err(OxenError::basic_str(format!(
                                "failed to upload version files to workspace: {}",
                                e
                            )));
                        }
                    }

                    log::debug!("consumer task complete, dropping upload permit");
                    drop(upload_permit);
                    Ok::<(), OxenError>(())
                });

                upload_handles.push(upload_handle);

                // Reset for the next batch.
                current_batch_size = 0;
                current_batch_parts = Vec::new();
                files_to_stage = Vec::new();
            }
        }

        // Upload remaining files in the pipeline
        if !current_batch_parts.is_empty() {
            log::debug!("uploading remaining files in pipeline");

            let client_clone = client.clone();
            let remote_repo_clone = remote_repo_clone.clone();
            let consumer_lock_clone = Arc::clone(&consumer_lock.clone());
            let workspace_id_clone = workspace_id.clone();
            let directory_str = directory_clone.clone();

            let batch_size = current_batch_size;
            let num_entries = current_batch_parts.len();
            let bar = Arc::clone(&progress_clone);

            let final_handle = tokio::spawn(async move {
                // First, upload the version files
                let mut form = reqwest::multipart::Form::new();
                for part in current_batch_parts {
                    form = form.part("file", part);
                }

                // TODO: Use blocking version
                match api::client::versions::workspace_multipart_batch_upload_parts(
                    &remote_repo_clone,
                    Arc::clone(&client_clone),
                    form,
                )
                .await
                {
                    Ok(err_files) => {
                        if !err_files.is_empty() {
                            bar.add_bytes(batch_size);
                            bar.add_files(num_entries as u64);

                            let mut consumer_err_files = consumer_lock_clone.lock();
                            consumer_err_files.extend(err_files.clone());
                        }

                        log::debug!("staging remaining files in pipeline");
                        match stage_files_to_workspace_with_retry(
                            &remote_repo_clone,
                            client_clone,
                            &workspace_id_clone,
                            Arc::new(files_to_stage.clone()),
                            &directory_str,
                            err_files,
                        )
                        .await
                        {
                            // If the staging operation returned successfully, record the err_files for re-upload
                            Ok(err_files) => {
                                log::debug!("Successfully staged files to workspace with errs {err_files:?}");

                                let mut consumer_err_files = consumer_lock.lock();
                                consumer_err_files.extend(err_files);
                            }
                            // If staging failed, cancel the operation
                            Err(e) => {
                                log::debug!("Failed to stage files to workspace: {}", e);
                                return Err(OxenError::basic_str(format!(
                                    "Failed to stage to workspace: {}",
                                    e
                                )));
                            }
                        }

                        files_to_stage.clear();
                    }
                    // If uploading the version files fails, cancel the operation
                    Err(e) => {
                        log::debug!("Failed to upload version files to workspace: {}", e);
                        return Err(OxenError::basic_str(format!(
                            "Failed to upload version files to workspace: {}",
                            e
                        )));
                    }
                }

                Ok::<(), OxenError>(())
            });
            upload_handles.push(final_handle);
        }

        // Join task handles, cancelling the operation on error
        futures::future::try_join_all(upload_handles)
            .await
            .map_err(|e| {
                log::error!("consumer task failed: {:?}", e);
                OxenError::basic_str(format!("consumer task failed: {}", e))
            })?;

        Ok::<(), OxenError>(())
    });

    // Wait for both the producer and consumer tasks to complete
    // If a task fails, the operation cancels and returns an error immediately
    let result = tokio::try_join!(producer_handle, consumer_handle)?;
    match result {
        (Ok(_producer_res), Ok(_consumer_res)) => {}
        (Err(e), _) => {
            log::error!("Producer task failed: {:?}", e);
            return Err(OxenError::basic_str(format!("Producer task failed: {e}")));
        }
        (_, Err(e)) => {
            log::error!("Consumer task panicked: {:?}", e);
            return Err(OxenError::basic_str(format!("Consumer task failed: {e}")));
        }
    }

    // Get the err_files from both processes
    let mutex = match Arc::try_unwrap(err_files) {
        Ok(mutex) => mutex,
        Err(e) => {
            let err = format!("Couldn't acquire mutex guard for err_files: {:?}", e);
            log::error!("{}", err);
            return Err(OxenError::basic_str(&err));
        }
    };

    let err_files = mutex.into_inner();
    let err_file_paths: Vec<PathBuf> = err_files
        .clone()
        .into_iter()
        .map(|f| f.path.unwrap())
        .collect();

    // TODO: Should we communicate to the user that we're retrying these files?
    // Should the operation return successfully even if these files don't upload?
    log::debug!(
        "Initial batches successfully uplaoded. Retrying {} err files: ",
        err_files.len()
    );

    // Retry workspace add on err_files
    if !err_file_paths.is_empty() {
        match api::client::versions::workspace_multipart_batch_upload_versions_with_retry(
            remote_repo,
            local_repo,
            client_clone.clone(),
            err_file_paths,
        )
        .await
        {
            Ok(result) => {
                match stage_files_to_workspace_with_retry(
                    remote_repo,
                    client_clone,
                    workspace_id_clone,
                    Arc::new(result.files_to_add),
                    directory_clone,
                    Vec::new(),
                )
                .await
                {
                    Ok(err_files) => {
                        if err_files.is_empty() {
                            log::debug!("Successfully added all files to workspace");
                        } else {
                            log::error!(
                                "Failed to stage {} files to workspace: {:?}",
                                err_files.len(),
                                err_files
                            );
                            return Err(OxenError::basic_str(format!(
                                "Failed to stage {} files to workspace: {:?}",
                                err_files.len(),
                                err_files
                            )));
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to add version files to workspace: {}", e);
                        return Err(OxenError::basic_str(format!(
                            "Failed to add version files to workspace: {}",
                            e
                        )));
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to upload batch of files: {}", e);
                return Err(OxenError::basic_str(format!(
                    "Failed to upload batch of files: {}",
                    e
                )));
            }
        }
    }

    log::debug!("All upload tasks completed");
    progress.finish();
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

// Retry stage_files_to_workspace until successful or retry limit breached
// If individual files fail, return them to be re-tried at the end
pub async fn stage_files_to_workspace_with_retry(
    remote_repo: &RemoteRepository,
    client: Arc<reqwest::Client>,
    workspace_id: impl AsRef<str>,
    files_to_add: Arc<Vec<FileWithHash>>,
    directory_str: impl AsRef<str>,
    err_files: Vec<ErrorFileInfo>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let mut retry_count: usize = 0;
    let directory_str = directory_str.as_ref();
    let workspace_id = workspace_id.as_ref().to_string();
    let max_retries = max_retries();

    while retry_count < max_retries {
        retry_count += 1;

        match stage_files_to_workspace(
            remote_repo,
            client.clone(),
            &workspace_id,
            files_to_add.clone(),
            directory_str,
            err_files.clone(),
        )
        .await
        {
            // If successful, return individual files that failed to stage
            Ok(stage_err_files) => {
                return Ok(stage_err_files);
            }
            Err(e) => {
                log::error!("Error staging files to workspace: {:?}", e);
                if retry_count == max_retries {
                    return Err(OxenError::basic_str(format!(
                        "failed to stage files to workspace after retries: {:?}",
                        e
                    )));
                }
            }
        }

        let wait_time = exponential_backoff(BASE_WAIT_TIME, retry_count, MAX_WAIT_TIME);
        sleep(Duration::from_millis(wait_time as u64)).await;
    }

    log::error!(
        "Error: Failed to stage files_to_add: {:?}",
        files_to_add.len()
    );
    Err(OxenError::basic_str(
        "failed to stage files to workspace after retries",
    ))
}

// Stage files to the workspace, filtering out files that previously failed to upload to version store
pub async fn stage_files_to_workspace(
    remote_repo: &RemoteRepository,
    client: Arc<reqwest::Client>,
    workspace_id: impl AsRef<str>,
    files_to_add: Arc<Vec<FileWithHash>>,
    directory_str: impl AsRef<str>,
    err_files: Vec<ErrorFileInfo>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory_str = directory_str.as_ref();
    let uri = format!("/workspaces/{workspace_id}/versions/{directory_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let files_to_send = if !err_files.is_empty() {
        let err_hashes: std::collections::HashSet<String> =
            err_files.iter().map(|f| f.hash.clone()).collect();
        files_to_add
            .iter()
            .filter(|f| !err_hashes.contains(&f.hash))
            .cloned()
            .collect()
    } else {
        files_to_add.to_vec()
    };

    println!("Files to send: {:?}", files_to_send.len());

    let response = client.post(&url).json(&files_to_send).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let response: ErrorFilesResponse = serde_json::from_str(&body)?;

    Ok(response.err_files)
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
    log::debug!("multipart_file_upload path: {path:?}");
    let Ok(file) = std::fs::read(path) else {
        let err = format!("Error reading file at path: {path:?}");
        return Err(OxenError::basic_str(err));
    };

    let uri = format!("/workspaces/{workspace_id}/files/{directory_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let file_name: String = path.file_name().unwrap().to_string_lossy().into();
    log::info!("api::client::workspaces::files::add sending file_name: {file_name:?}");

    let file_part = reqwest::multipart::Part::bytes(file).file_name(file_name);
    let form = reqwest::multipart::Form::new().part("file", file_part);
    let client = client::new_for_url(&url)?;
    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let result: Result<FilePathsResponse, serde_json::Error> = serde_json::from_str(&body);
    match result {
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

async fn p_upload_bytes_as_file(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    path: impl AsRef<Path>,
    mut buf: &[u8],
) -> Result<PathBuf, OxenError> {
    // Check if the total size of the files is too large (over 100mb for now)
    let limit = WORKSPACE_ADD_LIMIT;
    let total_size: u64 = buf.len().try_into().unwrap();
    if total_size > limit {
        let error_msg = format!("Total size of files to upload is too large. {} > {} Consider using `oxen push` instead for now until upload supports bulk push.", ByteSize::b(total_size), ByteSize::b(limit));
        return Err(OxenError::basic_str(error_msg));
    }

    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();
    let directory_name = directory.to_string_lossy();
    let path = path.as_ref();
    log::debug!("multipart_file_upload path: {:?}", path);

    let file_name: String = path.file_name().unwrap().to_string_lossy().into();
    log::info!("uploading bytes with file_name: {:?}", file_name);

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::copy(&mut buf, &mut encoder)?;
    let compressed_bytes = match encoder.finish() {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err(OxenError::basic_str(format!(
                "Failed to finish gzip for file {}: {}",
                &file_name, e
            )));
        }
    };

    let file_part = reqwest::multipart::Part::bytes(compressed_bytes)
        .file_name(file_name)
        .mime_str("application/gzip")?;

    let form = reqwest::multipart::Form::new().part("file[]", file_part);

    let uri = format!("/workspaces/{workspace_id}/files/{directory_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let result: Result<FilePathsResponse, serde_json::Error> = serde_json::from_str(&body);
    match result {
        Ok(val) => {
            log::debug!("File path response: {val:?}");
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

pub async fn add_many(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory_name: impl AsRef<str>,
    paths: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory_name = directory_name.as_ref();
    // Check if the total size of the files is too large (over 100mb for now)
    let limit = WORKSPACE_ADD_LIMIT;
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

// TODO: Merge this with 'rm_files'
// Splitting them is a temporary solution to preserve compatibility with the python repo
pub async fn rm(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/workspaces/{workspace_id}/files/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_file {url}");
    let client = client::new_for_url(&url)?;
    let response = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    log::debug!("rm_file got body: {body}");
    Ok(())
}

pub async fn rm_files(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    paths: Vec<PathBuf>,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();

    // Parse glob paths
    let glob_opts = GlobOpts {
        paths: paths.clone(),
        staged_db: false,
        merkle_tree: true,
        working_dir: false,
        walk_dirs: false,
    };

    let expanded_paths: HashSet<PathBuf> =
        util::glob::parse_glob_paths(&glob_opts, Some(local_repo))?;

    // Convert to relative paths
    let repo_path = &local_repo.path;
    let expanded_paths: Vec<PathBuf> = expanded_paths
        .iter()
        .map(|p| util::fs::path_relative_to_dir(p, repo_path).unwrap())
        .collect();

    let uri = format!("/workspaces/{workspace_id}/versions");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_files: {url}");
    let client = client::new_for_url(&url)?;
    let response = client.delete(&url).json(&expanded_paths).send().await?;

    if response.status().is_success() {
        let _body = client::parse_json_body(&url, response).await?;
        println!("🐂 oxen staged paths {paths:?} as removed in workspace {workspace_id}");

        // Remove files locally
        for path in expanded_paths {
            let full_path = local_repo.path.join(&path);
            if full_path.is_dir() {
                util::fs::remove_dir_all(&full_path)?;
            }

            if full_path.is_file() {
                util::fs::remove_file(&full_path)?;
            }
        }
    } else {
        log::error!("rm_files failed with status: {}", response.status());
        let body = client::parse_json_body(&url, response).await?;

        return Err(OxenError::basic_str(format!(
            "Error: Could not remove paths {body:?}"
        )));
    }

    Ok(())
}

pub async fn rm_files_from_staged(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    paths: Vec<PathBuf>,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();

    // Parse glob paths
    let repo_path = local_repo.path.clone();
    let mut expanded_paths: HashSet<PathBuf> = HashSet::new();

    for path in paths.clone() {
        let relative_path = util::fs::path_relative_to_dir(&path, local_repo.path.clone())?;
        let full_path = repo_path.join(&relative_path);
        if util::fs::is_glob_path(&full_path) {
            let Some(ref head_commit) = repositories::commits::head_commit_maybe(local_repo)?
            else {
                // TODO: Better error message?
                return Err(OxenError::basic_str(
                    "Error: Cannot rm with glob paths in remote-mode repo without HEAD commit",
                ));
            };
            let glob_pattern = relative_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let root_path = PathBuf::from("");
            let parent_path = relative_path.parent().unwrap_or(&root_path);

            // If dir not found in tree, skip glob path
            let Some(dir_node) =
                repositories::tree::get_dir_with_children(local_repo, head_commit, parent_path)?
            else {
                continue;
            };

            let dir_children = dir_node.list_paths()?;
            for child_path in dir_children {
                let child_str = child_path.to_string_lossy().to_string();
                if glob_match(&glob_pattern, &child_str) {
                    expanded_paths.insert(parent_path.join(child_path.clone()));
                }
            }
        } else {
            expanded_paths.insert(relative_path);
        }
    }

    log::debug!("expanded paths: {expanded_paths:?}");

    let expanded_paths: Vec<PathBuf> = expanded_paths.iter().cloned().collect();

    let uri = format!("/workspaces/{workspace_id}/staged");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_files: {url}");
    let client = client::new_for_url(&url)?;
    let response = client.delete(&url).json(&expanded_paths).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    log::debug!("rm_files got body: {body}");
    Ok(())
}

pub async fn download(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &str,
    output_path: Option<&Path>,
) -> Result<(), OxenError> {
    let uri = if util::fs::has_tabular_extension(path) {
        format!("/workspaces/{workspace_id}/data_frames/download/{path}")
    } else {
        format!("/workspaces/{workspace_id}/files/{path}")
    };

    log::debug!("Downloading file from {workspace_id}/{path} to {output_path:?}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("Downloading file from {url}");
    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;

    if response.status().is_success() {
        // Save the raw file contents from the response stream
        let output_path = output_path.unwrap_or_else(|| Path::new(path));
        let mut file = tokio::fs::File::create(&output_path).await?;
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
    } else {
        log::error!(
            "api::client::workspace::files::download failed with status: {}",
            response.status()
        );
        let body = client::parse_json_body(&url, response).await?;

        return Err(OxenError::basic_str(format!(
            "Error: Could not remove paths {body:?}"
        )));
    }

    Ok(())
}

pub async fn validate_upload_feasibility(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    total_size: u64,
) -> Result<(), OxenError> {
    let uri = format!("/workspaces/{workspace_id}/validate");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let body = ValidateUploadFeasibilityRequest { size: total_size };

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;
    client::parse_json_body(&url, response).await?;
    Ok(())
}

pub fn exponential_backoff(base_wait_time: usize, n: usize, max: usize) -> usize {
    (base_wait_time + n.pow(2) + jitter()).min(max)
}

// Parse the maximum number of concurrent uploads allowed from environment variable
// TODO: Should we enforce a limit on this?
fn max_concurrent_uploads() -> usize {
    if let Ok(max_uploads) = std::env::var("OXEN_MAX_CONCURRENT_UPLOADS") {
        // If the environment variable is set, use that
        if let Ok(max_uploads) = max_uploads.parse::<usize>() {
            max_uploads
        } else {
            // If parsing failed, fall back to default
            DEFAULT_MAX_CONCURRENT_UPLOADS
        }
    } else {
        // Environment variable not set, use default
        DEFAULT_MAX_CONCURRENT_UPLOADS
    }
}

// Parse the maximum number of retries allowed on upload from environment variable
// TODO: Should we enforce a limit on this?
fn max_retries() -> usize {
    if let Ok(max_retries) = std::env::var("OXEN_MAX_RETRIES") {
        // If the environment variable is set, use that
        if let Ok(max_retries) = max_retries.parse::<usize>() {
            max_retries
        } else {
            // If parsing failed, fall back to default
            DEFAULT_MAX_RETRIES
        }
    } else {
        // Environment variable not set, use default
        DEFAULT_MAX_RETRIES
    }
}

fn jitter() -> usize {
    thread_rng().gen_range(0..=500)
}

#[cfg(test)]
mod tests {

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{EntryDataType, NewCommitBody};
    use crate::opts::fetch_opts::FetchOpts;
    use crate::opts::CloneOpts;
    use crate::{api, constants};
    use crate::{repositories, test};
    use std::path::PathBuf;

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
            let workspace_id = uuid::Uuid::new_v4().to_string();
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = test::test_img_file();
            let result = api::client::workspaces::files::add(
                &remote_repo,
                &workspace_id,
                directory_name,
                vec![path],
                &None,
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
            let assert_path = PathBuf::from("images").join(PathBuf::from("dwight_vince.jpeg"));

            assert_eq!(
                entries.added_files.entries[0].filename(),
                assert_path.to_str().unwrap(),
            );

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
            let workspace_id = uuid::Uuid::new_v4().to_string();
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = test::test_30k_parquet();
            let result = api::client::workspaces::files::add(
                &remote_repo,
                &workspace_id,
                directory_name,
                vec![path],
                &None,
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

            let workspace_id = uuid::Uuid::new_v4().to_string();
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
            let workspace_id = uuid::Uuid::new_v4().to_string();
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
            println!("result: {result:?}");
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
            println!("result: {result:?}");
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
            println!("entries: {entries:?}");

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
            println!("result: {result:?}");
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
            println!("entries: {entries:?}");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_multiple_data_frames() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let workspace_id = uuid::Uuid::new_v4().to_string();
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
            println!("result: {result:?}");
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
            println!("result: {result:?}");
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
            println!("entries: {entries:?}");

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
            println!("result: {result:?}");
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
            println!("entries: {entries:?}");

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

            let workspace_id = uuid::Uuid::new_v4().to_string();
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
                println!("Looking for file at path: {path:?}");
                assert!(path.exists());

                Ok(())
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
            let workspace_id = uuid::Uuid::new_v4().to_string();
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

            let workspace_id = uuid::Uuid::new_v4().to_string();
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
            let workspace_id = uuid::Uuid::new_v4().to_string();
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
    async fn test_add_multiple_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
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
            let result = api::client::workspaces::files::add(
                &remote_repo,
                &workspace_id,
                directory,
                paths,
                &None,
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

    #[tokio::test]
    async fn test_add_file_with_absolute_path() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images-with-absolute-path";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let directory_name = "new-images";
            let workspace_id = uuid::Uuid::new_v4().to_string();
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // Get the absolute path to the file
            let path = test::test_img_file().canonicalize()?;
            let result = api::client::workspaces::files::add(
                &remote_repo,
                &workspace_id,
                directory_name,
                vec![path],
                &None,
            )
            .await;
            assert!(result.is_ok());

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new("");
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

            let assert_path = PathBuf::from("new-images").join(PathBuf::from("dwight_vince.jpeg"));
            assert_eq!(
                entries.added_files.entries[0].filename(),
                assert_path.to_str().unwrap(),
            );

            Ok(remote_repo)
        })
        .await
    }

    // Download file from the workspace's base repo using the workspace download endpoint
    #[tokio::test]
    async fn test_download_version_file_from_workspace() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let branch_name = constants::DEFAULT_BRANCH_NAME;

            let workspace_id = uuid::Uuid::new_v4().to_string();
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let bounding_box_path = PathBuf::from("README.md");
            let output_path = PathBuf::from("output.md");

            // Download the bounding box from the base repo to a new path
            api::client::workspaces::files::download(
                &remote_repo,
                &workspace_id,
                bounding_box_path.to_str().unwrap(),
                Some(&output_path),
            )
            .await?;

            assert!(output_path.exists());

            Ok(remote_repo)
        })
        .await
    }
}
