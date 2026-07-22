use crate::api::client;
use crate::api::client::internal_types::LocalOrBase;
use crate::constants::{max_retries, stream_segment_size};
use crate::core::progress::push_progress::PushProgress;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, RemoteRepository};
use crate::opts::GlobOpts;
use crate::util::{self, concurrency};
use crate::view::{ErrorFileInfo, FilePathsResponse};
use crate::{api, repositories, view, view::workspaces::ValidateUploadFeasibilityRequest};

use futures_util::StreamExt;
use glob_match::glob_match;

use parking_lot::Mutex;
use rand::{Rng, thread_rng};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

use futures::stream;
use tokio_stream::wrappers::ReceiverStream;

use flate2::Compression;
use flate2::write::GzEncoder;

const BASE_WAIT_TIME: usize = 300;
const MAX_WAIT_TIME: usize = 10_000;

/// All of the paths that failed to transfer to the remote repository during an upload operation.
///
/// When uploading many files, if most of them succeed, we don't want to treat the entire operation
/// as an `Err`. Uploads can have partial success.
pub type UploadFails = Vec<ErrorFileInfo>;

// TODO: Test adding removed files
pub async fn add(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<str>,
    paths: Vec<PathBuf>,
    local_repo: &Option<LocalRepository>,
) -> Result<UploadFails, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();

    // If no paths provided, return early
    if paths.is_empty() {
        return Ok(vec![]);
    }

    // Parse glob paths
    let glob_opts = GlobOpts {
        paths,
        staged_db: false,
        merkle_tree: false,
        working_dir: true,
        walk_dirs: true,
    };

    let expanded_paths = util::glob::parse_glob_paths(&glob_opts, local_repo.as_ref()).await?;
    let expanded_paths: Vec<PathBuf> = expanded_paths.iter().cloned().collect();
    // TODO: add a progress bar

    let n_expected_uploads = expanded_paths.len();

    let upload_result = upload_multiple_files(
        remote_repo,
        workspace_id,
        directory,
        expanded_paths,
        local_repo
            .clone()
            .map(|local| LocalOrBase::Local(Box::new(local.clone())))
            .as_ref(),
    )
    .await;

    match upload_result {
        Ok(failed_to_upload) => {
            print_add_result(workspace_id, n_expected_uploads, &failed_to_upload);
            Ok(failed_to_upload)
        }
        error => error,
    }
}

fn print_add_result(workspace_id: &str, n_total: usize, failed_to_upload: &[ErrorFileInfo]) {
    let n_fail = failed_to_upload.len();
    if n_fail == 0 {
        println!("🐂 oxen added {n_total} entries to workspace {workspace_id}");
    } else {
        let n_success = n_total - n_fail;
        println!(
            "🐂 oxen added {n_success} entries to workspace {workspace_id} but 😱 failed to upload {n_fail} entries",
        );
    }
}

pub struct AddResult {
    pub added: Option<(Commit, Vec<PathBuf>)>,
    pub not_in_base: Vec<PathBuf>,
    pub not_file: Vec<PathBuf>,
}

// either:
// 1) no commit => added is empty
// 2) commit => added is non-empty
//

/// Resolve paths and error on entries that don't exist/aren't files in the base directory.
#[allow(clippy::needless_range_loop)]
fn resolve_paths_in_place(base_dir: &Path, paths: &mut [PathBuf]) -> Result<(), OxenError> {
    for i in 0..paths.len() {
        if !paths[i].is_absolute() {
            paths[i] = base_dir.join(&paths[i]);
        }

        paths[i] = std::path::absolute(&(paths[i]))?;

        if !paths[i].is_file() {
            return Err(OxenError::basic_str(format!(
                "Cannot upload non-existent file: {}",
                paths[i].display()
            )));
        } else if !paths[i].starts_with(base_dir) {
            return Err(OxenError::basic_str(format!(
                "Cannot upload path that doesn't exist in base directory ({}): {}",
                base_dir.display(),
                paths[i].display()
            )));
        }
    }
    Ok(())
}

/// Add files to a remote workspace while preserving their relative paths within the repository.
///
/// Unlike `add`, which places files into a flat destination directory, this function uses each
/// file's path relative to the supplied base directory as the staging path for the server's
/// remote repository. Files are added into a temporary workspace which is then comitted.
///
/// The intended use case is to import a large pre-existing file-directory structure into a
/// repository.
pub async fn add_files(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    base_dir: impl AsRef<Path>,
    paths: Vec<PathBuf>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let base_dir = std::path::absolute(base_dir)?;

    if !base_dir.is_dir() {
        return Err(OxenError::NotADirectory(base_dir.to_path_buf()));
    }

    if paths.is_empty() {
        return Err(OxenError::NoPathsToAdd);
    }

    let workspace_id = workspace_id.as_ref();

    let paths: Vec<PathBuf> = {
        let mut paths = paths;
        resolve_paths_in_place(&base_dir, &mut paths)?;
        paths
    };

    let base_dir_enum = LocalOrBase::Base(base_dir);

    let n_expected_uploads = paths.len();
    match upload_multiple_files(
        remote_repo,
        workspace_id,
        "", // Each path has the right relative directory components, so it's crucial that they're
        //    "placed" at the repo root since the server API expects to add files into a directory
        //    for a single API call.
        paths,
        Some(&base_dir_enum),
    )
    .await
    {
        Ok(failed_to_upload) => {
            print_add_result(workspace_id, n_expected_uploads, &failed_to_upload);
            Ok(failed_to_upload)
        }
        error => error,
    }
}

/// Stage a single file from disk to a workspace, returning its remote path.
#[cfg(any(test, feature = "test-utils"))]
pub async fn upload_single_file(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let directory = directory.as_ref();
    let path = path.as_ref();
    let file_name = path
        .file_name()
        .ok_or_else(|| OxenError::basic_str(format!("Path has no file name: {path:?}")))?;
    let dst_path = directory.join(file_name);

    // Route through the shared dispatcher so small files batch and large files chunk-upload,
    // exactly like a multi-file add of a single file.
    let err_files = upload_multiple_files(
        remote_repo,
        workspace_id,
        directory,
        vec![path.to_path_buf()],
        None,
    )
    .await?;

    match err_files.into_iter().next() {
        Some(err) => Err(OxenError::basic_str(err.error)),
        None => Ok(dst_path),
    }
}

async fn upload_multiple_files(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    paths: Vec<PathBuf>,
    local_or_base: Option<&LocalOrBase>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    if paths.is_empty() {
        return Ok(vec![]);
    }

    let workspace_id = workspace_id.as_ref();
    let directory = directory.as_ref();

    let large_file_threshold = stream_segment_size();

    // Separate files by size, storing the file size with each path
    let mut large_files = Vec::new();
    let mut large_files_size = 0;
    let mut small_files = Vec::new();
    let mut small_files_size = 0;

    let mut failed_to_upload = vec![];

    // Group files by size
    for path in paths {
        // Adjustment for remote-mode repos
        let path = match local_or_base {
            Some(LocalOrBase::Local(local_repository)) => {
                let repo_path = &local_repository.path;
                let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
                repo_path.join(&relative_path)
            }
            Some(LocalOrBase::Base(_)) | None => path,
        };

        if !path.exists() {
            log::debug!("Path does not exist: {path:?}");
            return Err(OxenError::path_does_not_exist(path));
        }

        match path.metadata() {
            Ok(metadata) => {
                let file_size = metadata.len();
                if file_size > large_file_threshold {
                    // Large file goes directly to parallel upload
                    large_files.push((path, file_size));
                    large_files_size += file_size;
                } else {
                    // Small file goes to batch
                    small_files.push((path, file_size));
                    small_files_size += file_size;
                }
            }
            Err(err) => {
                log::debug!("Failed to get metadata for file {path:?}: {err}");
                return Err(OxenError::file_metadata_error(path, err));
            }
        }
    }

    let total_size = large_files_size + small_files_size;
    validate_upload_feasibility(remote_repo, workspace_id, total_size).await?;

    // Process large files individually with parallel upload
    for (path, _) in large_files {
        let dst_dir = match local_or_base {
            Some(LocalOrBase::Base(base_dir)) => {
                let rel = util::fs::path_relative_to_dir(&path, base_dir)?;
                rel.parent().map(|p| p.to_path_buf()).unwrap_or_default()
            }
            Some(LocalOrBase::Local(_)) | None => directory.to_path_buf(),
        };

        let hash = util::hasher::hash_file_contents(&path).unwrap_or_default();

        match api::client::versions::parallel_large_file_upload(
            remote_repo,
            &path,
            Some(&dst_dir),
            Some(workspace_id.to_string()),
            None,
            None,
        )
        .await
        {
            Ok(_) => log::debug!("Successfully uploaded large file: {path:?}"),
            Err(err) => {
                let msg = format!("Failed to upload large file {path:?}");
                log::error!("{msg}: {err}");
                failed_to_upload.push(ErrorFileInfo {
                    hash,
                    path: Some(path),
                    error: msg,
                });
            }
        }
    }

    // Upload small files in batches
    let err_files_small_upload = parallel_batched_small_file_upload(
        remote_repo,
        workspace_id,
        directory,
        small_files,
        small_files_size,
        local_or_base,
    )
    .await?;

    failed_to_upload.extend(err_files_small_upload);

    Ok(failed_to_upload)
}

pub(crate) async fn parallel_batched_small_file_upload(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    directory: impl AsRef<Path>,
    small_files: Vec<(PathBuf, u64)>,
    small_files_size: u64,
    local_or_base: Option<&LocalOrBase>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    if small_files.is_empty() {
        return Ok(vec![]);
    }

    let (base_or_repo_path, head_commit_local_repo_maybe, keep_relative_paths) = match local_or_base
    {
        Some(LocalOrBase::Local(local_repository)) => {
            let head_commit_maybe = repositories::commits::head_commit_maybe(local_repository)?;
            let head_commit_exists = head_commit_maybe.is_some();
            (
                local_repository.path.clone(),
                head_commit_maybe.map(|head_commit| (head_commit, local_repository.clone())),
                head_commit_exists,
            )
        }
        Some(LocalOrBase::Base(base_dir)) => (base_dir.to_path_buf(), None, true),
        None => (PathBuf::new(), None, false),
    };

    // Batch small files in groups of ~stream_segment_size() bytes
    log::debug!(
        "Uploading {} small files (total {} bytes)",
        small_files.len(),
        small_files_size
    );

    let workspace_id = workspace_id.as_ref().to_string();
    let directory = directory.as_ref().to_str().unwrap_or_default().to_string();

    // Represents unprocessed batches
    type PieceOfWork = Vec<(PathBuf, u64)>;

    // Split files into batches
    let mut file_batches: Vec<PieceOfWork> = Vec::new();
    let mut current_batch: PieceOfWork = Vec::new();
    let mut current_batch_size = 0;
    let mut total_size = 0;

    for (idx, (path, file_size)) in small_files.iter().enumerate() {
        current_batch.push((path.clone(), *file_size));
        current_batch_size += file_size;

        if current_batch_size > stream_segment_size() || idx >= small_files.len() - 1 {
            file_batches.push(current_batch.clone());

            current_batch.clear();
            total_size += current_batch_size;
            current_batch_size = 0;
        }
    }

    // Create a client for uploading batches
    let client = Arc::new(api::client::new_for_remote_repo(remote_repo)?);

    // For individual files
    let err_files: Arc<Mutex<Vec<ErrorFileInfo>>> = Arc::new(Mutex::new(vec![]));

    // For operations as a whole
    let errors = Arc::new(Mutex::new(Vec::new()));

    let worker_count = concurrency::num_threads_for_items(file_batches.len());
    let (tx, rx) = mpsc::channel(worker_count);

    let progress = Arc::new(PushProgress::new_with_totals(
        small_files.len() as u64,
        total_size,
    ));

    let producer_errors = Arc::clone(&errors);

    let head_commit_local_repo_maybe_clone = head_commit_local_repo_maybe.clone();

    // Initiate the producer
    let producer_handle = tokio::spawn(async move {
        stream::iter(file_batches)
            .for_each_concurrent(worker_count, {
                let head_commit_local_repo_maybe_clone = head_commit_local_repo_maybe_clone.clone();
                move |batch| {
                    let base_or_repo_path_clone = base_or_repo_path.clone();
                    let head_commit_local_repo_maybe_clone =
                        head_commit_local_repo_maybe_clone.clone();
                    let errors = Arc::clone(&producer_errors);
                    let tx_clone = tx.clone();

                    async move {
                        let base_or_repo_path_clone = base_or_repo_path_clone.clone();
                        let head_commit_local_repo_maybe_clone =
                            head_commit_local_repo_maybe_clone.clone();

                        let result: Result<(), OxenError> = async move {
                            let mut batch_size = 0;
                            let mut batch_parts = Vec::new();
                            let mut files_to_stage = Vec::new();

                            // Build the multiparts for each file
                            log::debug!(
                                "Starting file processing loop with {:?} files",
                                batch.len()
                            );
                            for (path, size) in batch {
                                let relative_path = util::fs::path_relative_to_dir(
                                    &path,
                                    &base_or_repo_path_clone,
                                )?;

                                // In remote-mode repos, skip adding files already present in
                                // the tree and unmodified. Done here in async context (rather
                                // than inside `spawn_blocking` below) so the mtime-tolerance
                                // comparison can `.await`.
                                if let Some((ref head_commit, ref local_repository)) =
                                    head_commit_local_repo_maybe_clone
                                    && let Some(file_node) = repositories::tree::get_file_by_path(
                                        local_repository,
                                        head_commit,
                                        &relative_path,
                                    )?
                                    && !local_repository
                                        .is_modified_from_node(&path, &file_node)
                                        .await?
                                {
                                    log::debug!("Skipping add on unmodified path {path:?}");
                                    continue;
                                }

                                let file_data_maybe = tokio::task::spawn_blocking(move || {
                                    // When preserve_paths is set or in remote-mode repos, use the
                                    // full relative path. Otherwise use just the filename. The
                                    // server reads this as the staging path and computes the
                                    // content hash itself.
                                    let staging_path = if keep_relative_paths {
                                        relative_path
                                    } else {
                                        let file_name =
                                            relative_path.file_name().ok_or_else(|| {
                                                OxenError::internal_error(format!(
                                                    "Invalid file path '{path:?}': no file name"
                                                ))
                                            })?;
                                        PathBuf::from(file_name)
                                    };

                                    let file = std::fs::read(&path).map_err(|e| {
                                        OxenError::internal_error(format!(
                                            "Failed to read file '{path:?}': {e}"
                                        ))
                                    })?;

                                    let file_part = build_gzip_staging_part(&file, &staging_path)?;

                                    Ok::<_, OxenError>(Some((file_part, path, staging_path, size)))
                                })
                                .await??;

                                let (file_part, disk_path, staging_path, file_size) =
                                    match file_data_maybe {
                                        Some(data) => data,
                                        None => continue,
                                    };

                                batch_parts.push(file_part);
                                files_to_stage.push(FileToStage {
                                    disk_path,
                                    staging_path,
                                });

                                batch_size += file_size;
                            }

                            // Once all the files in the batch are processed,
                            // Send them to the receiver for upload
                            let processed_batch = GzippedBatch {
                                batch_parts,
                                files_to_stage,
                                batch_size,
                            };
                            match tx_clone.send(processed_batch).await {
                                Ok(_) => Ok(()),
                                Err(e) => Err(OxenError::basic_str(format!("{e:?}"))),
                            }
                        }
                        .await;

                        if let Err(e) = result {
                            errors.lock().push(OxenError::basic_str(format!("{e:?}")));
                        }
                    }
                }
            })
            .await;
    });

    let client_clone = client.clone();
    let workspace_id_clone = workspace_id.clone();
    let remote_repo_clone = remote_repo.clone();
    let directory_clone = directory.clone();

    let consumer_err_files = Arc::clone(&err_files);
    let consumer_errors = Arc::clone(&errors);
    let progress_clone = Arc::clone(&progress);

    // Initiate the receiver
    let consumer_handle = tokio::spawn(async move {
        let rx_stream = ReceiverStream::new(rx);
        rx_stream
            .for_each_concurrent(worker_count, |processed_batch| {
                let client_clone = client_clone.clone();
                let remote_repo_clone = remote_repo_clone.clone();
                let workspace_id_clone = workspace_id_clone.clone();
                let directory_str = directory_clone.clone();

                let err_files_clone = Arc::clone(&consumer_err_files);
                let errors = Arc::clone(&consumer_errors);
                let bar = Arc::clone(&progress_clone);

                async move {
                    let result: Result<(), OxenError> = async move {
                        let GzippedBatch {
                            batch_parts,
                            files_to_stage,
                            batch_size,
                        } = processed_batch;
                        let num_entries = batch_parts.len();

                        // Stage the batch in one multipart request to the workspace files
                        // endpoint, which hashes the content, stores it, and stages it with
                        // server-computed metadata.
                        let mut form = reqwest::multipart::Form::new();
                        for part in batch_parts {
                            form = form.part("file[]", part);
                        }

                        let staging_err_files = stage_multipart_batch_to_workspace_with_retry(
                            &remote_repo_clone,
                            client_clone,
                            &workspace_id_clone,
                            &directory_str,
                            form,
                            files_to_stage,
                        )
                        .await?;

                        bar.add_bytes(batch_size);
                        bar.add_files(num_entries as u64);

                        if !staging_err_files.is_empty() {
                            err_files_clone.lock().extend(staging_err_files);
                        }

                        Ok(())
                    }
                    .await;

                    if let Err(e) = result {
                        errors.lock().push(OxenError::basic_str(format!("{e:?}")));
                    }
                }
            })
            .await;
    });

    // Join the tasks and run to completion
    tokio::try_join!(producer_handle, consumer_handle)?;

    // Get the err_files from both processes
    let mutex = match Arc::try_unwrap(err_files) {
        Ok(mutex) => mutex,
        Err(e) => {
            let err = format!("Couldn't acquire mutex guard for err_files: {e:?}");
            log::error!("{err}");
            return Err(OxenError::basic_str(&err));
        }
    };

    let err_files = mutex.into_inner();

    // Check for fatal operational errors (channel failures, compression errors, etc.)
    let operational_errors = match Arc::try_unwrap(errors) {
        Ok(mutex) => mutex.into_inner(),
        Err(e) => {
            let err = format!("Couldn't acquire mutex guard for errors: {e:?}");
            log::error!("{err}");
            return Err(OxenError::basic_str(&err));
        }
    };

    log::debug!("All upload tasks completed");
    progress.finish();

    if !operational_errors.is_empty() {
        log::error!(
            "Encountered {} fatal error(s) during upload",
            operational_errors.len()
        );
        // Return the first fatal error — these indicate batch-level failures
        // (e.g. channel send, staging) that aren't captured per-file in err_files.
        return Err(operational_errors.into_iter().next().unwrap());
    }

    if !err_files.is_empty() {
        log::error!("Failed to upload {} files after retry", err_files.len());
        Ok(err_files)
    } else {
        Ok(vec![])
    }
}

/// A file to stage via the multipart workspace files endpoint.
#[derive(Clone, Debug)]
struct FileToStage {
    /// Local path to re-read the file from on retry.
    disk_path: PathBuf,
    /// Path relative to the request directory, sent as the multipart filename.
    staging_path: PathBuf,
}

/// A gzipped batch ready to upload: the pre-gzipped multipart parts, the files they were built from
/// (kept to reconcile what staged and to rebuild the form on retry), and the batch's total byte
/// size.
struct GzippedBatch {
    batch_parts: Vec<reqwest::multipart::Part>,
    files_to_stage: Vec<FileToStage>,
    batch_size: u64,
}

/// The multipart filename for a file's staging path: forward-slashed so subdirectories round-trip
/// regardless of the client's platform. This is the exact token sent over the wire and the input
/// the server normalizes, so [`expected_staged_path`] derives the reconciliation key from it too —
/// keep the two in lockstep.
fn staging_filename(staging_path: &Path) -> String {
    util::fs::linux_path_str(&staging_path.to_string_lossy())
}

/// The path the workspace `files` endpoint stages a file at (and echoes back in its response),
/// given the request `directory` and the file's staging path. Used to reconcile which files staged.
/// Delegates to [`util::fs::workspace_staged_path`], the single source of truth the server `add`
/// handler also stages through, so the two can't drift across path normalization.
fn expected_staged_path(directory: &str, staging_path: &Path) -> Result<PathBuf, OxenError> {
    let filename = staging_filename(staging_path);
    util::fs::workspace_staged_path(directory, Path::new(&filename))
}

/// Build a gzipped multipart part whose filename is the staging path. The server reads the filename
/// as the destination path (relative to the request directory) and computes the content hash itself.
fn build_gzip_staging_part(
    bytes: &[u8],
    staging_path: &Path,
) -> Result<reqwest::multipart::Part, OxenError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut encoder, bytes).map_err(|e| {
        OxenError::internal_error(format!("Failed to gzip '{}': {e}", staging_path.display()))
    })?;
    let compressed = encoder.finish().map_err(|e| {
        OxenError::internal_error(format!(
            "Failed to finish gzip for '{}': {e}",
            staging_path.display()
        ))
    })?;
    Ok(reqwest::multipart::Part::bytes(compressed)
        .file_name(staging_filename(staging_path))
        .mime_str("application/gzip")?)
}

/// Rebuild a multipart form for the workspace files endpoint by reading and gzipping each file
/// fresh from disk. Used to retry the files that have not staged yet.
fn build_workspace_files_form(
    files_to_stage: &[FileToStage],
) -> Result<reqwest::multipart::Form, OxenError> {
    let mut form = reqwest::multipart::Form::new();
    for file in files_to_stage {
        let bytes = std::fs::read(&file.disk_path).map_err(|e| {
            OxenError::internal_error(format!("Failed to read file '{:?}': {e}", file.disk_path))
        })?;
        form = form.part(
            "file[]",
            build_gzip_staging_part(&bytes, &file.staging_path)?,
        );
    }
    Ok(form)
}

/// POST a multipart form to the workspace files endpoint and return the staging paths the server
/// reports as staged (each is the full path: `directory` joined with the file's staging path).
async fn post_workspace_files_batch(
    client: &reqwest::Client,
    url: &str,
    form: reqwest::multipart::Form,
) -> Result<Vec<PathBuf>, OxenError> {
    let response = client.post(url).multipart(form).send().await?;
    let body = client::parse_json_body(url, response).await?;
    let response: FilePathsResponse = serde_json::from_str(&body)?;
    Ok(response.paths)
}

/// Return the files the server did NOT stage, comparing each against [`expected_staged_path`]
/// (the same normalize-then-join the server applies) rather than the raw `directory.join(staging)`,
/// so a file that staged under a normalized name isn't misread as failed.
fn unstaged_files(
    files: Vec<FileToStage>,
    staged: &[PathBuf],
    directory: &str,
) -> Vec<FileToStage> {
    let staged: HashSet<&PathBuf> = staged.iter().collect();
    files
        .into_iter()
        .filter(
            |file| match expected_staged_path(directory, &file.staging_path) {
                Ok(expected) => !staged.contains(&expected),
                // If the staging path won't normalize, the server would have rejected it too; there is
                // no path it could have staged at, so treat it as unstaged.
                Err(_) => true,
            },
        )
        .collect()
}

/// Upload and stage a batch of files into a workspace via the multipart files endpoint, which
/// hashes the content, stores it, and stages it with server-computed metadata. `form` is the
/// pre-gzipped batch for the first attempt; `files_to_stage` reconciles what staged and rebuilds the
/// form on retry. Returns per-file errors for anything still unstaged after the request succeeds;
/// returns Err only on a persistent transport failure (aborting the operation).
async fn stage_multipart_batch_to_workspace_with_retry(
    remote_repo: &RemoteRepository,
    client: Arc<reqwest::Client>,
    workspace_id: &str,
    directory: &str,
    form: reqwest::multipart::Form,
    files_to_stage: Vec<FileToStage>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let uri = format!("/workspaces/{workspace_id}/files/{directory}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let max_retries = max_retries();

    // First attempt uses the pre-gzipped form built by the producer.
    let mut transport_error = None;
    let mut remaining = match post_workspace_files_batch(&client, &url, form).await {
        Ok(staged) => unstaged_files(files_to_stage, &staged, directory),
        Err(e) => {
            log::error!("Failed to stage multipart batch to workspace: {e}");
            transport_error = Some(e);
            files_to_stage
        }
    };

    let mut retry_count: usize = 1;
    while !remaining.is_empty() && retry_count < max_retries {
        retry_count += 1;
        let wait_time = exponential_backoff(BASE_WAIT_TIME, retry_count, MAX_WAIT_TIME);
        sleep(Duration::from_millis(wait_time as u64)).await;

        // Re-reading and gzipping from disk is blocking FS + CPU work, so keep it off the runtime.
        let files_for_form = remaining.clone();
        let form = tokio::task::spawn_blocking(move || build_workspace_files_form(&files_for_form))
            .await??;
        remaining = match post_workspace_files_batch(&client, &url, form).await {
            Ok(staged) => {
                transport_error = None;
                unstaged_files(remaining, &staged, directory)
            }
            Err(e) => {
                log::error!(
                    "Failed to stage multipart batch to workspace (retry {retry_count}): {e}"
                );
                transport_error = Some(e);
                remaining
            }
        };
    }

    // A persistent transport failure aborts the operation, matching the prior staging behavior.
    if let Some(e) = transport_error
        && !remaining.is_empty()
    {
        return Err(e);
    }

    // Files the server accepted the request for but did not stage are per-file failures. The server
    // computes the hash, so it is unknown to the client here; the staging path identifies the file.
    Ok(remaining
        .into_iter()
        .map(|file| ErrorFileInfo {
            hash: String::new(),
            path: Some(file.staging_path),
            error: "File was not staged by the workspace files endpoint".to_string(),
        })
        .collect())
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
        util::glob::parse_glob_paths(&glob_opts, Some(local_repo)).await?;

    // Convert to relative paths
    let repo_path = &local_repo.path;
    let expanded_paths: Vec<PathBuf> = expanded_paths
        .iter()
        .map(|p| util::fs::path_relative_to_dir(p, repo_path).unwrap())
        .collect();

    let uri = format!("/workspaces/{workspace_id}/files");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_files: {url}");
    let client = client::new_for_url(&url)?;
    let response = client.delete(&url).json(&expanded_paths).send().await?;

    if response.status().is_success() {
        let _body = client::parse_json_body(&url, response).await?;
        println!("🐂 oxen staged paths {paths:?} as removed in workspace {workspace_id}");

        if local_repo.is_remote_mode() {
            // Remove files locally if we're in remote mode
            for path in expanded_paths {
                let full_path = local_repo.path.join(&path);
                if full_path.is_dir() {
                    util::fs::remove_dir_all(&full_path)?;
                }

                if full_path.is_file() {
                    util::fs::remove_file(&full_path)?;
                }
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
            let Some(dir_node) = repositories::tree::get_dir_with_children(
                local_repo,
                head_commit,
                parent_path,
                None,
            )?
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

/// Move or rename a file within a workspace.
/// Sends a PATCH request to update the file's path.
pub async fn mv(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
) -> Result<view::StatusMessage, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();

    let uri = format!("/workspaces/{workspace_id}/files/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(&serde_json::json!({
        "new_path": new_path.as_ref().to_string_lossy()
    }))?;

    let client = client::new_for_url(&url)?;
    let res = client.patch(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<view::StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err = format!(
                "api::workspaces::files::mv error parsing from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
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
        let output_dir = output_path.parent().unwrap_or_else(|| Path::new(""));

        if !output_dir.exists() {
            util::fs::create_dir_all(output_dir)?;
        }

        let mut file = tokio::fs::File::create(&output_path).await?;
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
    } else {
        let status = response.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(OxenError::path_does_not_exist(path));
        }

        log::error!("api::client::workspace::files::download failed with status: {status}");
        let body = client::parse_json_body(&url, response).await?;
        return Err(OxenError::basic_str(format!(
            "Error: Could not download file {body:?}"
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

fn jitter() -> usize {
    thread_rng().gen_range(0..=500)
}

#[cfg(test)]
mod tests {

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{EntryDataType, NewCommitBody, RemoteRepository};
    use crate::opts::CloneOpts;
    use crate::opts::fetch_opts::FetchOpts;
    use crate::view::workspaces::WorkspaceResponseWithStatus;
    use crate::{api, constants};
    use crate::{repositories, test};
    use std::path::PathBuf;

    use std::path::Path;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;
    use uuid;

    #[test]
    fn test_expected_staged_path_mirrors_server_normalization() {
        // The reconciliation key must match what the server stages at and echoes back:
        // directory.join(validate_and_normalize_path(<forward-slashed filename>)).
        assert_eq!(
            super::expected_staged_path("data", Path::new("img.jpg")).unwrap(),
            PathBuf::from("data/img.jpg"),
        );
        // Subdirectory preserved.
        assert_eq!(
            super::expected_staged_path("data", Path::new("train/img.jpg")).unwrap(),
            PathBuf::from("data/train/img.jpg"),
        );
        // `.` components are normalized away server-side; a raw directory.join(staging_path) would
        // have produced "data/train/./img.jpg" and failed to reconcile.
        assert_eq!(
            super::expected_staged_path("data", Path::new("train/./img.jpg")).unwrap(),
            PathBuf::from("data/train/img.jpg"),
        );
        // A backslash in the name (a Windows client's separator, a literal on this unix test box)
        // is forward-slashed before normalization, matching the server's view.
        assert_eq!(
            super::expected_staged_path("data", Path::new("train\\img.jpg")).unwrap(),
            PathBuf::from("data/train/img.jpg"),
        );
        // The CLI's default directory is ".", which the server collapses when it normalizes the
        // joined path. The client must collapse it too, or a successfully-staged file reconciles
        // against "./img.jpg" while the server reports "img.jpg".
        assert_eq!(
            super::expected_staged_path(".", Path::new("img.jpg")).unwrap(),
            PathBuf::from("img.jpg"),
        );
        assert_eq!(
            super::expected_staged_path(".", Path::new("train/img.jpg")).unwrap(),
            PathBuf::from("train/img.jpg"),
        );
    }

    #[test]
    fn test_unstaged_files_recognizes_server_normalized_paths() {
        let files = vec![
            super::FileToStage {
                disk_path: PathBuf::from("/tmp/a"),
                staging_path: PathBuf::from("img.jpg"),
            },
            super::FileToStage {
                disk_path: PathBuf::from("/tmp/b"),
                staging_path: PathBuf::from("train/img.jpg"),
            },
            super::FileToStage {
                disk_path: PathBuf::from("/tmp/c"),
                staging_path: PathBuf::from("train/./nested.jpg"),
            },
        ];
        // What the server reports as staged: directory-joined and normalized.
        let staged = vec![
            PathBuf::from("data/img.jpg"),
            PathBuf::from("data/train/img.jpg"),
            PathBuf::from("data/train/nested.jpg"),
        ];
        let remaining = super::unstaged_files(files, &staged, "data");
        assert!(
            remaining.is_empty(),
            "all staged files should reconcile, got unstaged: {remaining:?}"
        );
    }

    #[test]
    fn test_unstaged_files_reconciles_dot_directory() {
        // Regression for the "added 0 entries ... failed to upload 1 entries" bug: `oxen workspace
        // add` stages with the default directory ".", the server collapses it and echoes back
        // "logo.png", and the client must reconcile that rather than reporting a false failure.
        let files = vec![super::FileToStage {
            disk_path: PathBuf::from("/tmp/logo.png"),
            staging_path: PathBuf::from("logo.png"),
        }];
        let staged = vec![PathBuf::from("logo.png")];
        let remaining = super::unstaged_files(files, &staged, ".");
        assert!(
            remaining.is_empty(),
            "a file staged under a '.' directory must reconcile, got unstaged: {remaining:?}"
        );
    }

    #[test]
    fn test_unstaged_files_reports_files_the_server_skipped() {
        let files = vec![
            super::FileToStage {
                disk_path: PathBuf::from("/tmp/a"),
                staging_path: PathBuf::from("kept.jpg"),
            },
            super::FileToStage {
                disk_path: PathBuf::from("/tmp/b"),
                staging_path: PathBuf::from("train/dropped.jpg"),
            },
        ];
        // Server staged only the first file.
        let staged = vec![PathBuf::from("data/kept.jpg")];
        let remaining = super::unstaged_files(files, &staged, "data");
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining[0].staging_path,
            PathBuf::from("train/dropped.jpg")
        );
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_stage_single_file() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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
            let result = result.unwrap();
            assert!(result.is_empty(), "{:?}", result);

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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_stage_large_file() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_stage_multiple_files() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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
            let result = api::client::workspaces::files::add(
                &remote_repo,
                &workspace_id,
                directory_name,
                paths,
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
            assert_eq!(entries.added_files.entries.len(), 2);
            assert_eq!(entries.added_files.total_entries, 2);

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_create_remote_readme_repo_and_commit_multiple_data_frames()
    -> Result<(), OxenError> {
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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_commit_staged_single_file_and_pull() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_commit_schema_on_branch() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "test-schema-issues";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let original_schemas = api::client::schemas::list(&remote_repo, branch_name).await?;
            let original_root_counts =
                api::client::dir::file_counts(&remote_repo, branch_name, "").await?;
            let count_of = |fc: &crate::model::metadata::MetadataDir, dt: &str| {
                fc.dir
                    .data_types
                    .iter()
                    .find(|d| d.data_type == dt)
                    .map(|d| d.count)
                    .unwrap_or(0)
            };
            let original_image_count = count_of(&original_root_counts, "image");
            let original_tabular_count = count_of(&original_root_counts, "tabular");

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

            // The freshly created subdirectory contains exactly the 2 files we just uploaded.
            let file_counts =
                api::client::dir::file_counts(&remote_repo, branch_name, directory_name).await?;
            assert_eq!(file_counts.dir.data_types.len(), 2);
            assert_eq!(count_of(&file_counts, "image"), 1);
            assert_eq!(count_of(&file_counts, "tabular"), 1);

            // The root counts pick up the same 2 files on top of whatever the seed already had.
            let file_counts = api::client::dir::file_counts(&remote_repo, branch_name, "").await?;
            assert_eq!(count_of(&file_counts, "image"), original_image_count + 1);
            assert_eq!(
                count_of(&file_counts, "tabular"),
                original_tabular_count + 1
            );

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_rm_file() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_stage_file_in_multiple_subdirectories() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_multiple_files() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_file_under_dot_directory_reports_no_failure() -> Result<(), OxenError> {
        // `oxen workspace add` stages under the default "." directory, which the server collapses
        // when it normalizes the joined path. The client must reconcile against the same collapsed
        // path or a staged file reads as failed. The load-bearing assertion is that the returned
        // failure list is empty -- the sibling add tests only check is_ok(), which stays true even
        // when every file is wrongly reported as failed.
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "add-under-dot-directory";
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

            // local_repo None mirrors the CLI: the staging path is the bare filename, sent under ".".
            let failed = api::client::workspaces::files::add(
                &remote_repo,
                &workspace_id,
                ".",
                vec![test::test_img_file()],
                &None,
            )
            .await?;
            assert!(
                failed.is_empty(),
                "a file staged under the default '.' directory must not read as failed, got: {failed:?}"
            );

            // Confirm the file actually staged at the workspace root.
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                Path::new(""),
                constants::DEFAULT_PAGE_NUM,
                constants::DEFAULT_PAGE_SIZE,
            )
            .await?;
            assert_eq!(entries.added_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_file_with_absolute_path() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
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
            let path = crate::util::fs::canonicalize(test::test_img_file())?;
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
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_download_version_file_from_workspace() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let branch_name = constants::DEFAULT_BRANCH_NAME;

            let workspace_id = uuid::Uuid::new_v4().to_string();
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let bounding_box_path = PathBuf::from("README.md");

            // Create a temporary directory for the output file
            let temp_dir = TempDir::new()?;
            let output_path = temp_dir.path().join("output.md");

            // Download the bounding box from the base repo to a new path
            api::client::workspaces::files::download(
                &remote_repo,
                &workspace_id,
                bounding_box_path.to_str().unwrap(),
                Some(&output_path),
            )
            .await?;

            assert!(output_path.exists());

            // TempDir will automatically clean up when it goes out of scope
            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_mv_file() -> Result<(), OxenError> {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_all_data_pushed(|remote_repo| async move {
            let branch_name = "mv-file-test";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = uuid::Uuid::new_v4().to_string();
            let workspace =
                api::client::workspaces::create(&remote_repo, branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // Use an image file that already exists in the repo (non-tabular to test files::mv)
            let original_path = "train/dog_1.jpg";
            let new_path = "renamed/images/dog_1_moved.jpg";

            // Move/rename the file
            let mv_response = api::client::workspaces::files::mv(
                &remote_repo,
                &workspace_id,
                original_path,
                new_path,
            )
            .await?;
            assert_eq!(mv_response.status, "success");

            // Commit the changes
            let body = NewCommitBody {
                message: "Moved file to new location".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::client::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await?;

            // Verify the file exists at the new path after commit
            let new_file =
                api::client::entries::get_entry(&remote_repo, new_path, &commit.id).await?;
            assert!(new_file.is_some(), "File should exist at new path");

            // Verify the actual file content is accessible at the new path
            let mut stream = api::client::file::get_file(
                &remote_repo,
                branch_name,
                Path::new(new_path),
                api::client::file::GetFileOpts::default(),
            )
            .await?;
            let mut file_bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                file_bytes.extend_from_slice(&chunk?);
            }
            assert!(
                !file_bytes.is_empty(),
                "File content should not be empty at new path"
            );

            // Verify the original path no longer exists
            let old_file =
                api::client::entries::get_entry(&remote_repo, original_path, &commit.id).await?;
            assert!(old_file.is_none(), "File should not exist at original path");

            Ok(remote_repo)
        })
        .await
    }

    // Test that downloading a non-existent file returns OxenError::ResourceNotFound
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_download_file_from_nonexistent_workspace() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let non_existent_workspace_id = "workspace_does_not_exist";

            // Verify the workspace doesn't exist
            let workspace =
                api::client::workspaces::get(&remote_repo, non_existent_workspace_id).await?;
            assert!(workspace.is_none());

            // Try to download a file from the non-existent workspace
            let temp_dir = TempDir::new()?;
            let output_path = temp_dir.path().join("output.md");

            let result = api::client::workspaces::files::download(
                &remote_repo,
                non_existent_workspace_id,
                "README.md",
                Some(&output_path),
            )
            .await;

            assert!(result.is_err());
            assert!(!output_path.exists());

            Ok(remote_repo)
        })
        .await
    }

    async fn make_workspace(
        remote_repo: &RemoteRepository,
    ) -> Result<WorkspaceResponseWithStatus, OxenError> {
        let workspace_id = uuid::Uuid::new_v4().to_string();
        let workspace = api::client::workspaces::create(
            remote_repo,
            &constants::DEFAULT_BRANCH_NAME,
            &workspace_id,
        )
        .await?;
        assert_eq!(
            workspace.id, workspace_id,
            "Expected to create workspace with ID {} but got ID {}",
            workspace_id, workspace.id
        );
        Ok(workspace)
    }

    // Test that downloading a file uploaded to the workspace works
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_download_uploaded_file_from_workspace() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let workspace_id = make_workspace(&remote_repo).await?.id;
            let temp_dir = TempDir::new()?;

            let test_filename = "file_to_upload.txt";
            let test_file = {
                let p = temp_dir.path().join(test_filename);
                tokio::fs::write(&p, b"Hello world! How are you today?").await?;
                p
            };

            let upload_path = {
                let upload_path = "images";
                api::client::workspaces::files::upload_single_file(
                    &remote_repo,
                    &workspace_id,
                    upload_path,
                    &test_file,
                )
                .await?;
                upload_path
            };

            let output_path = {
                let output_path = temp_dir.path().join("downloaded.jpeg");
                let file_path = format!("{upload_path}/{test_filename}");
                api::client::workspaces::files::download(
                    &remote_repo,
                    &workspace_id,
                    &file_path,
                    Some(&output_path),
                )
                .await?;
                assert!(
                    output_path.exists(),
                    "Expecting to have downloaded file to: {}",
                    output_path.display()
                );
                output_path
            };

            let downloaded_contents = tokio::fs::read_to_string(&output_path).await?;
            assert_eq!(downloaded_contents, "Hello world! How are you today?");

            Ok(remote_repo)
        })
        .await
    }

    // Test the fallback path: download from commit when file not in workspace
    // This tests the download_entry function which is used as fallback in CLI
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_download_entry_fallback_for_committed_file() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let workspace = make_workspace(&remote_repo).await?;
            let temp_dir = TempDir::new()?;

            // Download the README.md (which is in the commit, not uploaded to workspace)
            // using download_entry (the fallback path in CLI)
            let output_path = {
                let output_path = temp_dir.path().join("fallback_readme.md");
                api::client::entries::download_entry(
                    &remote_repo,
                    Path::new("README.md"),
                    &output_path,
                    &workspace.commit.id,
                )
                .await?;
                assert!(
                    output_path.exists(),
                    "Expecting to have downloaded output to: {}",
                    output_path.display()
                );
                output_path
            };

            let content = std::fs::read_to_string(&output_path)?;
            assert!(!content.is_empty(), "Expecting non-empty README.md file");

            Ok(remote_repo)
        })
        .await
    }

    // Test workspace lookup by name for download scenario
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_workspace_lookup_by_name_for_download() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let workspace_name = "my-download-workspace";
            let workspace = {
                let workspace_id = uuid::Uuid::new_v4().to_string();
                let workspace = api::client::workspaces::create_with_name(
                    &remote_repo,
                    &constants::DEFAULT_BRANCH_NAME,
                    &workspace_id,
                    workspace_name,
                )
                .await?;
                assert_eq!(workspace.id, workspace_id);
                assert_eq!(workspace.name, Some(workspace_name.to_string()));
                workspace
            };

            // Look up workspace by name (as CLI does)
            let found_workspace =
                api::client::workspaces::get_by_name(&remote_repo, workspace_name).await?;
            assert!(found_workspace.is_some());
            let found_workspace = found_workspace.unwrap();
            assert_eq!(found_workspace.id, workspace.id);

            let temp_dir = TempDir::new()?;
            let output_path = temp_dir.path().join("readme_by_name.md");

            api::client::workspaces::files::download(
                &remote_repo,
                &found_workspace.id,
                "README.md",
                Some(&output_path),
            )
            .await?;

            assert!(output_path.exists());

            Ok(remote_repo)
        })
        .await
    }

    // Test that workspace lookup by non-existent name returns None
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_workspace_lookup_by_nonexistent_name() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let non_existent_name = "workspace_name_does_not_exist";

            let workspace =
                api::client::workspaces::get_by_name(&remote_repo, non_existent_name).await?;
            assert!(workspace.is_none());

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_download_nonexistent_file_returns_path_dne() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let workspace_id = make_workspace(&remote_repo).await?.id;
            let temp_dir = TempDir::new()?;
            let output_path = temp_dir.path().join("output.txt");

            let result = api::client::workspaces::files::download(
                &remote_repo,
                &workspace_id,
                "this_file_does_not_exist.txt",
                Some(&output_path),
            )
            .await;

            assert!(result.is_err(), "Expected error for nonexistent file");
            let err = result.unwrap_err();
            assert!(
                matches!(err, OxenError::PathDoesNotExist(_)),
                "Expected PathDoesNotExist error, got: {err:?}"
            );
            assert!(
                !output_path.exists(),
                "Not expecting '{}' to exist",
                output_path.display()
            );

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_files_preserves_paths_local_repo_relative_paths() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            help_test_add_files_preserve_path(&remote_repo, &local_repo.path, true).await?;
            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_files_preserves_paths_local_repo_absolute_paths() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            help_test_add_files_preserve_path(
                &remote_repo,
                &std::path::absolute(local_repo.path).unwrap(),
                false,
            )
            .await?;
            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_files_preserves_paths_tempdir_relative_paths() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_, remote_repo| async move {
            let base_dir_guard = tempfile::tempdir()?;
            let base_dir = base_dir_guard.path().to_path_buf();
            help_test_add_files_preserve_path(&remote_repo, &base_dir, true).await?;
            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_files_preserves_paths_tempdir_absolute_paths() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_, remote_repo| async move {
            let base_dir_guard = tempfile::tempdir()?;
            let base_dir = base_dir_guard.path().to_path_buf();
            help_test_add_files_preserve_path(&remote_repo, &base_dir, false).await?;
            Ok(remote_repo)
        })
        .await
    }

    async fn help_test_add_files_preserve_path(
        remote_repo: &RemoteRepository,
        base_dir: &Path,
        use_relative_paths: bool,
    ) -> Result<(), OxenError> {
        let branch_name = "add-files-preserve-paths";
        let branch = api::client::branches::create_from_branch(
            remote_repo,
            branch_name,
            DEFAULT_BRANCH_NAME,
        )
        .await?;
        assert_eq!(branch.name, branch_name);

        let workspace_id = uuid::Uuid::new_v4().to_string();
        let workspace =
            api::client::workspaces::create(remote_repo, branch_name, &workspace_id).await?;
        assert_eq!(workspace.id, workspace_id);

        let sub_dir_1 = base_dir.join("data_being_added");
        let sub_dir_2 = sub_dir_1.join("nested");
        std::fs::create_dir_all(&sub_dir_2)?;

        let file_a = sub_dir_2.join("file_a.txt");
        let file_b = sub_dir_2.join("file_b.txt");
        let file_c = sub_dir_1.join("file_c.txt");
        let file_root = base_dir.join("root_file.txt");

        test::write_txt_file_to_path(&file_a, "content a")?;
        test::write_txt_file_to_path(&file_b, "content b")?;
        test::write_txt_file_to_path(&file_c, "contents c")?;

        // Also create a file at the repo root
        test::write_txt_file_to_path(&file_root, "root content")?;

        // Build paths (mix of absolute and relative)
        let paths: Vec<PathBuf> = {
            let paths = vec![file_a, file_b, file_c, file_root];
            if use_relative_paths {
                paths
                    .into_iter()
                    .map(|p| p.strip_prefix(base_dir).unwrap().to_path_buf())
                    .collect()
            } else {
                paths
                    .into_iter()
                    .map(|p| std::path::absolute(p).unwrap())
                    .collect()
            }
        };

        // Call add_files — should preserve relative paths
        let result =
            api::client::workspaces::files::add_files(remote_repo, &workspace_id, base_dir, paths)
                .await;
        assert!(result.is_ok(), "add_files failed: {result:?}");

        // Verify all 4 files were staged
        let page_num = constants::DEFAULT_PAGE_NUM;
        let page_size = constants::DEFAULT_PAGE_SIZE;
        let entries = api::client::workspaces::changes::list(
            remote_repo,
            &workspace_id,
            Path::new(""),
            page_num,
            page_size,
        )
        .await?;
        assert_eq!(
            entries.added_files.total_entries, 4,
            "Expected 4 staged files, got {}",
            entries.added_files.total_entries
        );

        // Collect the staged filenames and verify paths are preserved
        let staged: Vec<String> = entries
            .added_files
            .entries
            .iter()
            .map(|e| e.filename().to_string())
            .collect();

        for p in [
            format!(
                "data_being_added{}nested{}file_a.txt",
                std::path::MAIN_SEPARATOR_STR,
                std::path::MAIN_SEPARATOR_STR
            ),
            format!(
                "data_being_added{}nested{}file_b.txt",
                std::path::MAIN_SEPARATOR_STR,
                std::path::MAIN_SEPARATOR_STR
            ),
            format!(
                "data_being_added{}file_c.txt",
                std::path::MAIN_SEPARATOR_STR
            ),
            "root_file.txt".to_string(),
        ] {
            assert!(
                staged.contains(&p),
                "Expected '{p}' in staged paths, got: {staged:?}"
            )
        }

        Ok(())
    }
}
