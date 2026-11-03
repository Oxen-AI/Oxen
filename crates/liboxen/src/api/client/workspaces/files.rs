use crate::api::client;
use crate::api::client::internal_types::LocalOrBase;
use crate::constants::{max_retries, stream_segment_size};
use crate::core::progress::push_progress::PushProgress;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MerkleHash, RemoteRepository};
use crate::opts::GlobOpts;
use crate::util::{self, concurrency};
use crate::view::versions::WorkspaceDestination;
use crate::view::workspaces::{RenameRequest, ValidateUploadFeasibilityRequest};
use crate::view::{ErrorFileInfo, FilePathsResponse};
use crate::{api, repositories, view};

use futures_util::StreamExt;
use glob_match::glob_match;

use parking_lot::Mutex;
use rand::{RngExt, rng};
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

        let destination = WorkspaceDestination {
            workspace_id: workspace_id.to_string(),
            path: dst_dir.join(path.file_name().unwrap_or_default()),
        };
        let (hash, uploaded) = match util::hasher::u128_hash_file_contents(&path) {
            Ok(hash) => {
                let hash = MerkleHash::new(hash);
                let uploaded = api::client::versions::parallel_large_file_upload(
                    remote_repo,
                    &path,
                    hash,
                    Some(destination),
                    None,
                )
                .await;
                (hash.to_string(), uploaded)
            }
            Err(err) => (String::new(), Err(err)),
        };
        match uploaded {
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

/// Stage a copy of the committed file at `path` at `new_path` within a workspace.
pub async fn copy(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    new_path: &Path,
) -> Result<view::StatusMessage, OxenError> {
    let uri = format!("/workspaces/{workspace_id}/copy/{}", path.to_string_lossy());
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let request = RenameRequest {
        new_path: new_path.to_string_lossy().into_owned(),
    };
    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(&request).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    Ok(serde_json::from_str(&body)?)
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
    rng().random_range(0..=500)
}

#[cfg(test)]
mod tests {
    use crate::api::client::branches::create_from_branch;
    use crate::api::client::commits::get_by_id;
    use crate::api::client::workspaces::{self, changes};
    use crate::api::client::{dir, entries, schemas, versions};
    use crate::model::metadata::MetadataDir;
    use crate::repositories::commits::head_commit;
    use crate::util::fs;
    use crate::util::hasher::u128_hash_file_contents;
    use crate::view::versions::WorkspaceDestination;

    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::error::OxenError;
    use crate::model::{EntryDataType, MerkleHash, NewCommitBody, RemoteRepository};
    use crate::opts::fetch_opts::FetchOpts;
    use crate::repositories;
    use crate::test;
    use crate::view::RemoteStagedStatus;
    use std::path::{Path, PathBuf, absolute};

    use tempfile::TempDir;

    use super::{add, add_files, copy, download, mv, rm, upload_single_file};

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

    fn commit_body(message: &str) -> NewCommitBody {
        NewCommitBody {
            message: message.to_string(),
            author: "Test User".to_string(),
            email: "test@oxen.ai".to_string(),
        }
    }

    async fn staged(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
        directory: &str,
    ) -> Result<RemoteStagedStatus, OxenError> {
        changes::list(
            remote_repo,
            workspace_id,
            Path::new(directory),
            DEFAULT_PAGE_NUM,
            DEFAULT_PAGE_SIZE,
        )
        .await
    }

    async fn tabular_count(
        remote_repo: &RemoteRepository,
        revision: &str,
    ) -> Result<usize, OxenError> {
        let entries =
            entries::list_entries_with_type(remote_repo, "", revision, &EntryDataType::Tabular)
                .await?;
        Ok(entries.len())
    }

    /// Stages files under `base_dir` with `add_files` and checks each keeps its path relative to it.
    async fn check_add_files_preserves_paths(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
        base_dir: &Path,
        use_relative_paths: bool,
    ) -> Result<(), OxenError> {
        workspaces::create(remote_repo, DEFAULT_BRANCH_NAME, workspace_id).await?;
        let sub_dir_1 = base_dir.join("data_being_added");
        let sub_dir_2 = sub_dir_1.join("nested");
        fs::create_dir_all(&sub_dir_2)?;
        let files = [
            (sub_dir_2.join("file_a.txt"), "content a"),
            (sub_dir_2.join("file_b.txt"), "content b"),
            (sub_dir_1.join("file_c.txt"), "contents c"),
            (base_dir.join("root_file.txt"), "root content"),
        ];
        for (path, contents) in &files {
            test::write_txt_file_to_path(path, contents)?;
        }
        let paths: Vec<PathBuf> = files
            .into_iter()
            .map(|(path, _)| match use_relative_paths {
                true => path
                    .strip_prefix(base_dir)
                    .expect("under base_dir")
                    .to_path_buf(),
                false => absolute(path).expect("an absolute path"),
            })
            .collect();
        let result = add_files(remote_repo, workspace_id, base_dir, paths).await;
        assert!(result.is_ok(), "add_files failed: {result:?}");

        let entries = staged(remote_repo, workspace_id, "").await?;
        assert_eq!(entries.added_files.total_entries, 4, "{workspace_id}");
        let staged: Vec<String> = entries
            .added_files
            .entries
            .iter()
            .map(|e| e.filename().to_string())
            .collect();
        for path in [
            Path::new("data_being_added")
                .join("nested")
                .join("file_a.txt"),
            Path::new("data_being_added")
                .join("nested")
                .join("file_b.txt"),
            Path::new("data_being_added").join("file_c.txt"),
            PathBuf::from("root_file.txt"),
        ] {
            let path = path.to_string_lossy().to_string();
            assert!(
                staged.contains(&path),
                "{workspace_id}: expected '{path}' in {staged:?}"
            );
        }
        Ok(())
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_stage_commit_and_download_workspace_files() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let img_name = test::test_img_file()
                .file_name()
                .expect("a name")
                .to_string_lossy()
                .to_string();

            // Stage one image, two images, a file in deep subdirectories, and an absolute path
            for (workspace_id, directory, paths) in [
                ("single", "images", vec![test::test_img_file()]),
                (
                    "multiple",
                    "data",
                    vec![
                        test::test_img_file(),
                        test::test_img_file_with_name("cole_anthony.jpeg"),
                    ],
                ),
                ("deep", "my/images/dir/is/long", vec![test::test_img_file()]),
                (
                    "absolute",
                    "new-images",
                    vec![fs::canonicalize(test::test_img_file())?],
                ),
            ] {
                workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id).await?;
                let count = paths.len();
                let failed = add(&remote_repo, workspace_id, directory, paths, &None).await?;
                assert!(failed.is_empty(), "{workspace_id}: {failed:?}");
                let entries = staged(&remote_repo, workspace_id, directory).await?;
                assert_eq!(entries.added_files.entries.len(), count, "{workspace_id}");
                assert_eq!(entries.added_files.total_entries, count, "{workspace_id}");
                if count == 1 {
                    let expected = Path::new(directory).join(&img_name);
                    assert_eq!(
                        entries.added_files.entries[0].filename(),
                        expected.to_string_lossy()
                    );
                }
            }

            // `oxen workspace add` stages under the default "." directory, which the server
            // collapses: the file must not be reported as failed
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "dot").await?;
            let failed = add(&remote_repo, "dot", ".", vec![test::test_img_file()], &None).await?;
            assert!(
                failed.is_empty(),
                "a file staged under '.' is not a failure, got: {failed:?}"
            );
            assert_eq!(
                staged(&remote_repo, "dot", "")
                    .await?
                    .added_files
                    .total_entries,
                1
            );

            // A chunked upload into .oxen is refused, and a large file stages in chunks
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "large").await?;
            let large = test::test_30k_parquet();
            let refused = versions::parallel_large_file_upload(
                &remote_repo,
                &large,
                MerkleHash::new(u128_hash_file_contents(&large)?),
                Some(WorkspaceDestination {
                    workspace_id: "large".to_string(),
                    path: Path::new(".OXEN").join("large.parquet"),
                }),
                None,
            )
            .await;
            let Err(err) = refused else {
                panic!("a chunked upload into .oxen is refused");
            };
            assert!(err.to_string().contains(".oxen directory"), "{err}");
            add(&remote_repo, "large", "my_large_file", vec![large], &None).await?;
            assert_eq!(
                staged(&remote_repo, "large", "my_large_file")
                    .await?
                    .added_files
                    .total_entries,
                1
            );

            // Removing a staged file unstages it
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "rm").await?;
            let staged_path =
                upload_single_file(&remote_repo, "rm", "images", test::test_img_file()).await?;
            rm(&remote_repo, "rm", staged_path).await?;
            assert_eq!(
                staged(&remote_repo, "rm", "images")
                    .await?
                    .added_files
                    .total_entries,
                0
            );

            // Download committed, staged, and missing files through a workspace
            let workspace = workspaces::create_with_name(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "download_id",
                "download_name",
            )
            .await?;
            let found = workspaces::get_by_name(&remote_repo, "download_name").await?;
            assert_eq!(found.map(|w| w.id), Some(workspace.id.clone()));
            let temp_dir = TempDir::new()?;
            let output = temp_dir.path().join("readme.md");
            download(&remote_repo, "download_id", "README.md", Some(&output)).await?;
            assert_eq!(fs::read_from_path(&output)?, "Hello World");

            let output = temp_dir.path().join("fallback_readme.md");
            entries::download_entry(
                &remote_repo,
                Path::new("README.md"),
                &output,
                &workspace.commit.id,
            )
            .await?;
            assert_eq!(fs::read_from_path(&output)?, "Hello World");

            let upload = temp_dir.path().join("file_to_upload.txt");
            fs::write(&upload, "Hello world! How are you today?")?;
            upload_single_file(&remote_repo, "download_id", "images", &upload).await?;
            let output = temp_dir.path().join("downloaded.txt");
            download(
                &remote_repo,
                "download_id",
                "images/file_to_upload.txt",
                Some(&output),
            )
            .await?;
            assert_eq!(
                fs::read_from_path(&output)?,
                "Hello world! How are you today?"
            );

            let output = temp_dir.path().join("missing.txt");
            let result = download(
                &remote_repo,
                "download_id",
                "this_file_does_not_exist.txt",
                Some(&output),
            )
            .await;
            assert!(
                matches!(result, Err(OxenError::PathDoesNotExist(_))),
                "{result:?}"
            );
            assert!(!output.exists());

            assert!(
                workspaces::get(&remote_repo, "workspace_does_not_exist")
                    .await?
                    .is_none()
            );
            let result = download(
                &remote_repo,
                "workspace_does_not_exist",
                "README.md",
                Some(&output),
            )
            .await;
            assert!(result.is_err());
            assert!(!output.exists());

            // Commit a parquet file, a csv, and an unreadable parquet file one after another
            let branch_name = "data-frames";
            create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            let tabular_before = tabular_count(&remote_repo, branch_name).await?;
            for (directory, file, tabular_after) in [
                ("", test::test_1k_parquet(), tabular_before + 1),
                (
                    "moare_data",
                    test::test_csv_file_with_name("emojis.csv"),
                    tabular_before + 2,
                ),
                (
                    "broken_data",
                    test::test_invalid_parquet_file(),
                    tabular_before + 2,
                ),
            ] {
                workspaces::create(&remote_repo, branch_name, "data_frames").await?;
                upload_single_file(&remote_repo, "data_frames", directory, file).await?;
                let message = format!("Add a data frame to '{directory}'");
                workspaces::commit(
                    &remote_repo,
                    branch_name,
                    "data_frames",
                    &commit_body(&message),
                )
                .await?;
                assert_eq!(
                    tabular_count(&remote_repo, branch_name).await?,
                    tabular_after,
                    "{directory}"
                );
            }

            // Committing an image and a data frame adds a schema and counts both in each directory
            let branch_name = "test-schema-issues";
            create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            let original_schemas = schemas::list(&remote_repo, branch_name).await?;
            let count_of = |fc: &MetadataDir, dt: &str| {
                fc.dir
                    .data_types
                    .iter()
                    .find(|d| d.data_type == dt)
                    .map(|d| d.count)
                    .unwrap_or(0)
            };
            let root = dir::file_counts(&remote_repo, branch_name, "").await?;
            let (root_images, root_tabular) =
                (count_of(&root, "image"), count_of(&root, "tabular"));
            workspaces::create(&remote_repo, branch_name, "schema").await?;
            upload_single_file(&remote_repo, "schema", "tabular", test::test_1k_parquet()).await?;
            upload_single_file(&remote_repo, "schema", "tabular", test::test_img_file()).await?;
            let commit = workspaces::commit(
                &remote_repo,
                branch_name,
                "schema",
                &commit_body("Add one data frame and one image"),
            )
            .await?;
            assert!(commit.message.contains("Add one data frame and one image"));
            let schemas = schemas::list(&remote_repo, branch_name).await?;
            assert_eq!(schemas.len(), original_schemas.len() + 1);
            let counts = dir::file_counts(&remote_repo, branch_name, "tabular").await?;
            assert_eq!(counts.dir.data_types.len(), 2);
            assert_eq!(
                (count_of(&counts, "image"), count_of(&counts, "tabular")),
                (1, 1)
            );
            let root = dir::file_counts(&remote_repo, branch_name, "").await?;
            assert_eq!(
                (count_of(&root, "image"), count_of(&root, "tabular")),
                (root_images + 1, root_tabular + 1)
            );

            // Move a committed file
            let branch_name = "mv-file-test";
            create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            workspaces::create(&remote_repo, branch_name, "mv").await?;
            let (original_path, new_path) = ("README.md", "renamed/docs/README_moved.md");
            assert_eq!(
                mv(&remote_repo, "mv", original_path, new_path)
                    .await?
                    .status,
                "success"
            );
            let commit = workspaces::commit(
                &remote_repo,
                branch_name,
                "mv",
                &commit_body("Moved a file"),
            )
            .await?;
            let new_file = entries::get_entry(&remote_repo, new_path, &commit.id).await?;
            assert!(new_file.is_some(), "File should exist at new path");
            assert_eq!(
                test::file_bytes(&remote_repo, branch_name, Path::new(new_path)).await?,
                b"Hello World"
            );
            let old_file = entries::get_entry(&remote_repo, original_path, &commit.id).await?;
            assert!(old_file.is_none(), "File should not exist at original path");

            // Copy the moved file, in a new workspace on the branch the move committed to
            workspaces::create(&remote_repo, branch_name, "copy").await?;
            let copy_path = Path::new("README_copy.md");
            assert_eq!(
                copy(&remote_repo, "copy", Path::new(new_path), copy_path)
                    .await?
                    .status,
                "success"
            );
            let commit = workspaces::commit(
                &remote_repo,
                branch_name,
                "copy",
                &commit_body("Copied a file"),
            )
            .await?;
            assert_eq!(
                test::file_bytes(&remote_repo, &commit.id, copy_path).await?,
                b"Hello World",
                "the copy has the source's content"
            );
            let source = entries::get_entry(&remote_repo, new_path, &commit.id).await?;
            assert!(source.is_some(), "the copy leaves its source in place");

            // Pull a branch a workspace committed to
            let branch_name = "add-data";
            create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            workspaces::create(&remote_repo, branch_name, "add-data").await?;
            upload_single_file(&remote_repo, "add-data", "data", test::test_img_file()).await?;
            let commit = workspaces::commit(
                &remote_repo,
                branch_name,
                "add-data",
                &commit_body("Add one image"),
            )
            .await?;
            let remote_commit = get_by_id(&remote_repo, &commit.id).await?;
            assert_eq!(remote_commit.map(|c| c.id), Some(commit.id.clone()));
            let pulled_path = local_repo.path.join("data").join(&img_name);
            assert!(!pulled_path.exists());
            let mut fetch_opts = FetchOpts::new();
            fetch_opts.branch = branch_name.to_string();
            repositories::pull_remote_branch(&local_repo, &fetch_opts).await?;
            assert_eq!(head_commit(&local_repo)?.id, commit.id);
            assert!(pulled_path.exists());

            // `add_files` keeps paths relative to the base directory, inside a repo or not, and
            // whether the paths given are relative or absolute
            let temp_dir = TempDir::new()?;
            for (workspace_id, base_dir, use_relative_paths) in [
                ("repo-relative", local_repo.path.clone(), true),
                ("repo-absolute", absolute(&local_repo.path)?, false),
                ("tempdir-relative", temp_dir.path().to_path_buf(), true),
                ("tempdir-absolute", temp_dir.path().to_path_buf(), false),
            ] {
                check_add_files_preserves_paths(
                    &remote_repo,
                    workspace_id,
                    &base_dir,
                    use_relative_paths,
                )
                .await?;
            }

            Ok(remote_repo)
        })
        .await
    }
}
