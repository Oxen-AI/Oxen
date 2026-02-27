use crate::error::OxDropError;
use crate::state::{AppState, UploadJob};
use liboxen::api;
use liboxen::model::{NewCommitBody, Remote, RemoteRepository};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tauri::{Emitter, State};

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UploadProgressEvent {
    pub job_id: String,
    pub status: String,
    pub bytes_uploaded: u64,
    pub bytes_total: u64,
    pub files_uploaded: u64,
    pub files_total: u64,
    pub current_file: String,
    pub error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartUploadResult {
    pub job_id: String,
}

const MAX_BATCH_SIZE: usize = 50;
const MIN_PROGRESS_UPDATES: usize = 20;
const BATCH_BYTES: u64 = 100 * 1024 * 1024; // 100MB

/// Pick a batch size that yields ~MIN_PROGRESS_UPDATES updates.
/// For 5 files → 1 (per-file), for 100 files → 5, caps at MAX_BATCH_SIZE.
fn batch_size_for(total_files: usize) -> usize {
    (total_files / MIN_PROGRESS_UPDATES).clamp(1, MAX_BATCH_SIZE)
}

#[tauri::command]
pub async fn start_upload(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    repo: String,
    branch: Option<String>,
    remote_path: String,
    local_paths: Vec<String>,
    message: String,
) -> Result<StartUploadResult, OxDropError> {
    let job_id = uuid::Uuid::new_v4().to_string();
    let cancelled = Arc::new(AtomicBool::new(false));
    let branch = branch.unwrap_or_else(|| "main".to_string());

    let host = state.host.clone();
    let scheme = state.scheme.clone();

    // Store job
    {
        let mut jobs = state.upload_jobs.lock().await;
        jobs.insert(
            job_id.clone(),
            UploadJob {
                id: job_id.clone(),
                cancelled: cancelled.clone(),
                workspace_id: None,
                repo: repo.clone(),
            },
        );
    }

    let job_id_clone = job_id.clone();
    let cancelled_clone = cancelled.clone();

    // Spawn upload task
    tauri::async_runtime::spawn(async move {
        let result = run_upload(
            &app,
            &job_id_clone,
            &cancelled_clone,
            &host,
            &scheme,
            &repo,
            &branch,
            &remote_path,
            &local_paths,
            &message,
        )
        .await;

        if let Err(e) = result {
            let _ = app.emit(
                "upload-progress",
                UploadProgressEvent {
                    job_id: job_id_clone,
                    status: "failed".to_string(),
                    bytes_uploaded: 0,
                    bytes_total: 0,
                    files_uploaded: 0,
                    files_total: 0,
                    current_file: String::new(),
                    error: Some(e.message),
                },
            );
        }
    });

    Ok(StartUploadResult { job_id })
}

async fn run_upload(
    app: &tauri::AppHandle,
    job_id: &str,
    cancelled: &Arc<AtomicBool>,
    host: &str,
    scheme: &str,
    repo: &str,
    branch: &str,
    remote_path: &str,
    local_paths: &[String],
    message: &str,
) -> Result<(), OxDropError> {
    // Parse repo as "namespace/name"
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(OxDropError {
            message: "Invalid repo format. Expected 'namespace/name'.".to_string(),
            code: "INVALID_INPUT".to_string(),
        });
    }

    let url = format!("{}://{}/{}/{}", scheme, host, parts[0], parts[1]);
    let remote = Remote {
        name: "origin".to_string(),
        url,
    };

    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await
        .map_err(OxDropError::from)?
        .ok_or_else(|| OxDropError {
            message: format!("Repository '{}' not found.", repo),
            code: "NOT_FOUND".to_string(),
        })?;

    // Collect all files
    let mut all_files: Vec<PathBuf> = Vec::new();
    for path_str in local_paths {
        let path = PathBuf::from(path_str);
        if path.is_dir() {
            for entry in jwalk::WalkDir::new(&path).into_iter().flatten() {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    all_files.push(entry_path.to_path_buf());
                }
            }
        } else if path.is_file() {
            all_files.push(path);
        }
    }

    let files_total = all_files.len() as u64;
    let bytes_total: u64 = all_files
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();

    // Create workspace
    // NOTE: OxenHub ignores the workspace_id we provide and returns its own.
    // We must use the returned ID for all subsequent operations.
    let requested_id = uuid::Uuid::new_v4().to_string();
    let ws = api::client::workspaces::create(&remote_repo, branch, &requested_id)
        .await
        .map_err(OxDropError::from)?;
    let workspace_id = ws.id;

    // Emit initial progress
    let _ = app.emit(
        "upload-progress",
        UploadProgressEvent {
            job_id: job_id.to_string(),
            status: "uploading".to_string(),
            bytes_uploaded: 0,
            bytes_total,
            files_uploaded: 0,
            files_total,
            current_file: String::new(),
            error: None,
        },
    );

    // Batch and upload files with dynamic batch sizing
    let batch_size = batch_size_for(all_files.len());
    let mut files_uploaded: u64 = 0;
    let mut bytes_uploaded: u64 = 0;
    let mut batch: Vec<PathBuf> = Vec::new();
    let mut batch_bytes: u64 = 0;

    for file in &all_files {
        if cancelled.load(Ordering::Relaxed) {
            // Clean up workspace on cancel
            let _ = api::client::workspaces::delete(&remote_repo, &workspace_id).await;
            let _ = app.emit(
                "upload-progress",
                UploadProgressEvent {
                    job_id: job_id.to_string(),
                    status: "cancelled".to_string(),
                    bytes_uploaded,
                    bytes_total,
                    files_uploaded,
                    files_total,
                    current_file: String::new(),
                    error: None,
                },
            );
            return Ok(());
        }

        let file_size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
        batch.push(file.clone());
        batch_bytes += file_size;

        if batch.len() >= batch_size || batch_bytes >= BATCH_BYTES {
            upload_batch(
                &remote_repo,
                &workspace_id,
                remote_path,
                &batch,
                local_paths,
            )
            .await?;

            files_uploaded += batch.len() as u64;
            bytes_uploaded += batch_bytes;

            let current_file = batch
                .last()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let _ = app.emit(
                "upload-progress",
                UploadProgressEvent {
                    job_id: job_id.to_string(),
                    status: "uploading".to_string(),
                    bytes_uploaded,
                    bytes_total,
                    files_uploaded,
                    files_total,
                    current_file,
                    error: None,
                },
            );

            batch.clear();
            batch_bytes = 0;
        }
    }

    // Upload remaining files
    if !batch.is_empty() {
        upload_batch(
            &remote_repo,
            &workspace_id,
            remote_path,
            &batch,
            local_paths,
        )
        .await?;

        let _ = batch.len();
        let _ = batch_bytes;
    }

    // Emit committing status
    let _ = app.emit(
        "upload-progress",
        UploadProgressEvent {
            job_id: job_id.to_string(),
            status: "committing".to_string(),
            bytes_uploaded: bytes_total,
            bytes_total,
            files_uploaded: files_total,
            files_total,
            current_file: String::new(),
            error: None,
        },
    );

    // Commit workspace
    let user_config = liboxen::config::UserConfig::get_or_create().map_err(OxDropError::from)?;
    let commit_body = NewCommitBody {
        message: message.to_string(),
        author: user_config.name,
        email: user_config.email,
    };

    api::client::workspaces::commits::commit(&remote_repo, branch, &workspace_id, &commit_body)
        .await
        .map_err(OxDropError::from)?;

    // Emit completion
    let _ = app.emit(
        "upload-progress",
        UploadProgressEvent {
            job_id: job_id.to_string(),
            status: "complete".to_string(),
            bytes_uploaded: bytes_total,
            bytes_total,
            files_uploaded: files_total,
            files_total,
            current_file: String::new(),
            error: None,
        },
    );

    Ok(())
}

async fn upload_batch(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    remote_path: &str,
    batch: &[PathBuf],
    _local_paths: &[String],
) -> Result<(), OxDropError> {
    api::client::workspaces::files::add(
        remote_repo,
        workspace_id,
        remote_path,
        batch.to_vec(),
        &None,
    )
    .await
    .map_err(|e| OxDropError {
        message: format!(
            "Failed to upload batch of {} files: {}",
            batch.len(),
            e
        ),
        code: "UPLOAD_ERROR".to_string(),
    })?;

    Ok(())
}

#[tauri::command]
pub async fn cancel_upload(
    state: State<'_, AppState>,
    job_id: String,
) -> Result<(), OxDropError> {
    let jobs = state.upload_jobs.lock().await;
    if let Some(job) = jobs.get(&job_id) {
        job.cancelled.store(true, Ordering::Relaxed);
    }
    Ok(())
}
