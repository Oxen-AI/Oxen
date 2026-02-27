use crate::error::OxDropError;
use serde::Serialize;
use std::path::{Path, PathBuf};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileScanResult {
    pub total_files: u64,
    pub total_size: u64,
    pub folder_name: String,
    pub detected_types: Vec<String>,
    pub thumbnail_paths: Vec<String>,
}

#[tauri::command]
pub async fn scan_files(paths: Vec<String>) -> Result<FileScanResult, OxDropError> {
    let mut total_files: u64 = 0;
    let mut total_size: u64 = 0;
    let mut detected_types = std::collections::HashSet::new();
    let mut thumbnail_paths: Vec<String> = Vec::new();
    let mut folder_name = String::new();

    for path_str in &paths {
        let path = PathBuf::from(path_str);
        if path.is_dir() {
            if folder_name.is_empty() {
                folder_name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
            }
            for entry in jwalk::WalkDir::new(&path).into_iter().flatten() {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    total_files += 1;
                    if let Ok(meta) = std::fs::metadata(&entry_path) {
                        total_size += meta.len();
                    }
                    collect_type_and_thumbnail(
                        &entry_path,
                        &mut detected_types,
                        &mut thumbnail_paths,
                    );
                }
            }
        } else if path.is_file() {
            total_files += 1;
            if let Ok(meta) = std::fs::metadata(&path) {
                total_size += meta.len();
            }
            if folder_name.is_empty() {
                folder_name = path
                    .parent()
                    .and_then(|p| p.file_name())
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "files".to_string());
            }
            collect_type_and_thumbnail(&path, &mut detected_types, &mut thumbnail_paths);
        }
    }

    if folder_name.is_empty() {
        folder_name = "files".to_string();
    }

    Ok(FileScanResult {
        total_files,
        total_size,
        folder_name,
        detected_types: detected_types.into_iter().collect(),
        thumbnail_paths,
    })
}

fn collect_type_and_thumbnail(
    path: &Path,
    detected_types: &mut std::collections::HashSet<String>,
    thumbnail_paths: &mut Vec<String>,
) {
    if let Some(kind) = infer::get_from_path(path).ok().flatten() {
        let mime = kind.mime_type().to_string();
        if mime.starts_with("image/") && thumbnail_paths.len() < 8 {
            thumbnail_paths.push(path.to_string_lossy().to_string());
        }
        detected_types.insert(mime);
    } else if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let mime = match ext.to_lowercase().as_str() {
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "svg" => "image/svg+xml",
            "mp4" => "video/mp4",
            "mp3" => "audio/mpeg",
            "wav" => "audio/wav",
            "txt" => "text/plain",
            "csv" => "text/csv",
            "json" => "application/json",
            "parquet" => "application/parquet",
            _ => "application/octet-stream",
        };
        if mime.starts_with("image/") && thumbnail_paths.len() < 8 {
            thumbnail_paths.push(path.to_string_lossy().to_string());
        }
        detected_types.insert(mime.to_string());
    }
}
