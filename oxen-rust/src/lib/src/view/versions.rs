use std::{collections::HashMap, path::PathBuf};

use crate::model::MerkleHash;

use super::StatusMessage;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionFile {
    pub hash: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionFileResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub version: VersionFile,
}

#[derive(Clone)]
pub enum MultipartLargeFileUploadStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Clone)]
pub struct MultipartLargeFileUpload {
    pub local_path: PathBuf,      // Path to the file on the local filesystem
    pub dst_dir: Option<PathBuf>, // Path to upload the file to on the server
    pub hash: MerkleHash,         // Unique identifier for the file
    pub size: u64,                // Size of the file in bytes
    pub status: MultipartLargeFileUploadStatus, // Status of the upload
    pub reason: Option<String>,   // Reason for the upload failure
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompletedFileUpload {
    pub hash: String,
    pub file_name: String,        // The name of the file
    pub dst_dir: Option<PathBuf>, // The destination directory for the file
    // `upload_results` is all the headers from the chunk uploads
    // so that we can verify the upload results and re-upload
    // the file if there were any failures
    pub upload_results: Vec<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompleteVersionUploadRequest {
    pub files: Vec<CompletedFileUpload>,
    // If the workspace_id is provided, we will add the file to the workspace
    // otherwise, we will just add the file to the versions store
    pub workspace_id: Option<String>,
}
