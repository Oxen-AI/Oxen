use std::{collections::HashMap, path::PathBuf, time::Duration};

use super::StatusMessage;
use crate::model::merkle_tree::node::file_node::FileNodeOpts;
use crate::model::MerkleHash;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct VersionFile {
    pub hash: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct VersionFileResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub version: VersionFile,
}

#[derive(Clone, Debug)]
pub enum MultipartLargeFileUploadStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Clone)]
pub struct MultipartLargeFileUpload {
    pub local_path: PathBuf,       // Path to the file on the local filesystem
    pub dst_dir: Option<PathBuf>,  // Path to upload the file to on the server
    pub hash: MerkleHash,          // Unique identifier for the file
    pub size: u64,                 // Size of the file in bytes
    pub upload_id: Option<String>, // ID for the S3 multipart upload session
    pub status: MultipartLargeFileUploadStatus, // Status of the upload
    pub reason: Option<String>,    // Reason for the upload failure
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompletedFileUpload {
    pub hash: String,
    pub target_path: PathBuf, // The destination path for the file
    // `upload_results` is all the headers from the chunk uploads
    // so that we can verify the upload results and re-upload
    // the file if there were any failures
    pub upload_results: Vec<HashMap<String, String>>,
    // The upload ID for the S3 multipart upload session
    pub upload_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompleteFileChunk {
    pub offset: Option<u64>,
    pub chunk_number: Option<i32>,
    pub etag: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompleteVersionUploadRequest {
    pub files: Vec<CompletedFileUpload>,
    // If the workspace_id is provided, we will add the file to the workspace
    // otherwise, we will just add the file to the versions store
    pub workspace_id: Option<String>,
    pub file_node_opts: Option<FileNodeOpts>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateVersionUploadRequest {
    pub hash: String,
    pub file_name: String,
    pub size: u64,
    pub dst_dir: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateVersionUploadResponse {
    pub status: StatusMessage,
    pub upload_id: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CleanCorruptedVersionsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub result: CleanCorruptedVersionsResult,
}

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
pub struct CleanCorruptedVersionsResult {
    // Number of version files scanned
    pub scanned: u64,
    // Number of version files that were corrupted(hash mismatch)
    pub corrupted: u64,
    // Number of version files that were successfully deleted
    pub cleaned: u64,
    // Errors that occurred during the cleanup process
    pub errors: u64,
    // Elapsed time in seconds
    pub elapsed: Duration,
}
