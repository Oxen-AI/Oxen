use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::entries::ResourceVersion;
use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetadata {
    pub size: u64,
    pub data_type: String,
    pub resource: ResourceVersion,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FileMetadataResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub meta: FileMetadata,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FilePathsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub paths: Vec<PathBuf>,
}
#[derive(Deserialize, Serialize, Debug)]
pub struct FilesHashResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(default)]
    pub err_files: Vec<ErrorFileInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorFileInfo {
    pub hash: String,
    pub error: String,
}
