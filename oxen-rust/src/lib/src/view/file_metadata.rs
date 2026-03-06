use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::client::workspaces::files::ErrorFile;
use crate::error::OxenError;

use super::entries::ResourceVersion;
use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct FileMetadata {
    pub size: u64,
    pub data_type: String,
    pub resource: ResourceVersion,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct FileMetadataResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub meta: FileMetadata,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct FilePathsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[schema(value_type = Vec<String>)]
    pub paths: Vec<PathBuf>,
}
#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct ErrorFilesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(default)]
    pub err_files: Vec<ErrorFileInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct ErrorFileInfo {
    pub hash: String,
    #[schema(value_type = Option<String>)]
    pub path: Option<PathBuf>,
    pub error: String,
}

impl From<ErrorFile> for ErrorFileInfo {
    fn from(other: ErrorFile) -> Self {
        ErrorFileInfo {
            hash: other.hash,
            path: other.path,
            error: format!("{}", other.error),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct FileWithHash {
    pub hash: String,
    #[schema(value_type = String)]
    pub path: PathBuf,
}
