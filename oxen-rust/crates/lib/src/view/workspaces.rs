use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use utoipa::ToSchema;

use super::StatusMessage;
use crate::model::Commit;

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct NewWorkspace {
    pub workspace_id: String,
    pub branch_name: String,
    pub resource_path: Option<String>,
    pub entity_type: Option<String>,
    pub name: Option<String>,
    pub force: Option<bool>,
}
// HACK to get this to work with our hub where we don't keep parent_ids 🤦‍♂️
// TODO: it should just be a Commit object
#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct WorkspaceCommit {
    pub id: String,
    pub message: String,
    pub author: String,
    pub email: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}

impl From<WorkspaceCommit> for Commit {
    fn from(val: WorkspaceCommit) -> Self {
        Commit {
            id: val.id,
            message: val.message,
            author: val.author,
            email: val.email,
            timestamp: val.timestamp,
            parent_ids: vec![],
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct WorkspaceResponse {
    pub id: String,
    pub name: Option<String>,
    pub commit: WorkspaceCommit,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WorkspaceResponseWithStatus {
    pub id: String,
    pub name: Option<String>,
    pub commit: WorkspaceCommit,
    pub status: String,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct WorkspaceResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub workspace: WorkspaceResponse,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct ListWorkspaceResponseView {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub workspaces: Vec<WorkspaceResponse>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ValidateUploadFeasibilityRequest {
    pub size: u64,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct RenameRequest {
    #[schema(example = "path/to/new_file.txt")]
    pub new_path: String,
}
