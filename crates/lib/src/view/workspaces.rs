use serde::{Deserialize, Serialize};
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

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct WorkspaceResponse {
    pub id: String,
    pub name: Option<String>,
    pub commit: Commit,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WorkspaceResponseWithStatus {
    pub id: String,
    pub name: Option<String>,
    pub commit: Commit,
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
