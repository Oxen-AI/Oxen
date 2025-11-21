use crate::model::Branch;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::StatusMessage;
#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub branch: Branch,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchWithCacherStatusResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub branch: Branch,
    pub is_cacher_pending: bool,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct BranchLockResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub branch_name: String,
    pub is_locked: bool,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchNew {
    pub name: String,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchName {
    pub branch_name: String,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchNewFromBranchName {
    pub new_name: String,
    pub from_name: String,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchNewFromCommitId {
    pub new_name: String,
    pub commit_id: String,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchUpdate {
    pub commit_id: String,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BranchRemoteMerge {
    pub client_commit_id: String,
    pub server_commit_id: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct ListBranchesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub branches: Vec<Branch>,
}
