use crate::model::Commit;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct MergeConflictFile {
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct Mergeable {
    pub is_mergeable: bool,
    pub conflicts: Vec<MergeConflictFile>,
    pub commits: Vec<Commit>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct MergeableResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub mergeable: Mergeable,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct MergeResult {
    pub head: Commit,
    pub base: Commit,
    pub merge: Commit,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct MergeSuccessResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commits: MergeResult,
}
