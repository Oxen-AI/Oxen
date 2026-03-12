use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::model::diff::diff_entry_status::DiffEntryStatus;

use super::StatusMessage;
#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct DirTreeDiffResponse {
    pub dirs: Vec<DirDiffTreeSummary>,
    #[serde(flatten)]
    pub status: StatusMessage,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct DirDiffTreeSummary {
    #[schema(value_type = String)]
    pub name: PathBuf,
    pub status: DiffEntryStatus,
    pub num_subdirs: usize,
    pub can_display: bool,
    pub children: Vec<DirDiffStatus>,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct DirDiffStatus {
    #[schema(value_type = String)]
    pub name: PathBuf,
    pub status: DiffEntryStatus,
}
