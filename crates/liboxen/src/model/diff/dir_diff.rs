use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::dir_diff_summary::DirDiffSummary;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct DirDiff {
    #[serde(flatten)]
    pub summary: DirDiffSummary,
}
