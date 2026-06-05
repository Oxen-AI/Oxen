use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::model::diff::dir_diff_summary::DirDiffSummary;
use crate::model::diff::tabular_diff_summary::TabularDiffWrapper;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(untagged)]
pub enum GenericDiffSummary {
    DirDiffSummary(DirDiffSummary),
    TabularDiffWrapper(TabularDiffWrapper),
}
