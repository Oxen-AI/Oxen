use crate::model::diff::change_type::ChangeType;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct LineDiff {
    pub modification: ChangeType,
    pub text: String,
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct TextDiff {
    pub lines: Vec<LineDiff>,
    pub filename1: Option<String>,
    pub filename2: Option<String>,
}
