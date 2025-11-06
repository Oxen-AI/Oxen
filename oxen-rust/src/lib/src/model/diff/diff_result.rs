// use crate::model::diff::dir_diff::DirDiff;
use crate::model::diff::tabular_diff::TabularDiff;
use crate::model::diff::text_diff::TextDiff;

// ignoring because we likely won't have enough of these in memory to matter
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum DiffResult {
    Tabular(TabularDiff),
    Text(TextDiff),
}
