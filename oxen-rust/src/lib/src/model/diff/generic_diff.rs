use serde::{Deserialize, Serialize};

use crate::model::diff::dir_diff::DirDiff;
use crate::model::diff::text_diff::TextDiff;
use crate::view::tabular_diff_view::TabularDiffView;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum GenericDiff {
    DirDiff(DirDiff),
    TabularDiff(Box<TabularDiffView>),
    TextDiff(TextDiff),
}
