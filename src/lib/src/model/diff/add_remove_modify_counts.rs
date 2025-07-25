use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use crate::{constants::DIFF_STATUS_COL, error::OxenError};

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct AddRemoveModifyCounts {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
}

impl AddRemoveModifyCounts {
    pub fn from_diff_df(df: &DataFrame) -> Result<AddRemoveModifyCounts, OxenError> {
        let added_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "added").unwrap_or(false))
            .count();

        let removed_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "removed").unwrap_or(false))
            .count();

        let modified_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "modified").unwrap_or(false))
            .count();

        Ok(AddRemoveModifyCounts {
            added: added_rows,
            removed: removed_rows,
            modified: modified_rows,
        })
    }
}
