use polars::prelude::DataFrame;
use std::fmt;

use crate::model::Schema;

/*

summary:
* base_size 10x100
* head_size 9x100
* schema_change: true

full:
* added_rows: Option<DataFrame>,
* removed_rows: Option<DataFrame>,
* added_cols: Option<DataFrame>,
* removed_cols: Option<DataFrame>,
* base_schema: Option<Schema>
* head_schema: Option<Schema>

later:
be able to run queries on removed rows, added rows, etc. to see what changed
*/

pub struct DataFrameDiff {
    pub base_schema: Option<Schema>,
    pub head_schema: Option<Schema>,
    pub added_rows: Option<DataFrame>,
    pub removed_rows: Option<DataFrame>,
    pub added_cols: Option<DataFrame>,
    pub removed_cols: Option<DataFrame>,
}

impl fmt::Display for DataFrameDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut results: Vec<String> = vec![];
        if let Some(rows) = &self.added_rows
            && rows.height() > 0
            && rows.width() > 0
        {
            results.push(format!("Added Rows\n\n{rows}\n\n"));
        }

        if let Some(rows) = &self.removed_rows {
            results.push(format!("Removed Rows\n\n{rows}\n\n"));
        }

        if let Some(cols) = &self.added_cols {
            results.push(format!("Added Columns\n\n{cols}\n\n"));
        }

        if let Some(cols) = &self.removed_cols {
            results.push(format!("Removed Columns\n\n{cols}\n\n"));
        }
        write!(f, "{}", results.join("\n"))
    }
}
