use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::df_db;
use crate::error::OxenError;
use crate::model::{Schema, Workspace};
use crate::repositories;

use std::path::Path;

pub fn get_by_path(workspace: &Workspace, path: impl AsRef<Path>) -> Result<Schema, OxenError> {
    let file_path = path.as_ref();
    let staged_db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let conn = df_db::get_connection(staged_db_path)?;
    let df_schema = df_db::get_schema(&conn, TABLE_NAME)?;
    Ok(df_schema)
}
