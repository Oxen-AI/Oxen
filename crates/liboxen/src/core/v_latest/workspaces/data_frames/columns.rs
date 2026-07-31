use polars::frame::DataFrame;

use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::DataFrameError;
use crate::core::db::data_frames::columns;
use crate::core::db::data_frames::df_db::with_df_db_manager;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::v_latest::workspaces;
use crate::error::OxenError;
use crate::model::{LocalRepository, Schema, Workspace};

use crate::repositories;
use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    log::debug!("add_column() got db_path: {db_path:?}");
    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| columns::add_column(conn, new_column))
    })?;

    workspaces::files::track_modified_data_frame(workspace, file_path)?;

    Ok(result)
}

pub fn delete(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    log::debug!("delete_column() got db_path: {db_path:?}");
    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| columns::delete_column(conn, column_to_delete))
    })?;

    workspaces::files::track_modified_data_frame(workspace, file_path)?;

    Ok(result)
}

pub async fn update(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    log::debug!("update_column() got db_path: {db_path:?}");
    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
            columns::update_column(conn, column_to_update, &table_schema)
        })
    })?;

    let column_after_name = column_to_update
        .new_name
        .clone()
        .unwrap_or(column_to_update.name.clone());

    let og_schema = repositories::data_frames::schemas::get_by_path(
        &workspace.base_repo,
        &workspace.commit,
        file_path,
    )?;

    repositories::workspaces::data_frames::schemas::update_schema(
        workspace,
        file_path,
        &og_schema.ok_or_else(|| OxenError::basic_str("Original schema not found"))?,
        &column_to_update.name,
        &column_after_name,
    )?;

    repositories::workspaces::files::add(workspace, file_path).await?;

    Ok(result)
}

pub fn add_column_metadata(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: &Path,
    column: &str,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    super::stage_schema_metadata_update(repo, workspace, file_path, |schema| {
        let field = schema
            .fields
            .iter_mut()
            .find(|f| f.name == column)
            .ok_or_else(|| DataFrameError::ColumnNameNotFound(column.to_string()))?;
        field.metadata = Some(metadata.to_owned());
        Ok(())
    })
}
