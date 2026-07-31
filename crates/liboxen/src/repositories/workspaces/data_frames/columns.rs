use crate::core;
use crate::error::OxenError;
use crate::model::{LocalRepository, Schema, Workspace};

use crate::view::JsonDataFrameViews;
use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};

use polars::frame::DataFrame;
use std::path::{Path, PathBuf};

pub fn add(
    _repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::columns::add(workspace, file_path.as_ref(), new_column)
}

pub async fn update(
    _repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::columns::update(
        workspace,
        file_path.as_ref(),
        column_to_update,
    )
    .await
}

pub fn delete(
    _repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::columns::delete(
        workspace,
        file_path.as_ref(),
        column_to_delete,
    )
}

pub fn add_column_metadata(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: PathBuf,
    column: String,
    metadata: &serde_json::Value,
) -> Result<Schema, OxenError> {
    core::v_latest::workspaces::data_frames::columns::add_column_metadata(
        repo, workspace, &file_path, &column, metadata,
    )
}

/// Carry schema and column metadata staged in the workspace into the response
/// schemas, which are seeded from the *committed* schema.
pub fn update_column_schemas(new_schema: Option<Schema>, df_views: &mut JsonDataFrameViews) {
    if let Some(schema) = new_schema {
        df_views.source.schema.update_metadata_from_schema(&schema);
        df_views.view.schema.update_metadata_from_schema(&schema);
    }
}
