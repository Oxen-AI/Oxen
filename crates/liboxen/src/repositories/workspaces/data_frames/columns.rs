use crate::core;
use crate::error::OxenError;
use crate::model::{LocalRepository, Schema, Workspace};

use crate::view::JsonDataFrameViews;
use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};

use polars::frame::DataFrame;
use std::collections::HashMap;
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
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    core::v_latest::workspaces::data_frames::columns::add_column_metadata(
        repo, workspace, file_path, column, metadata,
    )
}

pub fn update_column_schemas(
    new_schema: Option<Schema>,
    df_views: &mut JsonDataFrameViews,
) -> Result<(), OxenError> {
    if let Some(schema) = new_schema {
        // The response schema is seeded from the *committed* schema, so carry
        // any schema-level metadata staged in the workspace across; when none
        // is staged, keep the committed value.
        if schema.metadata.is_some() {
            df_views.source.schema.metadata = schema.metadata.clone();
            df_views.view.schema.metadata = schema.metadata.clone();
        }

        // Update metadata for the source schema fields
        for field in df_views.source.schema.fields.iter_mut() {
            field.metadata = schema
                .fields
                .iter()
                .find(|f| f.name == field.name)
                .and_then(|f| f.metadata.clone());
        }

        // Update metadata for the view schema fields
        for field in df_views.view.schema.fields.iter_mut() {
            field.metadata = schema
                .fields
                .iter()
                .find(|f| f.name == field.name)
                .and_then(|f| f.metadata.clone());
        }
    }
    Ok(())
}
