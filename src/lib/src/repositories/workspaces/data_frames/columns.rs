use crate::core;
use crate::core::db;
use crate::core::db::data_frames::column_changes_db::get_all_data_frame_column_changes;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{LocalRepository, Schema, Workspace};
use crate::repositories;

use crate::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use crate::view::data_frames::DataFrameColumnChange;
use crate::view::JsonDataFrameViews;

use polars::frame::DataFrame;
use rocksdb::DB;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocksdb::IteratorMode;

use crate::model::data_frame::schema::field::{Changes, PreviousField};
use crate::model::data_frame::schema::Field;

pub fn add(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::data_frames::columns::add(
            workspace,
            file_path.as_ref(),
            new_column,
        ),
    }
}

pub async fn update(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::data_frames::columns::update(
            workspace,
            file_path.as_ref(),
            column_to_update,
        ).await,
    }
}

pub fn delete(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::data_frames::columns::delete(
            workspace,
            file_path.as_ref(),
            column_to_delete,
        ),
    }
}

pub fn add_column_metadata(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: PathBuf,
    column: String,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => Err(OxenError::basic_str("Not implemented")),
        _ => core::v_latest::workspaces::data_frames::columns::add_column_metadata(
            repo, workspace, file_path, column, metadata,
        ),
    }
}

pub async fn restore(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_restore: &ColumnToRestore,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::data_frames::columns::restore(
            workspace,
            file_path.as_ref(),
            column_to_restore,
        ).await,
    }
}

pub fn get_column_diff(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
) -> Result<Vec<DataFrameColumnChange>, OxenError> {
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    let opts = db::key_val::opts::default();

    match DB::open_for_read_only(&opts, dunce::simplified(&column_changes_path), false) {
        Ok(db) => get_all_data_frame_column_changes(&db),
        Err(_) => Ok(Vec::new()),
    }
}

pub fn decorate_fields_with_column_diffs(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    df_views: &mut JsonDataFrameViews,
) -> Result<(), OxenError> {
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path.as_ref());
    let opts = db::key_val::opts::default();
    let db_open_result = DB::open_for_read_only(
        &opts,
        dunce::simplified(column_changes_path.as_path()),
        false,
    );

    if db_open_result.is_err() {
        return Ok(());
    }

    let db = db_open_result?;

    df_views
        .source
        .schema
        .fields
        .iter_mut()
        .try_for_each(|field| {
            let column_name = &field.name;
            match db.get(column_name.as_bytes()) {
                Ok(Some(value_bytes)) => {
                    let value_result =
                        serde_json::from_slice::<DataFrameColumnChange>(&value_bytes);
                    match value_result {
                        Ok(value) => {
                            if let Some(changes) = handle_data_frame_column_change(value)? {
                                field.changes = Some(changes);
                            }
                            Ok(())
                        }
                        Err(_) => Err(OxenError::basic_str("Error deserializing value")),
                    }
                }
                Ok(None) => Ok(()),
                Err(e) => Err(OxenError::from(e)),
            }
        })?;

    df_views
        .view
        .schema
        .fields
        .iter_mut()
        .try_for_each(|field| {
            let column_name = &field.name;
            match db.get(column_name.as_bytes()) {
                Ok(Some(value_bytes)) => {
                    let value_result =
                        serde_json::from_slice::<DataFrameColumnChange>(&value_bytes);
                    match value_result {
                        Ok(value) => {
                            if let Some(changes) = handle_data_frame_column_change(value)? {
                                field.changes = Some(changes);
                            }
                            Ok(())
                        }
                        Err(_) => Err(OxenError::basic_str("Error deserializing value")),
                    }
                }
                Ok(None) => Ok(()),
                Err(e) => Err(OxenError::from(e)),
            }
        })?;

    Ok(())
}

pub fn handle_data_frame_column_change(
    change: DataFrameColumnChange,
) -> Result<Option<Changes>, OxenError> {
    match change.operation.as_str() {
        "added" => Ok(Some(Changes {
            status: "added".to_string(),
            previous: None,
        })),
        "deleted" => Ok(Some(Changes {
            status: "deleted".to_string(),
            previous: None,
        })),
        "modified" => {
            let column_before = change.column_before.ok_or(OxenError::basic_str(
                "A modified column needs to have a column before value",
            ))?;

            let previous_field = PreviousField {
                name: column_before.column_name.clone(),
                dtype: column_before.column_data_type.ok_or(OxenError::basic_str(
                    "A modified column needs to have a before datatype value",
                ))?,
                metadata: None,
            };

            Ok(Some(Changes {
                status: "modified".to_string(),
                previous: Some(previous_field),
            }))
        }
        _ => Ok(None),
    }
}

pub fn reinsert_deleted_columns_into_schema(
    db: &DB,
    df_views: &mut JsonDataFrameViews,
) -> Result<(), OxenError> {
    let mut deleted_columns = Vec::new();

    for item in db.iterator(IteratorMode::Start) {
        match item {
            Ok((_key, value_bytes)) => {
                let column_change: DataFrameColumnChange = serde_json::from_slice(&value_bytes)
                    .map_err(|_| OxenError::basic_str("Error deserializing value"))?;

                if column_change.operation == "deleted" {
                    deleted_columns.push(column_change.column_before);
                }
            }
            Err(_) => return Err(OxenError::basic_str("Error reading from db")),
        }
    }

    for deleted_column in &deleted_columns {
        let before_column = deleted_column.clone().ok_or(OxenError::basic_str(
            "A deleted column needs to have a column before value",
        ))?;

        df_views.source.schema.fields.push(Field {
            name: before_column.column_name.clone(),
            dtype: before_column
                .column_data_type
                .clone()
                .ok_or(OxenError::basic_str(
                    "A deleted column needs to have a before datatype value",
                ))?
                .clone(),
            metadata: None,
            changes: None,
        });

        df_views.view.schema.fields.push(Field {
            name: before_column.column_name.clone(),
            dtype: before_column
                .column_data_type
                .ok_or(OxenError::basic_str(
                    "A deleted column needs to have a before datatype value",
                ))?
                .clone(),
            metadata: None,
            changes: None,
        });
    }

    Ok(())
}

pub fn update_column_schemas(
    new_schema: Option<Schema>,
    df_views: &mut JsonDataFrameViews,
) -> Result<(), OxenError> {
    if let Some(schema) = new_schema {
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
