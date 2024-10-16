use polars::frame::DataFrame;
use rocksdb::{IteratorMode, DB};

use crate::constants::TABLE_NAME;
use crate::core::db;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::db::data_frames::{columns, df_db};
use crate::core::v0_10_0::index::workspaces;
use crate::core::v0_10_0::index::workspaces::data_frames::column_changes_db;
use crate::error::OxenError;
use crate::model::data_frame::schema::field::{Changes, PreviousField};
use crate::model::data_frame::schema::Field;
use crate::model::Workspace;
use crate::repositories;
use crate::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use crate::view::data_frames::{ColumnChange, DataFrameColumnChange};
use crate::view::JsonDataFrameViews;

use std::path::Path;

use super::column_changes_db::get_all_data_frame_column_changes;

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("add_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;
    let result = columns::add_column(&conn, new_column)?;

    let column_after = ColumnChange {
        column_name: new_column.name.clone(),
        column_data_type: Some(new_column.data_type.to_owned()),
    };

    columns::record_column_change(
        &column_changes_path,
        "added".to_owned(),
        None,
        Some(column_after),
    )?;

    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

pub fn delete(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("delete_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;
    let column_data_type =
        table_schema
            .get_field(&column_to_delete.name)
            .ok_or(OxenError::Basic(
                "A column with the given name does not exist".into(),
            ))?;

    let result = columns::delete_column(&conn, column_to_delete)?;

    let column_before = ColumnChange {
        column_name: column_to_delete.name.clone(),
        column_data_type: Some(column_data_type.dtype.clone()),
    };

    columns::record_column_change(
        &column_changes_path,
        "deleted".to_owned(),
        Some(column_before),
        None,
    )?;

    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

pub fn update(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("update_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let result = columns::update_column(&conn, column_to_update, &table_schema)?;

    let column_data_type = table_schema.get_field(&column_to_update.name).unwrap();

    let column_after_name = column_to_update
        .new_name
        .clone()
        .unwrap_or(column_to_update.name.clone());

    let column_after_data_type = column_to_update
        .new_data_type
        .clone()
        .unwrap_or(column_data_type.dtype.clone());

    let column_before = ColumnChange {
        column_name: column_to_update.name.clone(),
        column_data_type: Some(column_data_type.dtype.clone()),
    };

    let column_after = ColumnChange {
        column_name: column_after_name,
        column_data_type: Some(column_after_data_type),
    };

    columns::record_column_change(
        &column_changes_path,
        "modified".to_string(),
        Some(column_before),
        Some(column_after),
    )?;

    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

pub fn restore(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_restore: &ColumnToRestore,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);

    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(&column_changes_path))?;

    log::debug!("restore_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    if let Some(change) =
        column_changes_db::get_data_frame_column_change(&db, &column_to_restore.name)?
    {
        match change.operation.as_str() {
            "added" => {
                log::debug!("restore_column() column is added, deleting");
                let column_to_delete = ColumnToDelete {
                    name: change
                        .column_after
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore an add, the column after object has to be defined".into(),
                        ))?
                        .column_name
                        .clone(),
                };
                let result = columns::delete_column(&conn, &column_to_delete)?;
                columns::revert_column_changes(
                    &db,
                    &change
                        .column_after
                        .ok_or(OxenError::Basic(
                            "To restore an add, the column after object has to be defined".into(),
                        ))?
                        .column_name,
                )?;
                workspaces::stager::add(workspace, file_path)?;
                Ok(result)
            }
            "deleted" => {
                log::debug!("restore_column() column was removed, adding it back");
                let new_column = NewColumn {
                    name: change
                        .column_before
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore a delete, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_name,
                    data_type: change
                        .column_before
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore a delete, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_data_type
                        .ok_or(OxenError::Basic(
                            "Column data type is required but was None".into(),
                        ))?,
                };
                let result = columns::add_column(&conn, &new_column)?;
                columns::revert_column_changes(
                    &db,
                    &change
                        .column_before
                        .ok_or(OxenError::Basic(
                            "To restore a delete, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_name,
                )?;
                workspaces::stager::add(workspace, file_path)?;
                Ok(result)
            }
            "modified" => {
                log::debug!("restore_column() column was modified, reverting changes");
                let new_data_type = change
                    .column_before
                    .clone()
                    .ok_or(OxenError::Basic(
                        "To restore a modify, the column before object has to be defined".into(),
                    ))?
                    .column_data_type
                    .ok_or(OxenError::Basic(
                        "column_data_type is None, cannot unwrap".into(),
                    ))?;
                let column_to_update = ColumnToUpdate {
                    name: change
                        .column_after
                        .ok_or(OxenError::Basic(
                            "To restore a modify, the column after object has to be defined".into(),
                        ))?
                        .column_name,
                    new_data_type: Some(new_data_type.to_owned()),
                    new_name: Some(
                        change
                            .column_before
                            .clone()
                            .ok_or(OxenError::Basic(
                                "To restore a modify, the column before object has to be defined"
                                    .into(),
                            ))?
                            .column_name,
                    ),
                };

                let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;
                let result = columns::update_column(&conn, &column_to_update, &table_schema)?;
                columns::revert_column_changes(
                    &db,
                    &change
                        .column_before
                        .ok_or(OxenError::Basic(
                            "To restore a modify, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_name,
                )?;
                workspaces::stager::add(workspace, file_path)?;
                Ok(result)
            }
            _ => Err(OxenError::UnsupportedOperation(
                change.operation.clone().into(),
            )),
        }
    } else {
        Err(OxenError::ColumnNameNotFound(
            column_to_restore.name.clone().into(),
        ))
    }
}

pub fn get_column_diff(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
) -> Result<Vec<DataFrameColumnChange>, OxenError> {
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    let opts = db::key_val::opts::default();
    let db = DB::open_for_read_only(&opts, dunce::simplified(&column_changes_path), false)?;
    get_all_data_frame_column_changes(&db)
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

    // Because the schema is derived from the data, we need to reinsert the deleted columns into the schema to track diffs as they were deleted in the sql query.
    reinsert_deleted_columns_into_schema(&db, df_views)?;

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
