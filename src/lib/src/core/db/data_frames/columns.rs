use std::path::Path;

use duckdb::arrow::array::RecordBatch;
use polars::frame::DataFrame;
use rocksdb::DB;

use crate::core::db;
use crate::core::db::data_frames::workspace_df_db::{
    full_staged_table_schema, schema_without_oxen_cols,
};
use crate::core::v0_10_0::index::workspaces::data_frames::column_changes_db;
use crate::model::data_frame::schema::DataType;
use crate::model::Schema;
use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};
use crate::view::data_frames::{ColumnChange, DataFrameColumnChange};
use crate::{constants::TABLE_NAME, error::OxenError};

use super::df_db;

pub fn add_column(
    conn: &duckdb::Connection,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    if table_schema.has_column(&new_column.name) {
        return Err(OxenError::column_name_already_exists(&new_column.name));
    }

    let inserted_df = polar_insert_column(conn, TABLE_NAME, new_column)?;
    Ok(inserted_df)
}

pub fn delete_column(
    conn: &duckdb::Connection,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    if !table_schema.has_column(&column_to_delete.name) {
        return Err(OxenError::column_name_not_found(&column_to_delete.name));
    }

    let inserted_df = polar_delete_column(conn, TABLE_NAME, column_to_delete)?;
    Ok(inserted_df)
}

pub fn update_column(
    conn: &duckdb::Connection,
    column_to_update: &ColumnToUpdate,
    table_schema: &Schema,
) -> Result<DataFrame, OxenError> {
    if !table_schema.has_column(&column_to_update.name) {
        return Err(OxenError::column_name_not_found(&column_to_update.name));
    }

    let inserted_df = polar_update_column(conn, TABLE_NAME, column_to_update)?;
    Ok(inserted_df)
}

pub fn record_column_change(
    column_changes_path: &Path,
    operation: String,
    column_before: Option<ColumnChange>,
    column_after: Option<ColumnChange>,
) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(column_changes_path))?;

    if operation == "deleted" {
        if let Some(column) = &column_before {
            if let Some(previous_change) =
                column_changes_db::get_data_frame_column_change(&db, &column.column_name)?
            {
                if previous_change.operation == "added" {
                    // If we're deleting a previously added column, just remove the change
                    return revert_column_changes(&db, &column.column_name);
                }
            }
        }
    }

    println!("column_before: {:?}", column_before);

    let change = DataFrameColumnChange {
        operation,
        column_before: column_before.clone(),
        column_after: column_after.clone(),
    };

    let _ = maybe_revert_column_changes(&db, column_before);
    let _ = maybe_revert_column_changes(&db, column_after);

    column_changes_db::write_data_frame_column_change(&change, &db)
}

pub fn maybe_revert_column_changes(db: &DB, column: Option<ColumnChange>) -> Result<(), OxenError> {
    if let Some(column) = column {
        column_changes_db::get_data_frame_column_change(db, &column.column_name).and_then(
            |change_opt| match change_opt {
                Some(_) => revert_column_changes(db, &column.column_name.to_owned()),
                None => Ok(()),
            },
        )
    } else {
        Ok(())
    }
}

pub fn revert_column_changes(db: &DB, column_name: &str) -> Result<(), OxenError> {
    column_changes_db::delete_data_frame_column_changes(db, column_name)
}

pub fn polar_insert_column(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();
    let data_type = DataType::from_string(&new_column.data_type).to_sql();
    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        table_name, new_column.name, data_type
    );
    conn.execute(&sql, [])?;

    let sql_query = format!("SELECT * FROM {}", table_name);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    let table_schema = full_staged_table_schema(conn)?;

    df_db::record_batches_to_polars_df_explicit_nulls(result_set, &table_schema)
}

pub fn polar_delete_column(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();

    // Corrected to DROP COLUMN instead of ADD COLUMN
    let sql = format!(
        "ALTER TABLE {} DROP COLUMN {}",
        table_name, column_to_delete.name
    );
    conn.execute(&sql, [])?;

    let sql_query = format!("SELECT * FROM {}", table_name);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    let table_schema = full_staged_table_schema(conn)?;

    df_db::record_batches_to_polars_df_explicit_nulls(result_set, &table_schema)
}

pub fn polar_update_column(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();
    let mut sql_commands = Vec::new();

    if let Some(ref new_data_type) = column_to_update.new_data_type {
        let data_type = DataType::from_string(new_data_type).to_sql();

        let update_type_sql = format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
            table_name, column_to_update.name, data_type
        );
        sql_commands.push(update_type_sql);
    }

    if let Some(ref new_name) = column_to_update.new_name {
        let rename_sql = format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            table_name, column_to_update.name, new_name
        );
        sql_commands.push(rename_sql);
    }

    for sql in sql_commands {
        conn.execute(&sql, [])?;
    }

    let sql_query = format!("SELECT * FROM {}", table_name);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    let table_schema = full_staged_table_schema(conn)?;

    df_db::record_batches_to_polars_df_explicit_nulls(result_set, &table_schema)
}
