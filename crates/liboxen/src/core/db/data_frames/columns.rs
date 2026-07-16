use duckdb::arrow::array::RecordBatch;
use polars::frame::DataFrame;

use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::DataFrameError;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::model::Schema;
use crate::model::data_frame::schema::DataType;
use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};

use super::df_db;

pub fn add_column(
    conn: &duckdb::Connection,
    new_column: &NewColumn,
) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    if table_schema.has_column(&new_column.name) {
        return Err(DataFrameError::ColumnNameAlreadyExists(
            new_column.name.clone(),
        ));
    }

    let inserted_df = polar_insert_column(conn, TABLE_NAME, new_column)?;
    Ok(inserted_df)
}

pub fn delete_column(
    conn: &duckdb::Connection,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    if !table_schema.has_column(&column_to_delete.name) {
        return Err(DataFrameError::ColumnNameNotFound(
            column_to_delete.name.clone(),
        ));
    }

    let inserted_df = polar_delete_column(conn, TABLE_NAME, column_to_delete)?;
    Ok(inserted_df)
}

pub fn update_column(
    conn: &duckdb::Connection,
    column_to_update: &ColumnToUpdate,
    table_schema: &Schema,
) -> Result<DataFrame, DataFrameError> {
    if !table_schema.has_column(&column_to_update.name) {
        return Err(DataFrameError::ColumnNameNotFound(
            column_to_update.name.clone(),
        ));
    }

    let inserted_df = polar_update_column(conn, TABLE_NAME, column_to_update)?;
    Ok(inserted_df)
}

pub fn polar_insert_column(
    conn: &duckdb::Connection,
    table_name: &str,
    new_column: &NewColumn,
) -> Result<DataFrame, DataFrameError> {
    let data_type = DataType::from_string(&new_column.data_type).to_sql();
    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        table_name, new_column.name, data_type
    );
    conn.execute(&sql, [])?;

    let sql_query = format!("SELECT * FROM {table_name}");
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    df_db::record_batches_to_polars_df(result_set)
}

pub fn polar_delete_column(
    conn: &duckdb::Connection,
    table_name: &str,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, DataFrameError> {
    // Corrected to DROP COLUMN instead of ADD COLUMN
    let sql = format!(
        "ALTER TABLE {} DROP COLUMN {}",
        table_name, column_to_delete.name
    );
    conn.execute(&sql, [])?;

    let sql_query = format!("SELECT * FROM {table_name}");
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    df_db::record_batches_to_polars_df(result_set)
}

pub fn polar_update_column(
    conn: &duckdb::Connection,
    table_name: &str,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, DataFrameError> {
    let sql_commands = {
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
        sql_commands
    };

    for sql in sql_commands {
        conn.execute(&sql, [])?;
    }

    let sql_query = format!("SELECT * FROM {table_name}");
    let result_set: Vec<RecordBatch> = conn.prepare(&sql_query)?.query_arrow([])?.collect();

    df_db::record_batches_to_polars_df(result_set)
}
