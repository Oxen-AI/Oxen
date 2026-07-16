use std::collections::HashMap;

use duckdb::ToSql;
use duckdb::arrow::array::RecordBatch;
use polars::frame::DataFrame;
// use sql_query_builder as sql;

use crate::constants::{LEGACY_OXEN_COLS, OXEN_COLS, OXEN_ID_COL};
use crate::model::data_frame::schema::Schema;

use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::DataFrameError;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::df::tabular;
use crate::model::data_frame::schema::DataType;

use super::df_db;

pub fn append_row(conn: &duckdb::Connection, df: &DataFrame) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    let df_schema = df.schema();

    let df_names: Vec<String> = df_schema.iter_names().map(|s| s.to_string()).collect();
    if !table_schema.has_field_names(&df_names) {
        return Err(DataFrameError::IncompatibleSchemas {
            table_schema,
            df_cols: df_names,
        });
    }

    // Handle completely null {} create objects coming over from the hub:
    // insert a row of all defaults rather than building an empty INSERT.
    if df.height() == 0 || df.width() == 0 {
        let sql = format!("INSERT INTO {TABLE_NAME} DEFAULT VALUES RETURNING *");
        let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
        return df_db::record_batches_to_polars_df(result_set);
    }

    insert_polars_df(conn, TABLE_NAME, df)
}

/// Whether a column in an update payload should be dropped rather than applied.
/// The hub sends the oxen-internal columns (and, from older clients, the legacy
/// tracking columns) in row payloads; those must not be written — UNLESS the
/// data frame genuinely has a column of that name, in which case it is user
/// data and the update applies.
fn drop_from_update_payload(table_schema: &Schema, col: &str) -> bool {
    let is_reserved = OXEN_COLS.contains(&col) || LEGACY_OXEN_COLS.contains(&col);
    is_reserved && !table_schema.has_column(col)
}

pub fn modify_row(
    conn: &duckdb::Connection,
    df: &mut DataFrame,
    uuid: &str,
) -> Result<DataFrame, DataFrameError> {
    if df.height() != 1 {
        return Err(DataFrameError::ModifyOnly1Row);
    }

    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    // Exclude the OXEN_COLS the hub sends over (never modifiable), and any
    // legacy tracking columns the hub still round-trips — but only when they
    // aren't real columns in this table. A user data frame can legitimately
    // have a column named e.g. `_oxen_diff_status`, and updates to it must
    // apply, so a legacy name present in the schema is treated as user data.
    let schema = df.schema();
    let df_col_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
    let df_cols: Vec<String> = df_col_names
        .clone()
        .into_iter()
        .filter(|col| !drop_from_update_payload(&table_schema, col))
        .collect();
    let df = df.select(&df_cols)?;
    if !table_schema.has_field_names(&df_cols) {
        log::error!("modify_row incompatible_schemas {table_schema:?}\n{df_cols:?}");
        return Err(DataFrameError::IncompatibleSchemas {
            table_schema,
            df_cols,
        });
    }

    let result = df_db::modify_row_with_polars_df(conn, TABLE_NAME, uuid, &df)?;
    if result.height() == 0 {
        return Err(DataFrameError::MissingDataFrame(uuid.to_string()));
    }
    Ok(result)
}

pub fn modify_rows(
    conn: &duckdb::Connection,
    row_map: HashMap<String, DataFrame>,
) -> Result<DataFrame, DataFrameError> {
    let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;

    let mut update_map: HashMap<String, DataFrame> = HashMap::new();

    // Each entry may carry a different subset of columns, but the batch UPDATE
    // below builds one CASE expression per column across all rows — so merge
    // every change into its full current row first. Also excludes any of the
    // OXEN_COLS the hub sends over, which must not be modified.
    for (row_id, df) in row_map.iter() {
        if df.height() != 1 {
            return Err(DataFrameError::ModifyOnly1Row);
        }
        let schema = df.schema();
        let df_col_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
        let df_cols: Vec<String> = df_col_names
            .clone()
            .into_iter()
            .filter(|col| !drop_from_update_payload(&table_schema, col))
            .collect();
        let df = df.select(&df_cols)?;
        if !table_schema.has_field_names(&df_cols) {
            log::error!("modify_row incompatible_schemas {table_schema:?}\n{df_cols:?}");
            return Err(DataFrameError::IncompatibleSchemas {
                table_schema,
                df_cols,
            });
        }

        let current_row = df_db::select_raw_with_params(
            conn,
            &format!("SELECT * FROM {TABLE_NAME} WHERE \"{OXEN_ID_COL}\" = ?"),
            [row_id.as_str()],
        )?;
        // Fail before ANY row is written: proceeding with a missing id would
        // let the batch UPDATE run for the other rows and then error on the
        // count check afterward — a partial write reported as a failure.
        if current_row.height() == 0 {
            return Err(DataFrameError::MissingDataFrame(row_id.to_string()));
        }

        let mut new_row = current_row.clone();
        for col in df.get_columns() {
            // Replace that column - copy the entire Series to preserve complex types (lists, structs, etc.)
            let col_name = col.name();
            let col_series = df.column(col_name)?;
            if let Some(col_idx) = new_row.get_column_index(col_name) {
                new_row.replace_column(col_idx, col_series.clone())?;
            } else {
                new_row.with_column(col_series.clone())?;
            }
        }

        update_map.insert(row_id.to_owned(), new_row);
    }

    let result = df_db::modify_rows_with_polars_df(conn, TABLE_NAME, &update_map)?;
    if result.height() == 0 {
        return Err(DataFrameError::MissingDataFrame("".to_string()));
    }

    if result.height() != update_map.len() {
        return Err(DataFrameError::UnexpectedModifications {
            expected: update_map.len(),
            actual: result.height(),
        });
    }

    Ok(result)
}

pub fn delete_row(conn: &duckdb::Connection, uuid: &str) -> Result<DataFrame, DataFrameError> {
    // The id is bound, not interpolated, so a request-supplied id can't alter
    // the predicate and delete unrelated rows.
    let row_to_delete = df_db::select_raw_with_params(
        conn,
        &format!("SELECT * FROM {TABLE_NAME} WHERE \"{OXEN_ID_COL}\" = ?"),
        [uuid],
    )?;
    if row_to_delete.height() == 0 {
        return Err(DataFrameError::MissingDataFrame(uuid.to_string()));
    }

    conn.execute(
        &format!("DELETE FROM {TABLE_NAME} WHERE \"{OXEN_ID_COL}\" = ?"),
        [uuid],
    )?;

    Ok(row_to_delete)
}

/// Insert a row from a polars dataframe into a duckdb table.
pub fn insert_polars_df(
    conn: &duckdb::Connection,
    table_name: &str,
    df: &DataFrame,
) -> Result<DataFrame, DataFrameError> {
    let schema = df.schema();
    let field_names: Vec<&str> = schema.iter_names().map(|s| s.as_str()).collect();
    let column_names: Vec<String> = field_names
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect();

    let column_sql_types = column_sql_types_by_name(conn, table_name)?;
    let placeholders: String = field_names
        .iter()
        .map(|name| placeholder_for_column(&column_sql_types, name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
        table_name,
        column_names.join(", "),
        placeholders,
    );

    let mut stmt = conn.prepare(&sql)?;

    // TODO: is there a way to bulk insert this?
    let mut result_df = DataFrame::default();
    for idx in 0..df.height() {
        let row = df.get(idx).unwrap();
        let boxed_values: Vec<Box<dyn ToSql>> = row
            .iter()
            .map(|v| tabular::value_to_tosql(v.to_owned()))
            .collect();

        let params: Vec<&dyn ToSql> = boxed_values
            .iter()
            .map(|boxed_value| &**boxed_value as &dyn ToSql)
            .collect();

        // Convert to Vec<&RecordBatch>
        let result_set: Vec<RecordBatch> = stmt.query_arrow(params.as_slice())?.collect();

        let df = df_db::record_batches_to_polars_df(result_set)?;
        result_df = if df.height() == 0 || result_df.height() == 0 {
            df
        } else {
            result_df.vstack(&df).unwrap()
        };
    }

    Ok(result_df)
}

/// Build a column-name → SQL type map for the given DuckDB table.
///
/// Used to wrap List/Struct/Embedding placeholders in `CAST(? AS <sql_type>)` so that
/// JSON-encoded payloads bind unambiguously to typed list columns.
pub(crate) fn column_sql_types_by_name(
    conn: &duckdb::Connection,
    table_name: &str,
) -> Result<HashMap<String, String>, DataFrameError> {
    let schema = df_db::get_schema(conn, table_name)?;
    let mut by_name = HashMap::with_capacity(schema.fields.len());
    for field in schema.fields {
        let sql_type = DataType::from_string(&field.dtype).to_sql();
        by_name.insert(field.name, sql_type);
    }
    Ok(by_name)
}

/// Returns `CAST(? AS <sql_type>)` for List/Struct/Embedding columns and a bare `?` otherwise.
pub(crate) fn placeholder_for_column(
    column_sql_types: &HashMap<String, String>,
    column_name: &str,
) -> String {
    match column_sql_types.get(column_name) {
        Some(sql_type) if needs_explicit_cast(sql_type) => format!("CAST(? AS {sql_type})"),
        _ => "?".to_string(),
    }
}

fn needs_explicit_cast(sql_type: &str) -> bool {
    // List columns end with `[]` (e.g. INTEGER[], VARCHAR[]); fixed-size arrays / embeddings end with `[N]`.
    // Structs are stored as JSON (which already accepts string binds), but cast for symmetry / clarity.
    sql_type.ends_with(']') || sql_type == "JSON"
}
