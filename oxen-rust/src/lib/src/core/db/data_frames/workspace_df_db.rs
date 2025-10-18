use polars::frame::DataFrame;
use sql_query_builder as sql;

use crate::constants::{DIFF_STATUS_COL, OXEN_COLS, OXEN_ROW_ID_COL};

use crate::model::data_frame::schema::Field;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::Schema;
use crate::{constants::TABLE_NAME, error::OxenError};
use polars::prelude::*; // or use polars::lazy::*; if you're working in a lazy context

use super::df_db;

/// Builds on df_db, but for specific use cases involving remote staging -
/// i.e., handling additional virtual columns beyond the formal schema, table names, etc.
pub fn select_cols_from_schema(schema: &Schema) -> Result<String, OxenError> {
    // Check if OXEN_COLS are already in the schema
    let missing_oxen_cols: Vec<&str> = OXEN_COLS
        .iter()
        .filter(|col| !schema.fields.iter().any(|field| &field.name == *col))
        .copied()
        .collect();

    // Add the missing oxen cols
    let all_col_names = missing_oxen_cols
        .iter()
        .map(|col| format!("\"{col}\""))
        .chain(schema.fields.iter().map(|col| format!("\"{}\"", col.name)))
        .collect::<Vec<String>>()
        .join(", ");

    Ok(all_col_names)
}

// Returns the schema of the underlying table with the oxen cols prepended in a predictable
// order expected by the UI / API
pub fn full_staged_table_schema(conn: &duckdb::Connection) -> Result<Schema, OxenError> {
    let schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
    enhance_schema_with_oxen_cols(&schema)
}

pub fn schema_without_oxen_cols(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
) -> Result<Schema, OxenError> {
    let table_schema = df_db::get_schema_excluding_cols(conn, table_name, &OXEN_COLS)?;
    Ok(table_schema)
}

pub fn enhance_schema_with_oxen_cols(schema: &Schema) -> Result<Schema, OxenError> {
    let mut schema = schema.clone();

    // Check for missing oxen fields
    let missing_oxen_fields: Vec<Field> = OXEN_COLS
        .iter()
        .filter(|col| !schema.fields.iter().any(|field| &field.name == *col))
        .map(|col| Field {
            name: col.to_string(),
            dtype: if col == &OXEN_ROW_ID_COL {
                DataType::Int32.to_string()
            } else {
                DataType::String.to_string()
            },
            metadata: None,
            changes: None,
        })
        .collect();

    // Add the missing oxen fields
    schema.fields = missing_oxen_fields
        .iter()
        .chain(schema.fields.iter())
        .cloned()
        .collect();

    Ok(schema)
}

pub fn df_diff(conn: &duckdb::Connection) -> Result<DataFrame, OxenError> {
    let select = sql::Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!(
            "\"{}\" != '{}'",
            DIFF_STATUS_COL,
            StagedRowStatus::Unchanged
        ));

    let res = df_db::select(conn, &select, None)?;

    Ok(res)
}
