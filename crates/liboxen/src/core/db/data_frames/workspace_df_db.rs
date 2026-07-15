use crate::constants::OXEN_COLS;

use crate::core::db::data_frames::DataFrameError;
use crate::model::Schema;

use super::df_db;

/// Builds on df_db, but for specific use cases involving remote staging -
/// i.e., handling additional virtual columns beyond the formal schema, table names, etc.
pub fn select_cols_from_schema(schema: &Schema) -> String {
    // Check if OXEN_COLS are already in the schema
    let missing_oxen_cols: Vec<&str> = OXEN_COLS
        .iter()
        .filter(|col| !schema.fields.iter().any(|field| &field.name == *col))
        .copied()
        .collect();

    // Add the missing oxen cols

    missing_oxen_cols
        .iter()
        .map(|col| format!("\"{col}\""))
        .chain(schema.fields.iter().map(|col| format!("\"{}\"", col.name)))
        .collect::<Vec<String>>()
        .join(", ")
}

pub fn schema_without_oxen_cols(
    conn: &duckdb::Connection,
    table_name: &str,
) -> Result<Schema, DataFrameError> {
    let table_schema = df_db::get_schema_excluding_cols(conn, table_name, &OXEN_COLS)?;
    Ok(table_schema)
}
