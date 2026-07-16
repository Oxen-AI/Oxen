use crate::constants::OXEN_COLS;

use crate::core::db::data_frames::DataFrameError;
use crate::model::Schema;

use super::df_db;

pub fn schema_without_oxen_cols(
    conn: &duckdb::Connection,
    table_name: &str,
) -> Result<Schema, DataFrameError> {
    let table_schema = df_db::get_schema_excluding_cols(conn, table_name, &OXEN_COLS)?;
    Ok(table_schema)
}
