//! Core engine for importing data from external databases (Postgres, MySQL, SQLite)
//! into a workspace DuckDB table via SQLx + Arrow RecordBatch + DuckDB Appender.

use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray,
};
use duckdb::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlx::any::AnyRow;
use sqlx::{AnyPool, Column, Row, TypeInfo};

use crate::core::db::data_frames::df_db;
use crate::error::OxenError;
use crate::model::db_import::{DbImportConfig, DbImportResult, DbType};

/// Main entry: connect to an external DB, stream the query results in batches,
/// and load them into the given DuckDB at `duckdb_path` under the table `table_name`.
pub async fn import_from_database(
    duckdb_path: &std::path::Path,
    table_name: &str,
    config: &DbImportConfig,
) -> Result<DbImportResult, OxenError> {
    // 1. Validate the query is a SELECT statement
    validate_select_query(&config.query)?;

    // 2. Connect to external DB
    let url = config.full_connection_url();
    sqlx::any::install_default_drivers();
    let pool = AnyPool::connect(&url)
        .await
        .map_err(|e| OxenError::db_import_error(format!("Database connection failed: {e}")))?;

    // 3. Execute the query and collect all rows (SQLx's `Any` driver doesn't support
    //    true streaming for all backends, so we fetch all and batch locally).
    let rows: Vec<AnyRow> = sqlx::query(&config.query)
        .fetch_all(&pool)
        .await
        .map_err(|e| OxenError::db_import_error(format!("Query failed: {e}")))?;

    pool.close().await;

    let total_rows = rows.len();

    if rows.is_empty() {
        // Empty result: create table with no data. We still need column info.
        // Re-run with LIMIT 0 to get column metadata.
        let pool2 = AnyPool::connect(&url)
            .await
            .map_err(|e| OxenError::db_import_error(format!("Database connection failed: {e}")))?;
        let limit_query = format!("SELECT * FROM ({}) AS _subq LIMIT 0", config.query);
        let empty_rows: Vec<AnyRow> = sqlx::query(&limit_query)
            .fetch_all(&pool2)
            .await
            .unwrap_or_default();
        pool2.close().await;

        // If we still can't get columns, create an empty table
        if empty_rows.is_empty() {
            // Try to create a minimal empty table by running the query again
            // Fall through: the workspace export will create a headers-only file
            return Ok(DbImportResult {
                total_rows: 0,
                num_columns: 0,
            });
        }
    }

    // Get column metadata from the first row
    let columns: Vec<(String, String)> = if !rows.is_empty() {
        rows[0]
            .columns()
            .iter()
            .map(|col| {
                let name = col.name().to_string();
                let type_name = col.type_info().name().to_string();
                (name, type_name)
            })
            .collect()
    } else {
        return Ok(DbImportResult {
            total_rows: 0,
            num_columns: 0,
        });
    };

    let num_columns = columns.len();

    // 4. Build DuckDB CREATE TABLE SQL and Arrow schema
    let arrow_schema = build_arrow_schema(&columns, &config.db_type);
    let create_table_sql = build_create_table_sql(table_name, &columns, &config.db_type);

    // 5. Create the DuckDB table and load data in batches
    df_db::with_df_db_manager(duckdb_path, |manager| {
        manager.with_conn_mut(|conn| {
            // Create the table
            conn.execute(&create_table_sql, [])
                .map_err(|e| OxenError::db_import_error(format!("Failed to create table: {e}")))?;

            // Process rows in batches
            for chunk in rows.chunks(config.batch_size) {
                let record_batch =
                    rows_to_record_batch(chunk, &columns, &arrow_schema, &config.db_type)?;
                let mut appender = conn.appender(table_name).map_err(|e| {
                    OxenError::db_import_error(format!("Failed to create appender: {e}"))
                })?;
                appender.append_record_batch(record_batch).map_err(|e| {
                    OxenError::db_import_error(format!("Failed to append batch: {e}"))
                })?;
                appender.flush().map_err(|e| {
                    OxenError::db_import_error(format!("Failed to flush appender: {e}"))
                })?;
            }

            Ok(())
        })
    })?;

    Ok(DbImportResult {
        total_rows,
        num_columns,
    })
}

/// Validate that the SQL string is a SELECT statement (not INSERT, DROP, etc.)
fn validate_select_query(sql: &str) -> Result<(), OxenError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| OxenError::db_import_error(format!("SQL parse error: {e}")))?;

    if statements.is_empty() {
        return Err(OxenError::db_import_error("Empty SQL query"));
    }

    for stmt in &statements {
        if !matches!(stmt, sqlparser::ast::Statement::Query(_)) {
            return Err(OxenError::db_import_error(
                "Only SELECT queries are supported for database import",
            ));
        }
    }

    Ok(())
}

/// Map a SQLx type name to an Arrow DataType.
fn sqlx_type_to_arrow(type_name: &str, _db_type: &DbType) -> ArrowDataType {
    let upper = type_name.to_uppercase();
    match upper.as_str() {
        // Boolean
        "BOOL" | "BOOLEAN" => ArrowDataType::Boolean,

        // Integer types
        "INT2" | "SMALLINT" => ArrowDataType::Int16,
        "INT4" | "INT" | "INTEGER" | "SERIAL" => ArrowDataType::Int32,
        "INT8" | "BIGINT" | "BIGSERIAL" => ArrowDataType::Int64,

        // Float types
        "FLOAT4" | "FLOAT" | "REAL" => ArrowDataType::Float32,
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => ArrowDataType::Float64,

        // Text types
        "VARCHAR" | "TEXT" | "CHAR" | "CHARACTER VARYING" | "BPCHAR" | "NAME" | "CITEXT" => {
            ArrowDataType::Utf8
        }

        // Binary types
        "BYTEA" | "BLOB" | "BINARY" | "VARBINARY" => ArrowDataType::Binary,

        // Everything else: safe fallback to string
        _ => ArrowDataType::Utf8,
    }
}

/// Map a SQLx type name to a DuckDB SQL column type string.
fn sqlx_type_to_duckdb_sql(type_name: &str, _db_type: &DbType) -> &'static str {
    let upper = type_name.to_uppercase();
    match upper.as_str() {
        "BOOL" | "BOOLEAN" => "BOOLEAN",
        "INT2" | "SMALLINT" => "SMALLINT",
        "INT4" | "INT" | "INTEGER" | "SERIAL" => "INTEGER",
        "INT8" | "BIGINT" | "BIGSERIAL" => "BIGINT",
        "FLOAT4" | "FLOAT" | "REAL" => "FLOAT",
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => "DOUBLE",
        "BYTEA" | "BLOB" | "BINARY" | "VARBINARY" => "BLOB",
        _ => "VARCHAR",
    }
}

/// Build the Arrow schema from column metadata.
fn build_arrow_schema(columns: &[(String, String)], db_type: &DbType) -> ArrowSchema {
    let fields: Vec<ArrowField> = columns
        .iter()
        .map(|(name, type_name)| {
            ArrowField::new(name, sqlx_type_to_arrow(type_name, db_type), true)
        })
        .collect();
    ArrowSchema::new(fields)
}

/// Build the DuckDB CREATE TABLE statement.
fn build_create_table_sql(
    table_name: &str,
    columns: &[(String, String)],
    db_type: &DbType,
) -> String {
    let col_defs: Vec<String> = columns
        .iter()
        .map(|(name, type_name)| {
            let duckdb_type = sqlx_type_to_duckdb_sql(type_name, db_type);
            format!("\"{name}\" {duckdb_type}")
        })
        .collect();

    format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
        table_name,
        col_defs.join(", ")
    )
}

/// Convert a batch of SQLx AnyRows into an Arrow RecordBatch.
fn rows_to_record_batch(
    rows: &[AnyRow],
    columns: &[(String, String)],
    schema: &ArrowSchema,
    db_type: &DbType,
) -> Result<RecordBatch, OxenError> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for (col_idx, (_, type_name)) in columns.iter().enumerate() {
        let arrow_type = sqlx_type_to_arrow(type_name, db_type);
        let array = build_arrow_array(rows, col_idx, &arrow_type)?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| OxenError::db_import_error(format!("Failed to create RecordBatch: {e}")))?;

    Ok(batch)
}

/// Build an Arrow array of the given type from a column across all rows.
fn build_arrow_array(
    rows: &[AnyRow],
    col_idx: usize,
    arrow_type: &ArrowDataType,
) -> Result<ArrayRef, OxenError> {
    match arrow_type {
        ArrowDataType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.try_get::<bool, _>(col_idx).ok())
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        ArrowDataType::Int16 => {
            let values: Vec<Option<i16>> = rows
                .iter()
                .map(|row| row.try_get::<i16, _>(col_idx).ok())
                .collect();
            Ok(Arc::new(Int16Array::from(values)))
        }
        ArrowDataType::Int32 => {
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| row.try_get::<i32, _>(col_idx).ok())
                .collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        ArrowDataType::Int64 => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.try_get::<i64, _>(col_idx).ok())
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        ArrowDataType::Float32 => {
            let values: Vec<Option<f32>> = rows
                .iter()
                .map(|row| row.try_get::<f32, _>(col_idx).ok())
                .collect();
            Ok(Arc::new(Float32Array::from(values)))
        }
        ArrowDataType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.try_get::<f64, _>(col_idx).ok())
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        ArrowDataType::Binary => {
            let values: Vec<Option<&[u8]>> = rows
                .iter()
                .map(|row| row.try_get::<&[u8], _>(col_idx).ok())
                .collect();
            Ok(Arc::new(BinaryArray::from(values)))
        }
        // Default: read everything as String (safe fallback)
        _ => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| row.try_get::<String, _>(col_idx).ok())
                .collect();
            Ok(Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|v| v.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_select_query_accepts_select() {
        assert!(validate_select_query("SELECT * FROM users").is_ok());
        assert!(validate_select_query("SELECT id, name FROM users WHERE active = true").is_ok());
        assert!(validate_select_query("SELECT * FROM users LIMIT 10").is_ok());
    }

    #[test]
    fn test_validate_select_query_rejects_non_select() {
        assert!(validate_select_query("DROP TABLE users").is_err());
        assert!(validate_select_query("INSERT INTO users (name) VALUES ('test')").is_err());
        assert!(validate_select_query("DELETE FROM users").is_err());
        assert!(validate_select_query("UPDATE users SET name = 'test'").is_err());
    }

    #[test]
    fn test_validate_select_query_rejects_empty() {
        assert!(validate_select_query("").is_err());
    }

    #[test]
    fn test_build_create_table_sql() {
        let columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
            ("score".to_string(), "FLOAT8".to_string()),
        ];
        let sql = build_create_table_sql("test_table", &columns, &DbType::SQLite);
        assert!(sql.contains("\"id\" INTEGER"));
        assert!(sql.contains("\"name\" VARCHAR"));
        assert!(sql.contains("\"score\" DOUBLE"));
    }

    #[test]
    fn test_sqlx_type_to_arrow() {
        assert_eq!(
            sqlx_type_to_arrow("INTEGER", &DbType::SQLite),
            ArrowDataType::Int32
        );
        assert_eq!(
            sqlx_type_to_arrow("TEXT", &DbType::SQLite),
            ArrowDataType::Utf8
        );
        assert_eq!(
            sqlx_type_to_arrow("REAL", &DbType::SQLite),
            ArrowDataType::Float32
        );
        assert_eq!(
            sqlx_type_to_arrow("BOOLEAN", &DbType::Postgres),
            ArrowDataType::Boolean
        );
        assert_eq!(
            sqlx_type_to_arrow("BLOB", &DbType::SQLite),
            ArrowDataType::Binary
        );
        // Unknown types fall back to Utf8
        assert_eq!(
            sqlx_type_to_arrow("JSON", &DbType::Postgres),
            ArrowDataType::Utf8
        );
    }
}
