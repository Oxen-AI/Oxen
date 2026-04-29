//! Abstraction over DuckDB database to write and read dataframes from disk.
//!

use crate::constants::{
    DEFAULT_PAGE_SIZE, DUCKDB_DF_TABLE_NAME, OXEN_COLS, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME,
};

use crate::core::db::data_frames::rows;
use crate::core::df::tabular;
use crate::core::v_latest::workspaces::data_frames::{
    is_valid_export_extension, wrap_sql_for_export,
};
use crate::error::OxenError;

use crate::model::data_frame::schema::Field;
use crate::model::data_frame::schema::Schema;
use crate::opts::DFOpts;
use crate::{model, util};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{ToSql, params};
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use polars::prelude::*;
use sqlparser::ast::{self, Expr as SqlExpr, SelectItem, Statement, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use sql_query_builder as sql;

const DF_DB_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

// Static cache of DuckDB instances with LRU eviction
static DF_DB_INSTANCES: LazyLock<RwLock<LruCache<PathBuf, Arc<Mutex<duckdb::Connection>>>>> =
    LazyLock::new(|| RwLock::new(LruCache::new(DF_DB_CACHE_SIZE)));

/// Removes a database instance from the cache.
pub fn remove_df_db_from_cache(db_path: impl AsRef<Path>) -> Result<(), OxenError> {
    let db_path = db_path.as_ref().to_path_buf();
    let mut instances = DF_DB_INSTANCES.write();
    let _ = instances.pop(&db_path);
    Ok(())
}

/// Removes a database instance and all its subdirectories from the cache.
/// This is mostly useful in test cleanup to ensure all DB instances are removed.
pub fn remove_df_db_from_cache_with_children(
    db_path_prefix: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let db_path_prefix = db_path_prefix.as_ref();

    let mut dbs_to_remove: Vec<PathBuf> = vec![];
    let mut instances = DF_DB_INSTANCES.write();
    for (key, _) in instances.iter() {
        if key.starts_with(db_path_prefix) {
            dbs_to_remove.push(key.clone());
        }
    }

    for db in dbs_to_remove {
        let _ = instances.pop(&db); // drop immediately
    }

    Ok(())
}

#[derive(Clone)]
pub struct DfDBManager {
    df_db: Arc<Mutex<duckdb::Connection>>,
}

pub fn with_df_db_manager<F, T>(db_path: impl AsRef<Path>, operation: F) -> Result<T, OxenError>
where
    F: FnOnce(&DfDBManager) -> Result<T, OxenError>,
{
    let db_path = db_path.as_ref().to_path_buf();

    let df_db = {
        // 1. If df db exists in cache, return the existing connection
        // Fast path: try to get a cloned handle under a short-lived read lock.
        if let Some(db_lock) = {
            let cache_r = DF_DB_INSTANCES.read();
            cache_r.peek(&db_path).cloned()
        } {
            // Read lock has been dropped before executing user code.
            return operation(&DfDBManager { df_db: db_lock });
        }

        // 2. If not exists, create the directory and open the db
        let mut cache_w = DF_DB_INSTANCES.write();
        if let Some(db_lock) = cache_w.get(&db_path) {
            db_lock.clone()
        } else {
            // Cache miss: create directory and open DB
            if let Some(parent) = db_path.parent()
                && !parent.exists()
            {
                std::fs::create_dir_all(parent).map_err(|e| {
                    log::error!("Failed to create df db directory: {e}");
                    OxenError::basic_str(format!("Failed to create df db directory: {e}"))
                })?;
            }

            let conn = get_connection(&db_path).map_err(|e| {
                log::error!("Failed to open df db: {e}");
                OxenError::basic_str(format!("Failed to open df db: {e}"))
            })?;

            // Wrap the Connection in a Mutex and store it in the cache
            let db_lock = Arc::new(Mutex::new(conn));
            cache_w.put(db_path.clone(), db_lock.clone());
            db_lock
        }
    };

    let manager = DfDBManager { df_db };

    // Execute the operation with our DfDBManager instance
    operation(&manager)
}

impl DfDBManager {
    /// Execute an operation with the database connection
    pub fn with_conn<F, T>(&self, operation: F) -> Result<T, OxenError>
    where
        F: FnOnce(&duckdb::Connection) -> Result<T, OxenError>,
    {
        let conn = self.df_db.lock();
        operation(&conn)
    }

    /// Execute an operation with the database connection (mutable access)
    /// Note: This provides mutable access to the connection for functions that require it
    pub fn with_conn_mut<F, T>(&self, operation: F) -> Result<T, OxenError>
    where
        F: FnOnce(&mut duckdb::Connection) -> Result<T, OxenError>,
    {
        let mut conn = self.df_db.lock();

        operation(&mut conn)
    }
}

/// Get a connection to a duckdb database.
///
/// If the database has a stale or corrupt WAL file (e.g. from a prior crash or
/// unclean LRU eviction), this function will attempt to recover by removing the
/// WAL and retrying. If that still fails the database and WAL files are both
/// deleted so the caller can re-index from scratch. This is safe because these
/// DuckDB databases are derived data — they are always re-indexed from the
/// versioned source files stored in the Oxen version store.
pub fn get_connection(path: impl AsRef<Path>) -> Result<duckdb::Connection, OxenError> {
    let path = path.as_ref();
    log::debug!("get_connection: Opening new DuckDB connection for path: {path:?}");

    if let Some(parent) = path.parent() {
        log::debug!("get_connection: Ensuring parent directory exists: {parent:?}");
        util::fs::create_dir_all(parent)?;
    }

    let wal_path = wal_path_for(path);

    // Happy path — open succeeds on the first try.
    let initial_err = match duckdb::Connection::open(path) {
        Ok(conn) => return open_success(conn, path),
        Err(e) => e,
    };

    // Only attempt destructive recovery when a WAL file is present on disk.
    // A WAL file signals a prior unclean shutdown (killed container, OOM, etc.)
    // where stale or corrupt WAL data is the likely cause of the open failure.
    // Without a WAL file the failure is something else (permissions, lock held
    // by another process, etc.) and deleting files would risk data loss.
    if !wal_path.exists() {
        log::error!(
            "get_connection: Failed to open DuckDB at {path:?}: {initial_err}. \
             No WAL file present — skipping recovery."
        );
        return Err(OxenError::from(initial_err));
    }

    // First recovery: remove only the WAL file and retry. A stale or corrupt
    // WAL (e.g. from a killed container) is the most common failure mode.
    log::warn!(
        "get_connection: Failed to open DuckDB at {path:?}: {initial_err}. \
         WAL file present — attempting recovery by removing it."
    );
    remove_file_if_exists(&wal_path);

    if let Ok(conn) = duckdb::Connection::open(path) {
        log::info!("get_connection: Recovery succeeded after WAL removal for {path:?}");
        return open_success(conn, path);
    }

    // Second recovery: WAL removal wasn't enough — the db file is likely
    // corrupt too. Remove both so the caller can re-index from scratch. This
    // is safe because these databases are derived from versioned source files
    // stored in the Oxen version store.
    log::error!(
        "get_connection: Retry after WAL removal still failed for {path:?}. \
         Removing database files so the workspace can be re-indexed."
    );
    remove_file_if_exists(path);
    remove_file_if_exists(&wal_path);

    Err(OxenError::from(initial_err))
}

/// Flush any leftover WAL from a prior session so it cannot cause replay
/// issues later (e.g. after a crash or LRU eviction).
fn open_success(conn: duckdb::Connection, path: &Path) -> Result<duckdb::Connection, OxenError> {
    if let Err(e) = conn.execute_batch("CHECKPOINT") {
        log::warn!("get_connection: CHECKPOINT after open failed for {path:?}: {e}");
    }
    log::info!("get_connection: Successfully opened DuckDB connection for path: {path:?}");
    Ok(conn)
}

/// Best-effort file removal with error logging.
fn remove_file_if_exists(path: &Path) {
    if path.exists()
        && let Err(e) = std::fs::remove_file(path)
    {
        log::error!("get_connection: Failed to remove {path:?}: {e}");
    }
}

/// Returns the WAL file path for a given DuckDB database path.
fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut wal = db_path.as_os_str().to_owned();
    wal.push(".wal");
    PathBuf::from(wal)
}

/// Create a table in a duckdb database based on an oxen schema.
pub fn create_table_if_not_exists(
    conn: &duckdb::Connection,
    name: impl AsRef<str>,
    schema: &Schema,
) -> Result<String, OxenError> {
    p_create_table_if_not_exists(conn, name, &schema.fields)
}

/// Drop a table in a duckdb database.
pub fn drop_table(conn: &duckdb::Connection, table_name: impl AsRef<str>) -> Result<(), OxenError> {
    let table_name = table_name.as_ref();
    let sql = format!("DROP TABLE IF EXISTS {table_name}");
    log::debug!("drop_table sql: {sql}");
    conn.execute(&sql, []).map_err(OxenError::from)?;
    Ok(())
}

pub fn table_exists(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
) -> Result<bool, OxenError> {
    log::debug!("checking exists in path {conn:?}");
    let table_name = table_name.as_ref();
    let sql = "SELECT EXISTS (SELECT 1 FROM duckdb_tables WHERE table_name = ?) AS table_exists";
    let mut stmt = conn.prepare(sql)?;
    let exists: bool = stmt.query_row(params![table_name], |row| row.get(0))?;
    log::debug!("got exists: {exists}");
    Ok(exists)
}

/// Create a table from a set of oxen fields with data types.
fn p_create_table_if_not_exists(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    fields: &[Field],
) -> Result<String, OxenError> {
    let table_name = table_name.as_ref();
    let columns: Vec<String> = fields.iter().map(|f| f.to_sql()).collect();
    let columns = columns.join(" NOT NULL,\n");
    let sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (\n{columns});");
    log::debug!("create_table sql: {sql}");
    conn.execute(&sql, [])?;
    Ok(table_name.to_owned())
}

/// Get the schema from the table.
pub fn get_schema(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
) -> Result<Schema, OxenError> {
    let table_name = table_name.as_ref();
    let sql = format!(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name == '{table_name}'"
    );
    let mut stmt = conn.prepare(&sql)?;

    let mut fields = vec![];
    let rows = stmt.query_map([], |row| {
        let column_name: String = row.get(0)?;
        let data_type: String = row.get(1)?;

        Ok((column_name, data_type))
    })?;

    for row in rows {
        let (column_name, data_type) = row?;
        fields.push(Field::new(
            &column_name,
            &model::data_frame::schema::DataType::from_sql(data_type).as_str(),
        ));
    }

    Ok(Schema::new(fields))
}

// Get the schema from the table excluding specified columns - useful for virtual cols like .oxen.diff.status
pub fn get_schema_excluding_cols(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    cols: &[&str],
) -> Result<Schema, OxenError> {
    let table_name = table_name.as_ref();
    let sql = format!(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name == '{}' AND column_name NOT IN ({})",
        table_name,
        cols.iter()
            .map(|col| format!("'{}'", col.replace('\'', "''")))
            .collect::<Vec<String>>()
            .join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;

    let mut fields = vec![];
    let rows = stmt.query_map([], |row| {
        let column_name: String = row.get(0)?;
        let data_type: String = row.get(1)?;

        Ok((column_name, data_type))
    })?;

    for row in rows {
        let (column_name, data_type) = row?;
        fields.push(Field::new(
            &column_name,
            &model::data_frame::schema::DataType::from_sql(data_type).as_str(),
        ));
    }

    Ok(Schema::new(fields))
}

/// Query number of rows in a table.
pub fn count(conn: &duckdb::Connection, table_name: impl AsRef<str>) -> Result<usize, OxenError> {
    let table_name = table_name.as_ref();
    let sql = format!("SELECT count(*) FROM {table_name}");
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let size: usize = row.get(0)?;
        Ok(size)
    } else {
        Err(OxenError::basic_str(format!(
            "No rows in table {table_name}"
        )))
    }
}

/// Query number of rows in a table.
pub fn count_where(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    where_clause: impl AsRef<str>,
) -> Result<usize, OxenError> {
    let table_name = table_name.as_ref();
    let where_clause = where_clause.as_ref();
    let sql = format!("SELECT count(*) FROM {table_name} WHERE {where_clause}");
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let size: usize = row.get(0)?;
        Ok(size)
    } else {
        Err(OxenError::basic_str(format!(
            "No rows in table {table_name}"
        )))
    }
}

// IMPORTANT: with_explicit_nulls=True is used to extract complete derived schemas
// for situations (such as workspace_df_db) that use non-schema oxen virtual columns.
// This should be set to false in any cases which may have null array / struct fields
// (such as the commit metadata db queries, which it currently breaks.)

pub fn select(
    conn: &duckdb::Connection,
    stmt: &sql::Select,
    opts: Option<&DFOpts>,
) -> Result<DataFrame, OxenError> {
    let sql = stmt.as_string();
    let df = select_str(conn, sql, opts)?;
    Ok(df)
}

pub fn export(
    conn: &duckdb::Connection,
    sql: impl AsRef<str>,
    _opts: Option<&DFOpts>,
    tmp_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let tmp_path = tmp_path.as_ref();
    let sql = sql.as_ref();
    // let sql = prepare_sql(sql, opts)?;
    // Get the file extension from the tmp_path
    if !is_valid_export_extension(tmp_path) {
        return Err(OxenError::basic_str(
            "Invalid file type: expected .csv, .tsv, .parquet, .jsonl, .json, .ndjson",
        ));
    }
    let export_sql = wrap_sql_for_export(sql, tmp_path);
    log::debug!("export_sql: {export_sql}");
    conn.execute(&export_sql, [])?;
    Ok(())
}

pub fn prepare_sql(
    conn: &duckdb::Connection,
    stmt: impl AsRef<str>,
    opts: Option<&DFOpts>,
) -> Result<String, OxenError> {
    let mut sql = stmt.as_ref().to_string();
    let empty_opts = DFOpts::empty();
    let opts = opts.unwrap_or(&empty_opts);

    sql = add_special_columns(conn, &sql)?;

    if opts.sort_by.is_some() {
        let sort_by: String = opts.sort_by.clone().unwrap_or_default();
        sql.push_str(&format!(" ORDER BY \"{sort_by}\""));
    }

    let pagination_clause = if let Some(page) = opts.page {
        let page = if page == 0 { 1 } else { page };
        let page_size = opts.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
        format!(" LIMIT {} OFFSET {}", page_size, (page - 1) * page_size)
    } else {
        "".to_string()
    };
    sql.push_str(&pagination_clause);
    log::debug!("select_str() running sql: {sql}");
    Ok(sql)
}

fn add_special_columns(conn: &duckdb::Connection, sql: &str) -> Result<String, OxenError> {
    let original_schema = get_schema(conn, TABLE_NAME)?;
    let dialect = PostgreSqlDialect {}; // Use this for DuckDB
    let mut ast = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");

    if let Some(Statement::Query(query)) = ast.get_mut(0) {
        // Remove the existing LIMIT clause
        query.limit = None;

        // Add a new LIMIT clause
        query.limit = Some(SqlExpr::Value(SqlValue::Number("1".into(), false)));
    }

    // Convert the AST back to a SQL string
    let query_with_limit = ast
        .iter()
        .map(|stmt| stmt.to_string())
        .collect::<Vec<_>>()
        .join(";");

    let mut stmt = conn.prepare(&query_with_limit)?;
    let records: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    let mut result_fields = vec![];

    // Retrieve and print the schema (column names)
    if let Some(first_batch) = records.first() {
        let schema = first_batch.schema();
        for field in schema.fields() {
            result_fields.push(Field::new(
                field.name(),
                field.data_type().to_string().as_str(),
            ));
        }
    }

    let original_field_names: Vec<&str> = original_schema
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    let result_field_names: Vec<&str> = result_fields.iter().map(|f| f.name.as_str()).collect();

    let is_subset = result_field_names
        .iter()
        .all(|name| original_field_names.contains(name));

    let mut modified_sql = sql.to_string();

    if is_subset {
        let special_columns: Vec<&str> = OXEN_COLS
            .iter()
            .filter(|col| !result_field_names.contains(col))
            .copied()
            .collect();

        if !special_columns.is_empty() {
            let mut ast = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");

            if let Some(Statement::Query(query)) = ast.get_mut(0)
                && let ast::SetExpr::Select(select) = &mut *query.body
            {
                // Don't inject special columns into DISTINCT queries —
                // adding per-row unique cols like _oxen_id defeats deduplication.
                if select.distinct.is_some() {
                    return Ok(sql.to_string());
                }

                // Add new columns to the SELECT clause
                for special_column in special_columns {
                    select
                        .projection
                        .push(SelectItem::UnnamedExpr(SqlExpr::Identifier(
                            special_column.into(),
                        )));
                }
            }

            // Convert the AST back to a SQL string
            modified_sql = ast
                .iter()
                .map(|stmt| stmt.to_string())
                .collect::<Vec<_>>()
                .join(";");
        }
    }
    Ok(modified_sql)
}

pub fn select_str(
    conn: &duckdb::Connection,
    sql: impl AsRef<str>,
    opts: Option<&DFOpts>,
) -> Result<DataFrame, OxenError> {
    let sql = sql.as_ref();
    let sql = prepare_sql(conn, sql, opts)?;
    let df = select_raw(conn, &sql)?;
    log::debug!("select_str() got raw df {df:?}");
    Ok(df)
}

pub fn select_raw(conn: &duckdb::Connection, stmt: &str) -> Result<DataFrame, OxenError> {
    let mut stmt = conn.prepare(stmt)?;

    let records: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    if records.is_empty() {
        return Ok(DataFrame::default());
    }

    let df = record_batches_to_polars_df(records)?;

    Ok(df)
}

pub fn modify_row_with_polars_df(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    id: &str,
    df: &DataFrame,
) -> Result<DataFrame, OxenError> {
    if df.height() != 1 {
        return Err(OxenError::basic_str(
            "df must have exactly one row to be used for modification",
        ));
    }

    let table_name = table_name.as_ref();

    let schema = df.schema();
    let field_names: Vec<&str> = schema.iter_names().map(|s| s.as_str()).collect();
    let column_sql_types = rows::column_sql_types_by_name(conn, table_name)?;

    let set_clauses: String = field_names
        .iter()
        .map(|name| {
            let placeholder = rows::placeholder_for_column(&column_sql_types, name);
            format!("\"{name}\" = {placeholder}")
        })
        .collect::<Vec<String>>()
        .join(", ");

    let where_clause = format!("\"{OXEN_ID_COL}\" = '{id}'");

    let sql = format!("UPDATE {table_name} SET {set_clauses} WHERE {where_clause} RETURNING *");

    let values = df.get(0).unwrap(); // Checked above

    let boxed_values: Vec<Box<dyn ToSql>> = values
        .iter()
        .map(|v| tabular::value_to_tosql(v.to_owned()))
        .collect();

    let params: Vec<&dyn ToSql> = boxed_values
        .iter()
        .map(|boxed_value| &**boxed_value as &dyn ToSql)
        .collect();

    let mut stmt = conn.prepare(&sql)?;
    let result_set: Vec<RecordBatch> = stmt.query_arrow(params.as_slice())?.collect();

    let df = record_batches_to_polars_df(result_set)?;

    Ok(df)
}

pub fn modify_rows_with_polars_df(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    row_map: &HashMap<String, DataFrame>,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();
    let mut all_result_batches = Vec::new();

    let mut set_clauses = Vec::new();
    let mut all_params: Vec<Box<dyn ToSql>> = Vec::new();

    // Construct the SQL query with combined CASE statements
    let column_names: Vec<String> = match row_map.iter().next() {
        Some((_, df)) => df.schema().iter_names().map(|s| s.to_string()).collect(),
        None => Vec::new(),
    };

    let column_sql_types = rows::column_sql_types_by_name(conn, table_name)?;

    for col_name in &column_names {
        let placeholder = rows::placeholder_for_column(&column_sql_types, col_name);
        let mut case_clauses = Vec::new();
        for (id, df) in row_map.iter() {
            let series = df.column(col_name)?;
            let value = series.get(0)?;

            let boxed_value: Box<dyn ToSql> = Box::new(tabular::value_to_tosql(value));

            case_clauses.push(format!(
                "WHEN \"{OXEN_ID_COL}\" = '{id}' THEN {placeholder}"
            ));

            all_params.push(boxed_value);
        }
        set_clauses.push(format!(
            "\"{}\" = CASE {} END",
            col_name,
            case_clauses.join(" ")
        ));
    }

    // Add all row IDs to the parameters for the WHERE clause
    for id in row_map.keys() {
        all_params.push(Box::new(id.clone()));
    }

    let sql = format!(
        "UPDATE {} SET {} WHERE \"{}\" IN ({}) RETURNING *",
        table_name,
        set_clauses.join(", "),
        OXEN_ID_COL,
        row_map.keys().map(|_| "?").collect::<Vec<_>>().join(", ")
    );

    let params: Vec<&dyn ToSql> = all_params
        .iter()
        .map(|boxed_value| &**boxed_value as &dyn ToSql)
        .collect();

    let mut stmt = conn.prepare(&sql)?;
    let result_set: Vec<RecordBatch> = stmt.query_arrow(params.as_slice())?.collect();

    all_result_batches.extend(result_set);

    let df = record_batches_to_polars_df(all_result_batches)?;

    Ok(df)
}

pub fn index_file(path: &Path, conn: &duckdb::Connection) -> Result<(), OxenError> {
    log::debug!("df_db:index_file() at path {path:?}");
    let extension: &str = &util::fs::extension_from_path(path);
    let path_str = path.to_string_lossy().to_string();
    match extension {
        "csv" => {
            let query = format!(
                "CREATE TABLE {DUCKDB_DF_TABLE_NAME} AS SELECT * FROM read_csv('{path_str}')"
            );
            conn.execute(&query, [])?;
        }
        "tsv" => {
            let query = format!(
                "CREATE TABLE {DUCKDB_DF_TABLE_NAME} AS SELECT * FROM read_csv('{path_str}')"
            );
            conn.execute(&query, [])?;
        }
        "parquet" => {
            let query = format!(
                "CREATE TABLE {DUCKDB_DF_TABLE_NAME} AS SELECT * FROM read_parquet('{path_str}')"
            );
            conn.execute(&query, [])?;
        }
        "jsonl" | "json" | "ndjson" => {
            let query = format!(
                "CREATE TABLE {DUCKDB_DF_TABLE_NAME} AS SELECT * FROM read_json('{path_str}')"
            );
            conn.execute(&query, [])?;
        }
        _ => {
            return Err(OxenError::basic_str(
                "Invalid file type: expected .csv, .tsv, .parquet, .jsonl, .json, .ndjson",
            ));
        }
    }
    Ok(())
}

// TODO: We will eventually want to parse the actual type, not just the extension.
// For now, just treat the extension as law
pub fn index_file_with_id(
    path: &Path,
    conn: &duckdb::Connection,
    extension: &str,
) -> Result<(), OxenError> {
    log::debug!("df_db:index_file() at path {path:?} into path {conn:?}");
    let path_str = path.to_string_lossy().to_string();
    let counter = "counter";
    // Drop sequence if exists
    let drop_sequence_query = format!("DROP SEQUENCE IF EXISTS {counter}");
    conn.execute(&drop_sequence_query, [])?;

    let add_row_id_sequence_query = format!("CREATE SEQUENCE {counter} START 1");
    conn.execute(&add_row_id_sequence_query, [])?;

    match extension {
        "csv" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_csv('{}', AUTO_DETECT=TRUE, header=True);",
                DUCKDB_DF_TABLE_NAME,
                OXEN_ID_COL,
                path.to_string_lossy()
            );
            conn.execute(&query, [])?;
        }
        "tsv" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_csv('{}', AUTO_DETECT=TRUE, header=True);",
                DUCKDB_DF_TABLE_NAME,
                OXEN_ID_COL,
                path.to_string_lossy()
            );
            conn.execute(&query, [])?;
        }
        "parquet" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_parquet('{}');",
                DUCKDB_DF_TABLE_NAME,
                OXEN_ID_COL,
                path.to_string_lossy()
            );
            conn.execute(&query, [])?;
        }
        "jsonl" | "json" | "ndjson" => {
            let query = format!(
                "CREATE TABLE {DUCKDB_DF_TABLE_NAME} AS SELECT *, CAST(uuid() AS VARCHAR) AS {OXEN_ID_COL} FROM read_json('{path_str}');"
            );
            conn.execute(&query, [])?;

            // Convert STRUCT columns to JSON to avoid binding issues
            let alter_query = format!(
                "SELECT column_name FROM information_schema.columns WHERE table_name = '{DUCKDB_DF_TABLE_NAME}' AND data_type LIKE 'STRUCT%'"
            );
            let mut stmt = conn.prepare(&alter_query)?;
            let struct_cols: Vec<String> = stmt
                .query_map([], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();

            for col in struct_cols {
                let alter =
                    format!("ALTER TABLE {DUCKDB_DF_TABLE_NAME} ALTER COLUMN \"{col}\" TYPE JSON");
                conn.execute(&alter, [])?;
            }

            // Convert JSON[] columns to VARCHAR[]. `read_json` types a list as
            // JSON[] when the element type can't be inferred (e.g. every row
            // has `[]`, or elements are mixed scalars). JSON[] survives the
            // write path but corrupts the read path: each element is stored
            // as a JSON value (so a string element becomes the JSON string
            // `"foo"` with literal quotes), and polars/arrow surface those
            // quoted forms as plain VARCHARs, which `JsonWriter` then escapes
            // again. The result is one extra layer of `\"` per round-trip,
            // compounding for any flow that reads existing list elements and
            // writes them back.
            let alter_query = format!(
                "SELECT column_name FROM information_schema.columns WHERE table_name = '{DUCKDB_DF_TABLE_NAME}' AND data_type = 'JSON[]'"
            );
            let mut stmt = conn.prepare(&alter_query)?;
            let json_list_cols: Vec<String> = stmt
                .query_map([], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();

            for col in json_list_cols {
                let alter = format!(
                    "ALTER TABLE {DUCKDB_DF_TABLE_NAME} ALTER COLUMN \"{col}\" TYPE VARCHAR[] \
                     USING list_transform(\"{col}\", lambda x: json_extract_string(x, '$'))"
                );
                conn.execute(&alter, [])?;
            }
        }
        _ => {
            return Err(OxenError::basic_str(
                "Invalid file type: expected .csv, .tsv, .parquet, .jsonl, .json, .ndjson",
            ));
        }
    }

    let add_default_query = format!(
        "ALTER TABLE {DUCKDB_DF_TABLE_NAME} ALTER COLUMN {OXEN_ID_COL} SET DEFAULT CAST(uuid() AS VARCHAR);"
    );

    conn.execute(&add_default_query, [])?;

    let add_row_id_query = format!(
        "ALTER TABLE {DUCKDB_DF_TABLE_NAME} ADD COLUMN {OXEN_ROW_ID_COL} INTEGER DEFAULT nextval('{counter}');"
    );
    conn.execute(&add_row_id_query, [])?;

    Ok(())
}

pub fn from_clause_from_disk_path(path: &Path) -> Result<String, OxenError> {
    let extension: &str = &util::fs::extension_from_path(path);
    match extension {
        "csv" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_csv('{str_path}')"))
        }
        "tsv" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_csv('{str_path}')"))
        }
        "parquet" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_parquet('{str_path}')"))
        }
        "jsonl" | "json" | "ndjson" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_json('{str_path}')"))
        }
        _ => Err(OxenError::basic_str(
            "Invalid file type: expected .csv, .tsv, .parquet, .jsonl, .json, .ndjson",
        )),
    }
}

pub fn preview(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let table_name = table_name.as_ref();
    let query = format!("SELECT * FROM {table_name} LIMIT 10");
    let df = select_raw(conn, &query)?;
    Ok(df)
}

pub fn record_batches_to_polars_df(records: Vec<RecordBatch>) -> Result<DataFrame, OxenError> {
    if records.is_empty() {
        return Ok(DataFrame::default());
    }

    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, &records[0].schema())?;

    for batch in &records {
        writer.write(batch)?;
    }
    writer.finish()?;

    let content = Cursor::new(buf);
    let df = IpcReader::new(content).finish()?;

    Ok(df)
}

#[cfg(test)]
mod tests {
    use crate::test;

    use super::*;

    #[test]
    fn test_df_db_create() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let conn = get_connection(db_file)?;
            // bounding_box -> min_x, min_y, width, height
            let schema = test::schema_bounding_box();
            let table_name = "bounding_box";
            create_table_if_not_exists(&conn, table_name, &schema)?;

            let num_entries = count(&conn, table_name)?;
            assert_eq!(num_entries, 0);

            Ok(())
        })
    }

    #[test]
    fn test_select_distinct_not_defeated_by_special_columns() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let conn = get_connection(&db_file)?;

            // Create the table with the standard name and an _oxen_id column
            // so add_special_columns will attempt to inject it.
            conn.execute(
                &format!(
                    "CREATE TABLE {TABLE_NAME} (
                        color VARCHAR,
                        {OXEN_ID_COL} VARCHAR DEFAULT (uuid()::VARCHAR),
                        {OXEN_ROW_ID_COL} INTEGER
                    )"
                ),
                [],
            )?;

            // Insert rows with duplicate 'color' values
            conn.execute(
                &format!("INSERT INTO {TABLE_NAME} (color, {OXEN_ROW_ID_COL}) VALUES ('red', 1), ('red', 2), ('blue', 3)"),
                [],
            )?;

            let sql = format!("SELECT DISTINCT color FROM {TABLE_NAME}");
            let df = select_str(&conn, &sql, None)?;

            assert_eq!(df.height(), 2, "DISTINCT should deduplicate 'red': {df:?}");
            Ok(())
        })
    }

    #[test]
    fn test_df_db_get_schema() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let conn = get_connection(db_file)?;
            // bounding_box -> min_x, min_y, width, height
            let schema = test::schema_bounding_box();
            let table_name = "bounding_box";
            create_table_if_not_exists(&conn, table_name, &schema)?;

            let found_schema = get_schema(&conn, table_name)?;
            assert_eq!(found_schema, schema);

            Ok(())
        })
    }

    #[test]
    fn test_get_connection_wal_recovery_removes_wal_and_retries() -> Result<(), OxenError> {
        // Directly tests the WAL recovery path: create a valid db, plant a WAL
        // file that makes open() fail, and verify get_connection removes the WAL
        // and returns a working connection to the original data.
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let wal_file = data_dir.join("data.db.wal");

            // Create a valid database and checkpoint so data is in the main file.
            {
                let conn = duckdb::Connection::open(&db_file)?;
                conn.execute("CREATE TABLE t (val INTEGER)", [])?;
                conn.execute("INSERT INTO t VALUES (99)", [])?;
                conn.execute_batch("CHECKPOINT")?;
            }

            // Disable auto-checkpoint-on-shutdown so DuckDB leaves a real WAL
            // from database A that we can transplant onto B.
            let donor_dir = data_dir.join("donor");
            std::fs::create_dir_all(&donor_dir).expect("create donor dir");
            let donor_db = donor_dir.join("donor.db");
            {
                let conn = duckdb::Connection::open(&donor_db)?;
                conn.execute_batch("PRAGMA disable_checkpoint_on_shutdown")?;
                conn.execute("CREATE TABLE donor (x INT)", [])?;
                conn.execute("INSERT INTO donor VALUES (1)", [])?;
                // Drop — pragma ensures WAL persists on disk.
            }

            let donor_wal = donor_dir.join("donor.db.wal");
            // Plant the donor's WAL onto our target database. This WAL
            // references a different catalog, which can cause open() to fail
            // with a WAL replay error. If DuckDB handles it gracefully instead,
            // get_connection still succeeds — either way the contract holds.
            if donor_wal.exists() {
                std::fs::copy(&donor_wal, &wal_file).expect("plant donor WAL");
                assert!(
                    wal_file.exists(),
                    "WAL should be planted before get_connection"
                );
            } else {
                // DuckDB checkpointed despite the pragma — force the scenario
                // by writing a WAL with enough structure to be attempted.
                // A 64-byte header that doesn't match the db will cause failure.
                let fake_wal = vec![0u8; 64];
                std::fs::write(&wal_file, &fake_wal).expect("write synthetic WAL");
            }

            let conn = get_connection(&db_file)?;

            // The checkpointed data should survive recovery.
            let mut stmt = conn.prepare("SELECT val FROM t")?;
            let val: i64 = stmt.query_row([], |row| row.get(0))?;
            assert_eq!(val, 99, "checkpointed data should survive WAL recovery");

            Ok(())
        })
    }

    #[test]
    fn test_get_connection_removes_db_when_both_corrupt() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let wal_file = data_dir.join("data.db.wal");

            // Write garbage to both the db and WAL files.
            std::fs::write(&db_file, b"not a duckdb file").expect("failed to write corrupt db");
            std::fs::write(&wal_file, b"not a wal file").expect("failed to write corrupt WAL");

            // get_connection should fail but clean up both files so the caller
            // can re-index from scratch.
            let result = get_connection(&db_file);
            assert!(
                result.is_err(),
                "should fail when both db and WAL are corrupt"
            );

            // Both files should be cleaned up.
            assert!(!db_file.exists(), "corrupt db file should be removed");
            assert!(!wal_file.exists(), "corrupt WAL file should be removed");

            Ok(())
        })
    }

    #[test]
    fn test_get_connection_does_not_delete_db_without_wal() -> Result<(), OxenError> {
        // P1 regression test: when open() fails for a non-WAL reason (e.g.
        // corrupt db file with no WAL present), get_connection must NOT delete
        // the database file. Destructive recovery is only appropriate when a
        // WAL file is present, signaling a prior unclean shutdown.
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let wal_file = data_dir.join("data.db.wal");

            // Create a corrupt db file with NO WAL.
            std::fs::write(&db_file, b"not a duckdb file").expect("write corrupt db");
            assert!(!wal_file.exists(), "no WAL should exist for this test");

            let result = get_connection(&db_file);
            assert!(result.is_err(), "should fail with corrupt db");

            // The db file must NOT be deleted — without a WAL there's no
            // evidence this is a recoverable WAL-replay failure.
            assert!(
                db_file.exists(),
                "db file should be preserved when no WAL is present"
            );

            Ok(())
        })
    }

    #[test]
    fn test_get_connection_checkpoints_existing_wal() -> Result<(), OxenError> {
        // Verifies that get_connection runs CHECKPOINT on open, flushing WAL
        // contents into the main db file. We use disable_checkpoint_on_shutdown
        // to guarantee a WAL file exists before get_connection is called.
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let wal_file = data_dir.join("data.db.wal");

            // Create a database with disable_checkpoint_on_shutdown so the WAL
            // persists after close.
            {
                let conn = duckdb::Connection::open(&db_file)?;
                conn.execute_batch("PRAGMA disable_checkpoint_on_shutdown")?;
                conn.execute("CREATE TABLE wal_test (val INTEGER)", [])?;
                conn.execute("INSERT INTO wal_test VALUES (42)", [])?;
                // Drop — WAL should persist due to the pragma.
            }

            // Record the db file size before get_connection. If a WAL exists,
            // CHECKPOINT will flush it and grow the main file.
            let size_before = std::fs::metadata(&db_file).map(|m| m.len()).unwrap_or(0);
            let wal_existed = wal_file.exists();

            // Open via get_connection, which runs CHECKPOINT on open.
            let conn = get_connection(&db_file)?;

            // The data must be accessible.
            let mut stmt = conn.prepare("SELECT val FROM wal_test")?;
            let val: i64 = stmt.query_row([], |row| row.get(0))?;
            assert_eq!(
                val, 42,
                "WAL data should be preserved after checkpoint-on-open"
            );

            // If the WAL existed before get_connection, verify CHECKPOINT had
            // an observable effect: the main db file should have grown because
            // the WAL contents were flushed into it.
            if wal_existed {
                drop(conn);
                let size_after = std::fs::metadata(&db_file).map(|m| m.len()).unwrap_or(0);
                assert!(
                    size_after > size_before,
                    "db file should grow after CHECKPOINT flushes WAL \
                     (before: {size_before}, after: {size_after})"
                );
            }

            Ok(())
        })
    }

    #[test]
    fn test_wal_path_for() {
        let db_path = Path::new("/some/dir/db");
        let wal = wal_path_for(db_path);
        assert_eq!(wal, PathBuf::from("/some/dir/db.wal"));
    }

    #[test]
    fn test_with_df_db_manager_recovers_after_corrupt_wal() -> Result<(), OxenError> {
        test::run_empty_dir_test(|data_dir| {
            let db_file = data_dir.join("data.db");
            let wal_file = data_dir.join("data.db.wal");

            // Create a valid database with data and checkpoint it.
            {
                let conn = get_connection(&db_file)?;
                conn.execute(
                    &format!("CREATE TABLE {TABLE_NAME} (id INTEGER, name VARCHAR)"),
                    [],
                )?;
                conn.execute(&format!("INSERT INTO {TABLE_NAME} VALUES (1, 'test')"), [])?;
                conn.execute_batch("CHECKPOINT")?;
            }

            // Evict from cache so the next access creates a fresh connection.
            remove_df_db_from_cache(&db_file)?;

            // Write a corrupt WAL to simulate a container kill.
            std::fs::write(&wal_file, b"corrupt WAL data").expect("failed to write corrupt WAL");

            // with_df_db_manager should open a new connection (triggering
            // recovery in get_connection) and the operation should succeed.
            let exists = with_df_db_manager(&db_file, |manager| {
                manager.with_conn(|conn| table_exists(conn, TABLE_NAME))
            })?;

            assert!(
                exists,
                "table should exist after WAL recovery through with_df_db_manager"
            );

            // Clean up cache for other tests.
            remove_df_db_from_cache(&db_file)?;
            Ok(())
        })
    }

    /// Round-tripping a list of strings through `rows::modify_row` on a list
    /// column inferred from a JSONL with empty arrays must not add a
    /// JSON-string wrapper around each element. Without the fix in
    /// `index_file_with_id`, every write adds one layer: `["a"]` is stored
    /// as `["\"a\""]`, and any merge-and-write flow doubles it.
    ///
    /// `read_json` types `[]`-only columns as JSON[]. JSON[] survives the
    /// write path but corrupts the read path (DuckDB stores each element as
    /// a JSON value, polars surfaces that as a quoted VARCHAR, JsonWriter
    /// escapes the quotes again). The fix rewrites JSON[] columns to
    /// VARCHAR[] at index time. This test goes through that path end-to-end.
    #[test]
    fn test_rows_modify_row_round_trip_preserves_json_array_strings() -> Result<(), OxenError> {
        use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL};
        use crate::core::db::data_frames::rows;
        use crate::model::staged_row_status::StagedRowStatus;

        test::run_empty_dir_test(|data_dir| {
            let jsonl_path = data_dir.join("data.jsonl");
            // Two rows, all empty lists — forces read_json to type the
            // column as JSON[] (no element type to infer).
            std::fs::write(
                &jsonl_path,
                "{\"name\":\"a\",\"items\":[]}\n{\"name\":\"b\",\"items\":[]}\n",
            )
            .map_err(|e| OxenError::basic_str(format!("write fixture: {e}")))?;

            let db_file = data_dir.join("data.db");
            let conn = get_connection(&db_file)?;

            // Index via the real production path — `index_file_with_id` is
            // what the workspace controller calls and is where the column
            // type gets locked in.
            index_file_with_id(&jsonl_path, &conn, "jsonl")?;

            // `rows::modify_row` requires the diff-status bookkeeping columns
            // and a non-null status value. The full server flow adds these via
            // `add_row_status_cols` after indexing — replicate inline so this
            // test stays scoped to the type-coercion bug.
            conn.execute(
                &format!(
                    "ALTER TABLE {TABLE_NAME} ADD COLUMN {DIFF_STATUS_COL} VARCHAR DEFAULT '{}'",
                    StagedRowStatus::Unchanged
                ),
                [],
            )?;
            conn.execute(
                &format!("ALTER TABLE {TABLE_NAME} ADD COLUMN {DIFF_HASH_COL} VARCHAR DEFAULT '0'"),
                [],
            )?;
            conn.execute(
                &format!(
                    "UPDATE {TABLE_NAME} \
                       SET {DIFF_STATUS_COL} = '{}', {DIFF_HASH_COL} = '0'",
                    StagedRowStatus::Unchanged
                ),
                [],
            )?;

            // Grab the auto-assigned _oxen_id for row 'a'.
            let row_id: String = conn.query_row(
                &format!("SELECT {OXEN_ID_COL} FROM {TABLE_NAME} WHERE name = 'a'"),
                [],
                |row| row.get(0),
            )?;

            // First write: items -> ["first"]. Build the polars DataFrame
            // via `parse_json_to_df` so the path matches the `rows::update`
            // controller (JSON body → polars via JsonLineReader) — different
            // DataFrame construction routes produce different element-level
            // dtypes that affect the subsequent ToSql binding.
            let mut update_df = tabular::parse_json_to_df(&serde_json::json!({
                "items": ["first"]
            }))?;
            rows::modify_row(&conn, &mut update_df, &row_id)?;

            // Read each element back as a VARCHAR via UNNEST so we don't
            // depend on the column's outer DuckDB serialization (which differs
            // between JSON[] and VARCHAR[] on the way to text).
            let read_items = || -> Result<Vec<String>, OxenError> {
                let mut stmt = conn.prepare(&format!(
                    "SELECT unnest(items) FROM {TABLE_NAME} WHERE name = 'a'"
                ))?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                let mut out = Vec::new();
                for r in rows {
                    out.push(r?);
                }
                Ok(out)
            };

            assert_eq!(
                read_items()?,
                vec!["first".to_string()],
                "after one write, each element should be the bare string, not a JSON-encoded form"
            );

            // Second write: append a new element to the existing list and
            // write back. Exercises the same JsonLineReader path that
            // re-binds existing elements — the case that compounds the bug.
            let mut update_df = tabular::parse_json_to_df(&serde_json::json!({
                "items": ["first", "second"]
            }))?;
            rows::modify_row(&conn, &mut update_df, &row_id)?;

            assert_eq!(
                read_items()?,
                vec!["first".to_string(), "second".to_string()],
                "merge-and-write must not double-encode existing elements"
            );

            // The piece that surfaces in the API: when a row is read back as
            // a polars DataFrame and serialised via
            // `JsonDataFrameView::json_from_df`, each list element should
            // appear as a bare JSON string — not a JSON-encoded JSON string.
            // Polars reads JSON[] elements as VARCHAR-with-quotes, and the
            // writer faithfully escapes the quotes, which is what users
            // observe as `"\"first\""`.
            let mut df = select_raw(
                &conn,
                &format!("SELECT items FROM {TABLE_NAME} WHERE name = 'a'"),
            )?;
            let api_json = crate::view::JsonDataFrameView::json_from_df(&mut df);
            let elems = api_json[0]["items"]
                .as_array()
                .expect("items should be array");
            let elem_strings: Vec<&str> = elems.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(
                elem_strings,
                vec!["first", "second"],
                "API serialization must not preserve JSON-string quoting on JSON[] elements"
            );

            remove_df_db_from_cache(&db_file)?;
            Ok(())
        })
    }
}
