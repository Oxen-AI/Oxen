//! Abstraction over DuckDB database to write and read dataframes from disk.
//!

use crate::constants::{
    DEFAULT_PAGE_SIZE, DUCKDB_DF_TABLE_NAME, OXEN_COLS, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME,
};

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
use duckdb::{params, ToSql};
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
    let _ = instances.pop(&db_path); // drop immediately
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
            if let Some(parent) = db_path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        log::error!("Failed to create df db directory: {}", e);
                        OxenError::basic_str(format!("Failed to create df db directory: {}", e))
                    })?;
                }
            }

            let conn = get_connection(&db_path).map_err(|e| {
                log::error!("Failed to open df db: {}", e);
                OxenError::basic_str(format!("Failed to open df db: {}", e))
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
pub fn get_connection(path: impl AsRef<Path>) -> Result<duckdb::Connection, OxenError> {
    let path = path.as_ref();
    log::debug!(
        "get_connection: Opening new DuckDB connection for path: {:?}",
        path
    );

    if let Some(parent) = path.parent() {
        log::debug!(
            "get_connection: Ensuring parent directory exists: {:?}",
            parent
        );
        util::fs::create_dir_all(parent)?;
    }

    let conn = duckdb::Connection::open(path)?;
    log::info!(
        "get_connection: Successfully created new DuckDB connection for path: {:?}",
        path
    );
    Ok(conn)
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
    let sql = format!("DROP TABLE IF EXISTS {}", table_name);
    log::debug!("drop_table sql: {}", sql);
    conn.execute(&sql, []).map_err(OxenError::from)?;
    Ok(())
}

pub fn table_exists(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
) -> Result<bool, OxenError> {
    log::debug!("checking exists in path {:?}", conn);
    let table_name = table_name.as_ref();
    let sql = "SELECT EXISTS (SELECT 1 FROM duckdb_tables WHERE table_name = ?) AS table_exists";
    let mut stmt = conn.prepare(sql)?;
    let exists: bool = stmt.query_row(params![table_name], |row| row.get(0))?;
    log::debug!("got exists: {}", exists);
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
    let sql = format!("CREATE TABLE IF NOT EXISTS {} (\n{});", table_name, columns);
    log::debug!("create_table sql: {}", sql);
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
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name == '{}'",
        table_name
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
        table_name, cols.iter().map(|col| format!("'{}'", col.replace('\'', "''"))).collect::<Vec<String>>().join(", ")
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
    let sql = format!("SELECT count(*) FROM {}", table_name);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let size: usize = row.get(0)?;
        Ok(size)
    } else {
        Err(OxenError::basic_str(format!(
            "No rows in table {}",
            table_name
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
    let sql = format!("SELECT count(*) FROM {} WHERE {}", table_name, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let size: usize = row.get(0)?;
        Ok(size)
    } else {
        Err(OxenError::basic_str(format!(
            "No rows in table {}",
            table_name
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
    log::debug!("export_sql: {}", export_sql);
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
        sql.push_str(&format!(" ORDER BY \"{}\"", sort_by));
    }

    let pagination_clause = if let Some(page) = opts.page {
        let page = if page == 0 { 1 } else { page };
        let page_size = opts.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
        format!(" LIMIT {} OFFSET {}", page_size, (page - 1) * page_size)
    } else {
        "".to_string()
    };
    sql.push_str(&pagination_clause);
    log::debug!("select_str() running sql: {}", sql);
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

    let stmt = conn.prepare(&query_with_limit);
    let mut stmt = stmt.map_err(|error| OxenError::basic_str(error.to_string()))?;
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

            if let Some(Statement::Query(query)) = ast.get_mut(0) {
                if let ast::SetExpr::Select(select) = &mut *query.body {
                    // Add new columns to the SELECT clause
                    for special_column in special_columns {
                        select
                            .projection
                            .push(SelectItem::UnnamedExpr(SqlExpr::Identifier(
                                special_column.into(),
                            )));
                    }
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
    log::debug!("select_str() got raw df {:?}", df);
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
    let column_names: Vec<String> = schema
        .iter_fields()
        .map(|f| format!("\"{}\"", f.name()))
        .collect();

    let set_clauses: String = column_names
        .iter()
        .map(|col_name| format!("{} = ?", col_name))
        .collect::<Vec<String>>()
        .join(", ");

    let where_clause = format!("\"{}\" = '{}'", OXEN_ID_COL, id);

    let sql = format!(
        "UPDATE {} SET {} WHERE {} RETURNING *",
        table_name, set_clauses, where_clause
    );

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
    let mut column_names: Vec<String> = Vec::new();
    if let Some((_, df)) = row_map.iter().next() {
        let schema = df.schema();
        column_names = schema
            .iter_fields()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();
    }

    for col_name in &column_names {
        let mut case_clauses = Vec::new();
        for (id, df) in row_map.iter() {
            let series = df.column(col_name.trim_matches('"'))?;
            let value = series.get(0)?;

            let boxed_value: Box<dyn ToSql> = Box::new(tabular::value_to_tosql(value));

            case_clauses.push(format!("WHEN \"{}\" = '{}' THEN ?", OXEN_ID_COL, id));

            all_params.push(boxed_value);
        }
        set_clauses.push(format!(
            "{} = CASE {} END",
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
    log::debug!("df_db:index_file() at path {:?}", path);
    let extension: &str = &util::fs::extension_from_path(path);
    let path_str = path.to_string_lossy().to_string();
    match extension {
        "csv" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT * FROM read_csv('{}')",
                DUCKDB_DF_TABLE_NAME, path_str
            );
            conn.execute(&query, [])?;
        }
        "tsv" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT * FROM read_csv('{}')",
                DUCKDB_DF_TABLE_NAME, path_str
            );
            conn.execute(&query, [])?;
        }
        "parquet" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT * FROM read_parquet('{}')",
                DUCKDB_DF_TABLE_NAME, path_str
            );
            conn.execute(&query, [])?;
        }
        "jsonl" | "json" | "ndjson" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT * FROM read_json('{}')",
                DUCKDB_DF_TABLE_NAME, path_str
            );
            conn.execute(&query, [])?;
        }
        _ => {
            return Err(OxenError::basic_str(
                "Invalid file type: expected .csv, .tsv, .parquet, .jsonl, .json, .ndjson",
            ))
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
    log::debug!("df_db:index_file() at path {:?} into path {:?}", path, conn);
    let path_str = path.to_string_lossy().to_string();
    let counter = "counter";
    // Drop sequence if exists
    let drop_sequence_query = format!("DROP SEQUENCE IF EXISTS {}", counter);
    conn.execute(&drop_sequence_query, [])?;

    let add_row_id_sequence_query = format!("CREATE SEQUENCE {} START 1", counter);
    conn.execute(&add_row_id_sequence_query, [])?;

    match extension {
        "csv" => {
            let query = format!("CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_csv('{}', AUTO_DETECT=TRUE, header=True);", DUCKDB_DF_TABLE_NAME, OXEN_ID_COL, path.to_string_lossy());
            conn.execute(&query, [])?;
        }
        "tsv" => {
            let query = format!("CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_csv('{}', AUTO_DETECT=TRUE, header=True);", DUCKDB_DF_TABLE_NAME, OXEN_ID_COL, path.to_string_lossy());
            conn.execute(&query, [])?;
        }
        "parquet" => {
            let query = format!("CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_parquet('{}');", DUCKDB_DF_TABLE_NAME, OXEN_ID_COL, path.to_string_lossy());
            conn.execute(&query, [])?;
        }
        "jsonl" | "json" | "ndjson" => {
            let query = format!(
                "CREATE TABLE {} AS SELECT *, CAST(uuid() AS VARCHAR) AS {} FROM read_json('{}');",
                DUCKDB_DF_TABLE_NAME, OXEN_ID_COL, path_str
            );
            conn.execute(&query, [])?;
        }
        _ => {
            return Err(OxenError::basic_str(
                "Invalid file type: expected .csv, .tsv, .parquet, .jsonl, .json, .ndjson",
            ))
        }
    }

    let add_default_query = format!(
        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT CAST(uuid() AS VARCHAR);",
        DUCKDB_DF_TABLE_NAME, OXEN_ID_COL
    );

    conn.execute(&add_default_query, [])?;

    let add_row_id_query = format!(
        "ALTER TABLE {} ADD COLUMN {} INTEGER DEFAULT nextval('{}');",
        DUCKDB_DF_TABLE_NAME, OXEN_ROW_ID_COL, counter
    );
    conn.execute(&add_row_id_query, [])?;

    Ok(())
}

pub fn from_clause_from_disk_path(path: &Path) -> Result<String, OxenError> {
    let extension: &str = &util::fs::extension_from_path(path);
    match extension {
        "csv" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_csv('{}')", str_path))
        }
        "tsv" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_csv('{}')", str_path))
        }
        "parquet" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_parquet('{}')", str_path))
        }
        "jsonl" | "json" | "ndjson" => {
            let str_path = path.to_string_lossy().to_string();
            Ok(format!("read_json('{}')", str_path))
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
    let query = format!("SELECT * FROM {} LIMIT 10", table_name);
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
}
