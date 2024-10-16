//! Abstraction over DuckDB database to write and read dataframes from disk.
//!

use crate::constants::{
    DEFAULT_PAGE_SIZE, DUCKDB_DF_TABLE_NAME, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME,
};

use crate::core::df::tabular;
use crate::error::OxenError;

use crate::model::data_frame::schema::Field;
use crate::model::data_frame::schema::Schema;
use crate::opts::DFOpts;
use crate::{model, util};
use arrow_json::writer::JsonArray;
use arrow_json::WriterBuilder;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, ToSql};
use polars::prelude::*;
use std::io::Cursor;
use std::path::Path;

use sql_query_builder as sql;

/// Get a connection to a duckdb database.
pub fn get_connection(path: impl AsRef<Path>) -> Result<duckdb::Connection, OxenError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let conn = duckdb::Connection::open(path)?;
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
            model::data_frame::schema::DataType::from_sql(data_type).as_str(),
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
            model::data_frame::schema::DataType::from_sql(data_type).as_str(),
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
    with_explicit_nulls: bool,
    schema: Option<&Schema>,
    opts: Option<&DFOpts>,
) -> Result<DataFrame, OxenError> {
    let sql = stmt.as_string();
    let df = select_str(conn, sql, with_explicit_nulls, schema, opts)?;
    Ok(df)
}

pub fn select_str(
    conn: &duckdb::Connection,
    stmt: String,
    with_explicit_nulls: bool,
    schema: Option<&Schema>,
    opts: Option<&DFOpts>,
) -> Result<DataFrame, OxenError> {
    let mut sql = stmt.clone();
    let empty_opts = DFOpts::empty();
    let opts = opts.unwrap_or(&empty_opts);

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
    let df = select_raw(conn, &sql, with_explicit_nulls, schema)?;
    Ok(df)
}

pub fn select_raw(
    conn: &duckdb::Connection,
    stmt: &str,
    with_explicit_nulls: bool,
    schema: Option<&Schema>,
) -> Result<DataFrame, OxenError> {
    let mut stmt = conn.prepare(stmt)?;

    let records: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    if records.is_empty() {
        return Ok(DataFrame::default());
    }

    let df = if with_explicit_nulls {
        // Provide schema so that it can be filled in with nulls
        let schema = if let Some(schema) = schema {
            schema.clone()
        } else {
            get_schema(conn, TABLE_NAME)?
        };

        record_batches_to_polars_df_explicit_nulls(records, &schema)?
    } else {
        record_batches_to_polars_df(records)?
    };

    Ok(df)
}

pub fn modify_row_with_polars_df(
    conn: &duckdb::Connection,
    table_name: impl AsRef<str>,
    id: &str,
    df: &DataFrame,
    out_schema: &Schema,
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

    let df = record_batches_to_polars_df_explicit_nulls(result_set, out_schema)?;

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
    let df = select_raw(conn, &query, true, None)?;
    Ok(df)
}

fn record_batches_to_polars_df(records: Vec<RecordBatch>) -> Result<DataFrame, OxenError> {
    if records.is_empty() {
        return Ok(DataFrame::default());
    }
    let records: Vec<&RecordBatch> = records.iter().collect();

    let buf = Vec::new();
    let mut writer = arrow_json::writer::ArrayWriter::new(buf);
    writer.write_batches(&records[..])?;
    writer.finish()?;

    let json_bytes = writer.into_inner();

    let content = Cursor::new(json_bytes);

    let df = JsonReader::new(content).finish()?;

    Ok(df)
}

pub fn record_batches_to_polars_df_explicit_nulls(
    records: Vec<RecordBatch>,
    schema: &Schema,
) -> Result<DataFrame, OxenError> {
    if records.is_empty() {
        return Ok(DataFrame::default());
    }

    let records: Vec<&RecordBatch> = records.iter().collect::<Vec<_>>();
    let buf = Vec::new();
    let builder = WriterBuilder::new().with_explicit_nulls(true);
    let mut writer = builder.build::<_, JsonArray>(buf);
    writer.write_batches(&records[..]).unwrap();
    writer.finish().unwrap();
    let json_bytes = writer.into_inner();

    let content = Cursor::new(json_bytes);

    let df = JsonReader::new(content)
        .with_schema(Arc::new(schema.to_polars()))
        .finish()?;

    Ok(df)
}
#[cfg(test)]
mod tests {
    use crate::test;
    // use sql_query_builder as sql;

    use super::*;

    /*
    #[test]
    fn test_df_db_count() -> Result<(), OxenError> {
        // TODO: Create this db file in a temp dir
        let db_file = Path::new("data")
            .join("test")
            .join("db")
            .join("metadata.db");
        let conn = get_connection(db_file)?;

        let count = count(&conn, "metadata")?;

        assert_eq!(count, 16);

        Ok(())
    }

    #[test]
    fn test_df_db_select() -> Result<(), OxenError> {
        let db_file = Path::new("data")
            .join("test")
            .join("db")
            .join("metadata.db");
        let conn = get_connection(db_file)?;

        let offset = 0;
        let limit = 7;
        let fields = ["filename", "data_type"];

        let stmt = sql::Select::new()
            .select(&fields.join(", "))
            .offset(&offset.to_string())
            .limit(&limit.to_string())
            .from("metadata");

        let df = select(&conn, &stmt)?;

        assert!(df.width() == fields.len());
        assert!(df.height() == limit);

        Ok(())
    }
     */

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
