use arrow::array::FixedSizeListArray;
use arrow::array::{Float32Array, Float64Array, ListArray, RecordBatch};
use duckdb;
use polars::frame::DataFrame;

use crate::config::EMBEDDING_CONFIG_FILENAME;
use crate::config::EmbeddingConfig;
use crate::config::embedding_config::{EmbeddingColumn, EmbeddingStatus};
use crate::constants::{EXCLUDE_OXEN_COLS, TABLE_NAME};
use crate::core::db::data_frames::DataFrameError;
use crate::core::db::data_frames::df_db::{self, with_df_db_manager};
use crate::model::data_frame::schema::Field;
use crate::model::{Schema, Workspace};
use crate::opts::{EmbeddingQueryOpts, PaginateOpts};
use crate::{repositories, util};

use sqlparser::ast::{Expr, visit_expressions};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;
use std::path::Path;
use std::path::PathBuf;

fn embedding_config_path(workspace: &Workspace, path: &Path) -> PathBuf {
    let path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let parent = path.parent().unwrap();
    parent.join(EMBEDDING_CONFIG_FILENAME)
}

fn embedding_config(workspace: &Workspace, path: &Path) -> Result<EmbeddingConfig, DataFrameError> {
    let embedding_config = embedding_config_path(workspace, path);
    let config_data = util::fs::read_from_path(&embedding_config)
        .map_err(|e| DataFrameError::FailReadConfig(Box::new(e)))?;
    Ok(toml::from_str(&config_data)?)
}

fn write_embedding_size_to_config(
    workspace: &Workspace,
    path: &Path,
    column_name: &str,
    vector_length: usize,
) -> Result<(), DataFrameError> {
    let embedding_config = embedding_config_path(workspace, path);

    // Try to read existing config, create new one if it doesn't exist
    let config_data = util::fs::read_from_path(&embedding_config).unwrap_or_default();
    let mut config: EmbeddingConfig = if config_data.is_empty() {
        EmbeddingConfig::default()
    } else {
        toml::from_str(&config_data)?
    };

    let column = EmbeddingColumn {
        name: column_name.to_string(),
        vector_length,
        status: EmbeddingStatus::InProgress,
    };

    config.columns.insert(column_name.to_string(), column);

    let config_str = toml::to_string(&config)?;
    std::fs::write(embedding_config, config_str).map_err(DataFrameError::FailWriteConfig)?;
    Ok(())
}

fn update_embedding_status(
    workspace: &Workspace,
    path: &Path,
    column_name: &str,
    status: EmbeddingStatus,
) -> Result<(), DataFrameError> {
    let embedding_config = embedding_config_path(workspace, path);
    let config_data = util::fs::read_from_path(&embedding_config)
        .map_err(|e| DataFrameError::FailReadConfig(Box::new(e)))?;
    let config = {
        let mut config: EmbeddingConfig = toml::from_str(&config_data)?;
        if let Some(existing) = config.columns.get_mut(column_name) {
            existing.status = status;
        } else {
            return Err(DataFrameError::ColNotFoundInConfig(column_name.to_string()));
        }
        config
    };
    let config_str = toml::to_string(&config)?;
    std::fs::write(embedding_config, config_str).map_err(DataFrameError::FailWriteConfig)?;
    Ok(())
}

pub fn list_indexed_columns(
    workspace: &Workspace,
    path: &Path,
) -> Result<Vec<EmbeddingColumn>, DataFrameError> {
    let Ok(config) = embedding_config(workspace, path) else {
        return Ok(vec![]);
    };
    Ok(config.columns.values().cloned().collect())
}

fn perform_indexing(
    workspace: &Workspace,
    path: &Path,
    column_name: &str,
    vector_length: usize,
) -> Result<(), DataFrameError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            // Execute VSS commands separately
            conn.execute("INSTALL vss;", [])?;
            conn.execute("LOAD vss;", [])?;
            conn.execute("SET hnsw_enable_experimental_persistence = true;", [])?;

            // Convert column type
            let sql =
                format!("ALTER TABLE df ALTER COLUMN {column_name} TYPE FLOAT[{vector_length}];");
            log::debug!("Updating column type: {sql}");
            conn.execute(&sql, [])?;
            Ok(())
        })
    })?;

    log::debug!(
        "Completed indexing embeddings for column `{}` on {}",
        column_name,
        path.display()
    );
    update_embedding_status(workspace, path, column_name, EmbeddingStatus::Complete)?;

    Ok(())
}

pub fn index(
    workspace: &Workspace,
    path: &Path,
    column: &str,
    use_background_thread: bool,
) -> Result<(), DataFrameError> {
    log::debug!(
        "Indexing embeddings for column: {column} using background thread: {use_background_thread}"
    );

    let vector_length = get_embedding_length(workspace, path, column)?;

    if use_background_thread {
        // Clone necessary values for the background thread
        let workspace = workspace.clone();
        let path = path.to_path_buf();
        let column = column.to_string();
        // Spawn background thread for VSS setup
        std::thread::spawn(move || {
            if let Err(e) = perform_indexing(&workspace, &path, &column, vector_length) {
                log::error!("Error in background indexing thread: {e}");
            }
        });
    } else {
        perform_indexing(workspace, path, column, vector_length)?;
    }

    Ok(())
}

fn get_embedding_length(
    workspace: &Workspace,
    path: &Path,
    column: &str,
) -> Result<usize, DataFrameError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("Embedding index DB Path: {db_path:?}");
    let result_set = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            // Make sure the existing column is a float vector
            let sql = format!("SELECT {column} FROM df LIMIT 1;");
            let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
            Ok(result_set)
        })
    })?;
    let Some(item) = result_set.first() else {
        return Err(DataFrameError::NoRowsFound);
    };
    let first_column = item.column(0);
    log::debug!("First column: {first_column:?}");

    // Check if the column is a list of floats/doubles
    let vector_length = match first_column.data_type() {
        arrow::datatypes::DataType::List(field) => match field.data_type() {
            arrow::datatypes::DataType::Float32 => {
                let array = first_column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFrameError::FailListDowncast)?;
                if let Some(first_value) = array.value(0).as_any().downcast_ref::<Float32Array>() {
                    first_value.len()
                } else {
                    return Err(DataFrameError::ExpectedF32ArrayInside);
                }
            }
            arrow::datatypes::DataType::Float64 => {
                let array = first_column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFrameError::FailListDowncast)?;
                if let Some(first_value) = array.value(0).as_any().downcast_ref::<Float64Array>() {
                    first_value.len()
                } else {
                    return Err(DataFrameError::ExpectF64ArrayInside);
                }
            }
            _ => {
                return Err(DataFrameError::ExpectColFloats);
            }
        },
        arrow::datatypes::DataType::FixedSizeList(field, size) => match field.data_type() {
            arrow::datatypes::DataType::Float32 => *size as usize,
            _ => {
                return Err(DataFrameError::ExpectF32FixedSizeList);
            }
        },
        _ => return Err(DataFrameError::ExpectList),
    };

    log::debug!("Vector length: {vector_length}");
    // Write the vector length to a file we can use in the query
    write_embedding_size_to_config(workspace, path, column, vector_length)?;
    Ok(vector_length)
}

/// Reject an embedding lookup whose column or WHERE clause references a column
/// that does not exist in the staged table.
///
/// The referenced columns must be validated *before* the SQL reaches DuckDB: a
/// semantically-invalid query (unknown column) passes parsing and fails at bind
/// time, and a bind failure on this path escapes as an uncaught C++ exception
/// that aborts the whole server process instead of surfacing as a
/// `duckdb::Error`.
fn validate_embedding_query_columns(
    schema: &Schema,
    column: &str,
    query: &str,
) -> Result<(), DataFrameError> {
    let has_column = |name: &str| {
        schema
            .fields
            .iter()
            .any(|f| f.name.eq_ignore_ascii_case(name))
    };

    if !has_column(column) {
        return Err(DataFrameError::ColumnNameNotFound(column.to_string()));
    }

    let sql = format!("SELECT {column} FROM {TABLE_NAME} WHERE {query}");
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &sql)?;

    // A bare unquoted word on either side of the comparison parses as a column
    // identifier, so this also rejects unquoted string values — DuckDB would
    // reject them at bind time for the same reason.
    let unknown = visit_expressions(&statements, |expr| {
        let name = match expr {
            Expr::Identifier(ident) => Some(&ident.value),
            Expr::CompoundIdentifier(parts) => parts.first().map(|ident| &ident.value),
            _ => None,
        };
        if let Some(name) = name
            && !has_column(name)
        {
            return ControlFlow::Break(name.clone());
        }
        ControlFlow::Continue(())
    });

    match unknown {
        ControlFlow::Break(name) => Err(DataFrameError::ColumnNameNotFound(name)),
        ControlFlow::Continue(()) => Ok(()),
    }
}

pub fn embedding_from_query(
    conn: &duckdb::Connection,
    workspace: &Workspace,
    path: &Path,
    query: &EmbeddingQueryOpts,
) -> Result<(Vec<f32>, usize), DataFrameError> {
    let column = query.column.clone();
    let query = query.query.clone();

    let schema = df_db::get_schema(conn, TABLE_NAME)?;
    validate_embedding_query_columns(&schema, &column, &query)?;

    let sql = format!("SELECT {column} FROM df WHERE {query};");
    log::debug!("Executing: {sql}");
    let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
    // log::debug!("Result set: {:?}", result_set);

    // Read the vector length from the file we wrote in the index function
    let Ok(config) = embedding_config(workspace, path) else {
        return Err(DataFrameError::MustIndexEmbeddings);
    };
    let vector_length = config
        .columns
        .get(&column)
        .ok_or_else(|| DataFrameError::ColNotFoundInConfig(column.clone()))?
        .vector_length;
    // log::debug!("Vector length: {}", vector_length);
    // Average the embeddings
    let avg_embedding = get_avg_embedding(result_set)?;
    Ok((avg_embedding, vector_length))
}

/// Helper function that contains the common logic for building similarity query SQL
fn build_similarity_query_sql(
    column: &str,
    similarity_column: &str,
    avg_embedding: &[f32],
    vector_length: usize,
    schema: &Schema,
    exclude_cols: bool,
) -> String {
    let embedding_str = format!(
        "[{}]",
        avg_embedding
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",")
    );

    let columns = schema
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .filter(|c| !(EXCLUDE_OXEN_COLS.contains(c) && exclude_cols))
        .collect::<Vec<&str>>();

    let columns_str = columns.join(", ");
    format!(
        "SELECT {columns_str}, array_cosine_similarity({column}, {embedding_str}::FLOAT[{vector_length}]) as {similarity_column} FROM df ORDER BY {similarity_column} DESC"
    )
}

pub fn similarity_query(
    workspace: &Workspace,
    opts: &EmbeddingQueryOpts,
    exclude_cols: bool,
) -> Result<String, DataFrameError> {
    let column = opts.column.clone();
    let path = opts.path.clone();
    let similarity_column = opts.name.clone();

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);
    log::debug!("Embedding query DB Path: {db_path:?}");
    let (avg_embedding, vector_length) = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| embedding_from_query(conn, workspace, &path, opts))
    })?;

    let schema = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| Ok(df_db::get_schema(conn, TABLE_NAME)?))
    })?;

    Ok(build_similarity_query_sql(
        &column,
        &similarity_column,
        &avg_embedding,
        vector_length,
        &schema,
        exclude_cols,
    ))
}

/// Version of similarity_query that accepts a connection to avoid deadlock issues
pub fn similarity_query_with_conn(
    conn: &duckdb::Connection,
    workspace: &Workspace,
    opts: &EmbeddingQueryOpts,
    exclude_cols: bool,
) -> Result<String, DataFrameError> {
    let column = opts.column.clone();
    let path = opts.path.clone();
    let similarity_column = opts.name.clone();

    let (avg_embedding, vector_length) = embedding_from_query(conn, workspace, &path, opts)?;
    let schema = df_db::get_schema(conn, TABLE_NAME)?;

    Ok(build_similarity_query_sql(
        &column,
        &similarity_column,
        &avg_embedding,
        vector_length,
        &schema,
        exclude_cols,
    ))
}

pub fn nearest_neighbors(
    workspace: &Workspace,
    path: &Path,
    column: &str,
    embedding: Vec<f32>,
    pagination: &PaginateOpts,
    exclude_cols: bool,
) -> Result<DataFrame, DataFrameError> {
    // Time the query
    let start = std::time::Instant::now();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let vector_length = embedding.len();
    let similarity_column = "similarity";
    let (result_set, mut schema) = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            let schema = df_db::get_schema(conn, TABLE_NAME)?;

            // Build base SQL using helper function
            let base_sql = build_similarity_query_sql(
                column,
                similarity_column,
                &embedding,
                vector_length,
                &schema,
                exclude_cols,
            );

            // Add pagination
            let limit = pagination.page_size;
            let page_num = if pagination.page_num > 0 {
                pagination.page_num
            } else {
                1
            };
            let offset = (page_num - 1) * limit;
            let sql = format!("{base_sql} LIMIT {limit} OFFSET {offset}");

            // Print just the first 50 characters of the query
            log::debug!("Executing similarity query: {}", sql);

            let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
            Ok((result_set, schema))
        })
    })?;
    log::debug!("Similarity query took: {:?}", start.elapsed());

    schema.fields.push(Field::new(similarity_column, "f32"));

    let start = std::time::Instant::now();
    log::debug!("Serializing similarity query to Polars");
    let df = df_db::record_batches_to_polars_df(result_set)?;
    log::debug!(
        "Serializing similarity query to Polars took: {:?}",
        start.elapsed()
    );
    Ok(df)
}

/// Helper function that contains the common logic for executing similarity queries
fn execute_similarity_query(
    conn: &duckdb::Connection,
    sql: &str,
    similarity_column: &str,
) -> Result<(Vec<RecordBatch>, Schema), DataFrameError> {
    let result_set: Vec<RecordBatch> = conn.prepare(sql)?.query_arrow([])?.collect();
    let mut schema = df_db::get_schema(conn, TABLE_NAME)?;
    schema.fields.push(Field::new(similarity_column, "f32"));
    Ok((result_set, schema))
}

/// Version of query that accepts a connection to avoid deadlock issues
pub fn query_with_conn(
    conn: &duckdb::Connection,
    workspace: &Workspace,
    opts: &EmbeddingQueryOpts,
) -> Result<DataFrame, DataFrameError> {
    let similarity_column = opts.name.clone();

    // Get the base SQL using the connection
    let mut sql = similarity_query_with_conn(conn, workspace, opts, false)?;

    // Add LIMIT to the query, otherwise it will be slow to deserialize
    let limit = opts.pagination.page_size;
    let page_num = if opts.pagination.page_num > 0 {
        opts.pagination.page_num
    } else {
        1
    };
    let offset = (page_num - 1) * limit;
    sql = format!("{sql} LIMIT {limit} OFFSET {offset}");

    // Print just the first 50 characters of the query
    log::debug!("Executing similarity query: {}", &sql[..50]);

    // Time the query
    let start = std::time::Instant::now();
    let (result_set, _schema) = execute_similarity_query(conn, &sql, &similarity_column)?;
    log::debug!("Similarity query took: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    log::debug!("Serializing similarity query to Polars");
    let df = df_db::record_batches_to_polars_df(result_set)?;
    log::debug!(
        "Serializing similarity query to Polars took: {:?}",
        start.elapsed()
    );
    Ok(df)
}

pub fn query(
    workspace: &Workspace,
    opts: &EmbeddingQueryOpts,
) -> Result<DataFrame, DataFrameError> {
    let path = opts.path.clone();
    let similarity_column = opts.name.clone();

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);
    let mut sql = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| similarity_query_with_conn(conn, workspace, opts, false))
    })?;

    // Add LIMIT to the query, otherwise it will be slow to deserialize
    let limit = opts.pagination.page_size;
    let page_num = if opts.pagination.page_num > 0 {
        opts.pagination.page_num
    } else {
        1
    };
    let offset = (page_num - 1) * limit;
    sql = format!("{sql} LIMIT {limit} OFFSET {offset}");

    // Print just the first 50 characters of the query
    log::debug!("Executing similarity query: {}", &sql[..50]);
    // Time the query
    let start = std::time::Instant::now();
    let (result_set, _schema) = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| execute_similarity_query(conn, &sql, &similarity_column))
    })?;
    log::debug!("Similarity query took: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    log::debug!("Serializing similarity query to Polars");
    let df = df_db::record_batches_to_polars_df(result_set)?;
    log::debug!(
        "Serializing similarity query to Polars took: {:?}",
        start.elapsed()
    );
    Ok(df)
}

fn get_avg_embedding(result_set: Vec<RecordBatch>) -> Result<Vec<f32>, DataFrameError> {
    let mut embeddings: Vec<Vec<f32>> = Vec::new();
    let mut vector_length = 0;
    for batch in result_set {
        let first_column = batch.column(0);
        match first_column.data_type() {
            arrow::datatypes::DataType::List(field) => match field.data_type() {
                arrow::datatypes::DataType::Float32 => {
                    let array = first_column
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| DataFrameError::FailListDowncast)?;
                    if let Some(first_value) =
                        array.value(0).as_any().downcast_ref::<Float32Array>()
                    {
                        embeddings.push(first_value.values().to_vec());
                        if vector_length == 0 {
                            vector_length = first_value.len();
                        } else if first_value.len() != vector_length {
                            return Err(DataFrameError::EmbeddingLengthMismatch);
                        }
                    } else {
                        return Err(DataFrameError::ExpectedF32ArrayInside);
                    }
                }
                _ => {
                    return Err(DataFrameError::ExpectedF32);
                }
            },
            arrow::datatypes::DataType::FixedSizeList(field, _) => match field.data_type() {
                arrow::datatypes::DataType::Float32 => {
                    let array = first_column
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| DataFrameError::FailFixedSizeDowncast)?;
                    if let Some(first_value) =
                        array.value(0).as_any().downcast_ref::<Float32Array>()
                    {
                        embeddings.push(first_value.values().to_vec());
                        if vector_length == 0 {
                            vector_length = first_value.len();
                        } else if first_value.len() != vector_length {
                            return Err(DataFrameError::EmbeddingLengthMismatch);
                        }
                    }
                }
                _ => {
                    return Err(DataFrameError::ExpectF32FixedSizeList);
                }
            },
            _ => {
                return Err(DataFrameError::ExpectListInside);
            }
        }
    }

    if embeddings.is_empty() {
        return Err(DataFrameError::NoRowsFound);
    }

    if vector_length == 0 {
        return Err(DataFrameError::EmptyEmbedding);
    }

    // Average the embeddings along the columns
    let mut avg_embedding = vec![0.0; vector_length];
    for i in 0..vector_length {
        let sum: f32 = embeddings.iter().map(|v| v[i]).sum();
        avg_embedding[i] = sum / embeddings.len() as f32;
    }

    Ok(avg_embedding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::data_frame::schema::Field;

    fn schema_with(names: &[&str]) -> Schema {
        Schema::new(
            names
                .iter()
                .map(|name| Field::new(name, "str"))
                .collect::<Vec<Field>>(),
        )
    }

    #[test]
    fn test_validate_embedding_query_accepts_known_columns() {
        let schema = schema_with(&["id", "embedding", "prompt"]);
        assert!(validate_embedding_query_columns(&schema, "embedding", "id = 1").is_ok());
        assert!(
            validate_embedding_query_columns(&schema, "embedding", "prompt = 'hello'").is_ok()
        );
        // Column matching is case-insensitive, like DuckDB's binder.
        assert!(validate_embedding_query_columns(&schema, "EMBEDDING", "ID = 1").is_ok());
    }

    #[test]
    fn test_validate_embedding_query_rejects_unknown_columns() {
        let schema = schema_with(&["id", "embedding"]);

        // Unknown column in the WHERE clause.
        let result = validate_embedding_query_columns(&schema, "embedding", "nope = 1");
        assert!(matches!(result, Err(DataFrameError::ColumnNameNotFound(name)) if name == "nope"));

        // Unknown embedding column itself.
        let result = validate_embedding_query_columns(&schema, "nope", "id = 1");
        assert!(matches!(result, Err(DataFrameError::ColumnNameNotFound(name)) if name == "nope"));

        // An unquoted value parses as an identifier and is rejected, matching
        // what DuckDB's binder would do.
        let result = validate_embedding_query_columns(&schema, "embedding", "id = test");
        assert!(matches!(result, Err(DataFrameError::ColumnNameNotFound(name)) if name == "test"));
    }

    #[test]
    fn test_validate_embedding_query_rejects_unparseable_sql() {
        let schema = schema_with(&["id", "embedding"]);
        let result = validate_embedding_query_columns(&schema, "embedding", "id = = 1");
        assert!(matches!(result, Err(DataFrameError::SqlParse(_))));
    }
}
