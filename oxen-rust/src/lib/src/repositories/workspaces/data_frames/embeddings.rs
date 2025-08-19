use arrow::array::FixedSizeListArray;
use arrow::array::{Float32Array, Float64Array, ListArray, RecordBatch};
use duckdb;
use polars::frame::DataFrame;

use crate::config::embedding_config::{EmbeddingColumn, EmbeddingStatus};
use crate::config::EmbeddingConfig;
use crate::config::EMBEDDING_CONFIG_FILENAME;
use crate::constants::{EXCLUDE_OXEN_COLS, TABLE_NAME};
use crate::core::db::data_frames::df_db::{self, with_df_db_manager};
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::Workspace;
use crate::opts::{EmbeddingQueryOpts, PaginateOpts};
use crate::{repositories, util};

use std::path::Path;
use std::path::PathBuf;

fn embedding_config_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let parent = path.parent().unwrap();
    parent.join(EMBEDDING_CONFIG_FILENAME)
}

fn embedding_config(
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<EmbeddingConfig, OxenError> {
    let embedding_config = embedding_config_path(workspace, path);
    let config_data = util::fs::read_from_path(&embedding_config)?;
    Ok(toml::from_str(&config_data)?)
}

fn write_embedding_size_to_config(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    column_name: impl AsRef<str>,
    vector_length: usize,
) -> Result<(), OxenError> {
    let embedding_config = embedding_config_path(workspace, path);

    // Try to read existing config, create new one if it doesn't exist
    let config_data = util::fs::read_from_path(&embedding_config).unwrap_or_default();
    let mut config: EmbeddingConfig = if config_data.is_empty() {
        EmbeddingConfig::default()
    } else {
        toml::from_str(&config_data)?
    };

    let column = EmbeddingColumn {
        name: column_name.as_ref().to_string(),
        vector_length,
        status: EmbeddingStatus::InProgress,
    };

    config
        .columns
        .insert(column_name.as_ref().to_string(), column);

    let config_str = toml::to_string(&config)?;
    std::fs::write(embedding_config, config_str)?;
    Ok(())
}

fn update_embedding_status(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    column_name: impl AsRef<str>,
    status: EmbeddingStatus,
) -> Result<(), OxenError> {
    let embedding_config = embedding_config_path(workspace, path);
    let config_data = util::fs::read_from_path(&embedding_config)?;
    let mut config: EmbeddingConfig = toml::from_str(&config_data)?;
    config.columns.get_mut(column_name.as_ref()).unwrap().status = status;
    let config_str = toml::to_string(&config)?;
    std::fs::write(embedding_config, config_str)?;
    Ok(())
}

pub fn list_indexed_columns(
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<Vec<EmbeddingColumn>, OxenError> {
    let Ok(config) = embedding_config(workspace, path) else {
        return Ok(vec![]);
    };
    Ok(config.columns.values().cloned().collect())
}

fn perform_indexing(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    column_name: String,
    vector_length: usize,
) -> Result<(), OxenError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);
    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            // Execute VSS commands separately
            conn.execute("INSTALL vss;", [])?;
            conn.execute("LOAD vss;", [])?;
            conn.execute("SET hnsw_enable_experimental_persistence = true;", [])?;

            // Convert column type
            let sql = format!(
                "ALTER TABLE df ALTER COLUMN {} TYPE FLOAT[{}];",
                column_name, vector_length
            );
            log::debug!("Updating column type: {}", sql);
            conn.execute(&sql, [])?;
            Ok(())
        })
    })?;

    log::debug!(
        "Completed indexing embeddings for column `{}` on {}",
        column_name,
        path.as_ref().display()
    );
    update_embedding_status(workspace, path, column_name, EmbeddingStatus::Complete)?;

    Ok(())
}

pub fn index(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
    use_background_thread: bool,
) -> Result<(), OxenError> {
    let path = path.as_ref().to_path_buf();
    let column = column.as_ref();

    let column_name = column.to_string();
    log::debug!(
        "Indexing embeddings for column: {} using background thread: {}",
        column_name,
        use_background_thread
    );

    let vector_length = get_embedding_length(workspace, &path, column)?;

    if use_background_thread {
        // Clone necessary values for the background thread
        let workspace = workspace.clone();
        let column_name = column_name.clone();
        let path = path.clone();

        // Spawn background thread for VSS setup
        std::thread::spawn(move || {
            if let Err(e) = perform_indexing(&workspace, path, column_name, vector_length) {
                log::error!("Error in background indexing thread: {}", e);
            }
        });
    } else {
        perform_indexing(workspace, path, column_name, vector_length)?;
    }

    Ok(())
}

fn get_embedding_length(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
) -> Result<usize, OxenError> {
    let path = path.as_ref();
    let column = column.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("Embedding index DB Path: {:?}", db_path);
    let result_set = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            // Make sure the existing column is a float vector
            let sql = format!("SELECT {} FROM df LIMIT 1;", column);
            let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
            Ok(result_set)
        })
    })?;
    let Some(item) = result_set.first() else {
        return Err(OxenError::basic_str("No items found"));
    };
    let first_column = item.column(0);
    log::debug!("First column: {:?}", first_column);

    // Check if the column is a list of floats/doubles
    let vector_length = match first_column.data_type() {
        arrow::datatypes::DataType::List(field) => match field.data_type() {
            arrow::datatypes::DataType::Float32 => {
                let array = first_column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| OxenError::basic_str("Failed to downcast to ListArray"))?;
                if let Some(first_value) = array.value(0).as_any().downcast_ref::<Float32Array>() {
                    first_value.len()
                } else {
                    return Err(OxenError::basic_str(
                        "Expected Float32Array inside ListArray",
                    ));
                }
            }
            arrow::datatypes::DataType::Float64 => {
                let array = first_column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| OxenError::basic_str("Failed to downcast to ListArray"))?;
                if let Some(first_value) = array.value(0).as_any().downcast_ref::<Float64Array>() {
                    first_value.len()
                } else {
                    return Err(OxenError::basic_str(
                        "Expected Float64Array inside ListArray",
                    ));
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Column must be a list of float32 or float64",
                ))
            }
        },
        arrow::datatypes::DataType::FixedSizeList(field, size) => match field.data_type() {
            arrow::datatypes::DataType::Float32 => *size as usize,
            _ => {
                return Err(OxenError::basic_str(
                    "Column FixedSizeList must be a float32 type",
                ))
            }
        },
        _ => return Err(OxenError::basic_str("Column must be a list type")),
    };

    log::debug!("Vector length: {}", vector_length);
    // Write the vector length to a file we can use in the query
    write_embedding_size_to_config(workspace, path, column, vector_length)?;
    Ok(vector_length)
}

pub fn embedding_from_query(
    conn: &duckdb::Connection,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    query: &EmbeddingQueryOpts,
) -> Result<(Vec<f32>, usize), OxenError> {
    let path = path.as_ref();
    let column = query.column.clone();
    let query = query.query.clone();
    let sql = format!("SELECT {} FROM df WHERE {};", column, query);
    log::debug!("Executing: {}", sql);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
    // log::debug!("Result set: {:?}", result_set);

    // Read the vector length from the file we wrote in the index function
    let Ok(config) = embedding_config(workspace, path) else {
        return Err(OxenError::basic_str(
            "Must index embeddings before querying",
        ));
    };
    let vector_length = config.columns[&column].vector_length;
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
    schema: &crate::model::data_frame::schema::Schema,
    exclude_cols: bool,
) -> Result<String, OxenError> {
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
    let sql = format!("SELECT {columns_str}, array_cosine_similarity({column}, {embedding_str}::FLOAT[{vector_length}]) as {similarity_column} FROM df ORDER BY {similarity_column} DESC");
    Ok(sql)
}

pub fn similarity_query(
    workspace: &Workspace,
    opts: &EmbeddingQueryOpts,
    exclude_cols: bool,
) -> Result<String, OxenError> {
    let column = opts.column.clone();
    let path = opts.path.clone();
    let similarity_column = opts.name.clone();

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);
    log::debug!("Embedding query DB Path: {:?}", db_path);
    let (avg_embedding, vector_length) = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| embedding_from_query(conn, workspace, path.clone(), opts))
    })?;

    let schema = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| df_db::get_schema(conn, TABLE_NAME))
    })?;

    build_similarity_query_sql(
        &column,
        &similarity_column,
        &avg_embedding,
        vector_length,
        &schema,
        exclude_cols,
    )
}

/// Version of similarity_query that accepts a connection to avoid deadlock issues
pub fn similarity_query_with_conn(
    conn: &duckdb::Connection,
    workspace: &Workspace,
    opts: &EmbeddingQueryOpts,
    exclude_cols: bool,
) -> Result<String, OxenError> {
    let column = opts.column.clone();
    let path = opts.path.clone();
    let similarity_column = opts.name.clone();

    let (avg_embedding, vector_length) = embedding_from_query(conn, workspace, path.clone(), opts)?;
    let schema = df_db::get_schema(conn, TABLE_NAME)?;

    build_similarity_query_sql(
        &column,
        &similarity_column,
        &avg_embedding,
        vector_length,
        &schema,
        exclude_cols,
    )
}

pub fn nearest_neighbors(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
    embedding: Vec<f32>,
    pagination: &PaginateOpts,
    exclude_cols: bool,
) -> Result<DataFrame, OxenError> {
    // Time the query
    let start = std::time::Instant::now();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);

    let column = column.as_ref();
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
            )?;

            // Add pagination
            let limit = pagination.page_size;
            let page_num = if pagination.page_num > 0 {
                pagination.page_num
            } else {
                1
            };
            let offset = (page_num - 1) * limit;
            let sql = format!("{} LIMIT {} OFFSET {}", base_sql, limit, offset);

            // Print just the first 50 characters of the query
            log::debug!("Executing similarity query: {}", &sql);

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
) -> Result<(Vec<RecordBatch>, crate::model::data_frame::schema::Schema), OxenError> {
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
) -> Result<DataFrame, OxenError> {
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
    sql = format!("{} LIMIT {} OFFSET {}", sql, limit, offset);

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

pub fn query(workspace: &Workspace, opts: &EmbeddingQueryOpts) -> Result<DataFrame, OxenError> {
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
    sql = format!("{} LIMIT {} OFFSET {}", sql, limit, offset);

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

fn get_avg_embedding(result_set: Vec<RecordBatch>) -> Result<Vec<f32>, OxenError> {
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
                        .ok_or_else(|| OxenError::basic_str("Failed to downcast to ListArray"))?;
                    if let Some(first_value) =
                        array.value(0).as_any().downcast_ref::<Float32Array>()
                    {
                        embeddings.push(first_value.values().to_vec());
                        if vector_length == 0 {
                            vector_length = first_value.len();
                        } else if first_value.len() != vector_length {
                            return Err(OxenError::basic_str(
                                "All embeddings must be the same length",
                            ));
                        }
                    } else {
                        return Err(OxenError::basic_str(
                            "Expected Float32Array inside ListArray",
                        ));
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Expected arrow::datatypes::DataType::Float32 inside List",
                    ))
                }
            },
            arrow::datatypes::DataType::FixedSizeList(field, _) => match field.data_type() {
                arrow::datatypes::DataType::Float32 => {
                    let array = first_column
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| {
                            OxenError::basic_str("Failed to downcast to FixedSizeListArray")
                        })?;
                    if let Some(first_value) =
                        array.value(0).as_any().downcast_ref::<Float32Array>()
                    {
                        embeddings.push(first_value.values().to_vec());
                        if vector_length == 0 {
                            vector_length = first_value.len();
                        } else if first_value.len() != vector_length {
                            return Err(OxenError::basic_str(
                                "All embeddings must be the same length",
                            ));
                        }
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Column FixedSizeList must be a float32 type",
                    ))
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Expected arrow::datatypes::DataType::List inside as data type",
                ))
            }
        }
    }

    if embeddings.is_empty() {
        return Err(OxenError::NoRowsFound("Query returned no rows".into()));
    }

    if vector_length == 0 {
        return Err(OxenError::basic_str(
            "Vector's must have a length greater than 0",
        ));
    }

    // Average the embeddings along the columns
    let mut avg_embedding = vec![0.0; vector_length];
    for i in 0..vector_length {
        let sum: f32 = embeddings.iter().map(|v| v[i]).sum();
        avg_embedding[i] = sum / embeddings.len() as f32;
    }

    Ok(avg_embedding)
}
