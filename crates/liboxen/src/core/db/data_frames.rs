use strum::VariantNames;

use crate::{
    error::OxenError,
    model::{Schema, staged_row_status::StagedRowStatus},
};

pub mod changes_db;
pub mod column_changes_db;
pub mod columns;
pub mod df_db;
pub mod row_changes_db;
pub mod rows;
pub mod workspace_df_db;

#[derive(Debug, thiserror::Error)]
pub enum DataFrameError {
    #[error("df must have exactly one row to be used for modification")]
    ModifyOnly1Row,

    /// A column name was requested in a dataframe, but no such column exists.
    #[error("Column name not found: {0}")]
    ColumnNameNotFound(String),

    /// A column name already exists in the dataframe's schema and cannot be added again.
    #[error("Column name already exists: {0}")]
    ColumnNameAlreadyExists(String),

    #[error("Failed to create df db directory: {0}")]
    FailCreateDfDbDir(std::io::Error),

    #[error("Failed to open df db: {0}")]
    FailOpenDfDb(Box<Self>),

    #[error("No rows in table {0}")]
    NoRowsInTable(String),

    #[error("Invalid file type: expected .csv, .tsv, .parquet, .jsonl, .json, .ndjson")]
    InvalidFileType,

    #[error("modify_row incompatible_schemas {table_schema:?}\n{df_cols:?}")]
    IncompatibleSchemas {
        table_schema: Schema,
        df_cols: Vec<String>,
    },

    #[error("Diff status column is not a string")]
    DiffStatusColNotStr,

    #[error("Missing diff status column")]
    MissingDiffStatusCol,

    #[error("Diff hash column is not a string")]
    DiffHashColNotStr,

    #[error("Expected {expected} rows to be modified, but got {actual}")]
    UnexpectedModifications { expected: usize, actual: usize },

    #[error("Invalid row status: \"{0}\". Expecting one of: {valid}", valid=<StagedRowStatus as VariantNames>::VARIANTS.join(", "))]
    InvalidRowStatus(String),

    #[error("Row status not found")]
    RowStatusNotFound,

    #[error("DataFrame with UUID {0} not found.")]
    MissingDataFrame(String),

    #[error("Must index embeddings before querying")]
    MustIndexEmbeddings,

    #[error("All embeddings must be the same length")]
    EmbeddingLengthMismatch,

    #[error("Expected Float32Array inside ListArray")]
    ExpectedF32ArrayInside,

    #[error("Expected arrow::datatypes::DataType::Float32 inside List")]
    ExpectedF32,

    #[error("Failed to downcast to FixedSizeListArray")]
    FailFixedSizeDowncast,

    #[error("Column FixedSizeList must be a float32 type")]
    ExpectF32FixedSizeList,

    #[error("Expected arrow::datatypes::DataType::List inside as data type")]
    ExpectListInside,

    #[error("Failed to downcast to ListArray")]
    FailListDowncast,

    #[error("Expected Float64Array inside ListArray")]
    ExpectF64ArrayInside,

    #[error("Column must be a list of float32 or float64")]
    ExpectColFloats,

    #[error("Column must be a list type")]
    ExpectList,

    /// No rows were found for a given SQL query.
    #[error("Query returned no rows")]
    NoRowsFound,

    #[error("Vectors must have a length greater than 0")]
    EmptyEmbedding,

    #[error("Column does not exist in embedding configuration: {0}")]
    ColNotFoundInConfig(String),

    #[error("Failed to read embedding configuration file: {0}")]
    FailReadConfig(Box<OxenError>), // TODO: change to FsError when PR lands

    #[error("Failed to write embedding configuration: {0}")]
    FailWriteConfig(std::io::Error),

    #[error("No SELECT found in query")]
    NoSelectInQuery,

    #[error("No FROM found in query")]
    NoFromInQuery,

    #[error("Dataset is not indexed")]
    NotIndexed,

    #[error("Could not create parent directory for DuckDB file: {0}")]
    CreateParent(Box<OxenError>), // TODO: change to FsError when PR lands

    // #[error("Resource not found: {0}")]
    // ResourceNotFound(String),
    #[error("{0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("{0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("Failed to parse SQL: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),

    #[error("{0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("{0}")]
    SerdeJson(#[from] serde_json::error::Error),

    #[error("{0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("{0}")]
    TomlSer(#[from] toml::ser::Error),
}
