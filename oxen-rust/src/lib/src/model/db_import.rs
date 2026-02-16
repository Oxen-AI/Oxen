use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::OxenError;

/// Supported external database types for import.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DbType {
    Postgres,
    MySQL,
    SQLite,
}

impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbType::Postgres => write!(f, "postgres"),
            DbType::MySQL => write!(f, "mysql"),
            DbType::SQLite => write!(f, "sqlite"),
        }
    }
}

impl DbType {
    /// Parse the database type from a connection URL scheme.
    pub fn from_url(url: &str) -> Result<Self, OxenError> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(DbType::Postgres)
        } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Ok(DbType::MySQL)
        } else if url.starts_with("sqlite://") {
            Ok(DbType::SQLite)
        } else {
            let scheme = url.split("://").next().unwrap_or(url);
            Err(OxenError::db_import_error(format!(
                "Unsupported database type: {scheme}"
            )))
        }
    }
}

/// Configuration for importing data from an external database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbImportConfig {
    /// The type of database to connect to.
    pub db_type: DbType,
    /// The connection URL (e.g. "postgres://user@host/db").
    pub connection_url: String,
    /// Optional password (appended to URL or used separately).
    pub password: Option<String>,
    /// The SQL SELECT query to run against the database.
    pub query: String,
    /// Target path in the repo (e.g. "data/results.csv").
    pub output_path: PathBuf,
    /// Output format: "csv", "parquet", "jsonl", etc.
    pub output_format: String,
    /// Number of rows per batch when streaming from the DB.
    pub batch_size: usize,
}

impl DbImportConfig {
    pub const DEFAULT_BATCH_SIZE: usize = 10_000;

    /// Build a config, inferring db_type from the connection URL.
    pub fn new(
        connection_url: impl Into<String>,
        password: Option<String>,
        query: impl Into<String>,
        output_path: impl Into<PathBuf>,
        output_format: impl Into<String>,
        batch_size: Option<usize>,
    ) -> Result<Self, OxenError> {
        let connection_url = connection_url.into();
        let db_type = DbType::from_url(&connection_url)?;
        Ok(Self {
            db_type,
            connection_url,
            password,
            query: query.into(),
            output_path: output_path.into(),
            output_format: output_format.into(),
            batch_size: batch_size.unwrap_or(Self::DEFAULT_BATCH_SIZE),
        })
    }

    /// Build the full connection URL with password embedded if provided.
    pub fn full_connection_url(&self) -> String {
        if let Some(ref password) = self.password {
            // Insert password into the URL if not already present
            if let Ok(mut parsed) = url::Url::parse(&self.connection_url) {
                if parsed.password().is_none() {
                    let _ = parsed.set_password(Some(password));
                    return parsed.to_string();
                }
            }
        }
        self.connection_url.clone()
    }
}

/// Result of a database import operation.
#[derive(Debug, Clone)]
pub struct DbImportResult {
    /// Total number of rows imported.
    pub total_rows: usize,
    /// Number of columns in the result set.
    pub num_columns: usize,
}
