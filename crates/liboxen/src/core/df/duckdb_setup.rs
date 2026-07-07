//! One-shot DuckDB extension installation.

use crate::error::OxenError;

/// Install the JSON extension into DuckDB's user extensions directory so subsequent
/// connections load it without triggering the autoload flow.
///
/// Call once at startup before any parallel DuckDB use. On Windows, concurrent first-use
/// autoloads race on the extension file's temp→final rename (`Could not move file: Access
/// is denied`); preinstalling closes that window.
pub fn preload_extensions() -> Result<(), OxenError> {
    let conn = duckdb::Connection::open_in_memory().map_err(|e| {
        OxenError::basic_str(format!(
            "failed to open in-memory DuckDB connection for extension preload: {e}"
        ))
    })?;
    conn.execute_batch("INSTALL json;")
        .map_err(|e| OxenError::basic_str(format!("failed to install DuckDB json extension: {e}")))
}
