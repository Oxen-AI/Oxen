use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::df_db;
use crate::core::v0_10_0::index::CommitReader;
use crate::error::OxenError;
use crate::model::{LocalRepository, Schema, Workspace};
use crate::repositories::workspaces::data_frames::duckdb_path;
use std::path::Path;
use std::str;

pub struct DuckDBSchemaReader {
    workspace: Box<Workspace>,
}

impl DuckDBSchemaReader {
    pub fn new(
        _repository: &LocalRepository,
        _commit_id: &str,
        workspace: Workspace,
    ) -> Result<DuckDBSchemaReader, OxenError> {
        Ok(DuckDBSchemaReader {
            workspace: Box::new(workspace),
        })
    }

    pub fn new_from_head(
        repository: &LocalRepository,
        workspace: Workspace,
    ) -> Result<DuckDBSchemaReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        DuckDBSchemaReader::new(repository, &commit.id, workspace)
    }

    pub fn get_schema_for_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<Schema>, OxenError> {
        let staged_db_path = duckdb_path(&self.workspace, &path);
        let conn = df_db::get_connection(staged_db_path)?;

        let df_schema = df_db::get_schema(&conn, TABLE_NAME)?;

        Ok(Some(df_schema))
    }
}
