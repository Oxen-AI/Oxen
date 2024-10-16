use crate::error::OxenError;
use crate::model::entry::commit_entry::SchemaEntry;
use crate::model::workspace::Workspace;
use crate::model::{LocalRepository, Schema};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub mod duckdb_schema_reader;
pub mod objects_schema_reader;

use duckdb_schema_reader::DuckDBSchemaReader;
use objects_schema_reader::ObjectsSchemaReader;

pub enum SchemaReader {
    DuckDB(DuckDBSchemaReader),
    Objects(ObjectsSchemaReader),
}

impl SchemaReader {
    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<Self, OxenError> {
        Ok(SchemaReader::Objects(ObjectsSchemaReader::new(
            repository, commit_id,
        )?))
    }

    pub fn new_from_workspace(
        repository: &LocalRepository,
        commit_id: &str,
        workspace: &Workspace,
    ) -> Result<Self, OxenError> {
        Ok(SchemaReader::DuckDB(DuckDBSchemaReader::new(
            repository,
            commit_id,
            workspace.clone(),
        )?))
    }

    pub fn new_from_head(repository: &LocalRepository) -> Result<Self, OxenError> {
        Ok(SchemaReader::Objects(ObjectsSchemaReader::new_from_head(
            repository,
        )?))
    }

    pub fn new_from_head_with_workspace(
        repository: &LocalRepository,
        workspace: &Workspace,
    ) -> Result<Self, OxenError> {
        Ok(SchemaReader::DuckDB(DuckDBSchemaReader::new_from_head(
            repository,
            workspace.clone(),
        )?))
    }

    pub fn get_schema_for_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<Schema>, OxenError> {
        match self {
            SchemaReader::DuckDB(reader) => reader.get_schema_for_file(path),
            SchemaReader::Objects(reader) => reader.get_schema_for_file(path),
        }
    }

    pub fn list_schemas(&self) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        match self {
            SchemaReader::DuckDB(_) => Err(OxenError::basic_str(
                "list_schemas is not implemented for DuckDBSchemaReader",
            )),
            SchemaReader::Objects(reader) => reader.list_schemas(),
        }
    }

    pub fn list_schema_entries(&self) -> Result<Vec<SchemaEntry>, OxenError> {
        match self {
            SchemaReader::DuckDB(_) => Err(OxenError::basic_str(
                "list_schema_entries is not implemented for DuckDBSchemaReader",
            )),
            SchemaReader::Objects(reader) => reader.list_schema_entries(),
        }
    }

    pub fn list_schemas_for_ref(
        &self,
        schema_ref: impl AsRef<str>,
    ) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        match self {
            SchemaReader::DuckDB(_) => Err(OxenError::basic_str(
                "list_schemas_for_ref is not implemented for DuckDBSchemaReader",
            )),
            SchemaReader::Objects(reader) => reader.list_schemas_for_ref(schema_ref),
        }
    }
}
