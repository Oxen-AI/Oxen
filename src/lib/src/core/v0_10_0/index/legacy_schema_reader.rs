use crate::constants::{FILES_DIR, HISTORY_DIR, SCHEMAS_DIR};
use crate::core::db;
use crate::core::db::key_val::{str_json_db, str_val_db};
use crate::error::OxenError;
use crate::model::Schema;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str;

use crate::model::LocalRepository;

pub struct LegacySchemaReader {
    schema_db: DBWithThreadMode<MultiThreaded>,
    schema_files_db: DBWithThreadMode<MultiThreaded>,
}

impl LegacySchemaReader {
    pub fn schemas_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/schemas/schemas
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR) // double schemas/schemas is intentional because we have multiple dirs at this level
            .join(SCHEMAS_DIR)
    }

    pub fn schema_files_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        // .oxen/history/COMMIT_ID/schemas/files
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR)
            .join(FILES_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
    ) -> Result<LegacySchemaReader, OxenError> {
        let schema_db_path = LegacySchemaReader::schemas_db_dir(repository, commit_id);
        log::debug!("LegacySchemaReader db {:?}", schema_db_path);
        let schema_files_db_path = LegacySchemaReader::schema_files_db_dir(repository, commit_id);
        log::debug!("LegacySchemaReader files db {:?}", schema_files_db_path);
        let opts = db::key_val::opts::default();
        if !schema_db_path.exists() {
            std::fs::create_dir_all(&schema_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&schema_db_path))?;
        }

        if !schema_files_db_path.exists() {
            std::fs::create_dir_all(&schema_files_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&schema_files_db_path))?;
        }

        Ok(LegacySchemaReader {
            schema_db: DBWithThreadMode::open_for_read_only(&opts, &schema_db_path, false)?,
            schema_files_db: DBWithThreadMode::open_for_read_only(
                &opts,
                &schema_files_db_path,
                false,
            )?,
        })
    }

    /// See if a commit id exists
    pub fn schema_hash_exists(&self, hash: &str) -> bool {
        str_json_db::has_key(&self.schema_db, hash)
    }

    /// Get a commit object from an ID
    pub fn get_schema_by_hash<S: AsRef<str>>(&self, hash: S) -> Result<Option<Schema>, OxenError> {
        str_json_db::get(&self.schema_db, hash)
    }

    pub fn get_schema_hash_for_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<String>, OxenError> {
        str_val_db::get(
            &self.schema_files_db,
            path.as_ref().to_str().unwrap().replace('\\', "/"),
        )
    }

    pub fn get_schema_for_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Option<Schema>, OxenError> {
        if let Some(hash) = self.get_schema_hash_for_file(path)? {
            return self.get_schema_by_hash(hash);
        }
        Ok(None)
    }

    pub fn list_schemas(&self) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        // This is a map of paths to schema hashes
        let paths_to_hashes: HashMap<String, String> = str_val_db::hash_map(&self.schema_files_db)?;

        // This is a map of hashes to schemas
        let hash_to_schemas: HashMap<String, Schema> = str_json_db::hash_map(&self.schema_db)?;

        // For each path, get the schema
        let path_vals: HashMap<PathBuf, Schema> = paths_to_hashes
            .iter()
            .map(|(k, v)| (PathBuf::from(k), hash_to_schemas.get(v).unwrap().clone()))
            .collect();
        Ok(path_vals)
    }

    pub fn list_schemas_for_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        let path = path.as_ref();
        // This is a map of paths to schema hashes
        let paths_to_hashes: HashMap<String, String> = str_val_db::hash_map(&self.schema_files_db)?;

        // This is a map of hashes to schemas
        let hash_to_schemas: HashMap<String, Schema> = str_json_db::hash_map(&self.schema_db)?;

        // For each path, get the schema
        let path_vals: HashMap<PathBuf, Schema> = paths_to_hashes
            .iter()
            .map(|(k, v)| (PathBuf::from(k), hash_to_schemas.get(v).unwrap().clone()))
            .filter(|(k, v)| {
                k.to_string_lossy() == path.to_string_lossy() || v.hash == path.to_string_lossy()
            })
            .collect();
        Ok(path_vals)
    }
}
