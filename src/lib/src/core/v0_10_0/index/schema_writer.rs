use crate::core::db::key_val::str_json_db;
use crate::core::db::{self};
use crate::error::OxenError;
use crate::model::Schema;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::Path;
use std::str;

use crate::core::v0_10_0::index::schema_reader::objects_schema_reader;

use crate::model::LocalRepository;

use super::versioner;

pub struct SchemaWriter {
    db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
}

impl SchemaWriter {
    pub fn new(repository: &LocalRepository, commit_id: &str) -> Result<SchemaWriter, OxenError> {
        let db_path =
            objects_schema_reader::ObjectsSchemaReader::schemas_db_dir(repository, commit_id);
        let opts = db::key_val::opts::default();
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
        }

        let schema_files_db_path =
            objects_schema_reader::ObjectsSchemaReader::schema_files_db_dir(repository, commit_id);
        if !schema_files_db_path.exists() {
            std::fs::create_dir_all(&schema_files_db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&schema_files_db_path))?;
        }

        Ok(SchemaWriter {
            db: DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?,
            repository: repository.clone(),
        })
    }

    pub fn put_schema_for_file(&self, _path: &Path, schema: &Schema) -> Result<(), OxenError> {
        // All we want to do is make sure the schema is inserted into the versions directory by hash.
        versioner::backup_schema(&self.repository, schema)?;
        Ok(())
    }

    pub fn delete_schema_for_file(&self, _path: &Path, _schema: &Schema) -> Result<(), OxenError> {
        Ok(())
    }

    pub fn has_schema(&self, schema: &Schema) -> bool {
        str_json_db::has_key(&self.db, &schema.hash)
    }

    pub fn put_schema(&self, schema: &Schema) -> Result<(), OxenError> {
        // All we want to do is make sure the schema is inserted into the versions directory by hash.
        versioner::backup_schema(&self.repository, schema)?;
        Ok(())
    }

    pub fn update_schema(&self, schema: &Schema) -> Result<Schema, OxenError> {
        // All we want to do is make sure the schema is inserted into the versions directory by hash.
        versioner::backup_schema(&self.repository, schema)?;
        Ok(schema.clone())
    }

    pub fn delete_schema(&self, _schema: &Schema) -> Result<(), OxenError> {
        // We don't need this at all

        Ok(())
    }
}
