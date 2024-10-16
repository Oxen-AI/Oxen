//! versioner is responsible for interacting with entries in the versioned directory
//!

use std::io::Write;
use std::path::Path;

use crate::error::OxenError;
use crate::model::entry::commit_entry::{Entry, SchemaEntry};
use crate::model::{CommitEntry, LocalRepository, Schema};
use crate::util;

pub fn backup_schema(repository: &LocalRepository, schema: &Schema) -> Result<(), OxenError> {
    log::debug!("backing up schema {:?}", schema);
    let version_path = util::fs::version_path_from_schema(repository.path.clone(), schema);
    // Create all parent dirs that don't exist
    if let Some(parent) = version_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    log::debug!("got version path for schema {:?}", version_path);
    if should_copy_schema(schema, &version_path) {
        // Write the schema out to the version path name
        let mut file = std::fs::File::create(&version_path)?;
        // Write to path with serde
        let schema_json = serde_json::to_string(schema)?;

        file.write_all(schema_json.as_bytes())?;
    }
    Ok(())
}

pub fn should_copy_entry(entry: &CommitEntry, path: &Path) -> bool {
    !path.exists() || path_hash_is_different(entry, path)
}

// Don't unpack schema files to working dir
pub fn should_unpack_entry(entry: &Entry, path: &Path) -> bool {
    match entry {
        Entry::CommitEntry(entry) => should_copy_entry(entry, path),
        Entry::SchemaEntry(_schema_entry) => false,
    }
}

pub fn should_copy_schema_entry(_schema: &SchemaEntry, path: &Path) -> bool {
    !path.exists()
}

pub fn should_copy_schema(_schema: &Schema, path: &Path) -> bool {
    !path.exists()
}

fn path_hash_is_different(entry: &CommitEntry, path: &Path) -> bool {
    if let Ok(hash) = util::hasher::hash_file_contents(path) {
        return hash != entry.hash;
    }
    false
}
