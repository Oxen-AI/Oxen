//! # oxen schemas
//!
//! Interact with schemas
//!

use std::collections::HashMap;
use std::path::PathBuf;

use crate::core::v0_10_0::index::SchemaReader;
use crate::core::v0_10_0::index::Stager;

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, Schema};
use crate::repositories;

use std::path::Path;

pub fn list(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let schema_reader = SchemaReader::new(repo, &commit.id)?;
    schema_reader.list_schemas()
}

/// Get a schema for a specific revision
pub fn get_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    let path = path.as_ref();
    let schema_reader = SchemaReader::new(repo, &commit.id)?;
    schema_reader.get_schema_for_file(path)
}

/// Get a staged schema
pub fn get_staged(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    let path = path.as_ref();
    let stager = Stager::new(repo)?;
    stager.get_staged_schema(path)
}

/// List all the staged schemas
pub fn list_staged(repo: &LocalRepository) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let stager = Stager::new(repo)?;
    stager.list_staged_schemas()
}

/// Get a string representation of the schema given a schema ref
pub fn show(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    staged: bool,
    verbose: bool,
) -> Result<String, OxenError> {
    let path = path.as_ref();
    let schema = if staged {
        get_staged(repo, path)?
    } else {
        match repositories::commits::head_commit_maybe(repo)? {
            Some(commit) => repositories::data_frames::schemas::get_by_path(repo, &commit, path)?,
            None => None,
        }
    };

    let Some(schema) = schema else {
        return Err(OxenError::schema_does_not_exist(path));
    };

    let mut results = String::new();
    if verbose {
        let verbose_str = schema.verbose_str();
        results.push_str(&format!(
            "{} {}\n{}\n",
            path.to_string_lossy(),
            schema.hash,
            verbose_str
        ));
    } else {
        results.push_str(&format!(
            "{}\t{}\t{}",
            path.to_string_lossy(),
            schema.hash,
            schema
        ))
    }
    Ok(results)
}

/// Remove a schema override from the staging area
/// TODO: Currently undefined behavior for non-staged schemas
pub fn rm(repo: &LocalRepository, path: impl AsRef<Path>, staged: bool) -> Result<(), OxenError> {
    let path = path.as_ref();
    if !staged {
        panic!("Undefined behavior for non-staged schemas")
    }

    let stager = Stager::new(repo)?;
    stager.rm_schema(path)
}

/// Add metadata to the schema
pub fn add_schema_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let path = path.as_ref();
    let head_commit = repositories::commits::head_commit(repo)?;
    log::debug!("add_column_metadata head_commit: {}", head_commit);

    let mut results = HashMap::new();
    let stager = Stager::new(repo)?;

    let schema = repositories::data_frames::schemas::get_by_path(repo, &head_commit, path)?;

    let Some(mut schema) = schema else {
        return Err(OxenError::schema_does_not_exist(path));
    };

    schema.metadata = Some(metadata.to_owned());
    stager.update_schema_for_path(path, &schema)?;
    results.insert(path.to_path_buf(), schema);

    let staged_schema = stager.get_staged_schema(path)?;
    if let Some(mut staged_schema) = staged_schema {
        staged_schema.metadata = Some(metadata.to_owned());
        stager.update_schema_for_path(path, &staged_schema)?;
        results.insert(path.to_path_buf(), staged_schema);
    }
    Ok(results)
}

/// Add metadata to a specific column
pub fn add_column_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let path = path.as_ref();
    let column = column.as_ref();
    let head_commit = repositories::commits::head_commit(repo)?;
    log::debug!("add_column_metadata head_commit: {}", head_commit);

    let schema = repositories::data_frames::schemas::get_by_path(repo, &head_commit, path)?;

    let mut all_schemas: HashMap<PathBuf, Schema> = HashMap::new();
    if let Some(schema) = schema {
        all_schemas.insert(path.to_path_buf(), schema);
    }
    log::debug!(
        "add_schema_metadata column {} metadata: {}",
        column,
        metadata
    );

    let stager = Stager::new(repo)?;
    let staged_schemas = stager.get_staged_schema(path)?;

    if let Some(staged_schemas) = staged_schemas {
        all_schemas.insert(path.to_path_buf(), staged_schemas);
    }

    if all_schemas.is_empty() {
        return Err(OxenError::schema_does_not_exist(path));
    }

    let mut results = HashMap::new();
    for (path, mut schema) in all_schemas {
        schema.add_column_metadata(column, metadata);
        let schema = stager.update_schema_for_path(&path, &schema)?;
        results.insert(path, schema);
    }
    Ok(results)
}
