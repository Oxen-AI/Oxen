use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::constants::{DIRS_DIR, FILES_DIR, MODS_DIR, OXEN_HIDDEN_DIR, SCHEMAS_DIR, STAGED_DIR};
use crate::core::db;
use crate::core::db::key_val::{path_db, str_json_db};
use crate::core::v0_10_0::index::SchemaReader;
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::{StagedData, StagedDirStats, StagedEntry, StagedEntryStatus, StagedSchema};

fn files_db_path(workspace: &Workspace) -> PathBuf {
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join(FILES_DIR)
}

fn staged_files_db_path(workspace: &Workspace, directory: impl AsRef<Path>) -> PathBuf {
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(FILES_DIR)
        .join(directory.as_ref())
}

fn schemas_db_path(workspace: &Workspace) -> PathBuf {
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(SCHEMAS_DIR)
}

fn dirs_db_path(workspace: &Workspace) -> PathBuf {
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(DIRS_DIR)
}

pub fn status(workspace: &Workspace, directory: impl AsRef<Path>) -> Result<StagedData, OxenError> {
    log::debug!(
        "workspaces::stager::status for workspace {:?} and directory {:?}",
        workspace.id,
        directory.as_ref()
    );

    let mut status = StagedData::empty();
    list_staged_entries(workspace, directory.as_ref(), &mut status)?;
    Ok(status)
}

/// This function is more efficient than status because
/// it does not need to list the files in the working directory/workspace.
/// It just populates the StagedData struct with the staged and unstaged files, schemas, etc.
fn list_staged_entries(
    workspace: &Workspace,
    directory: impl AsRef<Path>,
    status: &mut StagedData,
) -> Result<(), OxenError> {
    let directory = directory.as_ref();

    // List modifications
    let mod_entries = list_files(workspace)?;
    for path in mod_entries {
        log::debug!(
            "list_staged_entries path: {:?} directory: {:?}",
            path,
            directory
        );
        if Path::new(".") == directory || path.starts_with(directory) {
            status.modified_files.insert(path.to_owned());
        }
    }

    // List additions
    let dirs = list_dirs(workspace)?;
    log::debug!("list_staged_entries dirs: {:?}", dirs);
    for dir in &dirs {
        let staged_entries = list_staged_files(workspace, dir)?;
        log::debug!(
            "list_staged_entries staged_entries: {}",
            staged_entries.len()
        );
        for (path, entry) in &staged_entries {
            let path = dir.join(path);
            log::debug!("list_staged_entries path: {:?} entry: {:?}", path, entry);
            status.staged_files.insert(path, entry.clone());
        }

        status.staged_dirs.add_stats(&StagedDirStats {
            path: dir.to_owned(),
            status: StagedEntryStatus::Added,
            num_files_staged: staged_entries.len(),
            total_files: 0,
        });
    }

    // List schemas
    let schemas = list_schemas(workspace)?;
    for (path, schema) in schemas {
        log::debug!("list_staged_entries path {:?} schema: {:?}", path, schema);
        status.staged_schemas.insert(path, schema);
    }
    Ok(())
}

pub fn list_dirs(workspace: &Workspace) -> Result<Vec<PathBuf>, OxenError> {
    let db_path = dirs_db_path(workspace);
    log::debug!("list_dirs from dirs_db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_keys(&db).map(|keys| keys.into_iter().map(PathBuf::from).collect())
}

pub fn list_files(workspace: &Workspace) -> Result<Vec<PathBuf>, OxenError> {
    let db_path = files_db_path(workspace);
    log::debug!("list_entries from files_db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}

fn list_staged_files(
    workspace: &Workspace,
    directory: impl AsRef<Path>,
) -> Result<HashMap<String, StagedEntry>, OxenError> {
    let directory = directory.as_ref();
    let db_path = staged_files_db_path(workspace, directory);
    log::debug!("list_staged_files from files_db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::hash_map(&db)
}

pub fn list_schemas(workspace: &Workspace) -> Result<HashMap<PathBuf, StagedSchema>, OxenError> {
    // schema reader to see if we already have schema metadata for the file
    let schema_reader = SchemaReader::new(&workspace.base_repo, &workspace.commit.id)?;
    let db_path = schemas_db_path(workspace);
    log::debug!("list_schemas from files_db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let mut schemas: HashMap<PathBuf, StagedSchema> = HashMap::new();
    let path_schemas: Vec<(PathBuf, StagedSchema)> =
        path_db::list_path_entries(&db, Path::new(""))?;
    for (path, schema) in path_schemas {
        if let Some(workspace_schema) = schema_reader.get_schema_for_file(&path)? {
            log::debug!(
                "list_schemas update from workspace_schema: {:?}",
                workspace_schema
            );
            let mut schema = schema.clone();
            schema.schema.update_metadata_from_schema(&workspace_schema);
            schemas.insert(path, schema);
        } else {
            log::debug!("list_schemas insert from schema: {:?}", schema);
            schemas.insert(path, schema);
        }
    }
    Ok(schemas)
}

pub fn add(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = files_db_path(workspace);
    log::debug!("workspaces::stager::add {path:?} to db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<MultiThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let key = path.to_string_lossy();
    str_json_db::put(&db, &key, &key)
}

pub fn rm(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let files_db_path = files_db_path(workspace);
    let files_db: DBWithThreadMode<MultiThreaded> =
        rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
    let key = path.as_ref().to_string_lossy();
    str_json_db::delete(&files_db, key)?;

    Ok(())
}
