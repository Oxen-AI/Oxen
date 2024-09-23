use polars::frame::DataFrame;

use crate::constants::{MODS_DIR, OXEN_HIDDEN_DIR, TABLE_NAME};
use crate::core::db::data_frames::{df_db, workspace_df_db};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, Workspace};
use crate::opts::DFOpts;
use crate::{repositories, util};

use std::path::{Path, PathBuf};

pub mod columns;
pub mod rows;
pub mod schemas;

pub fn is_behind(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let commit_path = previous_commit_ref_path(workspace, path);
    let commit_id = util::fs::read_from_path(commit_path)?;
    Ok(commit_id != workspace.commit.id)
}

pub fn is_indexed(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    todo!()
}

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    commit: &Commit,
    path: &PathBuf,
) -> Result<bool, OxenError> {
    todo!()
}

pub fn index(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    todo!()
}

pub fn unindex(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    todo!()
}

pub fn restore(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    todo!()
}

pub fn count(workspace: &Workspace, path: impl AsRef<Path>) -> Result<usize, OxenError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;

    let count = df_db::count(&conn, TABLE_NAME)?;
    Ok(count)
}

pub fn query(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    todo!()
}

pub fn diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DataFrame, OxenError> {
    let file_path = path.as_ref();
    let staged_db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let conn = df_db::get_connection(staged_db_path)?;
    let diff_df = workspace_df_db::df_diff(&conn)?;
    Ok(diff_df)
}

pub fn duckdb_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("db")
}

pub fn previous_commit_ref_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("COMMIT_ID")
}

pub fn column_changes_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("column_changes")
}

pub fn row_changes_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("row_changes")
}