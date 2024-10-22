use duckdb::Connection;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, TABLE_NAME};
use crate::core::db::data_frames::df_db;
use crate::core::v0_19_0::index::CommitMerkleTree;
use sql_query_builder::Delete;

use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode};
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, EntryDataType, LocalRepository, Workspace};
use crate::repositories;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};

pub mod columns;
pub mod rows;

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    commit: &Commit,
    path: &PathBuf,
) -> Result<bool, OxenError> {
    match get_queryable_data_frame_workspace(repo, path, commit) {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound() => Ok(false),
            _ => Err(e),
        },
    }
}

pub fn get_queryable_data_frame_workspace(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    commit: &Commit,
) -> Result<Workspace, OxenError> {
    let path = path.as_ref();
    log::debug!("get_queryable_data_frame_workspace path: {:?}", path);
    let file_node = repositories::tree::get_file_by_path(repo, commit, path)?
        .ok_or(OxenError::path_does_not_exist(path))?;
    if file_node.data_type != EntryDataType::Tabular {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }
    let workspaces = repositories::workspaces::list(repo)?;

    for workspace in workspaces {
        // Ensure the workspace is not editable and matches the commit ID of the resource
        if !workspace.is_editable && workspace.commit == *commit {
            // Construct the path to the DuckDB resource within the workspace
            let workspace_file_db_path =
                repositories::workspaces::data_frames::duckdb_path(&workspace, path);

            // Check if the DuckDB file exists in the workspace's directory
            if workspace_file_db_path.exists() {
                // The file exists in this non-editable workspace, and the commit IDs match
                return Ok(workspace);
            }
        }
    }

    Err(OxenError::QueryableWorkspaceNotFound())
}

pub fn index(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    // Is tabular just looks at the file extensions
    let file_node =
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?
            .ok_or(OxenError::path_does_not_exist(path))?;
    if file_node.data_type != EntryDataType::Tabular {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }

    log::debug!("core::v0_19_0::workspaces::data_frames::index({:?})", path);

    let repo = &workspace.base_repo;
    let commit = &workspace.commit;

    log::debug!(
        "core::v0_19_0::workspaces::data_frames::index({:?}) got commit {:?}",
        path,
        commit
    );

    let commit_merkle_tree = CommitMerkleTree::from_path(repo, commit, path, true)?;
    let file_hash = commit_merkle_tree.root.hash;

    log::debug!(
        "core::v0_19_0::workspaces::data_frames::index({:?}) got file hash {:?}",
        path,
        file_hash
    );

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let Some(parent) = db_path.parent() else {
        return Err(OxenError::basic_str(format!(
            "Failed to get parent directory for {:?}",
            db_path
        )));
    };

    if !parent.exists() {
        util::fs::create_dir_all(parent)?;
    }

    let conn = df_db::get_connection(db_path)?;
    if df_db::table_exists(&conn, TABLE_NAME)? {
        df_db::drop_table(&conn, TABLE_NAME)?;
    }
    let version_path = util::fs::version_path_from_node(repo, file_hash.to_string(), path);

    log::debug!(
        "core::v0_19_0::index::workspaces::data_frames::index({:?}) got version path: {:?}",
        path,
        version_path
    );

    let extension = match &commit_merkle_tree.root.node {
        EMerkleTreeNode::File(file_node) => file_node.extension.clone(),
        _ => {
            return Err(OxenError::basic_str("File node is not a file node"));
        }
    };

    df_db::index_file_with_id(&version_path, &conn, &extension)?;
    log::debug!(
        "core::v0_19_0::index::workspaces::data_frames::index({:?}) finished!",
        path
    );

    add_row_status_cols(&conn)?;

    // Save the current commit id so we know if the branch has advanced
    let commit_path =
        repositories::workspaces::data_frames::previous_commit_ref_path(workspace, path);
    util::fs::write_to_path(commit_path, &commit.id)?;

    Ok(())
}

fn add_row_status_cols(conn: &Connection) -> Result<(), OxenError> {
    let query_status = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT '{}'",
        TABLE_NAME,
        DIFF_STATUS_COL,
        StagedRowStatus::Unchanged
    );
    conn.execute(&query_status, [])?;

    let query_hash = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT NULL",
        TABLE_NAME, DIFF_HASH_COL
    );
    conn.execute(&query_hash, [])?;
    Ok(())
}

pub fn extract_file_node_to_working_dir(
    workspace: &Workspace,
    dir_path: impl AsRef<Path>,
    file_node: &FileNode,
) -> Result<PathBuf, OxenError> {
    let dir_path = dir_path.as_ref();
    log::debug!(
        "extract_file_node_to_working_dir dir_path: {:?} file_node: {}",
        dir_path,
        file_node
    );
    let workspace_repo = &workspace.workspace_repo;
    let path = PathBuf::from(file_node.name.clone());

    let working_path = workspace_repo.path.join(&path);
    log::debug!("extracting file node to working dir: {:?}", working_path);
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);
    let conn = df_db::get_connection(db_path)?;
    // Match on the extension
    if !working_path.exists() {
        util::fs::create_dir_all(
            working_path
                .parent()
                .expect("Failed to get parent directory"),
        )?;
    }

    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    let res = conn.execute(&delete.to_string(), [])?;
    log::debug!("delete query result is: {:?}", res);

    match path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&working_path, &conn)?,
            Some("tsv") => export_tsv(&working_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&working_path, &conn)?,
            Some("parquet") => export_parquet(&working_path, &conn)?,
            _ => {
                return Err(OxenError::basic_str(
                    "File format not supported, must be tabular.",
                ))
            }
        },
        None => {
            return Err(OxenError::basic_str(
                "File format not supported, must be tabular.",
            ))
        }
    }

    // let df_after = tabular::read_df(&working_path, DFOpts::empty())?;
    // log::debug!("extract_to_working_dir() got df_after: {:?}", df_after);

    Ok(working_path)
}

fn export_rest(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_rest() to {:?}", path);
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}';",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);
    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_rest() got df: {:?}", temp_res);

    conn.execute(&query, [])?;
    Ok(())
}

fn export_csv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_csv() to {:?}", path);
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER ',');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);

    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_csv() got df: {:?}", temp_res);

    conn.execute(&query, [])?;

    Ok(())
}

fn export_tsv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_tsv() to {:?}", path);
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER '\t');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    conn.execute(&query, [])?;
    Ok(())
}

fn export_parquet(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_parquet() to {:?}", path);
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");

    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (FORMAT PARQUET);",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );
    conn.execute(&query, [])?;

    Ok(())
}
