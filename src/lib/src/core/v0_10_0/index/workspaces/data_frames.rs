use duckdb::Connection;
use polars::frame::DataFrame;

use sql_query_builder::{Delete, Select};

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, TABLE_NAME};
use crate::core::db::data_frames::workspace_df_db::select_cols_from_schema;
use crate::core::db::data_frames::{df_db, workspace_df_db};
use crate::core::df::tabular;
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::core::v0_10_0::index::{self, workspaces};
use crate::model::diff::tabular_diff::{
    TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};

use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, CommitEntry, EntryDataType, LocalRepository, Workspace};
use crate::opts::DFOpts;
use crate::repositories;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};

pub mod column_changes_db;
pub mod columns;
pub mod row_changes_db;
pub mod rows;

pub fn count(workspace: &Workspace, path: impl AsRef<Path>) -> Result<usize, OxenError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;

    let count = df_db::count(&conn, TABLE_NAME)?;
    Ok(count)
}

pub fn get_queryable_data_frame_workspace(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    commit: &Commit,
) -> Result<Workspace, OxenError> {
    let path = path.as_ref();
    let entry_reader = CommitEntryReader::new(repo, commit)?;

    let entry = entry_reader
        .get_entry(path)?
        .ok_or_else(|| OxenError::basic_str("Entry not found"))?;

    let version_path = util::fs::version_path(repo, &entry);

    let data_type = util::fs::file_data_type(&version_path);

    if data_type != EntryDataType::Tabular {
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

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    commit: &Commit,
    path: &PathBuf,
) -> Result<bool, OxenError> {
    match index::workspaces::data_frames::get_queryable_data_frame_workspace(repo, path, commit) {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound() => Ok(false),
            _ => Err(e),
        },
    }
}

pub fn index(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    // Is tabular just looks at the file extensions
    if !util::fs::is_tabular(path) {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }

    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let reader = CommitEntryReader::new(repo, commit)?;
    let entry = reader.get_entry(path)?;
    let entry = match entry {
        Some(entry) => entry,
        None => return Err(OxenError::resource_not_found(path.to_string_lossy())),
    };

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &entry.path);

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
    let version_path = util::fs::version_path(repo, &entry);

    log::debug!(
        "core::v0_10_0::index::workspaces::data_frames::index({:?}) got version path: {:?}",
        entry.path,
        version_path
    );

    let extension: &str = &util::fs::extension_from_path(path);

    df_db::index_file_with_id(&version_path, &conn, extension)?;
    log::debug!(
        "core::v0_10_0::index::workspaces::data_frames::index({:?}) finished!",
        entry.path
    );

    add_row_status_cols(&conn)?;

    // Save the current commit id so we know if the branch has advanced
    let commit_path =
        repositories::workspaces::data_frames::previous_commit_ref_path(workspace, path);
    util::fs::write_to_path(commit_path, &commit.id)?;

    Ok(())
}

pub fn query(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("query_staged_df() got db_path: {:?}", db_path);

    let conn = df_db::get_connection(db_path)?;

    // Get the schema of this commit entry
    let schema = df_db::get_schema(&conn, TABLE_NAME)?;

    // Enrich w/ oxen cols
    let full_schema = workspace_df_db::enhance_schema_with_oxen_cols(&schema)?;

    let col_names = select_cols_from_schema(&schema)?;

    let select = Select::new().select(&col_names).from(TABLE_NAME);

    let df = df_db::select(&conn, &select, true, Some(&full_schema), Some(opts))?;

    Ok(df)
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

pub fn unindex(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;
    df_db::drop_table(&conn, TABLE_NAME)?;

    Ok(())
}

pub fn is_indexed(workspace: &Workspace, path: &Path) -> Result<bool, OxenError> {
    log::debug!("checking dataset is indexed for {:?}", path);
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("getting conn at path {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let table_exists = df_db::table_exists(&conn, TABLE_NAME)?;
    log::debug!("dataset_is_indexed() got table_exists: {:?}", table_exists);
    Ok(table_exists)
}

pub fn diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DiffResult, OxenError> {
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let path = path.as_ref();
    // Get commit for the branch head
    log::debug!("diff_workspace_df got repo at path {:?}", repo.path);

    let entry = repositories::entries::get_commit_entry(repo, commit, path)?
        .ok_or(OxenError::entry_does_not_exist(path))?;

    if !is_indexed(workspace, path)? {
        return Err(OxenError::basic_str("Dataset is not indexed"));
    };

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, entry.path);

    let conn = df_db::get_connection(db_path)?;

    let diff_df = workspace_df_db::df_diff(&conn)?;

    if diff_df.is_empty() {
        return Ok(DiffResult::Tabular(TabularDiff::empty()));
    }

    let row_mods = AddRemoveModifyCounts::from_diff_df(&diff_df)?;

    let schema = workspace_df_db::schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let schemas = TabularDiffSchemas {
        left: schema.clone(),
        right: schema.clone(),
        diff: schema.clone(),
    };

    let diff_summary = TabularDiffSummary {
        modifications: TabularDiffMods {
            row_counts: row_mods,
            col_changes: TabularSchemaDiff::empty(),
        },
        schemas,
        dupes: TabularDiffDupes::empty(),
    };

    let diff_result = TabularDiff {
        contents: diff_df,
        parameters: TabularDiffParameters::empty(),
        summary: diff_summary,
    };

    Ok(DiffResult::Tabular(diff_result))
}

pub fn extract_dataset_to_versions_dir(
    workspace: &Workspace,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let repo = &workspace.base_repo;
    let version_path = util::fs::version_path(repo, entry);
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;

    log::debug!("extracting to versions path: {:?}", version_path);

    // Filter out any with removed status before extracting
    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    conn.execute(&delete.to_string(), [])?;

    let df_before = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_before: {:?}",
        df_before
    );

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&version_path, &conn)?,
            Some("tsv") => export_tsv(&version_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&version_path, &conn)?,
            Some("parquet") => export_parquet(&version_path, &conn)?,
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

    let df_after = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_after: {:?}",
        df_after
    );

    Ok(())
}

// TODONOW combine with versions dir export fn and genericize on path
pub fn extract_to_working_dir(
    workspace: &Workspace,
    entry: &CommitEntry,
) -> Result<PathBuf, OxenError> {
    let workspace_repo = &workspace.workspace_repo;

    let working_path = workspace_repo.path.join(entry.path.clone());
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;
    // Match on the extension
    if !working_path.exists() {
        util::fs::create_dir_all(
            working_path
                .parent()
                .expect("Failed to get parent directory"),
        )?;
    }

    log::debug!("created working path: {:?}", working_path);

    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    let res = conn.execute(&delete.to_string(), [])?;
    log::debug!("delete query result is: {:?}", res);

    match entry.path.extension() {
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

    let df_after = tabular::read_df(&working_path, DFOpts::empty())?;
    log::debug!("extract_to_working_dir() got df_after: {:?}", df_after);

    Ok(working_path)
}

pub fn unstage(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    unindex(workspace, &path)?;
    workspaces::stager::rm(workspace, &path)?;

    Ok(())
}

pub fn restore(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    // Unstage and then restage the df
    unindex(workspace, &path)?;

    // TODO: we could do this more granularly without a full reset
    index(workspace, path.as_ref())?;

    Ok(())
}

fn export_rest(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_rest()");
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
    log::debug!("export_csv()");
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
    log::debug!("export_tsv()");
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
    log::debug!("export_parquet()");
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
