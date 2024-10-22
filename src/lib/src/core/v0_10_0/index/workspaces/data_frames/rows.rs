use polars::datatypes::AnyValue;
use polars::frame::DataFrame;

use polars::prelude::{NamedFrom, PlSmallStr};
use polars::series::Series;
use rocksdb::DB;
use serde_json::Value;
use sql_query_builder::Select;

use crate::constants::{DIFF_STATUS_COL, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME};
use crate::core::db;
use crate::opts::DFOpts;

use crate::core::db::data_frames::{df_db, rows, workspace_df_db};
use crate::core::df::tabular;
use crate::core::v0_10_0::index::workspaces;
use crate::error::OxenError;
use crate::model::data_frame::update_result::UpdateResult;
use crate::model::diff::DiffResult;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{CommitEntry, LocalRepository, Workspace};
use crate::repositories;
use crate::util;
use crate::view::data_frames::DataFrameRowChange;
use crate::view::JsonDataFrameView;

use std::path::Path;

use super::row_changes_db::get_all_data_frame_row_changes;

/// Get a single row by the _oxen_id val
pub fn get_by_id(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let row_id = row_id.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("get_row_by_id() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let schema = workspace_df_db::full_staged_table_schema(&conn)?;

    let query = Select::new()
        .select("*")
        .from(TABLE_NAME)
        .where_clause(&format!("{} = '{}'", OXEN_ID_COL, row_id));
    let data = df_db::select(&conn, &query, true, Some(&schema), None)?;
    log::debug!("get_row_by_id() got data: {:?}", data);
    Ok(data)
}

pub fn get_row_id(row_df: &DataFrame) -> Result<Option<String>, OxenError> {
    let id_col = PlSmallStr::from_str(OXEN_ID_COL);
    if row_df.height() == 1 && row_df.get_column_names().contains(&&id_col) {
        Ok(row_df
            .column(OXEN_ID_COL)
            .unwrap()
            .get(0)
            .map(|val| val.to_string().trim_matches('"').to_string())
            .ok())
    } else {
        Ok(None)
    }
}

pub fn get_row_status(row_df: &DataFrame) -> Result<Option<StagedRowStatus>, OxenError> {
    let diff_status_col = PlSmallStr::from_str(DIFF_STATUS_COL);
    if row_df.height() == 1 && row_df.get_column_names().contains(&&diff_status_col) {
        let anyval_status = row_df.column(DIFF_STATUS_COL).unwrap().get(0)?;
        let str_status = anyval_status
            .get_str()
            .ok_or_else(|| OxenError::basic_str("Row status not found"))?;
        let status = StagedRowStatus::from_string(str_status)?;
        Ok(Some(status))
    } else {
        Ok(None)
    }
}

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let row_changes_path =
        repositories::workspaces::data_frames::row_changes_path(workspace, file_path);

    log::debug!("add_row() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let df = tabular::parse_json_to_df(data)?;

    let mut result = rows::append_row(&conn, &df)?;

    let oxen_id_col = result
        .column("_oxen_id")
        .expect("Column _oxen_id not found");

    let last_idx = oxen_id_col.len() - 1;
    let last_value = oxen_id_col.get(last_idx)?;

    let row_id = last_value.to_string().trim_matches('"').to_string();

    let row = JsonDataFrameView::json_from_df(&mut result);

    rows::record_row_change(&row_changes_path, row_id, "added".to_owned(), row, None)?;

    workspaces::stager::add(workspace, file_path)?;

    Ok(result)
}

pub fn restore(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let entry = repositories::entries::get_commit_entry(repo, &workspace.commit, path.as_ref())?
        .ok_or_else(|| OxenError::entry_does_not_exist(path.as_ref()))?;
    let row_id = row_id.as_ref();
    let restored_row = restore_row_in_db(workspace, &entry, row_id)?;
    let diff = workspaces::data_frames::diff(workspace, &entry.path)?;

    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            log::debug!("no changes, deleting file from staged db");
            // Restored to original state == delete file from staged db
            workspaces::stager::rm(workspace, &entry.path)?;
        }
    }

    Ok(restored_row)
}

pub fn restore_row_in_db(
    workspace: &Workspace,
    entry: &CommitEntry,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let row_id = row_id.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &entry.path);
    let conn = df_db::get_connection(db_path)?;
    let opts = db::key_val::opts::default();
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, &entry.path);
    let db = DB::open(&opts, dunce::simplified(&column_changes_path))?;

    // Get the row by id
    let row = get_by_id(workspace, &entry.path, row_id)?;

    if row.height() == 0 {
        return Err(OxenError::resource_not_found(row_id));
    };

    let row_status =
        get_row_status(&row)?.ok_or_else(|| OxenError::basic_str("Row status not found"))?;

    let result_row = match row_status {
        StagedRowStatus::Added => {
            // Row is added, just delete it
            log::debug!("restore_row() row is added, deleting");
            rows::revert_row_changes(&db, row_id.to_owned())?;
            rows::delete_row(&conn, row_id)?
        }
        StagedRowStatus::Modified | StagedRowStatus::Removed => {
            // Row is modified, just delete it
            log::debug!("restore_row() row is modified, deleting");
            let mut insert_row =
                prepare_modified_or_removed_row(&workspace.base_repo, entry, &row)?;
            rows::revert_row_changes(&db, row_id.to_owned())?;
            rows::modify_row(&conn, &mut insert_row, row_id)?
        }
        StagedRowStatus::Unchanged => {
            // Row is unchanged, just return it
            row
        }
    };

    log::debug!("we're returning this row: {:?}", result_row);

    Ok(result_row)
}

pub fn get_row_idx(row_df: &DataFrame) -> Result<Option<usize>, OxenError> {
    let row_id_col = PlSmallStr::from_str(OXEN_ROW_ID_COL);
    if row_df.height() == 1 && row_df.get_column_names().contains(&&row_id_col) {
        let row_df_anyval = row_df.column(OXEN_ROW_ID_COL).unwrap().get(0)?;
        match row_df_anyval {
            AnyValue::UInt16(val) => Ok(Some(val as usize)),
            AnyValue::UInt32(val) => Ok(Some(val as usize)),
            AnyValue::UInt64(val) => Ok(Some(val as usize)),
            AnyValue::Int16(val) => Ok(Some(val as usize)),
            AnyValue::Int32(val) => Ok(Some(val as usize)),
            AnyValue::Int64(val) => Ok(Some(val as usize)),
            val => {
                log::debug!("unrecognized row index type {:?}", val);
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}

/// TODO: we should really be storing the original row contents
///       so that we can both do row level diffs and restore
///       this is very inefficient to load the entire original data frame
pub fn prepare_modified_or_removed_row(
    repo: &LocalRepository,
    entry: &CommitEntry,
    row_df: &DataFrame,
) -> Result<DataFrame, OxenError> {
    let row_idx =
        get_row_idx(row_df)?.ok_or_else(|| OxenError::basic_str("Row index not found"))?;
    let row_idx_og = (row_idx - 1) as i64;

    // let scan_rows = 10000 as usize;
    let committed_df_path = util::fs::version_path(repo, entry);

    // TODONOW should not be using all rows - just need to parse delim
    let lazy_df = tabular::read_df(committed_df_path, DFOpts::empty())?;

    // Get the row by index
    let mut row = lazy_df.slice(row_idx_og, 1_usize);

    // Added rows will error here on out of index, but we caught them earlier..
    // let mut row = row.collect()?;

    row.with_column(Series::new(
        PlSmallStr::from_str(DIFF_STATUS_COL),
        vec![StagedRowStatus::Unchanged.to_string()],
    ))?;

    Ok(row)
}

pub fn delete(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let row_changes_path = repositories::workspaces::data_frames::row_changes_path(workspace, path);

    let mut deleted_row = {
        let conn = df_db::get_connection(db_path)?;
        rows::delete_row(&conn, row_id)?
    };

    let row = JsonDataFrameView::json_from_df(&mut deleted_row);

    rows::record_row_change(
        &row_changes_path,
        row_id.to_owned(),
        "deleted".to_owned(),
        row,
        None,
    )?;

    // We track that the file has been modified
    workspaces::stager::add(workspace, path)?;

    // TODO: Better way of tracking when a file is restored to its original state without diffing
    //       this could be really slow
    let diff = workspaces::data_frames::diff(workspace, path)?;

    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            log::debug!("no changes, deleting file from staged db");
            // Restored to original state == delete file from staged db
            workspaces::stager::rm(workspace, path)?;
        }
    }
    Ok(deleted_row)
}

pub fn update(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;
    let row_changes_path = repositories::workspaces::data_frames::row_changes_path(workspace, path);

    let mut df = tabular::parse_json_to_df(data)?;

    let mut row = get_by_id(workspace, path, row_id)?;

    let mut result = rows::modify_row(&conn, &mut df, row_id)?;

    let row_before = JsonDataFrameView::json_from_df(&mut row);

    let row_after = JsonDataFrameView::json_from_df(&mut result);

    rows::record_row_change(
        &row_changes_path,
        row_id.to_owned(),
        "updated".to_owned(),
        row_before,
        Some(row_after),
    )?;

    workspaces::stager::add(workspace, path)?;

    let diff = workspaces::data_frames::diff(workspace, path)?;
    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            workspaces::stager::rm(workspace, path)?;
        }
    }

    Ok(result)
}

pub fn batch_update(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &Value,
) -> Result<Vec<UpdateResult>, OxenError> {
    let path = path.as_ref();
    if let Some(array) = data.as_array() {
        let results: Result<Vec<UpdateResult>, OxenError> = array
            .iter()
            .map(|obj| {
                let row_id = obj
                    .get("row_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OxenError::basic_str("Missing row_id"))?
                    .to_owned();
                let value = obj
                    .get("value")
                    .ok_or_else(|| OxenError::basic_str("Missing value"))?;

                match update(workspace, path, &row_id, value) {
                    Ok(data_frame) => Ok(UpdateResult::Success(row_id, data_frame)),
                    Err(e) => Ok(UpdateResult::Error(row_id, e)),
                }
            })
            .collect();

        results
    } else {
        Err(OxenError::basic_str("Data is not an array"))
    }
}

pub fn get_row_diff(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
) -> Result<Vec<DataFrameRowChange>, OxenError> {
    let row_changes_path =
        repositories::workspaces::data_frames::row_changes_path(workspace, file_path);
    let opts = db::key_val::opts::default();
    let db = DB::open_for_read_only(&opts, dunce::simplified(&row_changes_path), false)?;
    get_all_data_frame_row_changes(&db)
}
