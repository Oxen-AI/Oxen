use crate::core::db::data_frames::row_changes_db::get_all_data_frame_row_changes;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::data_frame::update_result::UpdateResult;
use crate::model::Workspace;
use crate::view::data_frames::DataFrameRowChange;

use polars::datatypes::AnyValue;

use polars::frame::DataFrame;
use polars::prelude::PlSmallStr;

use crate::{core, repositories};
use rocksdb::DB;
use sql_query_builder::Select;

use crate::constants::{DIFF_STATUS_COL, OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME};
use crate::core::db;

use crate::core::db::data_frames::{df_db, workspace_df_db};
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::LocalRepository;

use std::path::Path;

pub fn add(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::index::workspaces::data_frames::rows::add(
            workspace,
            file_path.as_ref(),
            data,
        ),
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::workspaces::data_frames::rows::add(workspace, file_path.as_ref(), data)
        }
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

pub fn update(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::index::workspaces::data_frames::rows::update(
            workspace,
            path.as_ref(),
            row_id,
            data,
        ),
        MinOxenVersion::V0_19_0 => core::v0_19_0::workspaces::data_frames::rows::update(
            workspace,
            path.as_ref(),
            row_id,
            data,
        ),
    }
}

pub fn batch_update(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<Vec<UpdateResult>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::data_frames::rows::batch_update(
                workspace,
                path.as_ref(),
                data,
            )
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::workspaces::data_frames::rows::batch_update(
            workspace,
            path.as_ref(),
            data,
        ),
    }
}

pub fn delete(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::index::workspaces::data_frames::rows::delete(
            workspace,
            path.as_ref(),
            row_id,
        ),
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::workspaces::data_frames::rows::delete(workspace, path.as_ref(), row_id)
        }
    }
}

pub fn restore(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::index::workspaces::data_frames::rows::restore(
            repo,
            workspace,
            path.as_ref(),
            row_id,
        ),
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::workspaces::data_frames::rows::restore(workspace, path.as_ref(), row_id)
        }
    }
}

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
    let oxen_id_col = PlSmallStr::from_str(OXEN_ID_COL);
    if row_df.height() == 1 && row_df.get_column_names().contains(&&oxen_id_col) {
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

pub fn get_row_idx(row_df: &DataFrame) -> Result<Option<usize>, OxenError> {
    let oxen_row_id_col = PlSmallStr::from_str(OXEN_ROW_ID_COL);
    if row_df.height() == 1 && row_df.get_column_names().contains(&&oxen_row_id_col) {
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
