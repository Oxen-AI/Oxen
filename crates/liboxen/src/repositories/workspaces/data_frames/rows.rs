use crate::error::OxenError;
use crate::model::Workspace;
use crate::model::data_frame::update_result::UpdateResult;

use polars::datatypes::AnyValue;

use polars::frame::DataFrame;
use polars::prelude::PlSmallStr;

use crate::{core, repositories};
use sql_query_builder::Select;

use crate::constants::{OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME};

use crate::core::db::data_frames::df_db::{self, with_df_db_manager};
use crate::model::LocalRepository;

use std::path::Path;

pub fn add(
    _repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::rows::add(workspace, file_path.as_ref(), data)
}

pub fn update(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::rows::update(workspace, path.as_ref(), row_id, data)
}

pub fn batch_update(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<Vec<UpdateResult>, OxenError> {
    core::v_latest::workspaces::data_frames::rows::batch_update(workspace, path.as_ref(), data)
}

pub fn delete(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::rows::delete(workspace, path.as_ref(), row_id)
}

pub fn get_by_id(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let row_id = row_id.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("get_row_by_id() got db_path: {db_path:?}");
    let data = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            let query = Select::new()
                .select("*")
                .from(TABLE_NAME)
                .where_clause(&format!("{OXEN_ID_COL} = '{row_id}'"));
            df_db::select(conn, &query, None)
        })
    })?;
    log::debug!("get_row_by_id() got data: {data:?}");
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
                log::debug!("unrecognized row index type {val:?}");
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}
