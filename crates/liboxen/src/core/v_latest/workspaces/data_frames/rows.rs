use polars::frame::DataFrame;

use serde_json::Value;

use crate::core::db::data_frames::df_db::with_df_db_manager;
use crate::core::db::data_frames::rows;
use crate::core::df::tabular;
use crate::core::v_latest::workspaces;
use crate::error::OxenError;
use crate::model::Workspace;
use crate::model::data_frame::update_result::UpdateResult;
use crate::repositories;

use std::collections::HashMap;
use std::path::Path;

pub fn add(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let df = tabular::parse_json_to_df(data)?;
    log::debug!("add() df: {df:?}");

    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| rows::append_row(conn, &df))
    })?;

    workspaces::files::track_modified_data_frame(workspace, path)?;

    Ok(result)
}

pub fn delete(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    log::debug!("delete() row_id: {row_id} from path: {:?}", path.as_ref());
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let deleted_row = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| rows::delete_row(conn, row_id))
    })?;
    log::debug!("delete() deleted_row: {deleted_row:?}");

    workspaces::files::track_modified_data_frame(workspace, path)?;

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

    let mut df = tabular::parse_json_to_df(data)?;
    let row = repositories::workspaces::data_frames::rows::get_by_id(workspace, path, row_id)?;
    if row.height() == 0 {
        return Err(OxenError::resource_not_found("row not found"));
    }

    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| rows::modify_row(conn, &mut df, row_id))
    })?;

    workspaces::files::track_modified_data_frame(workspace, path)?;

    Ok(result)
}

pub fn batch_update(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &Value,
) -> Result<Vec<UpdateResult>, OxenError> {
    let path = path.as_ref();

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let Some(array) = data.as_array() else {
        return Err(OxenError::basic_str("Data is not an array"));
    };

    let mut keys: Vec<String> = Vec::new();

    let row_map: HashMap<String, DataFrame> = array
        .iter()
        .map(|obj| {
            let row_id = obj
                .get("row_id")
                .and_then(Value::as_str)
                .ok_or_else(|| OxenError::basic_str("Missing row_id"))?
                .to_owned();

            keys.push(row_id.clone());

            let df = tabular::parse_json_to_df(
                obj.get("value")
                    .ok_or_else(|| OxenError::basic_str("Missing value"))?,
            )?;
            Ok((row_id, df))
        })
        .collect::<Result<_, OxenError>>()?;

    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| rows::modify_rows(conn, row_map))
    })?;

    workspaces::files::track_modified_data_frame(workspace, path)?;

    let results: Vec<UpdateResult> = keys
        .iter()
        .map(|key| UpdateResult(key.to_owned()))
        .collect();

    Ok(results)
}
