use crate::error::OxenError;
use crate::model::Workspace;
use crate::model::data_frame::update_result::UpdateResult;

use polars::frame::DataFrame;
use polars::prelude::PlSmallStr;

use crate::{core, repositories};

use crate::constants::{OXEN_ID_COL, OXEN_ROW_ID_COL, TABLE_NAME};

use crate::core::db::data_frames::df_db::{self, with_df_db_manager};
use crate::core::v_latest::workspaces::data_frames::rows as latest_rows;
use crate::model::LocalRepository;

use std::path::Path;
use tokio::task::spawn_blocking;

/// Append `data` as a new row and return it as staged, `_oxen_id` included.
pub fn add(
    _repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::rows::add(workspace, file_path, data)
}

/// [`add`], off the async worker.
pub async fn add_async(
    workspace: &Workspace,
    file_path: &Path,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let workspace = workspace.clone();
    let file_path = file_path.to_path_buf();
    let data = data.clone();
    spawn_blocking(move || latest_rows::add(&workspace, file_path, &data)).await?
}

/// Overwrite the row `row_id` with `data` and return it as staged.
pub fn update(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::rows::update(workspace, path, row_id, data)
}

/// [`update`], off the async worker.
pub async fn update_async(
    workspace: &Workspace,
    path: &Path,
    row_id: &str,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let workspace = workspace.clone();
    let path = path.to_path_buf();
    let row_id = row_id.to_string();
    let data = data.clone();
    spawn_blocking(move || latest_rows::update(&workspace, path, &row_id, &data)).await?
}

/// Overwrite each `{row_id, value}` pair in `data` and return the ids updated.
pub fn batch_update(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<Vec<UpdateResult>, OxenError> {
    core::v_latest::workspaces::data_frames::rows::batch_update(workspace, path, data)
}

/// [`batch_update`], off the async worker.
pub async fn batch_update_async(
    workspace: &Workspace,
    path: &Path,
    data: &serde_json::Value,
) -> Result<Vec<UpdateResult>, OxenError> {
    let workspace = workspace.clone();
    let path = path.to_path_buf();
    let data = data.clone();
    spawn_blocking(move || latest_rows::batch_update(&workspace, path, &data)).await?
}

/// Remove the row `row_id` and return it as it was before the delete.
pub fn delete(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    core::v_latest::workspaces::data_frames::rows::delete(workspace, path, row_id)
}

/// [`delete`], off the async worker.
pub async fn delete_async(
    workspace: &Workspace,
    path: &Path,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let workspace = workspace.clone();
    let path = path.to_path_buf();
    let row_id = row_id.to_string();
    spawn_blocking(move || latest_rows::delete(&workspace, path, &row_id)).await?
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
            // Bind the id rather than interpolate it into the predicate.
            df_db::select_raw_with_params(
                conn,
                &format!("SELECT * FROM {TABLE_NAME} WHERE \"{OXEN_ID_COL}\" = ?"),
                [row_id],
            )
        })
    })?;
    log::debug!("get_row_by_id() got data: {data:?}");
    // `_oxen_row_id` is an ordering key, not part of the data frame's schema —
    // keep it out of row results. (`_oxen_id` stays: clients address rows by it.)
    if data.column(OXEN_ROW_ID_COL).is_ok() {
        Ok(data.drop(OXEN_ROW_ID_COL)?)
    } else {
        Ok(data)
    }
}

/// [`get_by_id`], off the async worker.
pub async fn get_by_id_async(
    workspace: &Workspace,
    path: &Path,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let workspace = workspace.clone();
    let path = path.to_path_buf();
    let row_id = row_id.to_string();
    spawn_blocking(move || get_by_id(&workspace, path, row_id)).await?
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
