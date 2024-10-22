use polars::frame::DataFrame;

use polars::prelude::NamedFrom;
use polars::prelude::PlSmallStr;
use polars::series::Series;
use rocksdb::DB;
use serde_json::Value;

use crate::constants::DIFF_STATUS_COL;
use crate::core::db;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::opts::DFOpts;

use crate::core::db::data_frames::{df_db, rows};
use crate::core::df::tabular;
use crate::core::v0_19_0::{rm, workspaces};
use crate::error::OxenError;
use crate::model::data_frame::update_result::UpdateResult;
use crate::model::diff::DiffResult;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, LocalRepository, Workspace};
use crate::repositories;
use crate::util;
use crate::view::JsonDataFrameView;

use std::collections::HashSet;
use std::path::Path;

pub fn add(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let row_changes_path = repositories::workspaces::data_frames::row_changes_path(workspace, path);

    log::debug!(
        "add_row() path: {:?} got db_path: {:?}",
        row_changes_path,
        db_path
    );
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

    workspaces::files::track_modified_data_frame(workspace, path)?;

    Ok(result)
}

pub fn restore(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let row_id = row_id.as_ref();
    let restored_row = restore_row_in_db(workspace, path.as_ref(), row_id)?;
    let diff = repositories::workspaces::data_frames::full_diff(workspace, path.as_ref())?;
    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            log::debug!("no changes, deleting file from staged db");
            // Restored to original state == delete file from staged db
            // TODO: Implement this
            rm::remove_staged_recursively(
                &workspace.workspace_repo,
                &HashSet::from([path.as_ref().to_path_buf()]),
            )?;

            // loop over parents and delete from staged db
            let mut current_path = path.as_ref().to_path_buf();
            while let Some(parent) = current_path.parent() {
                rm::remove_staged_recursively(
                    &workspace.workspace_repo,
                    &HashSet::from([parent.to_path_buf()]),
                )?;
                current_path = parent.to_path_buf();
            }
        }
    }

    Ok(restored_row)
}

pub fn delete(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    log::debug!("delete() row_id: {row_id} from path: {:?}", path.as_ref());
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let row_changes_path = repositories::workspaces::data_frames::row_changes_path(workspace, path);

    let mut deleted_row = {
        let conn = df_db::get_connection(db_path)?;
        rows::delete_row(&conn, row_id)?
    };
    log::debug!("delete() deleted_row: {:?}", deleted_row);

    let row = JsonDataFrameView::json_from_df(&mut deleted_row);

    rows::record_row_change(
        &row_changes_path,
        row_id.to_owned(),
        "deleted".to_owned(),
        row,
        None,
    )?;

    // TODO: Better way of tracking when a file is restored to its original state without diffing
    //       this could be really slow
    let diff = repositories::workspaces::data_frames::full_diff(workspace, path)?;

    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            log::debug!("no changes, deleting file from staged db {:?}", path);
            // Restored to original state == delete file from staged db
            rm::remove_staged_recursively(
                &workspace.workspace_repo,
                &HashSet::from([path.to_path_buf()]),
            )?;
        } else {
            log::debug!("there are still changes, not deleting file from staged db");
            log::debug!("diff: {:?}", diff);
            // We track that the file has been modified
            log::debug!("rows::delete() tracking file to staged db: {:?}", path);
            workspaces::files::track_modified_data_frame(workspace, path)?;
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

    let mut row = repositories::workspaces::data_frames::rows::get_by_id(workspace, path, row_id)?;

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

    let diff = repositories::workspaces::data_frames::full_diff(workspace, path)?;
    log::debug!("update() diff: {:?}", diff);
    if let DiffResult::Tabular(diff) = diff {
        if !diff.has_changes() {
            rm::remove_staged_recursively(
                &workspace.workspace_repo,
                &HashSet::from([path.to_path_buf()]),
            )?;
        } else {
            workspaces::files::track_modified_data_frame(workspace, path)?;
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

pub fn prepare_modified_or_removed_row(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    row_df: &DataFrame,
) -> Result<DataFrame, OxenError> {
    let row_idx = repositories::workspaces::data_frames::rows::get_row_idx(row_df)?
        .ok_or_else(|| OxenError::basic_str("Row index not found"))?;
    let row_idx_og = (row_idx - 1) as i64;

    let commit_merkle_tree = CommitMerkleTree::from_path(repo, commit, &path, true)?;
    let file_node = match commit_merkle_tree.root.node {
        EMerkleTreeNode::File(file_node) => file_node,
        _ => return Err(OxenError::basic_str("File node not found")),
    };

    log::debug!(
        "prepare_modified_or_removed_row() commit_merkle_tree: {:?}",
        &commit_merkle_tree.root.hash.to_string()
    );

    // let scan_rows = 10000 as usize;
    let committed_df_path = util::fs::version_path_from_node(
        repo,
        commit_merkle_tree.root.hash.to_string(),
        path.as_ref(),
    );

    log::debug!(
        "prepare_modified_or_removed_row() committed_df_path: {:?}",
        committed_df_path
    );

    // TODONOW should not be using all rows - just need to parse delim
    let lazy_df =
        tabular::read_df_with_extension(committed_df_path, file_node.extension, &DFOpts::empty())?;

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

pub fn restore_row_in_db(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    row_id: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let row_id = row_id.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path.as_ref());
    let conn = df_db::get_connection(db_path)?;
    let opts = db::key_val::opts::default();
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, path.as_ref());
    let db = DB::open(&opts, dunce::simplified(&column_changes_path))?;

    // Get the row by id
    let row =
        repositories::workspaces::data_frames::rows::get_by_id(workspace, path.as_ref(), row_id)?;

    if row.height() == 0 {
        return Err(OxenError::resource_not_found(row_id));
    };

    let row_status = repositories::workspaces::data_frames::rows::get_row_status(&row)?
        .ok_or_else(|| OxenError::basic_str("Row status not found"))?;
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
            let mut insert_row = prepare_modified_or_removed_row(
                &workspace.base_repo,
                &workspace.commit,
                path.as_ref(),
                &row,
            )?;
            log::debug!("restore_row() insert_row: {:?}", insert_row);
            rows::revert_row_changes(&db, row_id.to_owned())?;
            log::debug!("restore_row() after revert");
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
