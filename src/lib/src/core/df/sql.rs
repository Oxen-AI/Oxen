use std::path::{Path, PathBuf};

use crate::core::df::tabular;
use crate::model::LocalRepository;
use crate::opts::DFOpts;
use crate::repositories;
use crate::{core::db::data_frames::df_db, error::OxenError};
use polars::frame::DataFrame;
use uuid::Uuid;

pub fn query_df_from_repo(
    sql: String,
    repo: &LocalRepository,
    path: &PathBuf,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let commit = repositories::commits::head_commit(repo)?;

    if !repositories::workspaces::data_frames::is_queryable_data_frame_indexed(repo, path, &commit)?
    {
        // If not, proceed to create a new workspace and index the data frame.
        let workspace_id = Uuid::new_v4().to_string();
        let workspace = repositories::workspaces::create(repo, &commit, workspace_id, false)?;
        repositories::workspaces::data_frames::index(repo, &workspace, path)?;
    }

    let workspace =
        crate::core::v0_19_0::workspaces::data_frames::get_queryable_data_frame_workspace(
            repo, path, &commit,
        )?;

    let db_path = repositories::workspaces::data_frames::duckdb_path(&workspace, path);
    let mut conn = df_db::get_connection(db_path)?;
    let df = query_df(&mut conn, sql, Some(opts))?;

    // If we are doing this from the CLI, we don't want to export the hidden Oxen columns
    let df = tabular::strip_excluded_cols(df)?;
    Ok(df)
}

pub fn query_df(
    conn: &mut duckdb::Connection,
    sql: String,
    opts: Option<&DFOpts>,
) -> Result<DataFrame, OxenError> {
    let df = df_db::select_str(conn, sql, false, None, opts)?;

    Ok(df)
}

pub fn export_df(
    conn: &duckdb::Connection,
    sql: String,
    opts: Option<&DFOpts>,
    tmp_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    df_db::export(conn, sql, opts, tmp_path)
}
