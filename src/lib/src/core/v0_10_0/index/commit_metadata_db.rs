//! Database for commit metadata entries.
//!

use polars::prelude::DataFrame;
use sql_query_builder as sql;
use std::path::{Path, PathBuf};

use crate::constants::{CACHE_DIR, HISTORY_DIR};
use crate::core::db::data_frames::df_db;
use crate::error::OxenError;
use crate::model::{Commit, DirMetadataItem, LocalRepository};
use crate::util;

use super::CommitEntryReader;

pub fn db_path(
    repo: &LocalRepository,
    commit: &Commit,
    // directory: impl AsRef<Path>
) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("metadata")
        // .join(directory)
        .join("metadata.duckdb")
}

/// Select entries from a directory
pub fn select(
    repo: &LocalRepository,
    commit: &Commit,
    directory: impl AsRef<Path>,
    offset: usize,
    limit: usize,
) -> Result<DataFrame, OxenError> {
    let directory = directory.as_ref();
    let conn = df_db::get_connection(db_path(repo, commit))?;
    let s = DirMetadataItem::schema();
    let table_name = "metadata";
    let fields: Vec<String> = s.fields.iter().map(|f| f.name.to_owned()).collect();

    let stmt = sql::Select::new()
        .select(&fields.join(", "))
        .where_clause(&format!("directory = '{}'", directory.to_string_lossy()))
        .offset(&offset.to_string())
        .limit(&limit.to_string())
        .from(table_name);

    let df = df_db::select(&conn, &stmt, false, None, None)?;
    Ok(df)
}

/// Recursively compute the full number of entries in a directory
pub fn full_size(
    repo: &LocalRepository,
    commit: &Commit,
    directory: impl AsRef<Path>,
) -> Result<(usize, usize), OxenError> {
    let directory = directory.as_ref();
    let mut dirs = CommitEntryReader::new(repo, commit)?.list_dir_children(directory)?;
    dirs.push(directory.to_path_buf());

    let s = DirMetadataItem::schema();
    let table_name = "metadata";
    let num_cols = s.fields.len();

    let conn = df_db::get_connection(db_path(repo, commit))?;

    let mut num_rows = 0;
    for dir in dirs {
        num_rows += df_db::count_where(
            &conn,
            table_name,
            format!("directory = '{}'", dir.to_string_lossy()),
        )?;
    }

    Ok((num_rows, num_cols))
}
