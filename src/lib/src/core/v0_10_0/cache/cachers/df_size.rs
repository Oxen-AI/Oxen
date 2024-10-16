use crate::constants::{CACHE_DIR, DATA_FRAMES_DIR, HISTORY_DIR};
use crate::core::df::tabular;
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::error::OxenError;
use crate::model::{Commit, DataFrameSize, LocalRepository};
use crate::opts::DFOpts;
use crate::util;
use polars::prelude::*;
use std::path::Path;
use std::path::PathBuf;

pub const COL_PATH: &str = "path";
pub const COL_WIDTH: &str = "width";
pub const COL_HEIGHT: &str = "height";

pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!(
        "Running compute_df_sizes on {:?} for commit {}",
        repo.path,
        commit.id
    );

    let cache_path = df_size_cache_path(repo, commit);
    let reader = CommitEntryReader::new(repo, commit)?;
    let entries = reader.list_entries()?;

    let mut df = get_cache_df(&cache_path)?;

    for entry in entries {
        let path = util::fs::version_path(repo, &entry);

        // The path may not exist if a file was not fully pushed
        if path.exists() && util::fs::is_tabular(&path) {
            // log::debug!("getting size for entry {:?} at path {:?}", entry, path);
            let data_frame_size = tabular::get_size(&path)?;
            // log::debug!("resulting df size is {:?}", data_frame_size);

            let new_df = df!(
                COL_PATH => [path.to_str()],
                COL_WIDTH => [data_frame_size.width.to_string()],
                COL_HEIGHT => [data_frame_size.height.to_string()])?;

            df = df.vstack(&new_df)?;
        }
    }

    tabular::write_df(&mut df, cache_path)
}

pub fn get_cache_for_version(
    repo: &LocalRepository,
    commit: &Commit,
    version_path: &PathBuf,
) -> Result<DataFrameSize, OxenError> {
    match get_from_cache(repo, commit, version_path) {
        Ok(result) => match result {
            Some(size) => Ok(size),
            None => tabular::get_size(version_path),
        },
        Err(e) => Err(e),
    }
}

fn get_from_cache(
    repo: &LocalRepository,
    commit: &Commit,
    version_path: &Path,
) -> Result<Option<DataFrameSize>, OxenError> {
    let cache_path = df_size_cache_path(repo, commit);

    if !cache_path.exists() {
        log::debug!("cache miss for version at path {:?}", version_path);
        return Ok(None);
    }

    // Don't need that many rows to scan
    let num_scan_rows = 10;
    let mut opts = DFOpts::empty();
    opts.slice = Some(format!("0..{}", num_scan_rows));

    if let Ok(df) = tabular::scan_df(cache_path, &opts, num_scan_rows) {
        let df_for_path = df
            .select([
                col(COL_PATH),
                col(COL_WIDTH).cast(DataType::UInt64),
                col(COL_HEIGHT).cast(DataType::UInt64),
            ])
            .filter(col(COL_PATH).eq(lit(version_path.to_string_lossy().to_string())))
            .collect()?;

        let column_width = df_for_path.column(COL_WIDTH)?.u64()?;

        let column_height = df_for_path.column(COL_HEIGHT)?.u64()?;

        if column_width.is_empty() || column_height.is_empty() {
            log::debug!("df_size::get_from_cache -> cache miss in df");
            return Ok(None);
        }

        if let (Some(width), Some(height)) = (column_width.get(0), column_height.get(0)) {
            // Converts to usize since Polars deals mostly with usize
            // TODO: Check if there is a better way to do handle the size while
            // keeping compatibility with Polars.
            let width: usize = width.try_into().unwrap();
            let height: usize = height.try_into().unwrap();
            return Ok(Some(DataFrameSize { width, height }));
        }
    }
    Ok(None)
}

fn get_cache_df(cache_path: &Path) -> Result<DataFrame, OxenError> {
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    Ok(DataFrame::default())
}

fn df_size_cache_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(DATA_FRAMES_DIR)
        .join("df_size.parquet")
}
