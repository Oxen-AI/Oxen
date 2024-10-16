//! Computes metadata we can extract from the entry files

use crate::constants::{CACHE_DIR, DIRS_DIR, HISTORY_DIR};
use crate::core::df::tabular;
use crate::core::v0_10_0::index::{CommitEntryReader, CommitReader};
use crate::error::OxenError;
use crate::model::{Commit, DirMetadataItem, LocalRepository};
use crate::util;

use indicatif::ProgressBar;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub fn dir_column_path(
    repo: &LocalRepository,
    commit: &Commit,
    dir: &Path,
    column: &str,
) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(DIRS_DIR)
        .join(dir)
        .join(format!("{}.parquet", column))
}

pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!("Running content_metadata");

    log::debug!("computing metadata {} -> {}", commit.id, commit.message);

    // Compute the metadata stats
    let items = compute_metadata_items(repo, commit)?;

    // Then for each directory, aggregate up the data_type and mime_type, and save as a dataframe file
    // that we can serve up.
    let reader = CommitEntryReader::new(repo, commit)?;
    let dirs = reader.list_dirs()?;

    log::debug!("Computing size of {} dirs", dirs.len());
    for dir in dirs {
        log::debug!("Aggregating data_type and mime_type for commit {commit:?}");
        let (mut data_type_df, mut mime_type_df) = aggregate_stats(repo, commit, &dir, &items)?;
        let path = dir_column_path(repo, commit, &dir, "data_type");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        tabular::write_df(&mut data_type_df, &path)?;
        let path = dir_column_path(repo, commit, &dir, "mime_type");
        tabular::write_df(&mut mime_type_df, &path)?;
    }
    Ok(())
}

fn compute_metadata_items(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<DirMetadataItem>, OxenError> {
    let entry_reader = CommitEntryReader::new(repo, commit)?;
    let entries = entry_reader.list_entries()?;
    let dirs = entry_reader.list_dirs()?;

    let commit_reader = CommitReader::new(repo)?;
    let num_entries = entries.len();
    let bar = ProgressBar::new(entries.len() as u64);

    log::debug!("compute metadata for {num_entries} entries in commit: {commit:?}");

    // Compute the metadata in parallel
    let meta_entries = entries
        .par_iter()
        .map(|entry| {
            // Takes some time to compute from_entry
            bar.inc(1);
            DirMetadataItem::from_entry(repo, entry, &commit_reader)
        })
        .collect::<Vec<_>>();

    // Gather dirs, except root to avoid double counting
    let meta_dirs = dirs
        .par_iter()
        .filter(|dir| *dir != &PathBuf::from(""))
        .map(|dir| {
            // Takes some time to compute from_dir
            DirMetadataItem::from_dir(dir, commit)
        })
        .collect::<Vec<_>>();

    let metas = meta_entries
        .into_iter()
        .chain(meta_dirs)
        .collect::<Vec<_>>();

    bar.finish();

    log::debug!(
        "done compute metadata for {} entries in commit: {} -> '{}'",
        entries.len(),
        commit.id,
        commit.message
    );

    Ok(metas)
}

/// Aggregate up the data type counts from all children directories
pub fn aggregate_stats(
    repo: &LocalRepository,
    commit: &Commit,
    directory: impl AsRef<Path>,
    items: &[DirMetadataItem],
) -> Result<(DataFrame, DataFrame), OxenError> {
    let directory = directory.as_ref();
    let mut dirs = CommitEntryReader::new(repo, commit)?.list_dir_children(directory)?;
    dirs.push(directory.to_path_buf());

    // make sure they are uniq
    let dirs: HashSet<&PathBuf> = HashSet::from_iter(dirs.iter());

    if dirs.is_empty() {
        return Err(OxenError::path_does_not_exist(directory));
    }

    if dirs.is_empty() {
        return Ok((DataFrame::default(), DataFrame::default()));
    }

    log::debug!("aggregating dirs {:?}", dirs);
    // Aggregate and count the data_type field in the items
    let mut data_type_counts: HashMap<String, i64> = HashMap::new();
    let mut mime_type_counts: HashMap<String, i64> = HashMap::new();
    for item in items {
        *data_type_counts.entry(item.data_type.clone()).or_insert(0) += 1;
        *mime_type_counts.entry(item.mime_type.clone()).or_insert(0) += 1;
    }

    // Sort the data_type_counts by count
    let mut data_type_counts = data_type_counts.into_iter().collect::<Vec<_>>();
    data_type_counts.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    // Sort the mime_type_counts by count
    let mut mime_type_counts = mime_type_counts.into_iter().collect::<Vec<_>>();
    mime_type_counts.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    // Create polars DataFrame of data_type,count from the HashMap
    let data_type_vec: Vec<String> = data_type_counts.iter().map(|(k, _)| k.clone()).collect();
    let count_vec: Vec<i64> = data_type_counts.iter().map(|(_, v)| *v).collect();

    let data_type_df = DataFrame::new(vec![
        Column::Series(Series::new(
            PlSmallStr::from_str("data_type"),
            data_type_vec,
        )),
        Column::Series(Series::new(PlSmallStr::from_str("count"), count_vec)),
    ])?;

    // Create polars DataFrame of mime_type,count from the HashMap
    let mime_type_vec: Vec<String> = mime_type_counts.iter().map(|(k, _)| k.clone()).collect();
    let count_vec: Vec<i64> = mime_type_counts.iter().map(|(_, v)| *v).collect();

    let mime_type_df = DataFrame::new(vec![
        Column::Series(Series::new(
            PlSmallStr::from_str("mime_type"),
            mime_type_vec,
        )),
        Column::Series(Series::new(PlSmallStr::from_str("count"), count_vec)),
    ])?;

    Ok((data_type_df, mime_type_df))
}
