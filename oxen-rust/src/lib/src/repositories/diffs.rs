//! # oxen diff
//!
//! Compare two files to find changes between them.
//!
//! ## Usage
//!
//! ```shell
//! oxen diff <file_1> <file_2> [options]
//! ```

use crate::constants::{CACHE_DIR, COMPARES_DIR, LEFT_COMPARE_COMMIT, RIGHT_COMPARE_COMMIT};
use crate::core::merge::entry_merge_conflict_reader::EntryMergeConflictReader;
use crate::core::versions::MinOxenVersion;
use crate::model::entry::commit_entry::CommitPath;
use crate::model::merkle_tree::node::FileNode;

use crate::core;
use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::diff::tabular_diff::{
    TabularDiff, TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};

use crate::model::staged_data::StagedDataOpts;
use crate::model::{
    Commit, CommitEntry, DataFrameDiff, DiffEntry, EntryDataType, LocalRepository, ParsedResource,
    Schema,
};

use crate::view::Pagination;
use crate::{constants, repositories, util};

use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::model::diff::diff_entries_counts::DiffEntriesCounts;
use crate::model::diff::schema_diff::SchemaDiff;
use crate::model::diff::AddRemoveModifyCounts;
use crate::model::diff::DiffResult;

use crate::opts::{DFOpts, DiffOpts};

pub mod join_diff;
pub mod utf8_diff;

const TARGETS_HASH_COL: &str = "_targets_hash";
const KEYS_HASH_COL: &str = "_keys_hash";
const DUPES_PATH: &str = "dupes.json";

fn is_files_tabular(file_1: impl AsRef<Path>, file_2: impl AsRef<Path>) -> bool {
    util::fs::is_tabular(file_1.as_ref()) && util::fs::is_tabular(file_2.as_ref())
}
fn is_files_utf8(file_1: impl AsRef<Path>, file_2: impl AsRef<Path>) -> bool {
    util::fs::is_utf8(file_1.as_ref()) && util::fs::is_utf8(file_2.as_ref())
}

pub fn diff(opts: DiffOpts) -> Result<Vec<DiffResult>, OxenError> {
    log::debug!(
        "Starting diff function with keys: {:?} and targets: {:?}",
        opts.keys,
        opts.targets
    );

    let repo = match &opts.repo_dir {
        Some(dir) => {
            log::debug!("dir: {:?}", dir);
            LocalRepository::from_dir(dir)?
        }
        None => {
            log::debug!("current dir");
            LocalRepository::from_current_dir()?
        }
    };

    log::debug!(
        "Processing paths and revisions - path_2: {:?}, revision_1: {:?}, revision_2: {:?}, path_1: {:?}",
        opts.path_2,
        opts.revision_1,
        opts.revision_2,
        opts.path_1
    );

    match (&opts.path_2, &opts.revision_1, &opts.revision_2) {
        (Some(path_2), Some(rev_1), Some(rev_2)) => {
            log::debug!(
                "🚀 Comparing revisions: {}:{} with {}:{}",
                rev_1,
                opts.path_1.display(),
                rev_2,
                path_2.display()
            );
            diff_revs(&repo, rev_1, &opts.path_1.clone(), rev_2, path_2, &opts)
        }

        // Compare same path with two revisions
        (None, Some(rev_1), Some(rev_2)) => diff_revs(
            &repo,
            rev_1,
            &opts.path_1.clone(),
            rev_2,
            &opts.path_1.clone(),
            &opts,
        ),

        // Compare rev_1 with current changes.
        (Some(path_2), Some(rev_1), None) => {
            diff_uncommitted(&repo, rev_1, &opts.path_1.clone(), path_2, &opts)
        }

        // Compare HEAD with current changes
        (None, Some(rev_1), None) => diff_uncommitted(
            &repo,
            rev_1,
            &opts.path_1.clone(),
            &opts.path_1.clone(),
            &opts,
        ),

        (Some(path_2), None, None) => {
            // Direct file comparison mode

            let result = diff_files(
                opts.path_1,
                path_2,
                opts.keys.clone(),
                opts.targets.clone(),
                vec![],
            )?;
            log::debug!("🚀 Direct file comparison completed successfully");
            return Ok(vec![result]);
        }

        // // Compare
        // (None, Some(rev_1), None) => {

        // }
        _ => {
            log::debug!("🚀 Single file comparison mode");
            Err(OxenError::basic_str(
                "Single file comparison mode not supported",
            ))
        }
    }
}

pub fn diff_uncommitted(
    repo: &LocalRepository,
    rev_1: &str,
    path_1: &PathBuf,
    path_2: &PathBuf,
    opts: &DiffOpts,
) -> Result<Vec<DiffResult>, OxenError> {
    log::debug!("🚀 Computing content diff for file: {:?}", path_1);
    let status_opts = StagedDataOpts::from_paths(&[path_1.clone()]);
    log::debug!("🚀 Computing content diff for file: {:?}", path_1);
    let status = repositories::status::status_from_opts(repo, &status_opts)?;
    log::debug!("🚀 Computing content diff for file: {:?}", path_1);
    let unstaged_files = status.unstaged_files();
    let commit_1 = repositories::revisions::get(&repo, rev_1)?
        .ok_or_else(|| OxenError::revision_not_found(rev_1.to_string().into()))?;
    let mut diff_result = Vec::new();

    for file in unstaged_files {
        log::debug!("🔥 Computing content diff for file: {:?}", file.as_path());
        let node_1 = Some(
            repositories::entries::get_file(repo, &commit_1, path_1)?.ok_or_else(|| {
                OxenError::ResourceNotFound(format!("{}@{}", path_1.display(), commit_1.id).into())
            })?,
        );
        diff_result.push(diff_file_and_node(
            &repo,
            path_2,
            &node_1.unwrap(),
            opts.keys.clone(),
            opts.targets.clone(),
            vec![],
        )?);
    }

    Ok(diff_result)
}

pub fn diff_revs(
    repo: &LocalRepository,
    rev_1: &str,
    path_1: &PathBuf,
    rev_2: &str,
    path_2: &PathBuf,
    opts: &DiffOpts,
) -> Result<Vec<DiffResult>, OxenError> {
    log::debug!(
        "🚀 Comparing revisions: {}:{} with {}:{}",
        rev_1,
        path_1.display(),
        rev_2,
        path_2.display()
    );
    let commit_1 = repositories::revisions::get(&repo, rev_1)?
        .ok_or_else(|| OxenError::revision_not_found(rev_1.to_string().into()))?;
    let commit_2 = repositories::revisions::get(&repo, rev_2)?
        .ok_or_else(|| OxenError::revision_not_found(rev_2.to_string().into()))?;

    let dir_diff = diff_path(&repo, &commit_1, &commit_2, path_1.clone(), path_2, &opts)?;
    log::debug!(
        "Directory structural diff found {} entries",
        dir_diff.entries.len()
    );

    let mut content_diffs = vec![];

    for entry in dir_diff.entries {
        if let (Some(head_res), Some(base_res)) = (entry.head_resource, entry.base_resource) {
            log::debug!("Computing content diff for file: {:?}", head_res.path);
            let cpath_1 = CommitPath {
                commit: Some(commit_1.clone()),
                path: head_res.path.clone(),
            };
            let cpath_2 = CommitPath {
                commit: Some(commit_2.clone()),
                path: base_res.path,
            };

            match diff_commits(
                &repo,
                cpath_1,
                cpath_2,
                opts.keys.clone(),
                opts.targets.clone(),
                vec![],
            ) {
                Ok(result) => {
                    log::debug!("Content diff successful for file: {:?}", head_res.path);
                    content_diffs.push(result);
                }
                Err(err) => {
                    log::error!(
                        "Failed to compute diff for file {:?}: {}",
                        head_res.path,
                        err
                    );
                }
            }
        }
    }
    log::debug!(
        "Directory diff completed with {} content diffs",
        content_diffs.len()
    );
    Ok(content_diffs)
}

pub fn diff_commits(
    repo: &LocalRepository,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<DiffResult, OxenError> {
    log::debug!(
        "Compare command called with: {:?} and {:?}",
        cpath_1,
        cpath_2
    );

    let (node_1, node_2) = match (cpath_1.commit, cpath_2.commit) {
        (Some(commit_1), Some(commit_2)) => {
            let node_1 = Some(
                repositories::entries::get_file(repo, &commit_1, &cpath_1.path)?.ok_or_else(
                    || {
                        OxenError::ResourceNotFound(
                            format!("{}@{}", cpath_1.path.display(), commit_1.id).into(),
                        )
                    },
                )?,
            );

            let merger = EntryMergeConflictReader::new(repo)?;

            let node_2 = match merger.has_conflicts()? {
                true => {
                    let merger = EntryMergeConflictReader::new(repo)?;
                    Some(
                        repositories::entries::get_file(
                            repo,
                            &merger.get_conflict_commit()?.unwrap(),
                            &cpath_2.path,
                        )?
                        .ok_or_else(|| {
                            OxenError::ResourceNotFound(
                                format!("{}@{}", cpath_2.path.display(), commit_2.id).into(),
                            )
                        })?,
                    )
                }
                false => Some(
                    repositories::entries::get_file(repo, &commit_2, &cpath_2.path)?.ok_or_else(
                        || {
                            OxenError::ResourceNotFound(
                                format!("{}@{}", cpath_2.path.display(), commit_2.id).into(),
                            )
                        },
                    )?,
                ),
            };

            (node_1, node_2)
        }
        _ => (None, None),
    };

    match (node_1, node_2) {
        (Some(node_1), Some(node_2)) => {
            let compare_result = repositories::diffs::diff_file_nodes(
                repo, &node_1, &node_2, keys, targets, display,
            )?;

            log::debug!("compare result: {:?}", compare_result);

            Ok(compare_result)
        }
        _ => {
            return Err(OxenError::basic_str(
                "Could not find one or both of the files to compare",
            ));
        }
    }
}

/// Diffs a directory between two commits, returning a summary of changes.
pub fn diff_path(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
    base_path: impl AsRef<Path>,
    head_path: impl AsRef<Path>,
    opts: &DiffOpts,
) -> Result<DiffEntriesCounts, OxenError> {
    match (base_path.as_ref().is_file(), head_path.as_ref().is_file()) {
        (true, true) => {
            let diff_entry = DiffEntry {
                filename: head_path.as_ref().to_string_lossy().to_string(),
                head_resource: Some(ParsedResource {
                    commit: Some(head_commit.clone()),
                    path: head_path.as_ref().to_path_buf(),
                    ..ParsedResource::default() //TODO: Fill in other fields as well
                }),
                base_resource: Some(ParsedResource {
                    commit: Some(base_commit.clone()),
                    path: base_path.as_ref().to_path_buf(),
                    ..ParsedResource::default() //TODO: Fill in other fields as well
                }),
                ..DiffEntry::default()
            };

            let diff_entries = DiffEntriesCounts {
                entries: vec![diff_entry],
                counts: AddRemoveModifyCounts {
                    added: 0,
                    removed: 0,
                    modified: 1, // Since we are comparing two files
                },
                pagination: Pagination::default(), // Assuming no pagination needed for file diff
            };
            Ok(diff_entries)
        }
        (false, false) => list_diff_entries(
            repo,
            base_commit,
            head_commit,
            base_path.as_ref().to_path_buf(),
            head_path.as_ref().to_path_buf(),
            opts.page,
            opts.page_size,
        ),
        _ => {
            println!("Unable to compare directory and file");
            Err(OxenError::basic_str(
                "Cannot compare a directory with a file",
            ))
        }
    }
}

pub fn diff_files(
    path_1: impl AsRef<Path>,
    path_2: impl AsRef<Path>,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<DiffResult, OxenError> {
    log::debug!(
        "Compare command called with: {:?} and {:?}",
        path_1.as_ref(),
        path_2.as_ref()
    );
    if is_files_tabular(&path_1, &path_2) {
        let result = tabular(path_1, path_2, keys, targets, display)?;
        Ok(DiffResult::Tabular(result))
    } else if is_files_utf8(&path_1, &path_2) {
        let result = utf8_diff::diff(path_1, path_2)?;
        Ok(DiffResult::Text(result))
    } else {
        Err(OxenError::invalid_file_type(format!(
            "Compare not supported for files, found {:?} and {:?}",
            path_1.as_ref(),
            path_2.as_ref()
        )))
    }
}

// TODO: merge this and diff_file_and_node
pub fn diff_file_and_node(
    repo: &LocalRepository,
    path_1: impl AsRef<Path>,
    file_2: &FileNode,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<DiffResult, OxenError> {
    match file_2.data_type() {
        EntryDataType::Tabular => {
            let result =
                diff_tabular_file_and_file_node(repo, path_1, file_2, keys, targets, display)?;
            Ok(DiffResult::Tabular(result))
        }
        EntryDataType::Text => {
            let result = diff_text_file_and_node(repo, path_1, file_2)?;
            Ok(result)
        }
        _ => Err(OxenError::invalid_file_type(format!(
            "Compare not supported for files, found {:?} and {:?}",
            path_1.as_ref(),
            file_2.hash().to_string()
        ))),
    }
}

pub fn diff_file_nodes(
    repo: &LocalRepository,
    file_1: &FileNode,
    file_2: &FileNode,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<DiffResult, OxenError> {
    let version_path_1 = util::fs::version_path_from_hash(repo, file_1.hash().to_string());
    let version_path_2 = util::fs::version_path_from_hash(repo, file_2.hash().to_string());

    log::debug!(
        " version_path_1: {:?}",
        *file_1.data_type() == EntryDataType::Tabular
            && *file_2.data_type() == EntryDataType::Tabular
    );

    if *file_1.data_type() == EntryDataType::Tabular
        && *file_2.data_type() == EntryDataType::Tabular
    {
        let mut result = diff_tabular_file_nodes(repo, file_1, file_2, keys, targets, display)?;
        result.filename1 = Some(file_1.name().to_string());
        result.filename2 = Some(file_2.name().to_string());
        Ok(DiffResult::Tabular(result))
    } else if is_files_utf8(&version_path_1, &version_path_2) {
        let mut result = utf8_diff::diff(version_path_1, version_path_2)?;
        result.filename1 = Some(file_1.name().to_string());
        result.filename2 = Some(file_2.name().to_string());
        Ok(DiffResult::Text(result))
    } else {
        Err(OxenError::invalid_file_type(format!(
            "Compare not supported for files, found {:?} and {:?}",
            version_path_1, version_path_2
        )))
    }
}

pub fn diff_tabular_file_and_file_node(
    repo: &LocalRepository,
    file_1_path: impl AsRef<Path>,
    file_2: &FileNode,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<TabularDiff, OxenError> {
    let file_2_path = util::fs::version_path_from_hash(repo, file_2.hash().to_string());

    let df_1 = tabular::read_df(file_1_path, DFOpts::empty())?;
    let df_2 = tabular::read_df(file_2_path, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    validate_required_fields(schema_1, schema_2, keys.clone(), targets.clone())?;

    diff_dfs(&df_1, &df_2, keys, targets, display)
}

pub fn diff_tabular_file_nodes(
    repo: &LocalRepository,
    file_1: &FileNode,
    file_2: &FileNode,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<TabularDiff, OxenError> {
    let version_path_1 = util::fs::version_path_from_hash(repo, file_1.hash().to_string());
    let version_path_2 = util::fs::version_path_from_hash(repo, file_2.hash().to_string());
    let df_1 =
        tabular::read_df_with_extension(version_path_1, file_1.extension(), &DFOpts::empty())?;
    let df_2 =
        tabular::read_df_with_extension(version_path_2, file_2.extension(), &DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    validate_required_fields(schema_1, schema_2, keys.clone(), targets.clone())?;

    diff_dfs(&df_1, &df_2, keys, targets, display)
}

pub fn diff_text_file_and_node(
    repo: &LocalRepository,
    file_1_path: impl AsRef<Path>,
    file_2: &FileNode,
) -> Result<DiffResult, OxenError> {
    let version_path_2 = util::fs::version_path_from_hash(repo, file_2.hash().to_string());
    let result = utf8_diff::diff(&file_1_path, &version_path_2)?;
    Ok(DiffResult::Text(result))
}

pub fn diff_text_file_nodes(
    repo: &LocalRepository,
    file_1: &FileNode,
    file_2: &FileNode,
) -> Result<DiffResult, OxenError> {
    let version_path_1 = util::fs::version_path_from_hash(repo, file_1.hash().to_string());
    let version_path_2 = util::fs::version_path_from_hash(repo, file_2.hash().to_string());

    let result = utf8_diff::diff(&version_path_1, &version_path_2)?;
    Ok(DiffResult::Text(result))
}

pub fn tabular(
    file_1: impl AsRef<Path>,
    file_2: impl AsRef<Path>,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<TabularDiff, OxenError> {
    let df_1 = tabular::read_df(file_1, DFOpts::empty())?;
    let df_2 = tabular::read_df(file_2, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    validate_required_fields(schema_1, schema_2, keys.clone(), targets.clone())?;

    diff_dfs(&df_1, &df_2, keys, targets, display)
}

fn validate_required_fields(
    schema_1: Schema,
    schema_2: Schema,
    keys: Vec<String>,
    targets: Vec<String>,
) -> Result<(), OxenError> {
    // Keys must be in both dfs
    if !schema_1.has_field_names(&keys) {
        return Err(OxenError::incompatible_schemas(schema_1.clone()));
    };

    if !schema_2.has_field_names(&keys) {
        return Err(OxenError::incompatible_schemas(schema_2));
    };

    // Targets must be in either df
    for target in targets {
        if !schema_1.has_field_name(&target) && !schema_2.has_field_name(&target) {
            return Err(OxenError::incompatible_schemas(schema_1));
        }
    }

    Ok(())
}

pub fn diff_dfs(
    df_1: &DataFrame,
    df_2: &DataFrame,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<TabularDiff, OxenError> {
    let schema_diff = get_schema_diff(df_1, df_2);

    let (keys, targets) = get_keys_targets_smart_defaults(keys, targets, &schema_diff)?;
    let display = get_display_smart_defaults(&keys, &targets, display, &schema_diff);

    log::debug!("df_1 is {:?}", df_1);
    log::debug!("df_2 is {:?}", df_2);

    let (df_1, df_2) = hash_dfs(df_1.clone(), df_2.clone(), &keys, &targets)?;

    let compare = join_diff::diff(&df_1, &df_2, schema_diff, &keys, &targets, &display)?;

    Ok(compare)
}

fn get_schema_diff(df1: &DataFrame, df2: &DataFrame) -> SchemaDiff {
    let df1_cols = df1.get_column_names();
    let df2_cols = df2.get_column_names();

    let mut df1_set = HashSet::new();
    let mut df2_set = HashSet::new();

    for col in df1_cols.iter() {
        df1_set.insert(col);
    }

    for col in df2_cols.iter() {
        df2_set.insert(col);
    }

    let added_cols: Vec<String> = df2_set
        .difference(&df1_set)
        .map(|s| (*s).to_string())
        .collect();
    let removed_cols: Vec<String> = df1_set
        .difference(&df2_set)
        .map(|s| (*s).to_string())
        .collect();
    let unchanged_cols: Vec<String> = df1_set
        .intersection(&df2_set)
        .map(|s| (*s).to_string())
        .collect();

    SchemaDiff {
        added_cols,
        removed_cols,
        unchanged_cols,
    }
}

fn get_keys_targets_smart_defaults(
    keys: Vec<String>,
    targets: Vec<String>,
    schema_diff: &SchemaDiff,
) -> Result<(Vec<String>, Vec<String>), OxenError> {
    log::debug!(
        "get_keys_targets_smart_defaults keys {:?} targets {:?}",
        keys,
        targets
    );
    let has_keys = !keys.is_empty();
    let has_targets = !targets.is_empty();

    match (has_keys, has_targets) {
        (true, true) => Ok((keys, targets)),
        (true, false) => {
            let filled_targets = schema_diff
                .unchanged_cols
                .iter()
                .filter(|c| !keys.contains(c))
                .cloned()
                .collect();
            Ok((keys, filled_targets))
        }
        (false, true) => Err(OxenError::basic_str(
            "Must specify at least one key column if specifying target columns.",
        )),
        (false, false) => {
            let filled_keys = schema_diff.unchanged_cols.to_vec();

            let filled_targets = schema_diff
                .added_cols
                .iter()
                .chain(schema_diff.removed_cols.iter())
                .cloned()
                .collect();
            Ok((filled_keys, filled_targets))
        }
    }
}

fn get_display_smart_defaults(
    keys: &[String],
    targets: &[String],
    display: Vec<String>,
    schema_diff: &SchemaDiff,
) -> Vec<String> {
    if !display.is_empty() {
        return display;
    }

    // All non-key non-target columns, with the appropriate suffix(es)
    let mut display_default = vec![];
    for col in &schema_diff.unchanged_cols {
        if !keys.contains(col) && !targets.contains(col) {
            display_default.push(format!("{}.left", col));
            display_default.push(format!("{}.right", col));
        }
    }

    for col in &schema_diff.removed_cols {
        if !keys.contains(col) && !targets.contains(col) {
            display_default.push(format!("{}.left", col));
        }
    }

    for col in &schema_diff.added_cols {
        if !keys.contains(col) && !targets.contains(col) {
            display_default.push(format!("{}.right", col));
        }
    }

    display_default
}

fn hash_dfs(
    mut left_df: DataFrame,
    mut right_df: DataFrame,
    keys: &[String],
    targets: &[String],
) -> Result<(DataFrame, DataFrame), OxenError> {
    left_df = tabular::df_hash_rows_on_cols(left_df, targets, TARGETS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, targets, TARGETS_HASH_COL)?;

    left_df = tabular::df_hash_rows_on_cols(left_df, keys, KEYS_HASH_COL)?;
    right_df = tabular::df_hash_rows_on_cols(right_df, keys, KEYS_HASH_COL)?;
    Ok((left_df, right_df))
}

pub fn count_added_rows(base_df: DataFrame, head_df: DataFrame) -> Result<usize, OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df)?;
    let head_df = tabular::df_hash_rows(head_df)?;

    // log::debug!("count_added_rows got base_df {}", base_df);
    // log::debug!("count_added_rows got head_df {}", head_df);

    let base_hash_indices: HashSet<String> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    let head_hash_indices: HashSet<String> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    // Count the number of new rows
    let num_new_rows = head_hash_indices.difference(&base_hash_indices).count();
    // log::debug!("count_added_rows got num_new_rows {}", num_new_rows);
    Ok(num_new_rows)
}

pub fn count_removed_rows(base_df: DataFrame, head_df: DataFrame) -> Result<usize, OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df)?;
    let head_df = tabular::df_hash_rows(head_df)?;

    // log::debug!("count_removed_rows got base_df {}", base_df);
    // log::debug!("count_removed_rows got head_df {}", head_df);

    let base_hash_indices: HashSet<String> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    let head_hash_indices: HashSet<String> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    // Count the number of removed rows
    let num_removed_rows = base_hash_indices.difference(&head_hash_indices).count();
    // log::debug!(
    //     "count_removed_rows got num_removed_rows {}",
    //     num_removed_rows
    // );
    Ok(num_removed_rows)
}

pub fn compute_new_row_indices(
    base_df: &DataFrame,
    head_df: &DataFrame,
) -> Result<(Vec<u32>, Vec<u32>), OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df.clone())?;
    let head_df = tabular::df_hash_rows(head_df.clone())?;

    log::debug!("diff_current got current hashes base_df {:?}", base_df);
    log::debug!("diff_current got current hashes head_df {:?}", head_df);

    let base_hash_indices: HashMap<String, u32> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    let head_hash_indices: HashMap<String, u32> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    // Added is all the row hashes that are in head that are not in base
    let mut added_indices: Vec<u32> = head_hash_indices
        .iter()
        .filter(|(hash, _indices)| !base_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    added_indices.sort(); // so is deterministic and returned in correct order

    // Removed is all the row hashes that are in base that are not in head
    let mut removed_indices: Vec<u32> = base_hash_indices
        .iter()
        .filter(|(hash, _indices)| !head_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    removed_indices.sort(); // so is deterministic and returned in correct order

    log::debug!("diff_current added_indices {:?}", added_indices.len());
    log::debug!("diff_current removed_indices {:?}", removed_indices.len());

    Ok((added_indices, removed_indices))
}

pub fn compute_new_rows(
    base_df: &DataFrame,
    head_df: &DataFrame,
    schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Compute row indices
    let (added_indices, removed_indices) = compute_new_row_indices(base_df, head_df)?;

    // Take added from the current df
    let added_rows = if !added_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(schema);
        let head_df = tabular::transform(head_df.clone(), opts)?;
        Some(tabular::take(head_df.lazy(), added_indices)?)
    } else {
        None
    };
    log::debug!("diff_current added_rows {:?}", added_rows);

    // Take removed from versioned df
    let removed_rows = if !removed_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(schema);
        let base_df = tabular::transform(base_df.clone(), opts)?;
        Some(tabular::take(base_df.lazy(), removed_indices)?)
    } else {
        None
    };
    log::debug!("diff_current removed_rows {:?}", removed_rows);

    Ok(DataFrameDiff {
        head_schema: Some(schema.to_owned()),
        base_schema: Some(schema.to_owned()),
        added_rows,
        removed_rows,
        added_cols: None,
        removed_cols: None,
    })
}

pub fn compute_new_rows_proj(
    // the lowest common schema dataframes
    base_df: &DataFrame,
    head_df: &DataFrame,
    // the original dataframes
    proj_base_df: &DataFrame,
    proj_head_df: &DataFrame,
    // have to pass in the correct schemas to select the new rows
    base_schema: &Schema,
    head_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Compute row indices
    let (added_indices, removed_indices) = compute_new_row_indices(base_df, head_df)?;

    // Take added from the current df
    let added_rows = if !added_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(head_schema);
        let proj_head_df = tabular::transform(proj_head_df.clone(), opts)?;
        Some(tabular::take(proj_head_df.lazy(), added_indices)?)
    } else {
        None
    };
    log::debug!("diff_current added_rows {:?}", added_rows);

    // Take removed from versioned df
    let removed_rows = if !removed_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(base_schema);
        let proj_base_df = tabular::transform(proj_base_df.clone(), opts)?;
        Some(tabular::take(proj_base_df.lazy(), removed_indices)?)
    } else {
        None
    };
    log::debug!("diff_current removed_rows {:?}", removed_rows);

    Ok(DataFrameDiff {
        head_schema: Some(base_schema.to_owned()),
        base_schema: Some(head_schema.to_owned()),
        added_rows,
        removed_rows,
        added_cols: None,
        removed_cols: None,
    })
}

pub fn compute_new_columns_from_dfs(
    base_df: DataFrame,
    head_df: DataFrame,
    base_schema: &Schema,
    head_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    let added_fields = head_schema.added_fields(base_schema);
    let removed_fields = head_schema.removed_fields(base_schema);

    let added_cols = if !added_fields.is_empty() {
        let opts = DFOpts::from_columns(added_fields);
        let df_added = tabular::transform(head_df, opts)?;
        log::debug!("Got added col df: {}", df_added);
        if df_added.width() > 0 {
            Some(df_added)
        } else {
            None
        }
    } else {
        None
    };

    let removed_cols = if !removed_fields.is_empty() {
        let opts = DFOpts::from_columns(removed_fields);
        let df_removed = tabular::transform(base_df, opts)?;
        log::debug!("Got removed col df: {}", df_removed);
        if df_removed.width() > 0 {
            Some(df_removed)
        } else {
            None
        }
    } else {
        None
    };

    Ok(DataFrameDiff {
        head_schema: Some(base_schema.to_owned()),
        base_schema: Some(base_schema.to_owned()),
        added_rows: None,
        removed_rows: None,
        added_cols,
        removed_cols,
    })
}

pub fn diff_entries(
    repo: &LocalRepository,
    file_path: impl AsRef<Path>,
    base_entry: Option<FileNode>,
    base_commit: &Commit,
    head_entry: Option<FileNode>,
    head_commit: &Commit,
    df_opts: DFOpts,
) -> Result<DiffEntry, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::diff::diff_entries(
            repo,
            file_path,
            base_entry,
            base_commit,
            head_entry,
            head_commit,
            df_opts,
        ),
    }
}

pub fn list_changed_dirs(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<(PathBuf, DiffEntryStatus)>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::diff::list_changed_dirs(repo, base_commit, head_commit),
    }
}

pub fn cache_tabular_diff(
    repo: &LocalRepository,
    compare_id: &str,
    commit_entry_1: CommitEntry,
    commit_entry_2: CommitEntry,
    diff: &TabularDiff,
) -> Result<(), OxenError> {
    write_diff_commit_ids(
        repo,
        compare_id,
        &Some(commit_entry_1),
        &Some(commit_entry_2),
    )?;
    write_diff_df_cache(repo, compare_id, diff)?;
    write_diff_dupes(repo, compare_id, &diff.summary.dupes)?;

    Ok(())
}

pub fn delete_df_diff(repo: &LocalRepository, compare_id: &str) -> Result<(), OxenError> {
    let compare_dir = get_diff_dir(repo, compare_id);

    if compare_dir.exists() {
        log::debug!(
            "delete_df_compare() found compare_dir, deleting: {:?}",
            compare_dir
        );
        std::fs::remove_dir_all(&compare_dir)?;
    }
    Ok(())
}

fn write_diff_dupes(
    repo: &LocalRepository,
    compare_id: &str,
    dupes: &TabularDiffDupes,
) -> Result<(), OxenError> {
    let compare_dir = get_diff_dir(repo, compare_id);

    if !compare_dir.exists() {
        util::fs::create_dir_all(&compare_dir)?;
    }

    let dupes_path = compare_dir.join(DUPES_PATH);

    std::fs::write(dupes_path, serde_json::to_string(&dupes)?)?;

    Ok(())
}

pub fn get_cached_diff(
    repo: &LocalRepository,
    compare_id: &str,
    compare_entry_1: Option<CommitEntry>,
    compare_entry_2: Option<CommitEntry>,
) -> Result<Option<DiffResult>, OxenError> {
    // Check if commits have changed since LEFT and RIGHT files were last cached
    let (cached_left_id, cached_right_id) = get_diff_commit_ids(repo, compare_id)?;

    // If commits cache files do not exist or have changed since last hash (via branch name) then return None to recompute
    if cached_left_id.is_none() || cached_right_id.is_none() {
        return Ok(None);
    }

    if compare_entry_1.is_none() || compare_entry_2.is_none() {
        return Ok(None);
    }

    // Checked these above
    let left_entry = compare_entry_1.unwrap();
    let right_entry = compare_entry_2.unwrap();

    // TODO this should be cached
    let left_full_df = tabular::read_df(
        repositories::revisions::get_version_file_from_commit_id(
            repo,
            left_entry.commit_id,
            &left_entry.path,
        )?,
        DFOpts::empty(),
    )?;
    let right_full_df = tabular::read_df(
        repositories::revisions::get_version_file_from_commit_id(
            repo,
            right_entry.commit_id,
            &right_entry.path,
        )?,
        DFOpts::empty(),
    )?;

    let schema_diff = TabularSchemaDiff::from_schemas(
        &Schema::from_polars(&left_full_df.schema()),
        &Schema::from_polars(&right_full_df.schema()),
    )?;

    let diff_df = tabular::read_df(get_diff_cache_path(repo, compare_id), DFOpts::empty())?;

    let schemas = TabularDiffSchemas {
        left: Schema::from_polars(&left_full_df.schema()),
        right: Schema::from_polars(&right_full_df.schema()),
        diff: Schema::from_polars(&diff_df.schema()),
    };

    let row_mods = AddRemoveModifyCounts::from_diff_df(&diff_df)?;

    let tab_diff_summary = TabularDiffSummary {
        schemas,
        modifications: TabularDiffMods {
            row_counts: row_mods,
            col_changes: schema_diff,
        },
        dupes: read_dupes(repo, compare_id)?,
    };

    let diff_results = TabularDiff {
        summary: tab_diff_summary,
        // Don't have or need server-updated keys, targets, display on cache hit
        parameters: TabularDiffParameters::empty(),
        contents: diff_df,
        filename1: None,
        filename2: None,
    };

    Ok(Some(DiffResult::Tabular(diff_results)))
}

fn read_dupes(repo: &LocalRepository, compare_id: &str) -> Result<TabularDiffDupes, OxenError> {
    let compare_dir = get_diff_dir(repo, compare_id);
    let dupes_path = compare_dir.join(DUPES_PATH);

    if !dupes_path.exists() {
        return Ok(TabularDiffDupes::empty());
    }

    let dupes: TabularDiffDupes = serde_json::from_str(&std::fs::read_to_string(dupes_path)?)?;

    Ok(dupes)
}

pub fn list_diff_entries(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
    base_dir: PathBuf,
    head_dir: PathBuf,
    page: usize,
    page_size: usize,
) -> Result<DiffEntriesCounts, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::diff::list_diff_entries(
            repo,
            base_commit,
            head_commit,
            base_dir,
            head_dir,
            page,
            page_size,
        ),
    }
}

fn write_diff_df_cache(
    repo: &LocalRepository,
    compare_id: &str,
    diff: &TabularDiff,
) -> Result<(), OxenError> {
    let compare_dir = get_diff_dir(repo, compare_id);
    if !compare_dir.exists() {
        util::fs::create_dir_all(&compare_dir)?;
    }
    // TODO: Expensive clone
    let mut df = diff.contents.clone();

    log::debug!("getting diff cache path");
    let diff_path = get_diff_cache_path(repo, compare_id);
    log::debug!("about to create at path {:?}", diff_path);
    tabular::write_df(&mut df, &diff_path)?;
    Ok(())
}

fn get_diff_commit_ids(
    repo: &LocalRepository,
    compare_id: &str,
) -> Result<(Option<String>, Option<String>), OxenError> {
    let compare_dir = get_diff_dir(repo, compare_id);

    if !compare_dir.exists() {
        return Ok((None, None));
    }

    let left_path = compare_dir.join(LEFT_COMPARE_COMMIT);
    let right_path = compare_dir.join(RIGHT_COMPARE_COMMIT);

    // Should exist together or not at all, but recalculate if for some reaosn one not present
    if !left_path.exists() || !right_path.exists() {
        return Ok((None, None));
    }

    let left_id = std::fs::read_to_string(left_path)?;
    let right_id = std::fs::read_to_string(right_path)?;

    Ok((Some(left_id), Some(right_id)))
}
fn write_diff_commit_ids(
    repo: &LocalRepository,
    compare_id: &str,
    left_entry: &Option<CommitEntry>,
    right_entry: &Option<CommitEntry>,
) -> Result<(), OxenError> {
    let compare_dir = get_diff_dir(repo, compare_id);

    if !compare_dir.exists() {
        util::fs::create_dir_all(&compare_dir)?;
    }

    let left_path = compare_dir.join(LEFT_COMPARE_COMMIT);
    let right_path = compare_dir.join(RIGHT_COMPARE_COMMIT);

    if let Some(commit_entry) = left_entry {
        let left_id = &commit_entry.commit_id;
        std::fs::write(left_path, left_id)?;
    }

    if let Some(commit_entry) = right_entry {
        let right_id = &commit_entry.commit_id;
        std::fs::write(right_path, right_id)?;
    }

    Ok(())
}

// TODO: linear scan is not the most efficient way to do this
pub fn get_add_remove_modify_counts(entries: &[DiffEntry]) -> AddRemoveModifyCounts {
    let mut added = 0;
    let mut removed = 0;
    let mut modified = 0;
    for entry in entries {
        if entry.is_dir {
            continue;
        }

        match DiffEntryStatus::from_str(&entry.status).unwrap() {
            DiffEntryStatus::Added => added += 1,
            DiffEntryStatus::Removed => removed += 1,
            DiffEntryStatus::Modified => modified += 1,
        }
    }
    AddRemoveModifyCounts {
        added,
        removed,
        modified,
    }
}

fn get_diff_cache_path(repo: &LocalRepository, compare_id: &str) -> PathBuf {
    let compare_dir = get_diff_dir(repo, compare_id);
    compare_dir.join("diff.parquet")
}

pub fn get_diff_dir(repo: &LocalRepository, compare_id: &str) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(CACHE_DIR)
        .join(COMPARES_DIR)
        .join(compare_id)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::model::diff::diff_entry_status::DiffEntryStatus;
    use crate::opts::DiffOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use polars::lazy::dsl::{col, lit};
    use polars::lazy::frame::IntoLazy;

    use crate::constants::DIFF_STATUS_COL;
    use crate::model::diff::{ChangeType, DiffResult};
    use crate::model::entry::commit_entry::CommitPath;

    #[tokio::test]
    async fn test_diff_entries_add_multiple() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            // get og commit
            let base_commit = repositories::commits::head_commit(&repo)?;

            // add a new file
            let hello_file = repo.path.join("Hello.txt");
            let world_file = repo.path.join("World.txt");
            test::write_txt_file_to_path(&hello_file, "Hello")?;
            test::write_txt_file_to_path(&world_file, "World")?;

            repositories::add(&repo, &hello_file).await?;
            repositories::add(&repo, &world_file).await?;
            let head_commit = repositories::commit(&repo, "Adding two files")?;

            let entries = repositories::diffs::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                PathBuf::from(""),
                0,
                10,
            )?;
            let entries = entries.entries;
            assert_eq!(2, entries.len());
            assert_eq!(DiffEntryStatus::Added.to_string(), entries[0].status);
            assert_eq!(DiffEntryStatus::Added.to_string(), entries[1].status);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_modify_one_tabular() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(bbox_filename);

            // get og commit
            let base_commit = repositories::commits::head_commit(&repo)?;

            // Remove a row
            let bbox_file = test::modify_txt_file(
                bbox_file,
                r"
file,label,min_x,min_y,width,height
train/dog_1.jpg,dog,101.5,32.0,385,330
train/dog_2.jpg,dog,7.0,29.5,246,247
train/cat_2.jpg,cat,30.5,44.0,333,396
",
            )?;

            repositories::add(&repo, bbox_file).await?;
            let head_commit = repositories::commit(&repo, "Removing a row from train bbox data")?;

            let entries = repositories::diffs::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                PathBuf::from(""),
                0,
                10,
            )?;
            let entries = entries.entries;
            // Recursively marks parent dirs as modified
            assert_eq!(3, entries.len());
            for entry in entries.iter() {
                println!("==================");
                println!("entry {:?}", entry);
                println!("==================");
                assert_eq!(DiffEntryStatus::Modified.to_string(), entry.status);
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_remove_one_tabular_file() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // get og commit
            let base_commit = repositories::commits::head_commit(&repo)?;

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            repositories::rm(&repo, &opts)?;
            let head_commit = repositories::commit(&repo, "Removing a the training data file")?;
            let entries = repositories::diffs::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                PathBuf::from(""),
                0,
                10,
            )?;

            let entries = entries.entries;
            for entry in entries.iter().enumerate() {
                println!("entry {}: {:?}", entry.0, entry.1);
            }

            // There should be 2 modifications (directories) and 1 removal (file)
            assert_eq!(3, entries.len());

            // Find the entry named "annotations" and check that it's modified
            let annotations_entry = entries.iter().find(|entry| entry.filename == "annotations");
            assert!(annotations_entry.is_some());
            assert_eq!(
                annotations_entry.unwrap().status,
                DiffEntryStatus::Modified.to_string()
            );
            println!("CHECK HERE");
            println!("entries: {entries:?}");

            // Ensure the filename is correct across operating systems
            let annotations_path = Path::new("annotations").join(Path::new("train"));
            let annotations_filename = annotations_path.to_str().unwrap();

            // Check that "annotations/train" is modified
            let annotations_train_entry = entries
                .iter()
                .find(|entry| entry.filename == annotations_filename);
            assert!(annotations_train_entry.is_some());
            assert_eq!(
                annotations_train_entry.unwrap().status,
                DiffEntryStatus::Modified.to_string()
            );

            // Check that "annotations/train/bounding_box.csv" is removed
            println!("ROUNT #");
            println!("entries: {entries:?}");

            let bounding_box_path = Path::new("annotations")
                .join(Path::new("train"))
                .join(Path::new("bounding_box.csv"));
            let bounding_box_filename = bounding_box_path.to_str().unwrap();

            let bounding_box_entry = entries
                .iter()
                .find(|entry| entry.filename == bounding_box_filename);
            assert!(bounding_box_entry.is_some());
            assert_eq!(
                bounding_box_entry.unwrap().status,
                DiffEntryStatus::Removed.to_string()
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_get_add_remove_modify_counts() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            // Get initial commit
            let base_commit = repositories::commits::head_commit(&repo)?;
            // Add two files
            let hello_file = repo.path.join("Hello.txt");
            let world_file = repo.path.join("World.txt");
            test::write_txt_file_to_path(&hello_file, "Hello")?;
            test::write_txt_file_to_path(&world_file, "World")?;

            repositories::add(&repo, &hello_file).await?;
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Removing a row from train bbox data")?;

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            repositories::rm(&repo, &opts)?;
            let head_commit = repositories::commit(&repo, "Removing a the training data file")?;

            let entries = repositories::diffs::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                PathBuf::from(""),
                0,
                10,
            )?;
            let entries = entries.entries;
            for entry in entries.iter().enumerate() {
                println!("entry {}: {:?}", entry.0, entry.1);
            }

            let counts = repositories::diffs::get_add_remove_modify_counts(&entries);

            assert_eq!(5, entries.len());
            assert_eq!(2, counts.added);
            assert_eq!(1, counts.removed);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_in_dir_at_root() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            let new_bbox_filename = Path::new("annotations")
                .join("train")
                .join("new_bounding_box.csv");
            let new_bbox_file = repo.path.join(&new_bbox_filename);

            let new_root_filename = Path::new("READMENOW.md");

            let new_root_file = repo.path.join(new_root_filename);

            let add_dir = PathBuf::from("annotations").join("schmannotations");
            let add_dir_added_file = PathBuf::from("annotations")
                .join("schmannotations")
                .join("added_file.txt");

            let add_root_dir = PathBuf::from("not_annotations");
            let add_root_dir_added_file = PathBuf::from("not_annotations").join("added_file.txt");

            util::fs::create_dir_all(repo.path.join(add_dir))?;
            util::fs::create_dir_all(repo.path.join(add_root_dir))?;

            test::write_txt_file_to_path(&new_root_file, "Hello,world")?;
            test::write_txt_file_to_path(&new_bbox_file, "Hello,world")?;
            test::write_txt_file_to_path(repo.path.join(add_dir_added_file), "Hello,world!!")?;
            test::write_txt_file_to_path(repo.path.join(add_root_dir_added_file), "Hello,world!!")?;

            // get og commit
            let base_commit = repositories::commits::head_commit(&repo)?;

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            repositories::rm(&repo, &opts)?;
            repositories::add(&repo, &repo.path).await?;
            let head_commit = repositories::commit(&repo, "Removing a the training data file")?;
            let entries = repositories::diffs::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                PathBuf::from(""),
                0,
                10,
            )?;

            let entries = entries.entries;

            let annotation_diff_entries = repositories::diffs::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from("annotations"),
                PathBuf::from("annotations"),
                0,
                10,
            )?;

            // We should have...
            // 1. A modification in the `annotations` directory
            // 2. The addition of the README.md file
            log::debug!("Got entries: {:?}", entries);

            assert_eq!(3, entries.len());

            assert_eq!(entries[0].status, DiffEntryStatus::Modified.to_string());
            assert_eq!(entries[1].status, DiffEntryStatus::Added.to_string());
            assert_eq!(entries[2].status, DiffEntryStatus::Added.to_string());

            // 1. Schmannotations dir added
            // 2. Train dir modified

            log::debug!(
                "Got annotation_diff_entries: {:?}",
                annotation_diff_entries.entries
            );

            assert_eq!(2, annotation_diff_entries.entries.len());

            assert_eq!(
                annotation_diff_entries.entries[0].status,
                DiffEntryStatus::Added.to_string()
            );
            assert_eq!(
                annotation_diff_entries.entries[1].status,
                DiffEntryStatus::Modified.to_string()
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_remove_one_tabular_in_dir() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // get og commit
            let base_commit = repositories::commits::head_commit(&repo)?;

            // Remove the file
            util::fs::remove_file(bbox_file)?;

            let opts = RmOpts::from_path(&bbox_filename);
            repositories::rm(&repo, &opts)?;
            let head_commit = repositories::commit(&repo, "Removing a the training data file")?;
            let entries = repositories::diffs::list_diff_entries(
                &repo,
                &base_commit,
                &head_commit,
                PathBuf::from(""),
                PathBuf::from(""),
                0,
                10,
            )?;

            println!("counts: {:?}", entries.counts);

            // Make sure there is one removed file in the counts
            assert_eq!(0, entries.counts.added);
            assert_eq!(1, entries.counts.removed);
            assert_eq!(0, entries.counts.modified);

            let entries = entries.entries;
            for entry in entries.iter().enumerate() {
                println!("entry {}: {:?}", entry.0, entry.1);
            }

            // We are just listing top level entries, so only one directory
            assert_eq!(1, entries.len());

            // Dir is modified because a child was removed
            assert_eq!(entries[0].status, DiffEntryStatus::Modified.to_string());

            Ok(())
        })
        .await
    }

    #[test]
    fn test_diff_txt_files() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let file1 = dir.join("file1.txt");
            let file2 = dir.join("file2.txt");
            println!("file1: {:?}", file1);
            println!("file2: {:?}", file2);

            util::fs::write_to_path(&file1, "hello\nhi\nhow are you?")?;
            util::fs::write_to_path(&file2, "hello\nhi\nhow are you doing?")?;

            let opts = DiffOpts {
                repo_dir: Some(dir.to_path_buf()),
                path_1: file1,
                path_2: Some(file2),
                keys: vec![],
                targets: vec![],
                revision_1: None,
                revision_2: None,
                output: None,
                page: 0,
                page_size: 10,
            };

            println!("!!!!!!!!!");
            let diff = repositories::diffs::diff(opts)?;

            match diff.first() {
                Some(DiffResult::Text(ref result)) => {
                    let lines = &result.lines;
                    println!("!!!!");

                    for line in lines {
                        println!("!!!!");
                        println!("{:?}", line);
                    }

                    assert_eq!(lines.len(), 4);

                    // should be 2 unchanged
                    assert_eq!(lines[0].modification, ChangeType::Unchanged);
                    assert_eq!(&lines[0].text, "hello");
                    assert_eq!(lines[1].modification, ChangeType::Unchanged);
                    assert_eq!(&lines[1].text, "hi");
                    // 1 removed
                    assert_eq!(lines[2].modification, ChangeType::Removed);
                    assert_eq!(&lines[2].text, "how are you?");
                    // 1 added
                    assert_eq!(lines[3].modification, ChangeType::Added);
                    assert_eq!(&lines[3].text, "how are you doing?");
                }
                _ => panic!("expected text result"),
            }

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_compare_one_added_one_removed_no_keys_no_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv1 = "a,b,c\n1,2,3\n4,5,6\n";
            let csv2 = "a,b,c\n1,2,3\n4,5,2\n";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            repositories::add(&repo, repo.path.clone()).await?;

            let commit = repositories::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result =
                repositories::diffs::diff_commits(&repo, c1, c2, vec![], vec![], vec![])?;

            let diff_col = DIFF_STATUS_COL;
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 2);
                    assert_eq!(df.width(), 4); // 3 (inferred) key columns + diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 1);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }
    #[tokio::test]
    async fn test_compare_all_types_with_keys_and_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Keying on "a" and "b" with target "c"
            // Removed: -> 5,6 and 7,8
            // Added: 9,10
            // Modified: 3,4 (1 -> 1234)
            // Unchanged: 1,2 (not included)

            let csv1 = "a,b,c\n1,2,1\n3,4,1\n5,6,1\n7,8,1";
            let csv2 = "a,b,c\n1,2,1\n3,4,1234\n9,10,1";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            repositories::add(&repo, repo.path.clone()).await?;

            let commit = repositories::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = repositories::diffs::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string()],
                vec![],
            )?;

            let diff_col = DIFF_STATUS_COL;
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 4);
                    assert_eq!(df.width(), 5); // 2 key columns, 1 target column * 2 views each, and diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 2);
                    assert_eq!(modified_df.height(), 1);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_all_types_with_keys_and_same_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // If all the targets are 1 - aka, using targets but none are unchanged, we want to make sure that
            // the column namings of .left and .right are consistently handled

            let csv1 = "a,b,c\n1,2,1\n3,4,1\n5,6,1\n7,8,1";
            let csv2 = "a,b,c\n1,2,1\n3,4,1\n9,10,1";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            repositories::add(&repo, repo.path.clone()).await?;

            let commit = repositories::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = repositories::diffs::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string()],
                vec![],
            )?;

            let diff_col = DIFF_STATUS_COL;
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 3);
                    assert_eq!(df.width(), 5); // 2 key columns, 1 target column * 2 views each, and diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 2);
                    assert_eq!(modified_df.height(), 0);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_same_files_with_targets() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n";
            let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            repositories::add(&repo, repo.path.clone()).await?;

            let commit = repositories::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = repositories::diffs::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()],
                vec![],
            )?;

            // Should return empty df
            let diff_col = DIFF_STATUS_COL;
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 0);
                    assert_eq!(df.width(), 7); // 2 key columns, 2 targets * 2(right+left) + diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 0);
                    assert_eq!(removed_df.height(), 0);
                    assert_eq!(modified_df.height(), 0);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn compare_no_keys_no_targets_added_column() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n8,7,6,5";
            let csv2 = "a,b,c,d,e\n1,2,3,4,5\n4,5,6,7,8\n9,8,7,6,5";
            // 2 modified (added row) 1 added 1 removed

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            repositories::add(&repo, repo.path.clone()).await?;

            let commit = repositories::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result =
                repositories::diffs::diff_commits(&repo, c1, c2, vec![], vec![], vec![])?;

            // Should return empty df
            let diff_col = DIFF_STATUS_COL;
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 4);
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 1);
                    assert_eq!(modified_df.height(), 2);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_keys_no_targets_implies_modified() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // We'll key on a, b, and c.
            // D will then be an implicit target bc it's a shared column
            // (1 added, 1 removed, 1 modified)
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n0,0,0,0\n";
            let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n1,1,1,1\n";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            repositories::add(&repo, repo.path.clone()).await?;

            let commit = repositories::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = repositories::diffs::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                vec![],
                vec![],
            )?;

            // Should return empty df
            let diff_col = DIFF_STATUS_COL;
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 3);
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 1);
                    assert_eq!(modified_df.height(), 1);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }
}
