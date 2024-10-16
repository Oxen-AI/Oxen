use polars::frame::DataFrame;

use sql_query_builder::Select;

use crate::constants::TABLE_NAME;
use crate::constants::{MODS_DIR, OXEN_HIDDEN_DIR};
use crate::core;
use crate::core::db::data_frames::workspace_df_db::select_cols_from_schema;
use crate::core::db::data_frames::{df_db, workspace_df_db};
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, Workspace};
use crate::opts::DFOpts;
use crate::{repositories, util};

use crate::model::diff::tabular_diff::{
    TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};

use std::path::{Path, PathBuf};

pub mod columns;
pub mod rows;
pub mod schemas;

pub fn is_behind(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let commit_path = previous_commit_ref_path(workspace, path);
    let commit_id = util::fs::read_from_path(commit_path)?;
    Ok(commit_id != workspace.commit.id)
}

pub fn is_indexed(workspace: &Workspace, path: &Path) -> Result<bool, OxenError> {
    log::debug!("checking dataset is indexed for {:?}", path);
    let db_path = duckdb_path(workspace, path);
    log::debug!("getting conn at path {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let table_exists = df_db::table_exists(&conn, TABLE_NAME)?;
    log::debug!("dataset_is_indexed() got table_exists: {:?}", table_exists);
    Ok(table_exists)
}

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    path: &PathBuf,
    commit: &Commit,
) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::data_frames::is_queryable_data_frame_indexed(
                repo, commit, path,
            )
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::workspaces::data_frames::is_queryable_data_frame_indexed(
                repo, commit, path,
            )
        }
    }
}

pub fn index(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::data_frames::index(workspace, path.as_ref())
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::workspaces::data_frames::index(workspace, path.as_ref())
        }
    }
}

pub fn unindex(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;
    df_db::drop_table(&conn, TABLE_NAME)?;

    Ok(())
}

pub fn restore(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // Unstage and then restage the df
    unindex(workspace, &path)?;

    // TODO: we could do this more granularly without a full reset
    index(repo, workspace, path.as_ref())?;

    Ok(())
}

pub fn count(workspace: &Workspace, path: impl AsRef<Path>) -> Result<usize, OxenError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;

    let count = df_db::count(&conn, TABLE_NAME)?;
    Ok(count)
}

pub fn query(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("query_staged_df() got db_path: {:?}", db_path);

    let conn = df_db::get_connection(db_path)?;

    // Get the schema of this commit entry
    let schema = df_db::get_schema(&conn, TABLE_NAME)?;

    // Enrich w/ oxen cols
    let full_schema = workspace_df_db::enhance_schema_with_oxen_cols(&schema)?;

    let col_names = select_cols_from_schema(&schema)?;

    let select = Select::new().select(&col_names).from(TABLE_NAME);

    let df = df_db::select(&conn, &select, true, Some(&full_schema), Some(opts))?;

    Ok(df)
}

pub fn diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DataFrame, OxenError> {
    let file_path = path.as_ref();
    let staged_db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let conn = df_db::get_connection(staged_db_path)?;
    let diff_df = workspace_df_db::df_diff(&conn)?;
    Ok(diff_df)
}

pub fn full_diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DiffResult, OxenError> {
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let path = path.as_ref();
    // Get commit for the branch head
    log::debug!("diff_workspace_df got repo at path {:?}", repo.path);

    repositories::CommitMerkleTree::from_path_recursive(repo, commit, path)?;

    if !is_indexed(workspace, path)? {
        return Err(OxenError::basic_str("Dataset is not indexed"));
    };

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let conn = df_db::get_connection(db_path)?;

    let diff_df = workspace_df_db::df_diff(&conn)?;
    log::debug!("full_diff() diff_df: {:?}", diff_df);

    if diff_df.is_empty() {
        return Ok(DiffResult::Tabular(TabularDiff::empty()));
    }

    let row_mods = AddRemoveModifyCounts::from_diff_df(&diff_df)?;

    let schema = workspace_df_db::schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let schemas = TabularDiffSchemas {
        left: schema.clone(),
        right: schema.clone(),
        diff: schema.clone(),
    };

    let diff_summary = TabularDiffSummary {
        modifications: TabularDiffMods {
            row_counts: row_mods,
            col_changes: TabularSchemaDiff::empty(),
        },
        schemas,
        dupes: TabularDiffDupes::empty(),
    };

    let diff_result = TabularDiff {
        contents: diff_df,
        parameters: TabularDiffParameters::empty(),
        summary: diff_summary,
    };

    Ok(DiffResult::Tabular(diff_result))
}

pub fn duckdb_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    log::debug!(
        "duckdb_path path: {:?} workspace: {:?}",
        path,
        workspace.dir()
    );
    let path_hash = util::hasher::hash_str(path.to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("db")
}

pub fn previous_commit_ref_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("COMMIT_ID")
}

pub fn column_changes_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("column_changes")
}

pub fn row_changes_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("row_changes")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;

    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, OXEN_ID_COL};
    use crate::core::df;
    use crate::error::OxenError;
    use crate::model::diff::DiffResult;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::repositories::workspaces;
    use crate::test;
    use crate::{repositories, util};

    #[test]
    fn test_add_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Append row
            let json_data = json!({
                "file": "dawg1.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;

            // List the files that are changed
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 1);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_delete_added_row_with_two_rows() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Append row
            let json_data = json!({
                "file": "dawg1.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let append_entry_1 =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;

            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?.to_string();
            let append_1_id = append_1_id.replace('"', "");

            let json_data = json!({
                "file": "dawg2.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let _append_entry_2 =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;

            // List the files that are changed
            let status = workspaces::status::status(&workspace)?;
            log::debug!("status is {:?}", status);
            assert_eq!(status.staged_files.len(), 1);

            // List the staged mods
            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Delete the first append
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, &append_1_id)?;

            // Should only be one mod now
            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_clear_changes() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Append the data to staging area
            let json_data = json!({
                "file": "dawg1.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let append_entry_1 =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;
            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?;
            let append_1_id = append_1_id.get_str().unwrap();
            log::debug!("added the row");

            let json_data = json!({
                "file": "dawg2.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let append_entry_2 =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;
            let append_2_id = append_entry_2.column(OXEN_ID_COL)?.get(0)?;
            let append_2_id = append_2_id.get_str().unwrap();

            // List the files that are changed
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 1);

            // List the staged mods
            let diff = workspaces::diff(&repo, &workspace, &file_path)?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }
            // Delete the first append
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, append_1_id)?;

            // Delete the second append
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, append_2_id)?;

            // Should be zero staged files
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 0);

            log::debug!("about to diff staged");
            // Should be zero mods left
            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            log::debug!("got diff staged");

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }
            Ok(())
        })
    }

    #[test]
    fn test_delete_committed_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, &id_to_delete)?;

            // List the files that are changed
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 1);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let status = repositories::status(&repo)?;
            log::debug!("got this status {:?}", status);

            // Commit the new file

            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Deleting a row allegedly".to_string(),
            };
            let commit_2 = workspaces::commit(&workspace, &new_commit, branch_name)?;

            let file_1 = repositories::revisions::get_version_file_from_commit_id(
                &repo, &commit.id, &file_path,
            )?;
            // copy the file to the same path but with .csv as the extension
            let file_1_csv = file_1.with_extension("csv");
            util::fs::copy(&file_1, &file_1_csv)?;
            log::debug!("copied file 1 to {:?}", file_1_csv);

            let file_2 = repositories::revisions::get_version_file_from_commit_id(
                &repo,
                commit_2.id,
                &file_path,
            )?;
            let file_2_csv = file_2.with_extension("csv");
            util::fs::copy(&file_2, &file_2_csv)?;
            log::debug!("copied file 2 to {:?}", file_2_csv);
            let diff_result =
                repositories::diffs::diff_files(file_1_csv, file_2_csv, vec![], vec![], vec![])?;

            log::debug!("diff result is {:?}", diff_result);
            match diff_result {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_modify_added_row() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Add a row
            let json_data = json!({
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let new_row =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;

            // 1 row added
            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let id_to_modify = new_row.column(OXEN_ID_COL)?.get(0)?;
            let id_to_modify = id_to_modify.get_str().unwrap();

            let json_data = json!({
                "height": 101
            });

            workspaces::data_frames::rows::update(
                &repo,
                &workspace,
                &file_path,
                id_to_modify,
                &json_data,
            )?;
            // List the files that are changed - this file should be back into unchanged state
            let status = workspaces::status::status(&workspace)?;
            log::debug!("found mod entries: {:?}", status);
            assert_eq!(status.staged_files.len(), 1);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_delete_added_single_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Add a row
            let json_data = json!({
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let new_row =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;

            // 1 row added
            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let id_to_delete = new_row.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, &id_to_delete)?;
            log::debug!("done deleting row");
            // List the files that are changed - this file should be back into unchanged state
            let status = workspaces::status::status(&workspace)?;
            log::debug!("found mod entries: {:?}", status);
            assert_eq!(status.staged_files.len(), 0);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_modify_row_back_to_original_state() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_modify = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_modify = id_to_modify.replace('"', "");

            let json_data = json!({
                "label": "doggo"
            });

            // Stage a modification
            workspaces::data_frames::rows::update(
                &repo,
                &workspace,
                &file_path,
                &id_to_modify,
                &json_data,
            )?;

            // List the files that are changed
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 1);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now modify the row back to its original state
            let json_data = json!({
                "label": "dog"
            });

            let res = workspaces::data_frames::rows::update(
                &repo,
                &workspace,
                &file_path,
                &id_to_modify,
                &json_data,
            )?;

            log::debug!("res is... {:?}", res);

            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 0);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }
    #[test]
    fn test_restore_row_after_modification() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_modify = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_modify = id_to_modify.replace('"', "");

            let json_data = json!({
                "label": "doggo"
            });

            // Stage a modification
            workspaces::data_frames::rows::update(
                &repo,
                &workspace,
                &file_path,
                &id_to_modify,
                &json_data,
            )?;

            // List the files that are changed
            let status = workspaces::status::status(&workspace)?;
            println!("status: {:?}", status);
            assert_eq!(status.staged_files.len(), 1);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now restore the row
            let res = workspaces::data_frames::rows::restore(
                &repo,
                &workspace,
                &file_path,
                &id_to_modify,
            )?;

            log::debug!("res is... {:?}", res);

            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 0);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 0);
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_restore_row_delete() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, &id_to_delete)?;
            let status = workspaces::status::status(&workspace)?;
            println!("status: {:?}", status);
            assert_eq!(status.staged_files.len(), 1);

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now restore the row
            workspaces::data_frames::rows::restore(&repo, &workspace, &file_path, &id_to_delete)?;

            let status = workspaces::status::status(&workspace)?;
            println!("status: {:?}", status);
            assert!(status.is_clean());

            let diff = workspaces::diff(&repo, &workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 0);
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_append_invalid_column() -> Result<(), OxenError> {
        // Skip if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            // Try stage an append
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let branch = repositories::branches::current_branch(&repo)?.unwrap();

            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            workspaces::data_frames::index(&repo, &workspace, &path)?;
            let json_data = json!({"NOT_REAL_COLUMN": "images/test.jpg"});
            let result = workspaces::data_frames::rows::add(&repo, &workspace, &path, &json_data);
            // Should be an error
            assert!(result.is_err());

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_appends_staged() -> Result<(), OxenError> {
        // Skip if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Stage an append
            let commit = repositories::commits::head_commit(&repo)?;
            let user = UserConfig::get()?.to_user();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;

            workspaces::data_frames::index(&repo, &workspace, &path)?;
            let json_data = json!({"file": "images/test.jpg", "label": "dog", "min_x": 2.0, "min_y": 3.0, "width": 100, "height": 120});
            workspaces::data_frames::rows::add(&repo, &workspace, &path, &json_data)?;
            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email,
                message: "Appending tabular data".to_string(),
            };

            let commit = workspaces::commit(&workspace, &new_commit, DEFAULT_BRANCH_NAME)?;

            // Make sure version file is updated
            let entry = repositories::entries::get_commit_entry(&repo, &commit, &path)?.unwrap();
            let version_file = util::fs::version_path(&repo, &entry);
            let extension = entry.path.extension().unwrap().to_str().unwrap();
            let data_frame =
                df::tabular::read_df_with_extension(version_file, extension, &DFOpts::empty())?;
            println!("{data_frame}");
            assert_eq!(
                format!("{data_frame}"),
                r"shape: (7, 6)
┌─────────────────┬───────┬───────┬───────┬───────┬────────┐
│ file            ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---             ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str             ┆ str   ┆ f64   ┆ f64   ┆ i64   ┆ i64    │
╞═════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ train/dog_1.jpg ┆ dog   ┆ 101.5 ┆ 32.0  ┆ 385   ┆ 330    │
│ train/dog_1.jpg ┆ dog   ┆ 102.5 ┆ 31.0  ┆ 386   ┆ 330    │
│ train/dog_2.jpg ┆ dog   ┆ 7.0   ┆ 29.5  ┆ 246   ┆ 247    │
│ train/dog_3.jpg ┆ dog   ┆ 19.0  ┆ 63.5  ┆ 376   ┆ 421    │
│ train/cat_1.jpg ┆ cat   ┆ 57.0  ┆ 35.5  ┆ 304   ┆ 427    │
│ train/cat_2.jpg ┆ cat   ┆ 30.5  ┆ 44.0  ┆ 333   ┆ 396    │
│ images/test.jpg ┆ dog   ┆ 2.0   ┆ 3.0   ┆ 100   ┆ 120    │
└─────────────────┴───────┴───────┴───────┴───────┴────────┘"
            );
            Ok(())
        })
    }
}
