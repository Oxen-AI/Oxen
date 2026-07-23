use crate::core::df::tabular::{write_df, write_df_parquet};
use crate::model::merkle_tree::node::FileNodeWithDir;
use crate::view::data_frames::columns::NewColumn;
use polars::frame::DataFrame;

use sql_query_builder::Select;

use crate::constants::{MAX_QUERYABLE_ROWS, OXEN_COLS, OXEN_ROW_ID_COL, TABLE_NAME};
use crate::constants::{MODS_DIR, OXEN_HIDDEN_DIR};
use crate::core;
use crate::core::db::data_frames::df_db::{with_df_db_manager, with_hardened_query_conn};
use crate::core::db::data_frames::{DataFrameError, df_db};
use crate::core::df::sql;
use crate::error::OxenError;
use crate::model::{Branch, Commit, EntryDataType, LocalRepository, NewCommitBody, Workspace};
use crate::opts::DFOpts;
use crate::{repositories, util};

use crate::core::db::data_frames::columns::polar_insert_column;
use duckdb::arrow::array::RecordBatch;
use std::path::{Path, PathBuf};

pub mod columns;
pub mod embeddings;
pub mod rows;
pub mod schemas;

pub fn is_indexed(workspace: &Workspace, path: &Path) -> Result<bool, DataFrameError> {
    log::debug!("checking dataset is indexed for {path:?}");
    let db_path = duckdb_path(workspace, path);
    log::debug!("getting conn at path {db_path:?}");

    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            // A partially-indexed table (missing some OXEN_COLS) can't be queried,
            // so report it as not indexed and let callers rebuild it.
            let fully_indexed = df_db::table_is_fully_indexed(conn, TABLE_NAME)?;
            log::debug!("dataset_is_indexed() got fully_indexed: {fully_indexed:?}");
            Ok(fully_indexed)
        })
    })
}

/// Whether a staged DuckDB table exists on disk for this data frame,
/// regardless of whether it passes the fully-indexed gate. A table that
/// exists but is not fully indexed was left by an older version (or an
/// interrupted index) and may hold staged edits the current code cannot
/// export.
pub fn has_staged_table(workspace: &Workspace, path: &Path) -> Result<bool, DataFrameError> {
    let db_path = duckdb_path(workspace, path);
    if !db_path.exists() {
        return Ok(false);
    }
    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| Ok(df_db::table_exists(conn, TABLE_NAME)?))
    })
}

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    commit: &Commit,
) -> Result<bool, OxenError> {
    core::v_latest::workspaces::data_frames::is_queryable_data_frame_indexed(repo, commit, path)
}

pub use crate::core::v_latest::workspaces::data_frames::get_queryable_data_frame_workspace;

pub async fn index(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    core::v_latest::workspaces::data_frames::index(workspace, path.as_ref()).await
}

pub use crate::core::v_latest::workspaces::data_frames::rename;

pub fn unindex(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), DataFrameError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            df_db::drop_table(conn, TABLE_NAME)?;
            Ok(())
        })
    })
}

pub async fn restore(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // Unstage and then restage the df
    unindex(workspace, &path)?;

    // TODO: we could do this more granularly without a full reset
    index(repo, workspace, path.as_ref()).await?;

    Ok(())
}

pub fn count(workspace: &Workspace, path: &Path) -> Result<usize, DataFrameError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            let count = df_db::count(conn, TABLE_NAME)?;
            Ok(count)
        })
    })
}

pub fn query(
    workspace: &Workspace,
    path: &Path,
    opts: &DFOpts,
) -> Result<DataFrame, DataFrameError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("query_staged_df() got db_path: {db_path:?}");
    log::debug!("query() opts: {opts:?}");

    // Embeddings need the vss extension, so they run on the cached read-write connection;
    // every other query is a read and runs on a hardened, read-only connection with no external
    // file access. (Embeddings and sql are mutually exclusive.)
    let df = if let Some(embedding_opts) = opts.get_sort_by_embedding_query() {
        log::debug!("querying embeddings: {embedding_opts:?}");
        with_df_db_manager(&db_path, |manager| {
            manager.with_conn_mut(|conn| {
                repositories::workspaces::data_frames::embeddings::query_with_conn(
                    conn,
                    workspace,
                    &embedding_opts,
                )
            })
        })?
    } else {
        with_hardened_query_conn(&db_path, |conn| {
            if let Some(sql) = &opts.sql {
                log::debug!("querying sql: {sql:?}");
                sql::query_df(conn, sql.clone(), None)
            } else {
                let mut select = Select::new().select("*").from(TABLE_NAME);
                // Deterministic page order: DuckDB UPDATEs physically relocate
                // rows, so without an ORDER BY an edited row jumps to another
                // page and the edit looks lost. (When the caller sorts,
                // prepare_sql appends that ORDER BY instead.)
                if opts.sort_by.is_none() {
                    select = select.order_by(OXEN_ROW_ID_COL);
                }
                df_db::select(conn, &select, Some(opts))
            }
        })?
    };

    // `_oxen_row_id` is an ordering key, not part of the data frame's
    // schema — keep it out of results. (`_oxen_id` stays: clients
    // address rows by it.)
    if df.column(OXEN_ROW_ID_COL).is_ok() {
        Ok(df.drop(OXEN_ROW_ID_COL)?)
    } else {
        Ok(df)
    }
}

pub fn export(
    workspace: &Workspace,
    path: &Path,
    opts: &DFOpts,
    temp_file: &Path,
) -> Result<(), OxenError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("export() got db_path: {db_path:?}");

    // Embeddings (vss extension) and the default full export use DuckDB's COPY
    // writer on the read-write connection. Caller-supplied SQL runs on the hardened
    // read-only connection and is serialized in Rust, never reaching the COPY path.
    if let Some(embedding_opts) = opts.get_sort_by_embedding_query() {
        with_df_db_manager(&db_path, |manager| {
            manager.with_conn(|conn| {
                let sql =
                    embeddings::similarity_query_with_conn(conn, workspace, &embedding_opts, true)?;
                sql::export_df(conn, sql, Some(opts), temp_file)?;
                Ok(())
            })
        })?;
    } else if let Some(sql) = opts.sql.clone() {
        let sql = add_exclude_to_sql(&sql)?;
        log::debug!("exporting data frame with sql: {sql:?}");
        // Cap the export at MAX_QUERYABLE_ROWS rows; larger results are refused, not truncated.
        let capped = with_hardened_query_conn(&db_path, |conn| {
            df_db::select_raw_capped(conn, &sql, MAX_QUERYABLE_ROWS)
        })?;
        let Some(mut df) = capped else {
            return Err(DataFrameError::ExportResultTooLarge(MAX_QUERYABLE_ROWS).into());
        };
        write_df(&mut df, temp_file)?;
    } else {
        with_df_db_manager(&db_path, |manager| {
            manager.with_conn(|conn| {
                // Ordered for the same reason as query(): an exported file must
                // carry the stable row order, not DuckDB's physical order.
                let sql = add_exclude_to_sql(&format!(
                    "SELECT * FROM {TABLE_NAME} ORDER BY {OXEN_ROW_ID_COL}"
                ))?;
                sql::export_df(conn, sql, Some(opts), temp_file)?;
                Ok(())
            })
        })?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn from_directory(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    extra_columns: &[NewColumn],
    recursive: bool,
    new_commit: &NewCommitBody,
    branch: &Branch,
) -> Result<Commit, OxenError> {
    let has_dir = repositories::tree::has_dir(repo, &workspace.commit, path.as_ref())?;
    if !has_dir {
        return Err(OxenError::basic_str(format!(
            "Directory not found: {:?}",
            path.as_ref()
        )));
    }

    let depth = if recursive { -1 } else { 1 };

    let subtree = repositories::tree::get_subtree(
        repo,
        &workspace.commit,
        &Some(path.as_ref().to_path_buf()),
        &Some(depth),
    )?;

    let files =
        repositories::tree::list_all_files(&subtree.unwrap(), &path.as_ref().to_path_buf())?;

    // Extract file paths from FileNodeWithDir
    let file_paths: Vec<String> = files
        .iter()
        .map(|file_with_dir| {
            file_with_dir
                .dir
                .join(file_with_dir.file_node.name())
                .to_string_lossy()
                .to_string()
        })
        .collect();

    let db_path = workspace.dir().join("temp_file_listing.db");

    let mut df = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            // Create initial table with just file_path column
            conn.execute("CREATE TABLE file_listing (file_path VARCHAR);", [])?;

            // Insert file paths using bulk insert
            if !file_paths.is_empty() {
                let values_clause = file_paths
                    .iter()
                    .map(|_| "(?)")
                    .collect::<Vec<_>>()
                    .join(", ");

                let bulk_insert_sql =
                    format!("INSERT INTO file_listing (file_path) VALUES {values_clause}");

                let params: Vec<&dyn duckdb::ToSql> = file_paths
                    .iter()
                    .map(|path| path as &dyn duckdb::ToSql)
                    .collect();

                conn.execute(&bulk_insert_sql, params.as_slice())?;
            }

            // Add each extra column using the existing function
            for new_column in extra_columns {
                polar_insert_column(conn, "file_listing", new_column)?;
            }

            // Get the final DataFrame
            let sql_query = "SELECT * FROM file_listing";
            let result_set: Vec<RecordBatch> = conn.prepare(sql_query)?.query_arrow([])?.collect();
            df_db::record_batches_to_polars_df(result_set)
        })
    })?;

    // Write the DataFrame as a parquet file
    let output_path = workspace.dir().join(output_path);

    // Check if output_path has a ".parquet" extension, if not, add it
    let output_path = if output_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
    {
        output_path
    } else {
        output_path.with_extension("parquet")
    };

    // Create parent directories if they don't exist
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    write_df_parquet(&mut df, &output_path)?;

    repositories::workspaces::files::add(workspace, &output_path).await?;

    let files_vec: Vec<FileNodeWithDir> = files.iter().cloned().collect();
    set_media_render_metadata_if_applicable(repo, workspace, &files_vec, &output_path).await?;

    let commit =
        repositories::workspaces::commit(workspace, new_commit, branch.name.as_str()).await?;
    println!(
        "Created parquet file with {} file paths at: {:?}",
        file_paths.len(),
        output_path
    );

    Ok(commit)
}

/// Determines the render function name for a given media type.
/// Returns None if the type doesn't support rendering.
fn render_function_for_media_type(data_type: &EntryDataType) -> Option<&'static str> {
    match data_type {
        EntryDataType::Image => Some("image"),
        EntryDataType::Video => Some("video"),
        _ => None,
    }
}

/// Checks if all files in the collection are of the same supported media type.
/// Returns the render function name if all files match, None otherwise.
fn get_uniform_media_type(files: &[FileNodeWithDir]) -> Option<&'static str> {
    if files.is_empty() {
        return None;
    }

    // Get the data type of the first file
    let first_data_type = files[0].file_node.data_type();
    let render_func = render_function_for_media_type(first_data_type)?;

    // Check if all files have the same media type
    let all_match = files
        .iter()
        .all(|file_with_dir| file_with_dir.file_node.data_type() == first_data_type);

    if all_match { Some(render_func) } else { None }
}

/// Sets render metadata for the file_path column if all files are of the same
/// supported media type (currently Image or Video).
///
/// This function automatically detects when all files in a dataset are images
/// or videos and sets appropriate render metadata so they can be displayed
/// correctly in the UI.
pub async fn set_media_render_metadata_if_applicable(
    repo: &LocalRepository,
    workspace: &Workspace,
    files: &[FileNodeWithDir],
    output_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    if let Some(render_func) = get_uniform_media_type(files) {
        let render_metadata = serde_json::json!({
            "_oxen": {
                "render": {
                    "func": render_func
                }
            }
        });

        repositories::workspaces::data_frames::columns::add_column_metadata(
            repo,
            workspace,
            output_path.as_ref().to_path_buf(),
            "file_path".to_string(),
            &render_metadata,
        )?;
    }

    Ok(())
}

pub fn duckdb_path(workspace: &Workspace, path: &Path) -> PathBuf {
    let path = util::fs::linux_path(path);
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

// Add this function after the existing imports
fn add_exclude_to_sql(sql: &str) -> Result<String, DataFrameError> {
    // Create the EXCLUDE clause
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{col}\""))
        .collect::<Vec<String>>()
        .join(", ");

    // Find the first SELECT in the query (case insensitive)
    let select_idx = sql
        .to_lowercase()
        .find("select")
        .ok_or_else(|| DataFrameError::NoSelectInQuery)?;

    // Find the first FROM after SELECT (case insensitive)
    let from_idx = sql[select_idx..]
        .to_lowercase()
        .find("from")
        .ok_or_else(|| DataFrameError::NoFromInQuery)?;

    // Split into parts
    let before_from = &sql[..select_idx + from_idx];
    let after_from = &sql[select_idx + from_idx..];

    // Check if this is an aggregation query by looking for GROUP BY
    let has_group_by = sql.to_lowercase().contains("group by");

    // If query has GROUP BY, don't modify it
    let modified_select = if has_group_by {
        before_from.trim().to_string()
    } else if before_from.trim().to_lowercase().ends_with("select *") {
        // For SELECT *, replace with SELECT * EXCLUDE (...)
        format!("SELECT * EXCLUDE ({excluded_cols})")
    } else {
        // For explicit column selections, add EXCLUDE after the columns
        let (select_part, columns_part) = before_from.split_at(select_idx + "select".len());
        let columns = columns_part.trim();
        format!("{select_part} {columns} EXCLUDE ({excluded_cols})")
    };

    Ok(format!("{modified_select} {after_from}"))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::SystemTime;

    use serde_json::json;

    use super::*;
    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, OXEN_ID_COL};
    use crate::core::df;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::model::diff::DiffResult;
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::repositories::workspaces;
    use crate::test;

    #[tokio::test]
    async fn test_add_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

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

            // The staged table has the original 6 rows plus the appended one
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 7);

            Ok(())
        })
        .await
    }

    /// Indexing adds exactly the hidden system columns (`_oxen_id`,
    /// `_oxen_row_id`) — the legacy tracking columns (`_oxen_diff_status`,
    /// `_oxen_diff_hash`) are no longer created.
    #[tokio::test]
    async fn test_index_adds_only_oxen_system_columns() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-index-cols")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let db_path = workspaces::data_frames::duckdb_path(&workspace, &file_path);
            let columns: Vec<String> = super::with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    let schema = df_db::get_schema(conn, crate::constants::TABLE_NAME)?;
                    Ok(schema.fields.into_iter().map(|f| f.name).collect())
                })
            })?;

            assert!(columns.contains(&OXEN_ID_COL.to_string()));
            assert!(columns.contains(&OXEN_ROW_ID_COL.to_string()));
            assert!(!columns.contains(&crate::constants::DIFF_STATUS_COL.to_string()));
            assert!(!columns.iter().any(|c| c == "_oxen_diff_hash"));

            Ok(())
        })
        .await
    }

    /// A table left behind by an older version (no index marker table) or by
    /// an interrupted index (missing `_oxen_id`) must read as NOT indexed so
    /// callers rebuild it, and re-indexing must repair it.
    #[tokio::test]
    async fn test_stale_or_partial_index_reads_as_not_indexed_and_reindex_repairs()
    -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-partial-index")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // A normal index produces a fully-indexed, queryable table.
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;
            assert!(workspaces::data_frames::is_indexed(&workspace, &file_path)?);

            let db_path = workspaces::data_frames::duckdb_path(&workspace, &file_path);

            // Simulate a table written by an older version: no index marker
            // table. Such a table may hold rows tombstoned as 'removed' that
            // the current code would wrongly export, so it must be rebuilt.
            super::with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    conn.execute(
                        &format!("DROP TABLE \"{}\"", crate::constants::INDEX_META_TABLE),
                        [],
                    )?;
                    Ok(())
                })
            })?;
            assert!(!workspaces::data_frames::is_indexed(
                &workspace, &file_path
            )?);

            // Re-indexing repairs the table.
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;
            assert!(workspaces::data_frames::is_indexed(&workspace, &file_path)?);

            // Simulate the partial-index corruption an interrupted index
            // leaves behind: a table that is missing `_oxen_id`.
            super::with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    conn.execute(
                        &format!(
                            "ALTER TABLE \"{}\" DROP COLUMN \"{}\"",
                            crate::constants::TABLE_NAME,
                            OXEN_ID_COL
                        ),
                        [],
                    )?;
                    Ok(())
                })
            })?;
            assert!(!workspaces::data_frames::is_indexed(
                &workspace, &file_path
            )?);

            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;
            assert!(workspaces::data_frames::is_indexed(&workspace, &file_path)?);

            Ok(())
        })
        .await
    }

    /// User data legitimately containing columns named like the old tracking
    /// columns (e.g. a committed `oxen diff` export, whose output includes
    /// `_oxen_diff_status`) must index and read back as indexed — staleness is
    /// signalled by the marker table, never by user-visible column names.
    #[tokio::test]
    async fn test_user_columns_named_like_legacy_tracking_are_indexable() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let file_path = Path::new("diff_output.csv");
            let full_path = repo.path.join(file_path);
            util::fs::write_to_path(
                &full_path,
                "file,label,_oxen_diff_status\na.jpg,dog,added\nb.jpg,cat,removed\n",
            )?;
            repositories::add(&repo, &full_path).await?;
            let commit = repositories::commit(&repo, "add diff export")?;

            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;

            workspaces::data_frames::index(&repo, &workspace, file_path).await?;
            assert!(workspaces::data_frames::is_indexed(&workspace, file_path)?);

            // The user's rows — including the 'removed'-status row, which is
            // plain data — are all present and queryable.
            let count = workspaces::data_frames::count(&workspace, file_path)?;
            assert_eq!(count, 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_added_row_with_two_rows() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

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
            log::debug!("status is {status:?}");
            assert_eq!(status.staged_files.len(), 1);

            // Both appended rows are in the staged table
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 8);

            // Delete the first append
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, &append_1_id)?;

            // Only the second appended row remains
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 7);

            Ok(())
        })
        .await
    }

    /// Edits are not reversible: even when deletes bring the table back to its
    /// original contents, the data frame stays staged as modified. Un-staging
    /// is an explicit operation (`workspaces::data_frames::restore`), not an
    /// automatic side effect of edits cancelling out.
    #[tokio::test]
    async fn test_delete_added_rows_keeps_data_frame_staged() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

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

            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 8);

            // Delete the first append
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, append_1_id)?;

            // Delete the second append
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, append_2_id)?;

            // The table is back to its original contents...
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 6);

            // ...but the data frame stays staged: edits are applied, not tracked,
            // so nothing detects that they cancelled out.
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_committed_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-append";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion — the row is deleted from the staged table
            // immediately, not tombstoned.
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, &id_to_delete)?;
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 5);

            // List the files that are changed
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 1);

            let status = repositories::status(&repo).await?;
            log::debug!("got this status {status:?}");

            // Commit the new file

            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Deleting a row allegedly".to_string(),
            };
            let commit_2 = workspaces::commit(&workspace, &new_commit, branch_name).await?;

            let version_store = repo.version_store();
            let node_1 = repositories::tree::get_file_by_path(&repo, &commit, &file_path)?
                .expect("file should exist in commit");
            let file_1_csv = repo.path.join("version_1.csv");
            version_store
                .copy_version_to_path(&node_1.hash().to_string(), &file_1_csv, SystemTime::now())
                .await?;
            log::debug!("copied file 1 to {file_1_csv:?}");

            let node_2 = repositories::tree::get_file_by_path(&repo, &commit_2, &file_path)?
                .expect("file should exist in commit_2");
            let file_2_csv = repo.path.join("version_2.csv");
            version_store
                .copy_version_to_path(&node_2.hash().to_string(), &file_2_csv, SystemTime::now())
                .await?;
            log::debug!("copied file 2 to {file_2_csv:?}");
            let diff_result =
                repositories::diffs::diff_files(file_1_csv, file_2_csv, vec![], vec![], vec![])
                    .await?;

            log::debug!("diff result is {diff_result:?}");
            match diff_result {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_all_rows_deleted_jsonl_fails_instead_of_corrupting()
    -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a single-row jsonl data frame
            let file_path = Path::new("data.jsonl");
            let full_path = repo.path.join(file_path);
            util::fs::write_to_path(
                &full_path,
                "{\"prompt\": \"hello\", \"status\": \"bootstrap\"}\n",
            )?;
            repositories::add(&repo, &full_path).await?;
            let commit = repositories::commit(&repo, "add data.jsonl")?;

            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;

            // Index the dataset and stage a removal of its only row
            workspaces::data_frames::index(&repo, &workspace, file_path).await?;

            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);
            let staged_df = workspaces::data_frames::query(&workspace, file_path, &page_opts)?;
            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");
            workspaces::data_frames::rows::delete(&repo, &workspace, file_path, &id_to_delete)?;

            // Committing now would export an empty jsonl file — no schema can
            // be inferred from it, so the commit must fail instead of writing
            // a Tabular FileNode with no metadata (which would make every
            // subsequent read fail with "File node does not have metadata").
            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Delete all rows".to_string(),
            };
            let result = workspaces::commit(&workspace, &new_commit, DEFAULT_BRANCH_NAME).await;
            assert!(
                result.is_err(),
                "commit of an empty tabular export must fail, got {result:?}"
            );

            // The previously committed version must still be fully readable.
            let file_node = repositories::tree::get_file_by_path(&repo, &commit, file_path)?
                .expect("original file node should still exist");
            assert!(
                file_node.metadata().is_some(),
                "original committed file node should still have tabular metadata"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_modify_added_row() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
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
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

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
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 7);

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
            // The data frame stays staged after the update
            let status = workspaces::status::status(&workspace)?;
            log::debug!("found mod entries: {status:?}");
            assert_eq!(status.staged_files.len(), 1);

            // The update was applied in place to the same row
            let row =
                workspaces::data_frames::rows::get_by_id(&workspace, &file_path, id_to_modify)?;
            assert_eq!(row.height(), 1);
            let height = row.column("height")?.get(0)?;
            assert_eq!(height.to_string(), "101");
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 7);

            Ok(())
        })
        .await
    }

    /// The default (no-sql, no-sort) read path must return rows in a stable
    /// order across edits: a DuckDB UPDATE physically relocates the row, and
    /// without the `_oxen_row_id` ORDER BY the edited row jumps pages. The
    /// ordering column itself must not leak into results.
    #[tokio::test]
    async fn test_query_keeps_row_order_after_modify() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-row-order";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let read_ids = |df: &polars::frame::DataFrame| -> Result<Vec<String>, OxenError> {
                let col = df.column(OXEN_ID_COL)?;
                (0..df.height())
                    .map(|i| {
                        Ok(col
                            .get(i)?
                            .get_str()
                            .expect("_oxen_id is a string column")
                            .to_string())
                    })
                    .collect()
            };

            let opts = DFOpts::empty();
            let df_before = workspaces::data_frames::query(&workspace, &file_path, &opts)?;
            let ids_before = read_ids(&df_before)?;

            // Edit the first row; DuckDB physically relocates it.
            let json_data = json!({ "height": 999 });
            workspaces::data_frames::rows::update(
                &repo,
                &workspace,
                &file_path,
                &ids_before[0],
                &json_data,
            )?;

            let df_after = workspaces::data_frames::query(&workspace, &file_path, &opts)?;
            assert_eq!(
                read_ids(&df_after)?,
                ids_before,
                "row order must be stable across an edit"
            );
            let height = df_after.column("height")?.get(0)?;
            assert_eq!(
                height.to_string(),
                "999",
                "the edit applied to the first row"
            );
            assert!(
                df_after.column(OXEN_ROW_ID_COL).is_err(),
                "the internal ordering column must not appear in results"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_added_single_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
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
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

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
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 7);

            let id_to_delete = new_row.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Delete the added row again
            workspaces::data_frames::rows::delete(&repo, &workspace, &file_path, &id_to_delete)?;
            log::debug!("done deleting row");

            // The table is back to its original contents, but the data frame
            // stays staged: edits are applied, not tracked.
            let count = workspaces::data_frames::count(&workspace, &file_path)?;
            assert_eq!(count, 6);
            let status = workspaces::status::status(&workspace)?;
            log::debug!("found mod entries: {status:?}");
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_modify_row_back_to_original_state() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
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
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

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

            let row =
                workspaces::data_frames::rows::get_by_id(&workspace, &file_path, &id_to_modify)?;
            let label = row.column("label")?.get(0)?;
            assert_eq!(label.get_str(), Some("doggo"));

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

            log::debug!("res is... {res:?}");

            // The value is back to the original...
            let row =
                workspaces::data_frames::rows::get_by_id(&workspace, &file_path, &id_to_modify)?;
            let label = row.column("label")?.get(0)?;
            assert_eq!(label.get_str(), Some("dog"));

            // ...but the data frame stays staged: edits are applied, not
            // tracked, so nothing detects that they cancelled out.
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
        .await
    }
    /// A batch update containing a row id that doesn't exist must fail before
    /// ANY row is written — not apply the valid rows and then report failure.
    #[tokio::test]
    async fn test_batch_update_with_missing_row_id_writes_nothing() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-batch-missing")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);
            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;
            let real_id = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let real_id = real_id.replace('"', "");

            let data = json!([
                {"row_id": real_id, "value": {"label": "should-not-apply"}},
                {"row_id": "no-such-row-id", "value": {"label": "phantom"}}
            ]);
            let result =
                workspaces::data_frames::rows::batch_update(&repo, &workspace, &file_path, &data);
            assert!(result.is_err(), "batch with a missing row id must fail");

            // The valid row's update must NOT have been applied.
            let row = workspaces::data_frames::rows::get_by_id(&workspace, &file_path, &real_id)?;
            let label = row.column("label")?.get(0)?;
            assert_ne!(label.get_str(), Some("should-not-apply"));

            Ok(())
        })
        .await
    }

    /// `_oxen_diff_status` is no longer a reserved name (the legacy tracking
    /// columns are fully unsupported), so a user data frame with a column of
    /// that name is an ordinary column: updates to it apply.
    #[tokio::test]
    async fn test_update_user_column_with_former_legacy_name_applies() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let file_path = Path::new("diff_output.csv");
            let full_path = repo.path.join(file_path);
            util::fs::write_to_path(
                &full_path,
                "file,_oxen_diff_status\na.jpg,added\nb.jpg,removed\n",
            )?;
            repositories::add(&repo, &full_path).await?;
            let commit = repositories::commit(&repo, "add diff export")?;

            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            workspaces::data_frames::index(&repo, &workspace, file_path).await?;

            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);
            let staged = workspaces::data_frames::query(&workspace, file_path, &page_opts)?;
            let id = staged
                .column(OXEN_ID_COL)?
                .get(0)?
                .to_string()
                .replace('"', "");

            let json_data = json!({ "_oxen_diff_status": "changed" });
            workspaces::data_frames::rows::update(&repo, &workspace, file_path, &id, &json_data)?;

            let row = workspaces::data_frames::rows::get_by_id(&workspace, file_path, &id)?;
            let val = row.column("_oxen_diff_status")?.get(0)?;
            assert_eq!(val.get_str(), Some("changed"), "the update applies");

            Ok(())
        })
        .await
    }

    /// The current reserved keys (`_oxen_id`, `_oxen_row_id`) are stripped
    /// from row-update payloads; the legacy tracking keys are no longer
    /// tolerated — a payload naming a column the table doesn't have is
    /// rejected like any other schema mismatch.
    #[tokio::test]
    async fn test_update_row_payload_reserved_and_unknown_keys() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-legacy-keys")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);
            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;
            let id = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id = id.replace('"', "");

            // Reserved system keys are stripped; the rest of the update applies.
            let json_data = json!({
                "label": "doggo",
                "_oxen_id": "someone-elses-id",
                "_oxen_row_id": 1
            });
            let updated = workspaces::data_frames::rows::update(
                &repo, &workspace, &file_path, &id, &json_data,
            )?;
            let label = updated.column("label")?.get(0)?;
            assert_eq!(label.get_str(), Some("doggo"));

            // A legacy tracking key names a column this table doesn't have:
            // rejected as a schema mismatch rather than silently stripped.
            let json_data = json!({
                "label": "doggo",
                "_oxen_diff_status": "unchanged"
            });
            let result = workspaces::data_frames::rows::update(
                &repo, &workspace, &file_path, &id, &json_data,
            );
            assert!(result.is_err(), "unknown columns must be rejected");

            Ok(())
        })
        .await
    }

    /// Committing a workspace whose staged table was left by an older version
    /// (no index marker) must fail loudly — the alternative is committing the
    /// base file and silently discarding the staged edits.
    #[tokio::test]
    async fn test_commit_stale_staged_table_errors_instead_of_dropping_edits()
    -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-stale-commit";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            // Stage an edit, then simulate a table written by an older
            // version by removing the index marker.
            let json_data = json!({
                "file": "stale.jpg", "label": "dog",
                "min_x": 1, "min_y": 2, "width": 3, "height": 4
            });
            workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;
            let db_path = workspaces::data_frames::duckdb_path(&workspace, &file_path);
            super::with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    conn.execute(
                        &format!("DROP TABLE \"{}\"", crate::constants::INDEX_META_TABLE),
                        [],
                    )?;
                    Ok(())
                })
            })?;

            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Committing stale staged df".to_string(),
            };
            let result = workspaces::commit(&workspace, &new_commit, branch_name).await;
            // Must be the typed stale-index error (maps to 409, not a retryable
            // 500), since the user has to re-index or unstage first.
            assert!(
                matches!(result, Err(OxenError::WorkspaceStaleStagedIndex(_))),
                "commit of a stale staged data frame must fail with WorkspaceStaleStagedIndex, got {result:?}"
            );

            // The base version is still intact and readable.
            let file_node = repositories::tree::get_file_by_path(&repo, &commit, &file_path)?
                .expect("base file node should still exist");
            assert!(file_node.metadata().is_some());

            Ok(())
        })
        .await
    }

    /// add_column_metadata reconciles the staged metadata schema with the
    /// workspace table: a column deleted by a workspace edit is dropped from
    /// the schema (not left as a phantom), and the reconciled schema matches
    /// the table's columns.
    #[tokio::test]
    async fn test_add_column_metadata_reconciles_deleted_column() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        use crate::view::data_frames::columns::ColumnToDelete;
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-reconcile")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            // Delete the `label` column via a workspace edit.
            workspaces::data_frames::columns::delete(
                &repo,
                &workspace,
                &file_path,
                &ColumnToDelete {
                    name: "label".to_string(),
                },
            )?;

            // Attaching metadata to a still-present column reconciles the schema.
            let results = workspaces::data_frames::columns::add_column_metadata(
                &repo,
                &workspace,
                file_path.clone(),
                "file".to_string(),
                &json!({"root": "images"}),
            )?;

            let schema = results.values().next().expect("a schema was returned");
            let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
            assert!(
                !names.contains(&"label"),
                "deleted column should be gone: {names:?}"
            );
            assert!(names.contains(&"file"));
            Ok(())
        })
        .await
    }

    /// A metadata-only workspace edit (add_column_metadata, no row changes)
    /// must survive commit: the no-op-export skip compares against the BASE
    /// commit's file node, so the metadata change — which the staged node
    /// already carries — is not mistaken for an unchanged file and dropped.
    #[tokio::test]
    async fn test_commit_keeps_metadata_only_edit() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-metadata-only";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            // Attach column metadata only — no row/content changes.
            workspaces::data_frames::columns::add_column_metadata(
                &repo,
                &workspace,
                file_path.clone(),
                "file".to_string(),
                &json!({"_oxen": {"render": {"func": "image"}}}),
            )?;

            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Metadata-only edit".to_string(),
            };
            let committed = workspaces::commit(&workspace, &new_commit, branch_name).await?;

            // The metadata edit must be present in the committed file node's
            // tabular schema — it must not have been skipped as a no-op.
            let file_node = repositories::tree::get_file_by_path(&repo, &committed, &file_path)?
                .expect("committed file node should exist");
            let has_metadata = matches!(
                file_node.metadata(),
                Some(crate::model::metadata::generic_metadata::GenericMetadata::MetadataTabular(m))
                    if m.tabular.schema.fields.iter().any(|f| f.name == "file" && f.metadata.is_some())
            );
            assert!(has_metadata, "metadata-only edit must survive commit");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_tabular_append_invalid_column() -> Result<(), OxenError> {
        // Skip if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            // Try stage an append
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let branch = repositories::branches::current_branch(&repo)?.unwrap();

            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            workspaces::data_frames::index(&repo, &workspace, &path).await?;
            let json_data = json!({"NOT_REAL_COLUMN": "images/test.jpg"});
            let result = workspaces::data_frames::rows::add(&repo, &workspace, &path, &json_data);
            // Should be an error
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_tabular_appends_staged() -> Result<(), OxenError> {
        // Skip if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Stage an append
            let commit = repositories::commits::head_commit(&repo)?;
            let user = UserConfig::get()?.to_user();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;

            workspaces::data_frames::index(&repo, &workspace, &path).await?;
            let json_data = json!({"file": "images/test.jpg", "label": "dog", "min_x": 2.0, "min_y": 3.0, "width": 100, "height": 120});
            workspaces::data_frames::rows::add(&repo, &workspace, &path, &json_data)?;
            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email,
                message: "Appending tabular data".to_string(),
            };

            let commit = workspaces::commit(&workspace, &new_commit, DEFAULT_BRANCH_NAME).await?;

            // Make sure version file is updated
            let entry = repositories::entries::get_commit_entry(&repo, &commit, &path)?.unwrap();
            let version_store = repo.version_store();
            let extension = entry.path.extension().unwrap().to_str().unwrap();
            let data_frame =
                df::tabular::read_version_df(&version_store, &entry.hash, extension, &DFOpts::empty())
                    .await?;
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
        }).await
    }

    /// Regression guard for the workspace-commit export on JSON-typed columns.
    /// Exercises the real index → add-row → commit path on a column DuckDB
    /// infers as `JSON`, so the export projection (`build_export_projection`)
    /// round-trips a JSON column through the export's `COPY ... (FORMAT JSON)`.
    ///
    /// DuckDB 1.5.2 validates JSON on every ingestion path, so a genuinely
    /// corrupt JSON-column value can't be synthesized through the public API;
    /// that tolerance branch is covered by the focused tests in
    /// `core::v_latest::workspaces::data_frames`. This test guards the happy
    /// path so the export projection can't regress normal JSON-column exports.
    #[tokio::test]
    async fn test_commit_workspace_with_json_column_round_trips() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            // Object values make read_json (and the workspace index) type
            // `error_details` as a JSON column.
            let path = Path::new("media.jsonl");
            let full_path = repo.path.join(path);
            std::fs::write(
                &full_path,
                "{\"id\":1,\"error_details\":{\"code\":402,\"msg\":\"x\"}}\n\
                 {\"id\":2,\"error_details\":{\"code\":200,\"msg\":\"ok\"}}\n",
            )?;
            repositories::add(&repo, &full_path).await?;
            let commit = repositories::commit(&repo, "add media jsonl")?;

            let user = UserConfig::get()?.to_user();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            workspaces::data_frames::index(&repo, &workspace, path).await?;

            // Append a row whose JSON column holds a valid object.
            let json_data = json!({"id": 3, "error_details": {"code": 500, "msg": "err"}});
            workspaces::data_frames::rows::add(&repo, &workspace, path, &json_data)?;

            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email,
                message: "Commit workspace with JSON column".to_string(),
            };
            // The export driven by this commit is where a corrupt legacy value
            // would abort; it must succeed for a valid JSON column.
            let commit = workspaces::commit(&workspace, &new_commit, DEFAULT_BRANCH_NAME).await?;

            // The committed file round-trips: re-read it and confirm every row survived.
            let entry = repositories::entries::get_commit_entry(&repo, &commit, path)?.unwrap();
            let version_store = repo.version_store();
            let extension = entry.path.extension().unwrap().to_str().unwrap();
            let data_frame = df::tabular::read_version_df(
                &version_store,
                &entry.hash,
                extension,
                &DFOpts::empty(),
            )
            .await?;
            assert_eq!(data_frame.height(), 3);
            assert!(data_frame.column("error_details").is_ok());
            Ok(())
        })
        .await
    }

    #[test]
    fn test_add_exclude_to_simple_select() -> Result<(), OxenError> {
        let sql = "SELECT * FROM table";
        let result = add_exclude_to_sql(sql)?;
        assert_eq!(
            result,
            "SELECT * EXCLUDE (\"_oxen_id\", \"_oxen_row_id\") FROM table"
        );
        Ok(())
    }

    #[test]
    fn test_add_exclude_to_complex_select() -> Result<(), OxenError> {
        let sql = "SELECT col1, col2, col3 FROM table WHERE col1 = 'value'";
        let result = add_exclude_to_sql(sql)?;
        assert_eq!(
            result,
            "SELECT col1, col2, col3 EXCLUDE (\"_oxen_id\", \"_oxen_row_id\") FROM table WHERE col1 = 'value'"
        );
        Ok(())
    }

    #[test]
    fn test_add_exclude_case_insensitive() -> Result<(), OxenError> {
        let sql = "select * from table";
        let result = add_exclude_to_sql(sql)?;
        assert_eq!(
            result,
            "SELECT * EXCLUDE (\"_oxen_id\", \"_oxen_row_id\") from table"
        );
        Ok(())
    }

    #[test]
    fn test_invalid_sql() {
        let sql = "DELETE FROM table";
        let result = add_exclude_to_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_add_exclude_to_aggregation_query() -> Result<(), OxenError> {
        let sql = "SELECT label, COUNT(*) FROM table GROUP BY label";
        let result = add_exclude_to_sql(sql)?;
        assert_eq!(result, "SELECT label, COUNT(*) FROM table GROUP BY label");
        Ok(())
    }

    /// Column names with spaces must survive the whole column lifecycle —
    /// the ALTER TABLE builders quote identifiers, so a name like "my col"
    /// can be added, renamed, and deleted.
    #[tokio::test]
    async fn test_column_lifecycle_with_space_in_name() -> Result<(), OxenError> {
        use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};

        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-spaced-col")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let new_column = NewColumn {
                name: "my col".to_string(),
                data_type: "str".to_string(),
            };
            let df =
                workspaces::data_frames::columns::add(&repo, &workspace, &file_path, &new_column)?;
            assert!(df.column("my col").is_ok());

            let rename = ColumnToUpdate {
                name: "my col".to_string(),
                new_name: Some("my col 2".to_string()),
                new_data_type: None,
            };
            let df =
                workspaces::data_frames::columns::update(&repo, &workspace, &file_path, &rename)
                    .await?;
            assert!(df.column("my col 2").is_ok());
            assert!(df.column("my col").is_err());

            // The renamed column must be usable through the row paths too:
            // append a row with a value in it, then edit that value.
            let json_data = json!({ "file": "spaced.jpg", "my col 2": "hello" });
            let added =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;
            let row_id = added
                .column(OXEN_ID_COL)?
                .get(0)?
                .to_string()
                .trim_matches('"')
                .to_string();
            let json_data = json!({ "my col 2": "world" });
            let updated = workspaces::data_frames::rows::update(
                &repo, &workspace, &file_path, &row_id, &json_data,
            )?;
            assert_eq!(updated.column("my col 2")?.get(0)?.get_str(), Some("world"));

            let delete = ColumnToDelete {
                name: "my col 2".to_string(),
            };
            let df =
                workspaces::data_frames::columns::delete(&repo, &workspace, &file_path, &delete)?;
            assert!(df.column("my col 2").is_err());

            // An embedded double quote is the hardest identifier case: every
            // SQL builder must escape it, not just wrap it.
            let quoted_name = "we\"ird col";
            let new_column = NewColumn {
                name: quoted_name.to_string(),
                data_type: "str".to_string(),
            };
            let df =
                workspaces::data_frames::columns::add(&repo, &workspace, &file_path, &new_column)?;
            assert!(df.column(quoted_name).is_ok());
            let json_data = json!({ "file": "quoted.jpg", quoted_name: "hi" });
            let added =
                workspaces::data_frames::rows::add(&repo, &workspace, &file_path, &json_data)?;
            assert_eq!(added.column(quoted_name)?.get(0)?.get_str(), Some("hi"));
            let delete = ColumnToDelete {
                name: quoted_name.to_string(),
            };
            let df =
                workspaces::data_frames::columns::delete(&repo, &workspace, &file_path, &delete)?;
            assert!(df.column(quoted_name).is_err());

            Ok(())
        })
        .await
    }

    /// Adds `column_name` of `data_type` (e.g. "list[i64]") to a workspace-indexed dataframe,
    /// adds a single row whose `column_name` is `list_value`, and returns the polars list series
    /// for that column read back from the staged DuckDB.
    async fn add_row_with_list_column(
        repo: &crate::model::LocalRepository,
        workspace: &crate::model::Workspace,
        file_path: &Path,
        column_name: &str,
        data_type: &str,
        list_value: serde_json::Value,
    ) -> Result<polars::prelude::Series, OxenError> {
        use crate::view::data_frames::columns::NewColumn;

        let new_column = NewColumn {
            name: column_name.to_string(),
            data_type: data_type.to_string(),
        };
        workspaces::data_frames::columns::add(repo, workspace, file_path, &new_column)?;

        let json_data = json!({
            "file": "list_row.jpg",
            "label": "dog",
            "min_x": 1.0,
            "min_y": 2.0,
            "width": 3,
            "height": 4,
            column_name: list_value,
        });
        let added = workspaces::data_frames::rows::add(repo, workspace, file_path, &json_data)?;
        let row_id = added
            .column(OXEN_ID_COL)?
            .get(0)?
            .to_string()
            .trim_matches('"')
            .to_string();

        let row = workspaces::data_frames::rows::get_by_id(workspace, file_path, &row_id)?;
        let column = row.column(column_name)?;
        Ok(column.as_materialized_series().clone())
    }

    fn list_typed_add_row_test_paths() -> std::path::PathBuf {
        Path::new("annotations")
            .join("train")
            .join("bounding_box.csv")
    }

    #[tokio::test]
    async fn test_export_with_sql_cannot_read_host_files() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let path = list_typed_add_row_test_paths();
            workspaces::data_frames::index(&repo, &workspace, &path).await?;

            // A host file the export must not be able to read.
            let secret = repo.path.join("secret.csv");
            std::fs::write(&secret, "col\nvalue\n")?;

            // Caller SQL that reads a host file runs on the hardened connection, so
            // it is blocked and writes no output.
            let out_bad = repo.path.join("bad_export.csv");
            let mut malicious = DFOpts::empty();
            malicious.sql = Some(format!(
                "SELECT * FROM read_csv('{}')",
                secret.to_string_lossy()
            ));
            let err = export(&workspace, &path, &malicious, &out_bad)
                .expect_err("export must not read host files through caller SQL");
            assert!(
                format!("{err}").contains("disabled by configuration"),
                "expected an external-access error, got: {err}"
            );
            assert!(
                !out_bad.exists(),
                "a blocked export must write no output file"
            );

            // A normal caller-SQL export still writes its output.
            let out_ok = repo.path.join("ok_export.csv");
            let mut normal = DFOpts::empty();
            normal.sql = Some(format!("SELECT * FROM {TABLE_NAME}"));
            export(&workspace, &path, &normal, &out_ok)?;
            assert!(out_ok.exists(), "a normal export must write an output file");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_row_with_list_i64_column() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-list-i64")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = list_typed_add_row_test_paths();
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &file_path,
                "scores",
                "list[i64]",
                json!([10, 20, 30]),
            )
            .await?;

            let inner = series.list()?.get_as_series(0).unwrap();
            let values: Vec<i64> = inner.i64()?.into_iter().map(|v| v.unwrap()).collect();
            assert_eq!(values, vec![10, 20, 30]);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_row_with_list_str_column_preserves_nulls() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-list-str")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = list_typed_add_row_test_paths();
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &file_path,
                "tags",
                "list[str]",
                json!(["a", null, "b"]),
            )
            .await?;

            let inner = series.list()?.get_as_series(0).unwrap();
            let values: Vec<Option<String>> = inner
                .str()?
                .into_iter()
                .map(|v| v.map(|s| s.to_string()))
                .collect();
            assert_eq!(
                values,
                vec![Some("a".to_string()), None, Some("b".to_string())]
            );
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_row_with_list_u32_column() -> Result<(), OxenError> {
        // Regression: previously, list[u32]/list[u8]/etc. panicked in value_to_tosql
        // because only Int32/Int64/Float32/Float64/String/Boolean were handled.
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-list-u32")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = list_typed_add_row_test_paths();
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &file_path,
                "ids",
                "list[u32]",
                json!([1u32, 2u32, 3u32]),
            )
            .await?;

            let inner = series.list()?.get_as_series(0).unwrap();
            let values: Vec<u32> = inner.u32()?.into_iter().map(|v| v.unwrap()).collect();
            assert_eq!(values, vec![1, 2, 3]);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_row_with_list_f64_column() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-list-f64")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
            let file_path = list_typed_add_row_test_paths();
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &file_path,
                "embedding",
                "list[f64]",
                json!([0.1, 0.2, 0.3]),
            )
            .await?;

            let inner = series.list()?.get_as_series(0).unwrap();
            let values: Vec<f64> = inner.f64()?.into_iter().map(|v| v.unwrap()).collect();
            assert_eq!(values, vec![0.1, 0.2, 0.3]);
            Ok(())
        })
        .await
    }
}
