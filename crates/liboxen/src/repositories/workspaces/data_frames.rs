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
use tokio::task::spawn_blocking;

pub mod columns;
pub mod embeddings;
pub mod rows;
pub mod schemas;

pub fn is_indexed(workspace: &Workspace, path: &Path) -> Result<bool, DataFrameError> {
    log::debug!("checking dataset is indexed for {path:?}");
    let db_path = duckdb_path(workspace, path);
    if !db_path.exists() {
        return Ok(false);
    }
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

/// [`is_indexed`], off the async worker.
pub async fn is_indexed_async(workspace: &Workspace, path: &Path) -> Result<bool, OxenError> {
    let workspace = workspace.clone();
    let path = path.to_path_buf();
    Ok(spawn_blocking(move || is_indexed(&workspace, &path)).await??)
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

/// Rebuild a staged data frame left by an older version of oxen from its own
/// rows, preserving the staged edits rather than discarding them. See
/// [`core::v_latest::workspaces::data_frames::reindex_preserving_rows`].
pub async fn reindex_preserving_rows(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    core::v_latest::workspaces::data_frames::reindex_preserving_rows(workspace, path).await
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
    {
        let workspace = workspace.clone();
        let path = path.as_ref().to_path_buf();
        spawn_blocking(move || unindex(&workspace, path)).await??;
    }

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

/// Number of rows the pages of a [`query`] with these `opts` span: the rows
/// `opts.sql` selects, or the whole frame when there is no `sql`. Pagination
/// metadata has to describe the same row set the page came out of, so it belongs
/// here rather than on [`count`], which only ever counts the whole frame.
pub fn count_for_query(
    workspace: &Workspace,
    path: &Path,
    opts: &DFOpts,
) -> Result<usize, DataFrameError> {
    let Some(sql) = &opts.sql else {
        return count(workspace, path);
    };

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    with_hardened_query_conn(&db_path, |conn| df_db::count_sql(conn, sql))
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
                sql::query_df(conn, sql.clone(), Some(opts))
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
    let output_path = util::fs::validate_and_normalize_path(output_path)?;
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
    let output_path = workspace.dir().join(&output_path);

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
    duckdb_path_in_dir(&workspace.dir(), path)
}

/// Path of the DuckDB index for the data frame at `path` within the workspace directory
/// `workspace_dir`.
pub(crate) fn duckdb_path_in_dir(workspace_dir: &Path, path: &Path) -> PathBuf {
    let path = util::fs::linux_path(path);
    log::debug!("duckdb_path path: {path:?} workspace dir: {workspace_dir:?}");
    let path_hash = util::hasher::hash_str(path.to_string_lossy());
    workspace_dir
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
    use std::slice;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::SystemTime;

    use indicatif::ProgressBar;
    use serde_json::json;

    use super::*;
    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DIFF_STATUS_COL, INDEX_META_TABLE, OXEN_ID_COL};
    use crate::core::df::tabular::read_version_df;
    use crate::core::staged::get_staged_db_manager;
    use crate::error::OxenError;
    use crate::model::diff::DiffResult;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::{NewCommitBody, ParsedResource, StagedEntryStatus};
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::repositories::data_frames::get_slice;
    use crate::repositories::data_frames::schemas::get_staged_schema_with_staged_db_manager;
    use crate::repositories::diffs::diff_files;
    use crate::repositories::workspaces;
    use crate::repositories::workspaces::{files, status};
    use crate::repositories::{branches, commits, entries, tree};
    use crate::test;
    use crate::util::fs::write_to_path;
    use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate};

    /// Stops a spinning helper thread when the test scope ends, on the assertion-failure path as
    /// well as the happy one — a blocking task left spinning keeps the test binary alive forever.
    struct StopOnDrop(Arc<AtomicBool>);

    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// Every concurrent append to one data frame lands, with the connection cache under enough
    /// pressure to close the data frame's connection whenever it is idle. Writers that overlap
    /// can end up on separate DuckDB databases over the same file, and the one that folds its
    /// state into the file last wins, so the other's rows are gone while each caller was told its
    /// own row was saved.
    ///
    /// The cache is what prevents that: its entry is the process's one connection to the file,
    /// and it must survive eviction while a writer holds it, so the next writer joins that
    /// connection rather than opening its own.
    #[tokio::test]
    async fn test_concurrent_row_appends_all_land() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::create_checkout(&repo, "test-concurrent-append")?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?
                .expect("branch should have a commit");
            let workspace =
                repositories::workspaces::create(&repo, &commit, UserConfig::identifier()?, true)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            workspaces::data_frames::index(&repo, &workspace, &file_path).await?;

            let committed_rows = workspaces::data_frames::count(&workspace, &file_path)?;

            let stop = Arc::new(AtomicBool::new(false));
            let stopper = StopOnDrop(stop.clone());
            let evictor = {
                let stop = stop.clone();
                // Under the test's own directory, so the test's cleanup removes the entries.
                let filler_dir = repo.path.join("lru-pressure");
                tokio::task::spawn_blocking(move || {
                    // Cycling through more entries than the cache keeps evicts this data frame's
                    // connection whenever no writer holds it, so writers reopen it throughout the
                    // run. The fillers are empty slots for paths that name no file, so nothing
                    // opens.
                    let mut i = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let filler =
                            filler_dir.join((i % (df_db::DF_DB_CACHE_SIZE.get() + 1)).to_string());
                        df_db::with_db_closed(&filler, || {});
                        i += 1;
                    }
                })
            };

            // Successive rounds compound the loss: once two connections disagree about the table,
            // a write through the stale one discards everything the other added. Each writer
            // appends back to back, so its later appends arrive while other writers hold the
            // connection and after the pressure has had time to evict it; a burst of one-shot
            // writers would all take the connection before the first eviction.
            let writers = 4;
            let appends_per_writer = 4;
            let appends_per_round = writers * appends_per_writer;
            for round in 0..3 {
                let mut handles = Vec::new();
                for writer in 0..writers {
                    let repo = repo.clone();
                    let workspace = workspace.clone();
                    let file_path = file_path.clone();
                    handles.push(tokio::task::spawn_blocking(
                        move || -> Result<(), OxenError> {
                            for i in 0..appends_per_writer {
                                let json_data = json!({
                                    "file": format!("concurrent-{round}-{writer}-{i}.jpg"),
                                    "label": "dog",
                                    "min_x": 1.0,
                                    "min_y": 2.0,
                                    "width": 10.0,
                                    "height": 10.0
                                });
                                workspaces::data_frames::rows::add(
                                    &repo, &workspace, &file_path, &json_data,
                                )?;
                            }
                            Ok(())
                        },
                    ));
                }
                for handle in handles {
                    handle.await.expect("join append task")?;
                }

                let expected = committed_rows + appends_per_round * (round + 1);
                let count = workspaces::data_frames::count(&workspace, &file_path)?;
                assert_eq!(
                    count, expected,
                    "round {round}: appends reported success but their rows are missing"
                );
            }

            drop(stopper);
            evictor.await.expect("join evictor task");

            Ok(())
        })
        .await
    }

    /// An editable workspace called `name` on `commit` with the bounding-box data frame indexed,
    /// and a branch called `name` at `commit` for it to commit to.
    async fn indexed_workspace(
        repo: &LocalRepository,
        commit: &Commit,
        name: &str,
    ) -> Result<Workspace, OxenError> {
        branches::create(repo, name, &commit.id)?;
        let workspace = workspaces::create(repo, commit, name, true)?;
        index(repo, &workspace, test::test_bounding_box_csv()).await?;
        Ok(workspace)
    }

    /// The `_oxen_id` of the first row of `df`.
    fn first_row_id(df: &DataFrame) -> Result<String, OxenError> {
        Ok(df
            .column(OXEN_ID_COL)?
            .get(0)?
            .to_string()
            .trim_matches('"')
            .to_string())
    }

    #[tokio::test]
    async fn test_index_query_and_export_a_data_frame() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let commit = commits::head_commit(&repo)?;
            let workspace = workspaces::create(&repo, &commit, "query", true)?;
            let path = test::test_bounding_box_csv();

            // A table left behind by an older version (no index marker table) or by an interrupted
            // index (missing `_oxen_id`) must report NOT indexed so callers rebuild it, and
            // re-indexing must repair it.
            let db_path = duckdb_path(&workspace, &path);
            assert!(!is_indexed(&workspace, &path)?);
            assert!(
                !db_path.exists(),
                "checking a never-indexed frame creates no database for it"
            );

            // A normal index produces a fully-indexed, queryable table.
            index(&repo, &workspace, &path).await?;
            assert!(is_indexed(&workspace, &path)?);

            // Simulate a table written by an older version: no index marker table. Such a table
            // may hold rows tombstoned as 'removed' that the current code would wrongly export, so
            // it must be rebuilt.
            with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    conn.execute(&format!("DROP TABLE \"{INDEX_META_TABLE}\""), [])?;
                    Ok(())
                })
            })?;
            assert!(!is_indexed(&workspace, &path)?);

            // Re-indexing repairs the table.
            index(&repo, &workspace, &path).await?;
            assert!(is_indexed(&workspace, &path)?);

            // Simulate the partial-index corruption an interrupted index leaves behind: a table
            // that is missing `_oxen_id`.
            with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    conn.execute(
                        &format!("ALTER TABLE \"{TABLE_NAME}\" DROP COLUMN \"{OXEN_ID_COL}\""),
                        [],
                    )?;
                    Ok(())
                })
            })?;
            assert!(!is_indexed(&workspace, &path)?);

            index(&repo, &workspace, &path).await?;
            assert!(is_indexed(&workspace, &path)?);

            // Indexing adds exactly the hidden system columns (`_oxen_id`, `_oxen_row_id`), and
            // not the legacy tracking columns (`_oxen_diff_status`, `_oxen_diff_hash`).
            let columns: Vec<String> = with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    let schema = df_db::get_schema(conn, TABLE_NAME)?;
                    Ok(schema.fields.into_iter().map(|f| f.name).collect())
                })
            })?;
            assert!(columns.contains(&OXEN_ID_COL.to_string()));
            assert!(columns.contains(&OXEN_ROW_ID_COL.to_string()));
            assert!(!columns.contains(&DIFF_STATUS_COL.to_string()));
            assert!(!columns.iter().any(|c| c == "_oxen_diff_hash"));

            // The read with no `sql` returns row-ordered pages of page_size.
            let first = query(&workspace, &path, &page_opts(1, 4, None))?;
            assert_eq!(
                column_values(&first, "min_x")?,
                vec!["101.5", "102.5", "7.0", "19.0"]
            );
            let second = query(&workspace, &path, &page_opts(2, 4, None))?;
            assert_eq!(column_values(&second, "min_x")?, vec!["57.0", "30.5"]);

            // A `sql` read returns the page it was asked for. Pages are disjoint, they run out,
            // and they count against the query's own row set rather than the whole frame.
            // 4 of the fixture's 6 rows are labelled dog.
            let dogs = format!("SELECT * FROM {TABLE_NAME} WHERE label = 'dog'");
            let dog_page = |page| page_opts(page, 2, Some(&dogs));

            let first = query(&workspace, &path, &dog_page(1))?;
            assert_eq!(first.height(), 2, "a page must hold at most page_size rows");
            let second = query(&workspace, &path, &dog_page(2))?;
            assert_eq!(second.height(), 2);
            assert_eq!(
                column_values(&first, "min_x")?,
                vec!["101.5", "102.5"],
                "an unsorted query pages in row order"
            );
            assert_eq!(column_values(&second, "min_x")?, vec!["7.0", "19.0"]);
            let third = query(&workspace, &path, &dog_page(3))?;
            assert_eq!(third.height(), 0, "pages past the result set are empty");

            // Pagination values too large to represent must clamp rather than overflow the offset
            // or exceed what the LIMIT/OFFSET types accept: the furthest page reads past the end,
            // the widest page holds everything.
            let furthest = page_opts(usize::MAX, 2, Some(&dogs));
            assert_eq!(
                query(&workspace, &path, &furthest)?.height(),
                0
            );
            let widest = page_opts(1, usize::MAX, Some(&dogs));
            assert_eq!(
                query(&workspace, &path, &widest)?.height(),
                4
            );

            assert_eq!(
                count_for_query(&workspace, &path, &dog_page(1))?,
                4,
                "a query's count is what it selects, not what the frame holds"
            );
            assert_eq!(
                count_for_query(&workspace, &path, &page_opts(1, 2, None))?,
                6,
                "without sql the count is the whole frame"
            );

            // A `sql` read pages within whatever the caller's SQL bounds and orders itself to,
            // instead of colliding with its LIMIT or overriding its sort. The 3 smallest min_x of
            // the 6 rows are 7.0, 19.0, and 30.5.
            let smallest = format!("SELECT * FROM {TABLE_NAME} ORDER BY min_x LIMIT 3");
            let smallest_page = |page| page_opts(page, 2, Some(&smallest));

            let first = query(&workspace, &path, &smallest_page(1))?;
            assert_eq!(
                column_values(&first, "min_x")?,
                vec!["7.0", "19.0"],
                "the caller's sort survives pagination"
            );
            let second = query(&workspace, &path, &smallest_page(2))?;
            assert_eq!(
                column_values(&second, "min_x")?,
                vec!["30.5"],
                "the last page is short because the caller's LIMIT bounds it"
            );
            let third = query(&workspace, &path, &smallest_page(3))?;
            assert_eq!(third.height(), 0);
            assert_eq!(
                count_for_query(&workspace, &path, &smallest_page(1))?,
                3,
                "the count must respect the caller's LIMIT too"
            );

            // A single row addressed by offset: the caller bounds the result to one row and the
            // page has to leave that row intact.
            let offset_row = format!("SELECT * FROM {TABLE_NAME} LIMIT 1 OFFSET 3");
            let opts = page_opts(1, 100, Some(&offset_row));
            let one = query(&workspace, &path, &opts)?;
            assert_eq!(column_values(&one, "min_x")?, vec!["19.0"]);

            // A query whose shape can't resolve `_oxen_row_id` (it aggregates or dedupes) still
            // paginates rather than failing to bind an added sort.
            let grouped_sql =
                format!("SELECT label, COUNT(*) AS n FROM {TABLE_NAME} GROUP BY label ORDER BY label");
            let opts = page_opts(1, 10, Some(&grouped_sql));
            let grouped = query(&workspace, &path, &opts)?;
            assert_eq!(column_values(&grouped, "label")?, vec!["cat", "dog"]);
            assert_eq!(column_values(&grouped, "n")?, vec!["2", "4"]);

            let distinct_sql = format!("SELECT DISTINCT label FROM {TABLE_NAME}");
            let opts = page_opts(1, 1, Some(&distinct_sql));
            let distinct = query(&workspace, &path, &opts)?;
            assert_eq!(distinct.height(), 1, "a DISTINCT query paginates as well");

            // A statement written the way it would be typed at a prompt (terminated by `;`, or
            // trailing a comment) pages and counts like any other. Both are harmless at the end of
            // the text and change its meaning in the middle, so they have to come off before a
            // page's bounds are composed on. Each of these reaches pagination by a different
            // route: a plain projection is rewritten to carry `_oxen_id`, while an aggregate and a
            // DISTINCT are passed through as written.
            let selected = format!("SELECT * FROM {TABLE_NAME};");
            let aggregated = format!("SELECT label, COUNT(*) AS n FROM {TABLE_NAME} GROUP BY label;");
            let deduped = format!("SELECT DISTINCT label FROM {TABLE_NAME};");
            // A comment would otherwise swallow the composed bounds and quietly return the whole
            // frame rather than failing.
            let commented = format!("SELECT * FROM {TABLE_NAME} -- every row");
            for (sql, rows, of) in [
                (&selected, 2, 6),
                (&aggregated, 1, 2),
                (&deduped, 1, 2),
                (&commented, 2, 6),
            ] {
                let opts = page_opts(1, rows, Some(sql));
                let df = query(&workspace, &path, &opts)?;
                assert_eq!(df.height(), rows, "{sql} should page to {rows} rows");
                let count = count_for_query(&workspace, &path, &opts)?;
                assert_eq!(count, of, "{sql} should count {of} rows");
            }

            // Caller SQL that reads a host file runs on the hardened connection, so it is blocked
            // and writes no output.
            let secret = repo.path.join("secret.csv");
            write_to_path(&secret, "col\nvalue\n")?;
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

            // A row naming a column the frame does not have is refused.
            let json_data = json!({"NOT_REAL_COLUMN": "images/test.jpg"});
            assert!(rows::add(&repo, &workspace, &path, &json_data).is_err());

            // A batch update containing a row id that doesn't exist must fail before ANY row is
            // written, not apply the valid rows and then report failure.
            let staged_df = query(&workspace, &path, &page_opts(0, 10, None))?;
            let real_id = first_row_id(&staged_df)?;
            let data = json!([
                {"row_id": real_id, "value": {"label": "should-not-apply"}},
                {"row_id": "no-such-row-id", "value": {"label": "phantom"}}
            ]);
            let result = rows::batch_update(&repo, &workspace, &path, &data);
            assert!(result.is_err(), "batch with a missing row id must fail");
            let row = rows::get_by_id(&workspace, &path, &real_id)?;
            let label = row.column("label")?.get(0)?;
            assert_ne!(label.get_str(), Some("should-not-apply"));
            assert_eq!(
                count(&workspace, &path)?,
                6,
                "refused edits leave the frame as committed"
            );

            // Recovery fails loudly (WorkspaceStaleStagedIndex) rather than committing an empty
            // data frame when a stale table holds only internal columns, so there is nothing to
            // recover.
            with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    for col in ["file", "label", "min_x", "min_y", "width", "height"] {
                        conn.execute(
                            &format!("ALTER TABLE \"{TABLE_NAME}\" DROP COLUMN \"{col}\""),
                            [],
                        )?;
                    }
                    conn.execute(&format!("DROP TABLE \"{INDEX_META_TABLE}\""), [])?;
                    Ok(())
                })
            })?;
            let result = reindex_preserving_rows(&workspace, &path).await;
            assert!(
                matches!(result, Err(OxenError::WorkspaceStaleStagedIndex(_))),
                "recovery with no user columns must fail with WorkspaceStaleStagedIndex, got {result:?}"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_row_and_column_edits_in_a_workspace() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let commit = commits::head_commit(&repo)?;
            let workspace = indexed_workspace(&repo, &commit, "row-edits").await?;
            let path = test::test_bounding_box_csv();
            let row_count = || count(&workspace, &path);
            let staged_files = || -> Result<usize, OxenError> {
                Ok(status::status(&workspace)?.staged_files.len())
            };
            let committed_rows = row_count()?;
            let dog = |file: &str| {
                json!({
                    "file": file,
                    "label": "dog",
                    "min_x": 13,
                    "min_y": 14,
                    "width": 100,
                    "height": 100
                })
            };

            let first = rows::add(&repo, &workspace, &path, &dog("dawg1.jpg"))?;
            assert_eq!(staged_files()?, 1);
            assert_eq!(row_count()?, committed_rows + 1);

            // A row added while a commit runs leaves the data frame staged once that commit
            // removes the entries it read.
            let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
            let (_, snapshot) =
                staged_db_manager.read_staged_entries_for_commit(&ProgressBar::hidden())?;
            let second = rows::add(&repo, &workspace, &path, &dog("dawg2.jpg"))?;
            staged_db_manager.remove_unchanged(&snapshot)?;
            drop(staged_db_manager);
            assert_eq!(
                staged_files()?,
                1,
                "a row edit after the snapshot keeps the data frame staged"
            );
            assert_eq!(
                row_count()?,
                committed_rows + 2,
                "both appended rows are in the staged table"
            );

            rows::delete(&repo, &workspace, &path, &first_row_id(&first)?)?;
            assert_eq!(
                row_count()?,
                committed_rows + 1,
                "only the second appended row remains"
            );

            // Edits are not reversible: even when deletes bring the table back to its original
            // contents, the data frame stays staged as modified. Un-staging is an explicit
            // operation (`restore`), not an automatic side effect of
            // edits cancelling out.
            rows::delete(&repo, &workspace, &path, &first_row_id(&second)?)?;
            assert_eq!(row_count()?, committed_rows);
            assert_eq!(staged_files()?, 1);

            // The same holds for a committed row modified and then modified back, which leaves the
            // table matching the commit again.
            let staged_df = query(&workspace, &path, &page_opts(0, 10, None))?;
            let committed_id = first_row_id(&staged_df)?;
            for label in ["doggo", "dog"] {
                rows::update(
                    &repo,
                    &workspace,
                    &path,
                    &committed_id,
                    &json!({"label": label}),
                )?;
                let row = rows::get_by_id(&workspace, &path, &committed_id)?;
                assert_eq!(row.column("label")?.get(0)?.get_str(), Some(label));
            }
            assert_eq!(
                staged_files()?,
                1,
                "a committed row modified back to its committed value keeps the data frame staged"
            );

            // An update applies in place to the row it addresses.
            let json_data = json!({"min_x": 13, "min_y": 14, "width": 100, "height": 100});
            let new_row = rows::add(&repo, &workspace, &path, &json_data)?;
            assert_eq!(row_count()?, committed_rows + 1);
            let id_to_modify = first_row_id(&new_row)?;
            rows::update(
                &repo,
                &workspace,
                &path,
                &id_to_modify,
                &json!({"height": 101}),
            )?;
            assert_eq!(staged_files()?, 1);
            let row = rows::get_by_id(&workspace, &path, &id_to_modify)?;
            assert_eq!(row.height(), 1);
            assert_eq!(row.column("height")?.get(0)?.to_string(), "101");
            assert_eq!(row_count()?, committed_rows + 1);

            // The default (no-sql, no-sort) read path must return rows in a stable order across
            // edits: a DuckDB UPDATE physically relocates the row, and without the `_oxen_row_id`
            // ORDER BY the edited row jumps pages. The ordering column itself must not leak into
            // results.
            let opts = DFOpts::empty();
            let ids_before = column_values(&query(&workspace, &path, &opts)?, OXEN_ID_COL)?;
            rows::update(
                &repo,
                &workspace,
                &path,
                &ids_before[0],
                &json!({ "height": 999 }),
            )?;
            let df_after = query(&workspace, &path, &opts)?;
            assert_eq!(
                column_values(&df_after, OXEN_ID_COL)?,
                ids_before,
                "row order must be stable across an edit"
            );
            assert_eq!(
                df_after.column("height")?.get(0)?.to_string(),
                "999",
                "the edit applied to the first row"
            );
            assert!(
                df_after.column(OXEN_ROW_ID_COL).is_err(),
                "the internal ordering column must not appear in results"
            );

            // The reserved keys (`_oxen_id`, `_oxen_row_id`) are stripped from row-update
            // payloads, and the rest of the update applies.
            let json_data = json!({
                "label": "doggo",
                "_oxen_id": "someone-elses-id",
                "_oxen_row_id": 1
            });
            let updated = rows::update(&repo, &workspace, &path, &committed_id, &json_data)?;
            assert_eq!(updated.column("label")?.get(0)?.get_str(), Some("doggo"));

            // A legacy tracking key names a column this table doesn't have: rejected as a schema
            // mismatch rather than silently stripped.
            let json_data = json!({"label": "doggo", DIFF_STATUS_COL: "unchanged"});
            let result = rows::update(&repo, &workspace, &path, &committed_id, &json_data);
            assert!(result.is_err(), "unknown columns must be rejected");

            // List columns of each element type round-trip through a row add. list[u32] once
            // panicked in value_to_tosql, which handled only Int32/Int64/Float32/Float64/String/
            // Boolean.
            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &path,
                "scores",
                "list[i64]",
                json!([10, 20, 30]),
            )
            .await?;
            let inner = series
                .list()?
                .get_as_series(0)
                .expect("the row holds a list");
            let values: Vec<i64> = inner.i64()?.iter().map(|v| v.unwrap()).collect();
            assert_eq!(values, vec![10, 20, 30]);

            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &path,
                "tags",
                "list[str]",
                json!(["a", null, "b"]),
            )
            .await?;
            let inner = series
                .list()?
                .get_as_series(0)
                .expect("the row holds a list");
            let values: Vec<Option<String>> = inner
                .str()?
                .iter()
                .map(|v| v.map(|s| s.to_string()))
                .collect();
            assert_eq!(
                values,
                vec![Some("a".to_string()), None, Some("b".to_string())]
            );

            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &path,
                "ids",
                "list[u32]",
                json!([1u32, 2u32, 3u32]),
            )
            .await?;
            let inner = series
                .list()?
                .get_as_series(0)
                .expect("the row holds a list");
            let values: Vec<u32> = inner.u32()?.iter().map(|v| v.unwrap()).collect();
            assert_eq!(values, vec![1, 2, 3]);

            let series = add_row_with_list_column(
                &repo,
                &workspace,
                &path,
                "embedding",
                "list[f64]",
                json!([0.1, 0.2, 0.3]),
            )
            .await?;
            let inner = series
                .list()?
                .get_as_series(0)
                .expect("the row holds a list");
            let values: Vec<f64> = inner.f64()?.iter().map(|v| v.unwrap()).collect();
            assert_eq!(values, vec![0.1, 0.2, 0.3]);

            // Column names with spaces must survive the whole column lifecycle: the ALTER TABLE
            // builders quote identifiers, so a name like "my col" can be added, renamed, and
            // deleted.
            let new_column = NewColumn {
                name: "my col".to_string(),
                data_type: "str".to_string(),
            };
            let df = columns::add(&repo, &workspace, &path, &new_column)?;
            assert!(df.column("my col").is_ok());

            let rename = ColumnToUpdate {
                name: "my col".to_string(),
                new_name: Some("my col 2".to_string()),
                new_data_type: None,
            };
            let (_, snapshot) = get_staged_db_manager(&workspace.workspace_repo)?
                .read_staged_entries_for_commit(&ProgressBar::hidden())?;
            let df = columns::update(&repo, &workspace, &path, &rename)?;
            assert!(df.column("my col 2").is_ok());
            assert!(df.column("my col").is_err());
            get_staged_db_manager(&workspace.workspace_repo)?.remove_unchanged(&snapshot)?;
            assert_eq!(
                staged_files()?,
                1,
                "a column rename while a commit runs leaves the data frame staged"
            );

            // The renamed column must be usable through the row paths too: append a row with a
            // value in it, then edit that value.
            let json_data = json!({ "file": "spaced.jpg", "my col 2": "hello" });
            let added = rows::add(&repo, &workspace, &path, &json_data)?;
            let json_data = json!({ "my col 2": "world" });
            let updated =
                rows::update(&repo, &workspace, &path, &first_row_id(&added)?, &json_data)?;
            assert_eq!(updated.column("my col 2")?.get(0)?.get_str(), Some("world"));

            let delete = ColumnToDelete {
                name: "my col 2".to_string(),
            };
            let df = columns::delete(&repo, &workspace, &path, &delete)?;
            assert!(df.column("my col 2").is_err());

            // An embedded double quote is the hardest identifier case: every SQL builder must
            // escape it, not just wrap it.
            let quoted_name = "we\"ird col";
            let new_column = NewColumn {
                name: quoted_name.to_string(),
                data_type: "str".to_string(),
            };
            let df = columns::add(&repo, &workspace, &path, &new_column)?;
            assert!(df.column(quoted_name).is_ok());
            let json_data = json!({ "file": "quoted.jpg", quoted_name: "hi" });
            let added = rows::add(&repo, &workspace, &path, &json_data)?;
            assert_eq!(added.column(quoted_name)?.get(0)?.get_str(), Some("hi"));
            let delete = ColumnToDelete {
                name: quoted_name.to_string(),
            };
            let df = columns::delete(&repo, &workspace, &path, &delete)?;
            assert!(df.column(quoted_name).is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_data_frame_metadata_edits_stay_staged() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let commit = commits::head_commit(&repo)?;
            let path = test::test_bounding_box_csv();
            let render_image = json!({"_oxen": {"render": {"func": "image"}}});

            let workspace = indexed_workspace(&repo, &commit, "metadata").await?;
            let staged_schema = || {
                get_staged_schema_with_staged_db_manager(
                    &workspace.workspace_repo,
                    &path,
                )
            };

            // A schema-metadata write must be readable back through the staged schema, which is
            // where the read path picks it up when building a response.
            let settings = json!({"_oxen": {"view": {"columns": ["label", "file"]}}});
            schemas::add_schema_metadata(&repo, &workspace, &path, &settings)?;
            let staged = staged_schema()?.expect("a staged schema exists after the write");
            assert_eq!(
                staged.metadata,
                Some(settings.clone()),
                "schema metadata should be staged"
            );

            // A row edit re-stages the file node from the *committed* tree, which knows nothing of
            // workspace metadata edits, so schema settings and column render funcs staged
            // beforehand must survive the write.
            columns::add_column_metadata(
                &repo,
                &workspace,
                path.clone(),
                "file".to_string(),
                &render_image,
            )?;
            let row = json!({"file": "images/x.jpg", "label": "cat"});
            rows::add(&repo, &workspace, &path, &row)?;
            let staged = staged_schema()?.expect("a staged schema still exists after the row write");
            assert_eq!(
                staged.metadata,
                Some(settings),
                "schema metadata must survive a row write"
            );
            let file_field = staged
                .fields
                .iter()
                .find(|f| f.name == "file")
                .expect("the file column still exists");
            assert_eq!(
                file_field.metadata,
                Some(render_image.clone()),
                "column metadata must survive a row write"
            );

            // Schema-level and per-column metadata are independent: a file-level write must not
            // clobber a column's metadata.
            let view = json!({"_oxen": {"view": {"columns": ["file", "label"]}}});
            let schema =
                schemas::add_schema_metadata(&repo, &workspace, &path, &view)?;
            assert_eq!(schema.metadata, Some(view), "schema metadata should be written");
            let file_field = schema
                .fields
                .iter()
                .find(|f| f.name == "file")
                .expect("the file column still exists");
            assert_eq!(
                file_field.metadata,
                Some(render_image.clone()),
                "column metadata should survive a schema-level write"
            );

            // add_column_metadata reconciles the staged metadata schema with the workspace table:
            // a column deleted by a workspace edit is dropped from the schema (not left as a
            // phantom), and the reconciled schema matches the table's columns.
            columns::delete(
                &repo,
                &workspace,
                &path,
                &ColumnToDelete {
                    name: "label".to_string(),
                },
            )?;
            let schema = columns::add_column_metadata(
                &repo,
                &workspace,
                path.clone(),
                "file".to_string(),
                &json!({"root": "images"}),
            )?;
            let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
            assert!(
                !names.contains(&"label"),
                "deleted column should be gone: {names:?}"
            );
            assert!(names.contains(&"file"));

            // The staged node's column count has to track the reconciled field list, since that is
            // what the diff summary and the data frame size report.
            let staged = get_staged_db_manager(&workspace.workspace_repo)?
                .read_from_staged_db(&path)?
                .expect("data frame should be staged");
            let Some(GenericMetadata::MetadataTabular(m)) = staged.node.file()?.metadata() else {
                panic!("staged node should carry tabular metadata");
            };
            assert_eq!(m.tabular.width, m.tabular.schema.fields.len());

            // A metadata edit keeps whatever status is already staged, so it can't turn a newly
            // added file into a modified one, and it can't turn a removal into a modification.
            let workspace = workspaces::create(&repo, &commit, "metadata-status", true)?;
            let file_node = tree::get_file_by_path(&repo, &commit, &path)?
                .expect("the committed data frame should exist");
            let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;

            staged_db_manager.upsert_file_node(&path, StagedEntryStatus::Added, &file_node)?;
            schemas::add_schema_metadata(
                &repo,
                &workspace,
                &path,
                &json!({"edited": "while added"}),
            )?;
            let staged = staged_db_manager
                .read_from_staged_db(&path)?
                .expect("the metadata edit should remain staged");
            assert_eq!(
                staged.status,
                StagedEntryStatus::Added,
                "a metadata edit must not change the existing staged status"
            );

            staged_db_manager.upsert_file_node(&path, StagedEntryStatus::Removed, &file_node)?;
            let result = schemas::add_schema_metadata(
                &repo,
                &workspace,
                &path,
                &json!({"edited": "while removed"}),
            );
            assert!(
                matches!(result, Err(OxenError::PathStagedForRemoval(_))),
                "editing a path staged for removal must be refused, got {result:?}"
            );
            let staged = staged_db_manager
                .read_from_staged_db(&path)?
                .expect("the removal should still be staged");
            assert_eq!(
                staged.status,
                StagedEntryStatus::Removed,
                "a refused edit must leave the removal staged"
            );
            drop(staged_db_manager);

            // A metadata-only workspace edit (add_column_metadata, no row changes) must survive
            // commit: the no-op-export skip compares against the BASE commit's file node, so the
            // metadata change, which the staged node already carries, is not mistaken for an
            // unchanged file and dropped.
            let workspace = indexed_workspace(&repo, &commit, "metadata-only").await?;
            columns::add_column_metadata(
                &repo,
                &workspace,
                path.clone(),
                "file".to_string(),
                &render_image,
            )?;
            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Metadata-only edit".to_string(),
            };
            let committed = workspaces::commit(&workspace, &new_commit, "metadata-only").await?;
            let file_node = tree::get_file_by_path(&repo, &committed, &path)?
                .expect("committed file node should exist");
            let has_metadata = matches!(
                file_node.metadata(),
                Some(GenericMetadata::MetadataTabular(m))
                    if m.tabular.schema.fields.iter().any(|f| f.name == "file" && f.metadata.is_some())
            );
            assert!(has_metadata, "metadata-only edit must survive commit");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_data_frame_edits_from_workspaces() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let commit = commits::head_commit(&repo)?;
            let path = test::test_bounding_box_csv();
            let version_store = repo.version_store();
            let body = |message: &str| NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: message.to_string(),
            };

            // A committed row deleted in a workspace is deleted from the staged table right away,
            // not tombstoned, and the commit removes it from the file.
            let workspace = indexed_workspace(&repo, &commit, "delete-committed-row").await?;
            let staged_df = query(&workspace, &path, &page_opts(0, 10, None))?;
            rows::delete(&repo, &workspace, &path, &first_row_id(&staged_df)?)?;
            assert_eq!(count(&workspace, &path)?, 5);
            assert_eq!(status::status(&workspace)?.staged_files.len(), 1);
            let committed =
                workspaces::commit(&workspace, &body("Deleting a row"), "delete-committed-row").await?;

            let node_1 = tree::get_file_by_path(&repo, &commit, &path)?
                .expect("file should exist in commit");
            let file_1_csv = repo.path.join("version_1.csv");
            version_store
                .copy_version_to_path(&node_1.hash().to_string(), &file_1_csv, SystemTime::now())
                .await?;
            let node_2 = tree::get_file_by_path(&repo, &committed, &path)?
                .expect("file should exist in the new commit");
            let file_2_csv = repo.path.join("version_2.csv");
            version_store
                .copy_version_to_path(&node_2.hash().to_string(), &file_2_csv, SystemTime::now())
                .await?;
            match diff_files(file_1_csv, file_2_csv, vec![], vec![], vec![])
                .await?
            {
                DiffResult::Tabular(tabular_diff) => {
                    assert_eq!(tabular_diff.summary.modifications.row_counts.removed, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // A staged append commits into the data frame's version file.
            let workspace = indexed_workspace(&repo, &commit, "append").await?;
            let user = UserConfig::get()?.to_user();
            let json_data = json!({"file": "images/test.jpg", "label": "dog", "min_x": 2.0, "min_y": 3.0, "width": 100, "height": 120});
            rows::add(&repo, &workspace, &path, &json_data)?;
            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email,
                message: "Appending tabular data".to_string(),
            };
            let appended = workspaces::commit(&workspace, &new_commit, "append").await?;
            let entry = entries::get_commit_entry(&repo, &appended, &path)?
                .expect("the appended data frame is committed");
            let data_frame = read_version_df(
                &version_store,
                entry.hash.parse()?,
                entry.num_bytes,
                "csv",
                &DFOpts::empty(),
            )
            .await?;
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

            // Committing a workspace whose staged table was left by an older version of oxen
            // (legacy tracking columns, tombstoned deletes, no index marker) must recover the
            // staged rows rather than error or drop them: the staged append survives the commit
            // and a row the old flow tombstoned as 'removed' stays deleted.
            let workspace = indexed_workspace(&repo, &commit, "stale-recover").await?;
            let json_data = json!({
                "file": "stale.jpg", "label": "dog",
                "min_x": 1, "min_y": 2, "width": 3, "height": 4
            });
            rows::add(&repo, &workspace, &path, &json_data)?;

            // Downgrade the DuckDB table to what an older version of oxen left behind: legacy
            // tracking columns, one committed row tombstoned as 'removed', no `_oxen_row_id`
            // ordering column, and no index marker.
            let db_path = duckdb_path(&workspace, &path);
            with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    conn.execute_batch(&format!(
                        "ALTER TABLE \"{t}\" ADD COLUMN \"{status}\" VARCHAR DEFAULT 'unchanged'; \
                         ALTER TABLE \"{t}\" ADD COLUMN \"_oxen_diff_hash\" VARCHAR DEFAULT '0'; \
                         UPDATE \"{t}\" SET \"{status}\" = 'added' WHERE \"file\" = 'stale.jpg'; \
                         UPDATE \"{t}\" SET \"{status}\" = 'removed' WHERE \"file\" = 'train/cat_2.jpg'; \
                         ALTER TABLE \"{t}\" DROP COLUMN \"{row_id}\"; \
                         DROP TABLE \"{marker}\";",
                        t = TABLE_NAME,
                        status = DIFF_STATUS_COL,
                        row_id = OXEN_ROW_ID_COL,
                        marker = INDEX_META_TABLE,
                    ))?;
                    Ok(())
                })
            })?;

            // The table now reports not indexed, but the staged rows are still physically present
            // in DuckDB (6 committed + 1 appended = 7).
            assert!(!is_indexed(&workspace, &path)?);
            assert_eq!(count(&workspace, &path)?, 7);

            // Commit succeeds: the stale table is recovered from its own rows.
            let recovered = workspaces::commit(
                &workspace,
                &body("Committing recovered stale staged df"),
                "stale-recover",
            )
            .await?;

            // The committed file keeps the appended row and drops the tombstoned one: 6 committed
            // - 1 removed + 1 appended = 6 rows.
            let entry = entries::get_commit_entry(&repo, &recovered, &path)?
                .expect("committed entry should exist");
            let df = read_version_df(
                &version_store,
                entry.hash.parse()?,
                entry.num_bytes,
                "csv",
                &DFOpts::empty(),
            )
            .await?;
            assert_eq!(df.height(), 6, "recovered commit row count: {df}");
            let rendered = format!("{df}");
            assert!(
                rendered.contains("stale.jpg"),
                "staged append must survive recovery: {rendered}"
            );
            assert!(
                !rendered.contains("cat_2"),
                "tombstoned row must stay deleted: {rendered}"
            );

            // Unstaging a data frame discards its staged row edits, not just the staging marker.
            // While the DuckDB index survived an unstage, the next edit re-staged the file and the
            // discarded rows came back with it.
            let workspace = indexed_workspace(&repo, &commit, "unstage-discards").await?;
            let committed_rows = count(&workspace, &path)?;
            rows::add(
                &repo,
                &workspace,
                &path,
                &json!({"file": "images/discarded.jpg", "label": "cat"}),
            )?;
            assert_eq!(
                count(&workspace, &path)?,
                committed_rows + 1
            );

            files::unstage(&workspace, &path)?;
            {
                let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
                assert!(
                    staged_db_manager.read_from_staged_db(&path)?.is_none(),
                    "unstage should drop the staged entry"
                );
            }
            assert!(
                !is_indexed(&workspace, &path)?,
                "unstage should drop the index holding the discarded edits"
            );

            // Re-indexing rebuilds from the committed file, so the discarded row is really gone.
            index(&repo, &workspace, &path).await?;
            assert_eq!(
                count(&workspace, &path)?,
                committed_rows,
                "the discarded row must not survive into a fresh index"
            );

            rows::add(
                &repo,
                &workspace,
                &path,
                &json!({"file": "images/kept.jpg", "label": "dog"}),
            )?;
            let new_commit =
                workspaces::commit(&workspace, &body("Keep only the second row"), "unstage-discards")
                    .await?;
            let resource = ParsedResource {
                path: path.clone(),
                version: PathBuf::from(new_commit.id.to_string()),
                resource: path.clone(),
                workspace: None,
                commit: Some(new_commit.clone()),
                branch: None,
            };
            let df = get_slice(&repo, &resource, &path, &DFOpts::empty())
                .await?;
            assert_eq!(
                df.total_entries,
                committed_rows + 1,
                "only the row added after the unstage should be committed"
            );

            // Adding a data frame's committed file over a staged replacement of it unstages the
            // replacement, and discards the row edits its index holds, as unstaging does.
            let workspace = indexed_workspace(&repo, &commit, "add-reverts").await?;
            rows::add(
                &repo,
                &workspace,
                &path,
                &json!({"file": "images/reverted.jpg", "label": "cat"}),
            )?;
            let workspace_file = workspace.workspace_repo.path.join(&path);
            write_to_path(&workspace_file, "file,label\nreplaced.jpg,dog\n")?;
            files::add(&workspace, &workspace_file).await?;
            util::fs::copy(repo.path.join(&path), &workspace_file)?;
            files::add(&workspace, &workspace_file).await?;
            {
                let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
                assert!(
                    staged_db_manager.read_from_staged_db(&path)?.is_none(),
                    "adding the committed file drops the staged entry"
                );
            }
            assert!(
                !is_indexed(&workspace, &path)?,
                "adding the committed file drops the index holding the discarded edits"
            );
            index(&repo, &workspace, &path).await?;
            assert_eq!(
                count(&workspace, &path)?,
                committed_rows,
                "the discarded row must not survive into a fresh index"
            );

            // Staging a data frame for removal drops its DuckDB index. If the index survived, then
            // the frame stayed editable and the next row edit re-staged it as `Modified`, so the
            // removal silently never happened.
            let workspace = indexed_workspace(&repo, &commit, "rm-unindexes").await?;
            assert!(is_indexed(&workspace, &path)?);

            let err_files = files::rm(&workspace, slice::from_ref(&path)).await?;
            assert!(err_files.is_empty(), "rm reported errors: {err_files:?}");
            assert!(
                !is_indexed(&workspace, &path)?,
                "a data frame staged for removal must not keep an editable index"
            );

            // The edit that used to resurrect the file now has no index to edit.
            let row = json!({"file": "images/x.jpg", "label": "cat"});
            assert!(
                rows::add(&repo, &workspace, &path, &row).is_err(),
                "editing rows of a removed data frame must not succeed"
            );

            let removal =
                workspaces::commit(&workspace, &body("Remove the data frame"), "rm-unindexes").await?;
            assert!(
                tree::get_file_by_path(&repo, &removal, &path)?.is_none(),
                "the removal must survive to the commit"
            );

            // Moving a directory carries a data frame's staged row edits with it: the edit reads
            // at the new path before the commit and is in the committed file after it.
            let train = path.parent().expect("the data frame sits in a directory");
            let moved = Path::new("annotations").join("moved_train");
            let moved_path = moved.join("bounding_box.csv");
            let workspace = indexed_workspace(&repo, &commit, "move-edited").await?;
            let committed_rows = count(&workspace, &path)?;
            let row = json!({"file": "dawg1.jpg", "label": "dog", "min_x": 13, "min_y": 14, "width": 100, "height": 100});
            rows::add(&repo, &workspace, &path, &row)?;
            files::mv(&workspace, train, &moved).await?;
            assert_eq!(count(&workspace, &moved_path)?, committed_rows + 1);
            let moved_commit =
                workspaces::commit(&workspace, &body("Move the edited data frame"), "move-edited")
                    .await?;
            assert!(tree::get_file_by_path(&repo, &moved_commit, &path)?.is_none());
            let reader = workspaces::create(&repo, &moved_commit, "move-edited-reader", true)?;
            index(&repo, &reader, &moved_path).await?;
            assert_eq!(
                count(&reader, &moved_path)?,
                committed_rows + 1,
                "the moved edit is in the committed file"
            );

            // A directory move that fails partway moves nothing: an index move that cannot
            // complete puts back the indexes already moved and leaves the staged db untouched.
            // It needs a second data frame in the directory, so it builds on a commit of its own.
            let second = train.join("second.csv");
            write_to_path(repo.path.join(&second), "label,count\ndog,1\ncat,2\n")?;
            repositories::add(&repo, repo.path.join(&second)).await?;
            let two_frames = repositories::commit(&repo, "Add a second data frame")?;
            let workspace = workspaces::create(&repo, &two_frames, "failed-move", true)?;
            index(&repo, &workspace, &path).await?;
            index(&repo, &workspace, &second).await?;
            rows::add(&repo, &workspace, &path, &row)?;
            // Indexes move in path order, so `path` moves before `second` hits this file where
            // its index directory would go.
            let blocker = duckdb_path(&workspace, &moved.join("second.csv"))
                .parent()
                .expect("an index sits in a directory")
                .to_path_buf();
            util::fs::create_dir_all(blocker.parent().expect("the index directory has a parent"))?;
            std::fs::write(&blocker, "not a directory")?;
            let staged_snapshot = || -> Result<Vec<_>, OxenError> {
                Ok(get_staged_db_manager(&workspace.workspace_repo)?
                    .read_entries_under(Path::new(""))?
                    .into_iter()
                    .map(|(path, entry)| (path, entry.status, entry.node.hash))
                    .collect())
            };
            let before = staged_snapshot()?;
            assert!(files::mv(&workspace, train, &moved).await.is_err());
            assert_eq!(staged_snapshot()?, before);
            assert!(
                get_staged_db_manager(&workspace.workspace_repo)?
                    .read_entries_under(&moved)?
                    .is_empty()
            );
            assert!(duckdb_path(&workspace, &path).exists());
            assert!(duckdb_path(&workspace, &second).exists());
            assert!(!duckdb_path(&workspace, &moved_path).exists());
            assert_eq!(
                count(&workspace, &path)?,
                committed_rows + 1,
                "the edit stays with the index that was moved back"
            );

            Ok(())
        })
        .await
    }

    /// Data frames whose columns carry the legacy tracking names, and data frames with a JSON
    /// column, which take the JSON export and import paths a csv does not.
    #[tokio::test]
    async fn test_edit_data_frames_with_legacy_named_and_json_columns() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let diff_output = Path::new("diff_output.csv");
            let single_row = Path::new("data.jsonl");
            let single_row_array = Path::new("data.json");
            let media = Path::new("media.jsonl");
            write_to_path(
                repo.path.join(diff_output),
                "file,label,_oxen_diff_status\na.jpg,dog,added\nb.jpg,cat,removed\n",
            )?;
            write_to_path(
                repo.path.join(single_row),
                "{\"prompt\": \"hello\", \"status\": \"bootstrap\"}\n",
            )?;
            write_to_path(
                repo.path.join(single_row_array),
                "[{\"prompt\": \"hello\", \"status\": \"bootstrap\"}]\n",
            )?;
            // Object values make read_json (and the workspace index) type `error_details` as a
            // JSON column.
            write_to_path(
                repo.path.join(media),
                "{\"id\":1,\"error_details\":{\"code\":402,\"msg\":\"x\"}}\n\
                 {\"id\":2,\"error_details\":{\"code\":200,\"msg\":\"ok\"}}\n",
            )?;
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "add data frames")?;
            let version_store = repo.version_store();
            let user = UserConfig::get()?.to_user();
            let body = |message: &str| NewCommitBody {
                author: user.name.to_owned(),
                email: user.email.to_owned(),
                message: message.to_string(),
            };

            // User data containing columns named like the old tracking columns (e.g. a committed
            // `oxen diff` export, whose output includes `_oxen_diff_status`) must index and read
            // back as indexed: staleness is signalled by the marker table, never by user-visible
            // column names. The column is an ordinary one, so updates to it apply.
            let workspace = workspaces::create(&repo, &commit, "legacy-names", true)?;
            index(&repo, &workspace, diff_output).await?;
            assert!(is_indexed(&workspace, diff_output)?);
            // The user's rows, including the 'removed'-status row, which is plain data, are all
            // present and queryable.
            assert_eq!(count(&workspace, diff_output)?, 2);
            let staged = query(&workspace, diff_output, &page_opts(0, 10, None))?;
            let id = first_row_id(&staged)?;
            let json_data = json!({ DIFF_STATUS_COL: "changed" });
            rows::update(&repo, &workspace, diff_output, &id, &json_data)?;
            let row = rows::get_by_id(&workspace, diff_output, &id)?;
            let val = row.column(DIFF_STATUS_COL)?.get(0)?;
            assert_eq!(val.get_str(), Some("changed"), "the update applies");

            // Committing a deletion of a data frame's only row would export a file with no
            // columns to record: an empty jsonl file infers no schema, and a json array, which
            // DuckDB writes as `[\n\t\n]` with no rows, infers one with no columns. The commit
            // must fail instead of writing a Tabular FileNode that every subsequent read fails on
            // or sees no columns in.
            for (path, workspace_name) in [
                (single_row, "empty-jsonl"),
                (single_row_array, "empty-json"),
            ] {
                let workspace = workspaces::create(&repo, &commit, workspace_name, true)?;
                index(&repo, &workspace, path).await?;
                let staged = query(&workspace, path, &page_opts(0, 10, None))?;
                rows::delete(&repo, &workspace, path, &first_row_id(&staged)?)?;
                let result =
                    workspaces::commit(&workspace, &body("Delete all rows"), DEFAULT_BRANCH_NAME)
                        .await;
                assert!(
                    matches!(result, Err(OxenError::TabularExportMissingMetadata(_))),
                    "commit of an empty {path:?} export must fail, got {result:?}"
                );
                // The previously committed version must still be fully readable.
                let file_node = tree::get_file_by_path(&repo, &commit, path)?
                    .expect("original file node should still exist");
                assert!(
                    file_node.metadata().is_some(),
                    "original committed file node should still have tabular metadata"
                );
            }

            // The workspace-commit export round-trips a JSON-typed column through its
            // `COPY ... (FORMAT JSON)`. DuckDB 1.5.2 validates JSON on every ingestion path, so a
            // corrupt JSON-column value can't be synthesized through the public API. That
            // tolerance branch is covered by the focused tests in
            // `core::v_latest::workspaces::data_frames`.
            let appended = json!({"id": 3, "error_details": {"code": 500, "msg": "err"}});
            let workspace = workspaces::create(&repo, &commit, "json-round-trip", true)?;
            index(&repo, &workspace, media).await?;
            rows::add(&repo, &workspace, media, &appended)?;
            let committed = workspaces::commit(
                &workspace,
                &body("Commit workspace with JSON column"),
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            let entry = entries::get_commit_entry(&repo, &committed, media)?
                .expect("the data frame is committed");
            let data_frame = read_version_df(
                &version_store,
                entry.hash.parse()?,
                entry.num_bytes,
                "jsonl",
                &DFOpts::empty(),
            )
            .await?;
            assert_eq!(data_frame.height(), 3, "every row survives the commit");
            assert!(data_frame.column("error_details").is_ok());

            // Recovering a stale `.jsonl` frame exercises the JSON export/import path a csv frame
            // does not: the export wraps the JSON column and writes it through
            // `COPY ... (FORMAT JSON)`, and the re-index runs the STRUCT/JSON handling in
            // `index_file_with_id`. The staged append survives, the JSON column round-trips, and a
            // row tombstoned as 'removed' stays deleted.
            branches::create(&repo, "stale-jsonl", &commit.id)?;
            let workspace = workspaces::create(&repo, &commit, "stale-jsonl", true)?;
            index(&repo, &workspace, media).await?;
            rows::add(&repo, &workspace, media, &appended)?;

            // Downgrade to a table left by an older version of oxen: legacy tracking columns, one
            // committed row tombstoned as 'removed', no `_oxen_row_id` ordering column, and no
            // index marker.
            let db_path = duckdb_path(&workspace, media);
            with_df_db_manager(&db_path, |manager| {
                manager.with_conn(|conn| {
                    conn.execute_batch(&format!(
                        "ALTER TABLE \"{t}\" ADD COLUMN \"{status}\" VARCHAR DEFAULT 'unchanged'; \
                         ALTER TABLE \"{t}\" ADD COLUMN \"_oxen_diff_hash\" VARCHAR DEFAULT '0'; \
                         UPDATE \"{t}\" SET \"{status}\" = 'added' WHERE \"id\" = 3; \
                         UPDATE \"{t}\" SET \"{status}\" = 'removed' WHERE \"id\" = 1; \
                         ALTER TABLE \"{t}\" DROP COLUMN \"{row_id}\"; \
                         DROP TABLE \"{marker}\";",
                        t = TABLE_NAME,
                        status = DIFF_STATUS_COL,
                        row_id = OXEN_ROW_ID_COL,
                        marker = INDEX_META_TABLE,
                    ))?;
                    Ok(())
                })
            })?;
            assert!(!is_indexed(&workspace, media)?);
            assert_eq!(count(&workspace, media)?, 3);

            // Commit succeeds: the stale jsonl table is recovered from its own rows, JSON column
            // and all.
            let recovered = workspaces::commit(
                &workspace,
                &body("Committing recovered stale jsonl df"),
                "stale-jsonl",
            )
            .await?;

            // 2 committed - 1 removed + 1 appended = 2 rows, and the JSON column round-tripped
            // through the recovery.
            let entry = entries::get_commit_entry(&repo, &recovered, media)?
                .expect("committed entry should exist");
            let df = read_version_df(
                &version_store,
                entry.hash.parse()?,
                entry.num_bytes,
                "jsonl",
                &DFOpts::empty(),
            )
            .await?;
            assert_eq!(df.height(), 2, "recovered jsonl row count: {df}");
            assert!(
                df.column("error_details").is_ok(),
                "JSON column must survive recovery: {df}"
            );
            let rendered = format!("{df}");
            assert!(
                rendered.contains("500"),
                "staged append must survive recovery: {rendered}"
            );
            assert!(
                !rendered.contains("402"),
                "tombstoned row must stay deleted: {rendered}"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_queryable_lookup_skips_editable_and_unloadable_workspaces()
    -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let commit = commits::head_commit(&repo)?;
            let file_path = test::test_bounding_box_csv();

            // Nothing here is queryable, so the lookup cannot return early and has to walk past
            // every workspace: one pinned to a commit the merkle tree doesn't hold, one whose
            // pinned id isn't a hash at all. Both must be skipped, not raised.
            let dangling = workspaces::create(&repo, &commit, "dangling-df", true)?;
            let unparseable = workspaces::create(&repo, &commit, "unparseable-df", true)?;
            repoint_workspace(&dangling, &"f".repeat(32))?;
            repoint_workspace(&unparseable, "not-a-hash")?;

            assert!(!is_queryable_data_frame_indexed(
                &repo, &file_path, &commit
            )?);
            assert!(matches!(
                get_queryable_data_frame_workspace(&repo, &file_path, &commit),
                Err(OxenError::QueryableWorkspaceNotFound)
            ));

            // An editable workspace's indexed table holds staged edits, not a queryable index.
            let editable = workspaces::create(&repo, &commit, "editable-df", true)?;
            index(&repo, &editable, &file_path).await?;
            assert!(!is_queryable_data_frame_indexed(
                &repo, &file_path, &commit
            )?);

            let queryable = workspaces::create(&repo, &commit, "queryable-df", false)?;
            index(&repo, &queryable, &file_path).await?;
            assert!(is_queryable_data_frame_indexed(&repo, &file_path, &commit)?);
            let found = get_queryable_data_frame_workspace(&repo, &file_path, &commit)?;
            assert_eq!(found.id, queryable.id);

            // Point a sibling workspace at a commit the merkle tree doesn't hold, so nothing can
            // load it. The lookup still answers from the workspace holding the index.
            repoint_workspace(&editable, &"f".repeat(32))?;

            assert!(is_queryable_data_frame_indexed(&repo, &file_path, &commit)?);
            let found = get_queryable_data_frame_workspace(&repo, &file_path, &commit)?;
            assert_eq!(found.id, queryable.id);

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

    /// Values of `name` in the frame's current row order, so a page can be
    /// compared against the page before it.
    fn column_values(df: &DataFrame, name: &str) -> Result<Vec<String>, OxenError> {
        let column = df.column(name)?;
        (0..df.height())
            .map(|i| Ok(column.get(i)?.to_string().trim_matches('"').to_string()))
            .collect()
    }

    fn page_opts(page: usize, page_size: usize, sql: Option<&str>) -> DFOpts {
        let mut opts = DFOpts::empty();
        opts.page = Some(page);
        opts.page_size = Some(page_size);
        opts.sql = sql.map(str::to_string);
        opts
    }

    /// Rewrites a workspace's config to be non-editable and pinned to `commit_id`, which
    /// create-time validation would otherwise reject.
    fn repoint_workspace(workspace: &Workspace, commit_id: &str) -> Result<(), OxenError> {
        let mut config = workspaces::read_config(&workspace.dir())?
            .expect("a just-created workspace has a config");
        config.is_editable = false;
        config.workspace_commit_id = commit_id.to_string();
        workspaces::write_config(&workspace.dir(), &config)
    }
}
