use async_tempfile::TempFile;
use duckdb::Connection;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, EXCLUDE_OXEN_COLS, TABLE_NAME};
use crate::core::db::data_frames::df_db;
use crate::core::db::data_frames::df_db::with_df_db_manager;
use crate::core::staged::get_staged_db_manager;
use crate::core::v_latest::workspaces::files::{add, track_modified_data_frame};
use parking_lot::Mutex;
use sql_query_builder::Delete;
use std::collections::HashSet;
use std::sync::Arc;

use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode};
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{
    Commit, EntryDataType, LocalRepository, MerkleHash, StagedEntryStatus, Workspace,
};
use crate::repositories;
use crate::storage::version_store::VersionLocation;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};

pub mod columns;
pub mod rows;
pub mod schemas;

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    match get_queryable_data_frame_workspace(repo, path, commit) {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound => Ok(false),
            _ => Err(e),
        },
    }
}

// Annoying that we have to pass in the path and the file node here
pub fn is_queryable_data_frame_indexed_from_file_node(
    repo: &LocalRepository,
    file_node: &FileNode,
    path: &Path,
) -> Result<bool, OxenError> {
    match get_queryable_data_frame_workspace_from_file_node(repo, file_node.last_commit_id(), path)
    {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound => Ok(false),
            _ => Err(e),
        },
    }
}

pub fn get_queryable_data_frame_workspace_from_file_node(
    repo: &LocalRepository,
    commit_id: &MerkleHash,
    path: &Path,
) -> Result<Workspace, OxenError> {
    let workspaces = repositories::workspaces::list(repo)?;
    log::debug!("Looking for workspace with commit id {commit_id:?}");

    for workspace in workspaces {
        // log::debug!("is workspace editable: {:?}", workspace.is_editable);
        // log::debug!("workspace commit id: {:?}", workspace.commit.id);
        // Ensure the workspace is not editable and matches the commit ID of the resource
        if !workspace.is_editable && workspace.commit.id == commit_id.to_string() {
            // Construct the path to the DuckDB resource within the workspace
            let workspace_file_db_path =
                repositories::workspaces::data_frames::duckdb_path(&workspace, path);

            // Check if the DuckDB file exists in the workspace's directory
            if workspace_file_db_path.exists() {
                // The file exists in this non-editable workspace, and the commit IDs match
                return Ok(workspace);
            }
        }
    }

    Err(OxenError::QueryableWorkspaceNotFound)
}

pub fn get_queryable_data_frame_workspace(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    commit: &Commit,
) -> Result<Workspace, OxenError> {
    let path = path.as_ref();
    log::debug!("get_queryable_data_frame_workspace path: {path:?}");
    let file_node = repositories::tree::get_file_by_path(repo, commit, path)?
        .ok_or_else(|| OxenError::path_does_not_exist(path))?;
    if *file_node.data_type() != EntryDataType::Tabular {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }
    get_queryable_data_frame_workspace_from_file_node(repo, &commit.id.parse()?, path)
}

pub async fn index(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    // Is tabular just looks at the file extensions
    let file_node =
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?
            .ok_or_else(|| OxenError::path_does_not_exist(path))?;
    if *file_node.data_type() != EntryDataType::Tabular {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }

    log::debug!("core::v_latest::workspaces::data_frames::index({path:?})");

    let repo = &workspace.base_repo;
    let commit = &workspace.commit;

    log::debug!("core::v_latest::workspaces::data_frames::index({path:?}) got commit {commit:?}");

    let Ok(Some(commit_merkle_tree)) =
        repositories::tree::get_node_by_path_with_children(repo, commit, path)
    else {
        return Err(OxenError::basic_str(format!(
            "Merkle tree for commit {commit} not found"
        )));
    };

    let file_hash = commit_merkle_tree.hash;

    log::debug!(
        "core::v_latest::workspaces::data_frames::index({path:?}) got file hash {file_hash:?}"
    );

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let Some(parent) = db_path.parent() else {
        return Err(OxenError::basic_str(format!(
            "Failed to get parent directory for {db_path:?}"
        )));
    };
    util::fs::create_dir_all(parent)?;

    let version_store = repo.version_store();
    let hash_str = file_hash.to_string();

    // DuckDB's `read_*()` can only ingest a local filesystem path. Local stores expose the
    // on-disk version file directly (zero copy); S3 streams the object to a temp file kept alive
    // until indexing finishes (RAII cleanup on drop). The temp file is created in the workspace's
    // DuckDB directory (`parent`) rather than the OS temp dir: that directory lives on the
    // oxen-server data volume, which is sized to hold version files, whereas `/tmp` may be tiny.
    let (version_path, _temp_file) = match version_store.version_location(&hash_str).await? {
        VersionLocation::Local(path) => (path, None),
        VersionLocation::S3 { .. } => {
            let temp = TempFile::new_in(parent)
                .await
                .map_err(|e| OxenError::basic_str(format!("Failed to create temp file: {e}")))?;
            let temp_path = temp.file_path().to_path_buf();
            version_store
                .copy_version_to_path(&hash_str, &temp_path)
                .await?;
            (temp_path, Some(temp))
        }
    };

    log::debug!(
        "core::v_latest::index::workspaces::data_frames::index({path:?}) got version path: {version_path:?}"
    );

    let extension = match &commit_merkle_tree.node {
        EMerkleTreeNode::File(file_node) => file_node.extension().to_string(),
        _ => {
            return Err(OxenError::basic_str("File node is not a file node"));
        }
    };

    // DuckDB indexing is blocking file IO plus a full parse, so run it off the async runtime per
    // the sync-core / async-edge policy. The DuckDB connection lives entirely inside the closure
    // (it never crosses an `.await`), and `_temp_file` is held on the async side until the
    // blocking work finishes, so a materialized S3 temp file outlives the read.
    tokio::task::spawn_blocking(move || {
        with_df_db_manager(&db_path, |manager| {
            manager.with_conn(|conn| {
                if df_db::table_exists(conn, TABLE_NAME)? {
                    df_db::drop_table(conn, TABLE_NAME)?;
                }

                df_db::index_file_with_id(&version_path, conn, &extension)?;
                add_row_status_cols(conn)?;
                Ok(())
            })
        })
    })
    .await??;

    log::debug!("core::v_latest::index::workspaces::data_frames::index({path:?}) finished!");

    // Save the current commit id so we know if the branch has advanced
    let commit_path =
        repositories::workspaces::data_frames::previous_commit_ref_path(workspace, path);
    util::fs::atomic_write_to_path(&commit_path, commit.id.as_bytes())?;

    Ok(())
}

pub async fn rename(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    let new_path = new_path.as_ref();
    let workspace_repo = &workspace.workspace_repo;

    // Handle duckdb file operations first
    let og_db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let og_db_path_parent = og_db_path.parent().unwrap();
    let new_db_path = repositories::workspaces::data_frames::duckdb_path(workspace, new_path);
    let new_db_path_parent = new_db_path.parent().unwrap();

    // Explicitly checkpoint and then close the cached connection before copying.
    // CHECKPOINT forces DuckDB to flush its WAL into the main database file.
    // Without this, the copy could include a WAL file that references the
    // original catalog name, causing replay failures when the copy is opened.
    with_df_db_manager(&og_db_path, |manager| {
        manager.with_conn(|conn| {
            if let Err(e) = conn.execute_batch("CHECKPOINT") {
                log::warn!("rename: CHECKPOINT before copy failed for {og_db_path:?}: {e}");
            }
            Ok(())
        })
    })?;
    df_db::remove_df_db_from_cache(&og_db_path)?;

    if !new_db_path_parent.exists() {
        util::fs::create_dir_all(new_db_path_parent)?;
    }

    util::fs::copy_dir_all(og_db_path_parent, new_db_path_parent)?;

    util::fs::remove_dir_all(og_db_path_parent)?;

    // Use staged_db_manager
    let staged_db_manager = get_staged_db_manager(workspace_repo)?;
    let mut staged_entry = staged_db_manager.read_from_staged_db(path)?;

    if staged_entry.is_none() {
        let workspace_file_path = workspace.workspace_repo.path.join(new_path);

        // Export the file from the version path to the new path
        if let Some(existing_file_node) =
            repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?
        {
            let version_store = workspace.base_repo.version_store();
            let hash = existing_file_node.hash().to_string();
            version_store
                .copy_version_to_path(&hash, &workspace_file_path)
                .await?;
        }

        // Check if the new path exists in the merkle tree, if it does, it is modified
        let is_modified = repositories::tree::get_file_by_path(
            &workspace.base_repo,
            &workspace.commit,
            new_path,
        )?
        .is_some();
        log::debug!(
            "rename is_modified: {is_modified:?} workspace_file_path: {workspace_file_path:?}"
        );

        if is_modified {
            track_modified_data_frame(workspace, new_path)?;
        } else {
            add(workspace, &workspace_file_path).await?;
        }

        // Read the staged entry again after adding
        staged_entry = get_staged_db_manager(workspace_repo)?.read_from_staged_db(new_path)?;
        log::debug!("rename: staged_entry after add: {staged_entry:?}");
    }

    let mut new_staged_entry = staged_entry
        .ok_or_else(|| OxenError::basic_str(format!("rename: staged entry not found: {path:?}")))?;

    // Update the file name in the staged entry
    if let EMerkleTreeNode::File(file) = &mut new_staged_entry.node.node {
        file.set_name(new_path.to_str().unwrap());
    }

    // Set status to Added since we're moving to a new location
    new_staged_entry.status = StagedEntryStatus::Added;

    // Get the file node from the staged entry
    let file_node = new_staged_entry.node.file()?;

    let staged_db_manager = get_staged_db_manager(workspace_repo)?;
    // Add the file node at the new path using staged_db_manager
    staged_db_manager.upsert_file_node(new_path, new_staged_entry.status, &file_node)?;

    // Delete the old path entry
    staged_db_manager.delete_entry(path)?;

    // Add parent directories for the new path
    if let Some(parents) = new_path.parent() {
        let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
        for dir in parents.ancestors() {
            staged_db_manager.add_directory(dir, &seen_dirs)?;
            if dir == Path::new("") {
                break;
            }
        }
    }

    let relative_path = util::fs::path_relative_to_dir(new_path, &workspace_repo.path)?;
    Ok(relative_path)
}

fn add_row_status_cols(conn: &Connection) -> Result<(), duckdb::Error> {
    let query_status = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT '{}'",
        TABLE_NAME,
        DIFF_STATUS_COL,
        StagedRowStatus::Unchanged
    );
    conn.execute(&query_status, [])?;

    let query_hash =
        format!("ALTER TABLE \"{TABLE_NAME}\" ADD COLUMN \"{DIFF_HASH_COL}\" VARCHAR DEFAULT NULL");
    conn.execute(&query_hash, [])?;
    Ok(())
}

pub fn extract_file_node_to_working_dir(
    workspace: &Workspace,
    dir_path: &Path,
    file_node: &FileNode,
) -> Result<PathBuf, OxenError> {
    log::debug!("extract_file_node_to_working_dir dir_path: {dir_path:?} file_node: {file_node}");
    let workspace_repo = &workspace.workspace_repo;
    let path = PathBuf::from(file_node.name());

    let working_path = workspace_repo.path.join(&path);
    log::debug!("extracting file node to working dir: {working_path:?}");
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);

    // Match on the extension
    if !working_path.exists() {
        util::fs::create_dir_all(
            working_path
                .parent()
                .expect("Failed to get parent directory"),
        )?;
    }

    with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
                "\"{}\" = '{}'",
                DIFF_STATUS_COL,
                StagedRowStatus::Removed
            ));
            let res = conn.execute(&delete.to_string(), [])?;
            log::debug!("delete query result is: {res:?}");

            let projection = build_export_projection(conn, TABLE_NAME)?;
            let sql = format!("SELECT {projection} FROM '{TABLE_NAME}'");
            let query = wrap_sql_for_export(&sql, &working_path);
            log::debug!("extracting file node to working dir query: {query:?}");
            conn.execute(&query, [])?;
            Ok(())
        })
    })?;

    Ok(working_path)
}

pub const VALID_EXPORT_EXTENSIONS: [&str; 6] = ["csv", "tsv", "parquet", "jsonl", "json", "ndjson"];

pub fn is_valid_export_extension(path: &Path) -> bool {
    let extension = path
        .extension()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    VALID_EXPORT_EXTENSIONS.contains(&extension)
}

pub fn wrap_sql_for_export(sql: &str, path: &Path) -> String {
    let extension = path
        .extension()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    match extension {
        "csv" => format!(
            "COPY ({}) TO '{}' (HEADER, DELIMITER ',');",
            sql,
            path.to_string_lossy()
        ),
        "tsv" => format!(
            "COPY ({}) TO '{}' (HEADER, DELIMITER '\t');",
            sql,
            path.to_string_lossy()
        ),
        "parquet" => format!(
            "COPY ({}) TO '{}' (FORMAT PARQUET);",
            sql,
            path.to_string_lossy()
        ),
        "jsonl" | "ndjson" => format!(
            "COPY ({}) TO '{}' (FORMAT JSON);",
            sql,
            path.to_string_lossy()
        ),
        "json" => format!(
            "COPY ({}) TO '{}' (FORMAT JSON, ARRAY true);",
            sql,
            path.to_string_lossy()
        ),
        _ => sql.to_string(),
    }
}

/// Build the explicit `SELECT` projection used when exporting the staged DuckDB
/// table to the working tree, omitting the oxen-internal columns
/// ([`EXCLUDE_OXEN_COLS`]).
///
/// Columns whose DuckDB `data_type` is `JSON` or `JSON[]` are wrapped so that a
/// stored value which isn't valid JSON is preserved as a JSON string rather than
/// aborting the export:
///
/// ```sql
/// CASE WHEN "col" IS NULL OR json_valid(CAST("col" AS VARCHAR))
///      THEN "col"
///      ELSE to_json(CAST("col" AS VARCHAR)) END AS "col"
/// ```
///
/// Rows written before insert-time JSON validation was added can hold raw
/// non-JSON text in JSON-typed columns. `COPY ... (FORMAT JSON)` re-parses each
/// value and throws a "Malformed JSON" error on those, failing the whole
/// data-frame export. `json_valid` returns false (it does not throw) on invalid
/// input, so the wrap is a no-op for valid data and only rewrites
/// genuinely-corrupt values. Only the JSON writer (jsonl/ndjson/json) re-parses
/// these values; the csv/tsv/parquet export paths are unaffected, and this
/// projection is a safe identity for them.
///
/// Columns are emitted in `ordinal_position` order so the exported schema matches
/// the table.
fn build_export_projection(conn: &Connection, table_name: &str) -> Result<String, duckdb::Error> {
    let cols_query = format!(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = '{table_name}' ORDER BY ordinal_position"
    );

    let cols: Vec<(String, String)> = {
        let mut stmt = conn.prepare(&cols_query)?;
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(Result::ok)
        .collect()
    };

    let projection: Vec<String> = cols
        .into_iter()
        .filter(|(name, _)| !EXCLUDE_OXEN_COLS.contains(&name.as_str()))
        .map(|(name, data_type)| {
            if data_type == "JSON" || data_type == "JSON[]" {
                json_tolerant_export_expr(&name)
            } else {
                format!("\"{name}\"")
            }
        })
        .collect();

    Ok(projection.join(", "))
}

/// Projection expression for a single `JSON`/`JSON[]` column that tolerates a
/// stored value which isn't valid JSON. `json_valid` returns false (never throws)
/// on invalid input, so valid values pass through unchanged (the `THEN` branch)
/// and only genuinely-corrupt values are rewritten into a valid JSON string (the
/// `ELSE` branch). See [`build_export_projection`] for why corrupt values exist.
fn json_tolerant_export_expr(name: &str) -> String {
    format!(
        "CASE WHEN \"{name}\" IS NULL OR json_valid(CAST(\"{name}\" AS VARCHAR)) \
         THEN \"{name}\" ELSE to_json(CAST(\"{name}\" AS VARCHAR)) END AS \"{name}\""
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::data_frames::df_db;
    use crate::test;

    /// The export projection must wrap `JSON` and `JSON[]` columns in the
    /// invalid-JSON-tolerant `CASE` expression, project everything else as-is,
    /// drop the oxen-internal columns, and preserve `ordinal_position` order.
    #[test]
    fn test_build_export_projection_wraps_json_columns() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_dir_test(|dir| {
            let conn = df_db::get_connection(&dir.join("db"))?;
            // Column order here is the order information_schema reports.
            conn.execute(
                &format!(
                    "CREATE TABLE {TABLE_NAME} (\
                       \"name\" VARCHAR, \
                       \"meta\" JSON, \
                       \"count\" BIGINT, \
                       \"tags\" JSON[], \
                       \"{oxen_id}\" VARCHAR, \
                       \"{DIFF_STATUS_COL}\" VARCHAR, \
                       \"{DIFF_HASH_COL}\" VARCHAR)",
                    oxen_id = crate::constants::OXEN_ID_COL,
                ),
                [],
            )?;

            let projection = build_export_projection(&conn, TABLE_NAME)?;

            let expected = format!(
                "\"name\", {}, \"count\", {}",
                json_tolerant_export_expr("meta"),
                json_tolerant_export_expr("tags"),
            );
            assert_eq!(projection, expected);

            // The wrap must reference json_valid / to_json so a corrupt value
            // can't abort the COPY ... (FORMAT JSON) re-serialization.
            assert!(projection.contains("json_valid"));
            assert!(projection.contains("to_json"));
            // Oxen-internal columns are dropped, never emitted.
            assert!(!projection.contains(crate::constants::OXEN_ID_COL));
            assert!(!projection.contains(DIFF_STATUS_COL));
            assert!(!projection.contains(DIFF_HASH_COL));
            Ok(())
        })
    }

    /// The tolerant `CASE` wrap must turn a value that isn't valid JSON into a
    /// JSON string that survives `COPY ... (FORMAT JSON)`, while valid values
    /// pass through untouched. DuckDB 1.5.2 validates JSON on every ingestion
    /// path (insert, cast, `read_json`, `read_parquet`), so the corrupt
    /// JSON-column state this guards against can only originate from data
    /// written by older engines and can't be synthesized here. We instead
    /// exercise the exact projection SQL: `CAST(json_col AS VARCHAR)` yields the
    /// raw stored bytes, so feeding the wrap a `VARCHAR` reproduces the runtime
    /// behavior on those bytes.
    #[test]
    fn test_json_tolerant_export_neutralizes_invalid_json() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_dir_test(|dir| {
            let conn = df_db::get_connection(&dir.join("db"))?;
            // 'Insufficient credits' mimics a legacy value: a plain string
            // stored in a JSON-typed column; '{"a":1}' is genuinely valid
            // JSON; NULL takes the IS NULL guard.
            conn.execute("CREATE TABLE t (v VARCHAR, ord INTEGER)", [])?;
            conn.execute(
                "INSERT INTO t VALUES ('Insufficient credits', 1), ('{\"a\":1}', 2), (NULL, 3)",
                [],
            )?;

            let expr = json_tolerant_export_expr("v");
            let out = dir.join("out.jsonl");
            let query = wrap_sql_for_export(&format!("SELECT {expr} FROM t ORDER BY ord"), &out);

            // Must NOT raise "Malformed JSON" the way a bare `SELECT v` would on
            // a corrupt JSON column.
            conn.execute(&query, [])?;

            // Bad value is emitted as a valid JSON string, not raw text.
            let content = std::fs::read_to_string(&out)?;
            assert!(
                content.contains("{\"v\":\"Insufficient credits\"}"),
                "expected the invalid value to be wrapped as a JSON string, got:\n{content}"
            );
            assert!(
                content.contains("{\"v\":{\"a\":1}}"),
                "expected valid JSON to pass through unchanged, got:\n{content}"
            );

            // Output round-trips: read_json re-parses it without error.
            let count: i64 = conn.query_row(
                &format!(
                    "SELECT count(*) FROM read_json('{}')",
                    out.to_string_lossy()
                ),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 3);
            Ok(())
        })
    }

    /// Runs the export projection against a real `JSON[]` column to confirm the
    /// array round-trips — its CASE unifies a `JSON[]` (`THEN`) with a scalar
    /// `JSON` (`ELSE`), which only surfaces when the projection actually executes.
    #[test]
    fn test_export_executes_on_json_array_column() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_dir_test(|dir| {
            let conn = df_db::get_connection(&dir.join("db"))?;
            conn.execute(
                &format!("CREATE TABLE {TABLE_NAME} (name VARCHAR, tags JSON[])"),
                [],
            )?;
            conn.execute(
                &format!(
                    "INSERT INTO {TABLE_NAME} VALUES ('a', ['{{\"k\":1}}', '[2,3]']), ('b', NULL)"
                ),
                [],
            )?;

            let projection = build_export_projection(&conn, TABLE_NAME)?;
            assert_eq!(
                projection,
                format!("\"name\", {}", json_tolerant_export_expr("tags"))
            );

            let out = dir.join("out.jsonl");
            let query = wrap_sql_for_export(
                &format!("SELECT {projection} FROM {TABLE_NAME} ORDER BY name"),
                &out,
            );
            // Must type-unify (JSON[] THEN vs scalar JSON ELSE) and execute.
            conn.execute(&query, [])?;

            // The valid array round-trips as a JSON array, not a stringified blob.
            let content = std::fs::read_to_string(&out)?;
            assert!(
                content.contains("{\"name\":\"a\",\"tags\":[{\"k\":1},[2,3]]}"),
                "expected the JSON[] value to round-trip as an array, got:\n{content}"
            );

            let count: i64 = conn.query_row(
                &format!(
                    "SELECT count(*) FROM read_json('{}')",
                    out.to_string_lossy()
                ),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 2);
            Ok(())
        })
    }
}
