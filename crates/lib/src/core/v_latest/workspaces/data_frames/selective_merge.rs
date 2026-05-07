use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use polars::frame::DataFrame;
use polars::prelude::AnyValue;
use sql_query_builder::Select;

use crate::constants::{DIFF_STATUS_COL, EXCLUDE_OXEN_COLS, OXEN_ROW_ID_COL, TABLE_NAME};
use crate::core;
use crate::core::db::data_frames::df_db::{self, with_df_db_manager};
use crate::core::df::tabular;
use crate::core::staged::get_staged_db_manager;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode};
use crate::model::{Commit, LocalRepository, NewCommitBody, Workspace};
use crate::opts::DFOpts;
use crate::repositories;
use crate::view::JsonDataFrameView;
use crate::view::data_frames::{
    RowMergeOp, RowSelection, SelectiveMergeTarget, SelectiveRowMergeRequest,
    SelectiveRowMergeWithAssetsRequest, SyncFromBranchRequest,
};

const SELECTIVE_MERGE_AUTHOR: &str = "Oxen Selective Merge";
const SELECTIVE_MERGE_EMAIL: &str = "selective-merge@oxen.ai";

struct PreparedSelection {
    op: RowMergeOp,
    values: Option<serde_json::Value>,
    target_row_id: Option<String>,
}

pub async fn selective_merge(
    repo: &LocalRepository,
    source_workspace: &Workspace,
    request: &SelectiveRowMergeRequest,
) -> Result<Commit, OxenError> {
    if request.selections.is_empty() {
        return Err(OxenError::basic_str("no selections provided"));
    }

    if !repositories::workspaces::data_frames::is_indexed(source_workspace, &request.source_path)? {
        return Err(OxenError::basic_str(format!(
            "source dataframe {:?} is not indexed in workspace {}",
            request.source_path, source_workspace.id,
        )));
    }

    let author = request
        .commit_author
        .clone()
        .unwrap_or_else(|| SELECTIVE_MERGE_AUTHOR.to_string());
    let email = request
        .commit_email
        .clone()
        .unwrap_or_else(|| SELECTIVE_MERGE_EMAIL.to_string());

    match &request.target {
        SelectiveMergeTarget::Branch { name, path } => {
            let branch = repositories::branches::get_by_name(repo, name)?;
            let commit =
                repositories::commits::get_by_id(repo, &branch.commit_id)?.ok_or_else(|| {
                    OxenError::basic_str(format!("branch {name} HEAD commit not found"))
                })?;
            let temporary = repositories::workspaces::create_temporary(repo, &commit).await?;
            apply_and_commit(
                repo,
                source_workspace,
                &request.source_path,
                &temporary,
                path,
                name,
                &request.selections,
                &request.message,
                &author,
                &email,
            )
            .await
        }
        SelectiveMergeTarget::Workspace {
            workspace_id,
            path,
            branch,
        } => {
            let target = repositories::workspaces::get(repo, workspace_id)?.ok_or_else(|| {
                OxenError::basic_str(format!("target workspace {workspace_id} not found"))
            })?;
            apply_and_commit(
                repo,
                source_workspace,
                &request.source_path,
                &target,
                path,
                branch,
                &request.selections,
                &request.message,
                &author,
                &email,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_and_commit(
    repo: &LocalRepository,
    source_workspace: &Workspace,
    source_path: &Path,
    target_workspace: &Workspace,
    target_path: &Path,
    branch_name: &str,
    selections: &[RowSelection],
    message: &str,
    author: &str,
    email: &str,
) -> Result<Commit, OxenError> {
    if !repositories::workspaces::data_frames::is_indexed(target_workspace, target_path)? {
        repositories::workspaces::data_frames::index(repo, target_workspace, target_path).await?;
    }

    let prepared = prepare_selections(
        source_workspace,
        source_path,
        target_workspace,
        target_path,
        selections,
    )?;

    for sel in &prepared {
        apply_selection(repo, target_workspace, target_path, sel)?;
    }

    let body = NewCommitBody {
        message: message.to_string(),
        author: author.to_string(),
        email: email.to_string(),
    };
    repositories::workspaces::commit(target_workspace, &body, branch_name).await
}

fn prepare_selections(
    source_workspace: &Workspace,
    source_path: &Path,
    target_workspace: &Workspace,
    target_path: &Path,
    selections: &[RowSelection],
) -> Result<Vec<PreparedSelection>, OxenError> {
    let mut prepared = Vec::with_capacity(selections.len());
    for sel in selections {
        let source_row = repositories::workspaces::data_frames::rows::get_by_id(
            source_workspace,
            source_path,
            &sel.row_id,
        )?;
        if source_row.height() == 0 {
            return Err(OxenError::basic_str(format!(
                "source row_id {} not found in workspace {}",
                sel.row_id, source_workspace.id,
            )));
        }
        match sel.op {
            RowMergeOp::Add => {
                let values = row_values_excluding_oxen_cols(source_row)?;
                prepared.push(PreparedSelection {
                    op: sel.op,
                    values: Some(values),
                    target_row_id: None,
                });
            }
            RowMergeOp::Modify => {
                let target_row_id =
                    lookup_target_row_id_by_position(target_workspace, target_path, &source_row)?;
                let values = row_values_excluding_oxen_cols(source_row)?;
                prepared.push(PreparedSelection {
                    op: sel.op,
                    values: Some(values),
                    target_row_id: Some(target_row_id),
                });
            }
            RowMergeOp::Delete => {
                let target_row_id =
                    lookup_target_row_id_by_position(target_workspace, target_path, &source_row)?;
                prepared.push(PreparedSelection {
                    op: sel.op,
                    values: None,
                    target_row_id: Some(target_row_id),
                });
            }
        }
    }
    Ok(prepared)
}

fn lookup_target_row_id_by_position(
    target_workspace: &Workspace,
    target_path: &Path,
    source_row: &DataFrame,
) -> Result<String, OxenError> {
    let row_idx = repositories::workspaces::data_frames::rows::get_row_idx(source_row)?
        .ok_or_else(|| OxenError::basic_str("source row missing _oxen_row_id"))?;
    let target_db =
        repositories::workspaces::data_frames::duckdb_path(target_workspace, target_path);
    let target_row = with_df_db_manager(&target_db, |manager| {
        manager.with_conn(|conn| {
            let query = Select::new()
                .select("*")
                .from(TABLE_NAME)
                .where_clause(&format!("{OXEN_ROW_ID_COL} = {row_idx}"));
            df_db::select(conn, &query, None)
        })
    })?;
    if target_row.height() == 0 {
        return Err(OxenError::basic_str(format!(
            "no row with _oxen_row_id {row_idx} in target dataframe at {target_path:?}",
        )));
    }
    repositories::workspaces::data_frames::rows::get_row_id(&target_row)?
        .ok_or_else(|| OxenError::basic_str("target row missing _oxen_id"))
}

fn row_values_excluding_oxen_cols(row: DataFrame) -> Result<serde_json::Value, OxenError> {
    let mut row = row;
    for col in EXCLUDE_OXEN_COLS {
        if row.get_column_names().iter().any(|n| n.as_str() == col) {
            let _ = row.drop_in_place(col);
        }
    }
    let json = JsonDataFrameView::json_from_df(&mut row);
    if let serde_json::Value::Array(arr) = &json
        && let Some(first) = arr.first()
    {
        return Ok(first.clone());
    }
    Err(OxenError::basic_str("source row produced no json data"))
}

fn apply_selection(
    repo: &LocalRepository,
    target_workspace: &Workspace,
    target_path: &Path,
    sel: &PreparedSelection,
) -> Result<(), OxenError> {
    match sel.op {
        RowMergeOp::Add => {
            let values = sel
                .values
                .as_ref()
                .ok_or_else(|| OxenError::basic_str("internal: Add missing values"))?;
            repositories::workspaces::data_frames::rows::add(
                repo,
                target_workspace,
                target_path,
                values,
            )?;
        }
        RowMergeOp::Modify => {
            let values = sel
                .values
                .as_ref()
                .ok_or_else(|| OxenError::basic_str("internal: Modify missing values"))?;
            let target_id = sel
                .target_row_id
                .as_ref()
                .ok_or_else(|| OxenError::basic_str("internal: Modify missing target_row_id"))?;
            repositories::workspaces::data_frames::rows::update(
                repo,
                target_workspace,
                target_path,
                target_id,
                values,
            )?;
        }
        RowMergeOp::Delete => {
            let target_id = sel
                .target_row_id
                .as_ref()
                .ok_or_else(|| OxenError::basic_str("internal: Delete missing target_row_id"))?;
            repositories::workspaces::data_frames::rows::delete(
                repo,
                target_workspace,
                target_path,
                target_id,
            )?;
        }
    }
    Ok(())
}

//
// Selective merge with assets — same as `selective_merge` but also copies any
// binary assets the selected rows reference into the same commit.
//

struct AssetCopy {
    target_path: PathBuf,
    hash: String,
    version_path: PathBuf,
}

pub async fn selective_merge_with_assets(
    repo: &LocalRepository,
    source_workspace: &Workspace,
    request: &SelectiveRowMergeWithAssetsRequest,
) -> Result<Commit, OxenError> {
    if request.selections.is_empty() {
        return Err(OxenError::basic_str("no selections provided"));
    }
    if !repositories::workspaces::data_frames::is_indexed(source_workspace, &request.source_path)? {
        return Err(OxenError::basic_str(format!(
            "source dataframe {:?} is not indexed in workspace {}",
            request.source_path, source_workspace.id,
        )));
    }

    let author = request
        .commit_author
        .clone()
        .unwrap_or_else(|| SELECTIVE_MERGE_AUTHOR.to_string());
    let email = request
        .commit_email
        .clone()
        .unwrap_or_else(|| SELECTIVE_MERGE_EMAIL.to_string());

    match &request.target {
        SelectiveMergeTarget::Branch { name, path } => {
            let branch = repositories::branches::get_by_name(repo, name)?;
            let commit =
                repositories::commits::get_by_id(repo, &branch.commit_id)?.ok_or_else(|| {
                    OxenError::basic_str(format!("branch {name} HEAD commit not found"))
                })?;
            let temporary = repositories::workspaces::create_temporary(repo, &commit).await?;
            apply_with_assets_and_commit(
                repo,
                source_workspace,
                &request.source_path,
                &temporary,
                path,
                name,
                &request.selections,
                &request.asset_columns,
                request.request_data_column.as_deref(),
                &request.message,
                &author,
                &email,
            )
            .await
        }
        SelectiveMergeTarget::Workspace {
            workspace_id,
            path,
            branch,
        } => {
            let target = repositories::workspaces::get(repo, workspace_id)?.ok_or_else(|| {
                OxenError::basic_str(format!("target workspace {workspace_id} not found"))
            })?;
            apply_with_assets_and_commit(
                repo,
                source_workspace,
                &request.source_path,
                &target,
                path,
                branch,
                &request.selections,
                &request.asset_columns,
                request.request_data_column.as_deref(),
                &request.message,
                &author,
                &email,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_with_assets_and_commit(
    repo: &LocalRepository,
    source_workspace: &Workspace,
    source_path: &Path,
    target_workspace: &Workspace,
    target_path: &Path,
    branch_name: &str,
    selections: &[RowSelection],
    asset_columns: &[String],
    request_data_column: Option<&str>,
    message: &str,
    author: &str,
    email: &str,
) -> Result<Commit, OxenError> {
    if !repositories::workspaces::data_frames::is_indexed(target_workspace, target_path)? {
        repositories::workspaces::data_frames::index(repo, target_workspace, target_path).await?;
    }

    // Validate row selections (and resolve target row ids for modify/delete)
    let prepared = prepare_selections(
        source_workspace,
        source_path,
        target_workspace,
        target_path,
        selections,
    )?;

    // Collect every asset path referenced by the Add selections (deduped, deterministic order)
    let mut asset_paths: BTreeSet<PathBuf> = BTreeSet::new();
    for sel in selections {
        if !matches!(sel.op, RowMergeOp::Add) {
            continue;
        }
        let source_row = repositories::workspaces::data_frames::rows::get_by_id(
            source_workspace,
            source_path,
            &sel.row_id,
        )?;
        let row_paths = extract_asset_paths_from_row(
            &source_row,
            asset_columns,
            request_data_column,
            &source_workspace.id,
        )?;
        for p in row_paths {
            asset_paths.insert(p);
        }
    }

    // Validate every asset path resolves in source + decide skip/error/stage on target
    let plan = build_asset_plan(repo, source_workspace, target_workspace, &asset_paths).await?;

    // Stage assets first — once row ops run they can also stage the dataframe file,
    // so order doesn't strictly matter, but doing assets first keeps intent obvious.
    for asset in &plan {
        core::v_latest::workspaces::files::add_version_file(
            target_workspace,
            &asset.version_path,
            &asset.target_path,
            &asset.hash,
            false,
        )
        .await?;
    }

    // Apply row ops (uses the existing helper from selective_merge)
    for sel in &prepared {
        apply_selection(repo, target_workspace, target_path, sel)?;
    }

    let body = NewCommitBody {
        message: message.to_string(),
        author: author.to_string(),
        email: email.to_string(),
    };
    repositories::workspaces::commit(target_workspace, &body, branch_name).await
}

async fn build_asset_plan(
    repo: &LocalRepository,
    source_workspace: &Workspace,
    target_workspace: &Workspace,
    asset_paths: &BTreeSet<PathBuf>,
) -> Result<Vec<AssetCopy>, OxenError> {
    let version_store = repo.version_store()?;
    let mut plan = Vec::new();
    for path in asset_paths {
        let source_node = find_workspace_file_node(source_workspace, path)?.ok_or_else(|| {
            OxenError::AssetNotFoundInSource(path.to_string_lossy().into_owned().into())
        })?;
        let source_hash = source_node.hash().to_string();

        if let Some(target_node) = find_workspace_file_node(target_workspace, path)? {
            let target_hash = target_node.hash().to_string();
            if target_hash == source_hash {
                continue;
            }
            return Err(OxenError::AssetConflictOnTarget(
                path.to_string_lossy().into_owned().into(),
            ));
        }

        let version_path = version_store
            .get_version_path(&source_hash)
            .await?
            .to_pathbuf();
        plan.push(AssetCopy {
            target_path: path.clone(),
            hash: source_hash,
            version_path,
        });
    }
    Ok(plan)
}

fn find_workspace_file_node(
    workspace: &Workspace,
    path: &Path,
) -> Result<Option<FileNode>, OxenError> {
    let staged_db = get_staged_db_manager(&workspace.workspace_repo)?;
    if let Some(staged_node) = staged_db.read_from_staged_db(path)?
        && let EMerkleTreeNode::File(f) = staged_node.node.node
    {
        return Ok(Some(f));
    }
    repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)
}

fn extract_asset_paths_from_row(
    row: &DataFrame,
    asset_columns: &[String],
    request_data_column: Option<&str>,
    source_workspace_id: &str,
) -> Result<Vec<PathBuf>, OxenError> {
    let mut row = row.clone();
    let json = JsonDataFrameView::json_from_df(&mut row);
    let row_obj = match &json {
        serde_json::Value::Array(arr) if !arr.is_empty() => arr[0].clone(),
        _ => return Err(OxenError::basic_str("source row produced no json data")),
    };

    let mut paths = Vec::new();

    for col in asset_columns {
        if let Some(serde_json::Value::String(s)) = row_obj.get(col)
            && !s.is_empty()
            && let Some(p) = clean_asset_column_value(s)
        {
            paths.push(p);
        }
    }

    if let Some(col) = request_data_column
        && let Some(value) = row_obj.get(col)
    {
        // request_data is typically stored as a JSON-encoded string, but be permissive
        // if it's already a parsed object/array.
        let parsed = match value {
            serde_json::Value::String(s) if !s.is_empty() => {
                serde_json::from_str::<serde_json::Value>(s).ok()
            }
            serde_json::Value::String(_) | serde_json::Value::Null => None,
            _ => Some(value.clone()),
        };
        if let Some(p) = parsed {
            walk_for_workspace_paths(&p, source_workspace_id, &mut paths);
        }
    }

    Ok(paths)
}

fn walk_for_workspace_paths(
    value: &serde_json::Value,
    source_workspace_id: &str,
    out: &mut Vec<PathBuf>,
) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(p) = extract_workspace_url_path(s, source_workspace_id) {
                out.push(p);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                walk_for_workspace_paths(v, source_workspace_id, out);
            }
        }
        serde_json::Value::Object(obj) => {
            for v in obj.values() {
                walk_for_workspace_paths(v, source_workspace_id, out);
            }
        }
        _ => {}
    }
}

/// Returns the workspace-relative path encoded in `s` if `s` looks like a URL
/// pointing at this source workspace's `/files/` route. External URLs and
/// non-matching strings return `None`.
fn extract_workspace_url_path(s: &str, source_workspace_id: &str) -> Option<PathBuf> {
    let marker = format!("/workspaces/{source_workspace_id}/files/");
    let idx = s.find(&marker)?;
    let after = &s[idx + marker.len()..];
    let path = after.split('?').next().unwrap_or(after);
    let path = path.split('#').next().unwrap_or(path);
    if path.is_empty() {
        return None;
    }
    Some(PathBuf::from(path))
}

/// Normalize a value pulled from one of the `asset_columns`. The contract is
/// that the column holds a clean relative path, but we tolerate values that
/// arrive with a query string or fragment — strip them and return the path.
fn clean_asset_column_value(s: &str) -> Option<PathBuf> {
    let stripped = s.split('?').next().unwrap_or(s);
    let stripped = stripped.split('#').next().unwrap_or(stripped);
    if stripped.is_empty() {
        return None;
    }
    Some(PathBuf::from(stripped))
}

//
// sync_from_branch — append-only pull of a committed branch's rows + assets
// into a target workspace. Same machinery as selective_merge_with_assets, but
// the source is a branch (not a workspace) and the row delta is computed by
// the server.
//

pub struct SyncResult {
    /// `None` when the workspace was already up to date with the source branch.
    pub commit: Option<Commit>,
    pub rows_added: usize,
    pub assets_added: usize,
}

pub async fn sync_from_branch(
    repo: &LocalRepository,
    target_workspace: &Workspace,
    request: &SyncFromBranchRequest,
) -> Result<SyncResult, OxenError> {
    // Resolve source branch + source path
    let source_branch = repositories::branches::get_by_name(repo, &request.source.branch)?;
    let source_commit = repositories::commits::get_by_id(repo, &source_branch.commit_id)?
        .ok_or_else(|| {
            OxenError::basic_str(format!(
                "source branch {} HEAD commit not found",
                request.source.branch
            ))
        })?;
    let source_file_node =
        repositories::tree::get_file_by_path(repo, &source_commit, &request.source.path)?
            .ok_or_else(|| {
                OxenError::SourcePathNotFound(
                    request.source.path.to_string_lossy().into_owned().into(),
                )
            })?;

    // Resolve target branch + target path
    let target_branch = repositories::branches::get_by_name(repo, &request.target_branch)?;
    let target_branch_commit = repositories::commits::get_by_id(repo, &target_branch.commit_id)?
        .ok_or_else(|| {
            OxenError::basic_str(format!(
                "target branch {} HEAD commit not found",
                request.target_branch
            ))
        })?;
    let target_branch_file =
        repositories::tree::get_file_by_path(repo, &target_branch_commit, &request.target_path)?
            .ok_or_else(|| {
                OxenError::TargetPathNotFound(
                    request.target_path.to_string_lossy().into_owned().into(),
                )
            })?;

    // Read the source dataframe rows. JSONL is read line-by-line (no Polars
    // schema inference, which would otherwise cap at infer_schema_length and
    // can drop columns). Other formats go through Polars + json_from_df.
    let version_store = repo.version_store()?;
    let source_version_path = version_store
        .get_version_path(&source_file_node.hash().to_string())
        .await?
        .to_pathbuf();
    let source_rows = read_rows_as_json(
        source_version_path.clone(),
        source_file_node.extension().to_string(),
    )
    .await?;

    log::debug!(
        "sync_from_branch source {} rows read; first-row keys: {:?}",
        source_rows.len(),
        source_rows
            .first()
            .and_then(|v| v.as_object())
            .map(|o| o.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
    );

    // Verify the source side has the uuid column. Emit a structured error so
    // callers can see which dataframe was missing it and what schema we saw.
    if !source_rows.is_empty() && !rows_have_key(&source_rows, &request.uuid_column) {
        return Err(OxenError::UuidColumnMissing {
            side: "source_branch".to_string(),
            schema: collect_observed_keys(&source_rows),
        });
    }

    // Build the set of uuids already accounted for: target_branch HEAD's rows
    // plus any rows the user has staged-added in the target workspace. Skipping
    // both classes keeps the sync append-only and preserves the user's staged
    // pending state.
    let target_uuids = collect_target_view_uuids(
        repo,
        target_workspace,
        &request.target_path,
        &target_branch_file,
        &request.uuid_column,
    )
    .await?;

    let mut delta_rows: Vec<serde_json::Value> = Vec::new();
    let mut asset_paths: BTreeSet<PathBuf> = BTreeSet::new();
    for row in &source_rows {
        let row_uuid = match row.get(&request.uuid_column).and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => continue,
        };
        if target_uuids.contains(&row_uuid) {
            continue;
        }

        // asset_columns
        for col in &request.asset_columns {
            if let Some(serde_json::Value::String(s)) = row.get(col)
                && !s.is_empty()
                && let Some(p) = clean_asset_column_value(s)
            {
                asset_paths.insert(p);
            }
        }

        // request_data_column — wildcard workspace URL match (we don't know the
        // committing workspace's id, just that the URL points at *some* workspace's
        // /files/ route)
        if let Some(col) = &request.request_data_column
            && let Some(value) = row.get(col)
        {
            let parsed = match value {
                serde_json::Value::String(s) if !s.is_empty() => {
                    serde_json::from_str::<serde_json::Value>(s).ok()
                }
                serde_json::Value::String(_) | serde_json::Value::Null => None,
                _ => Some(value.clone()),
            };
            if let Some(p) = parsed {
                walk_for_workspace_paths_wildcard(&p, &mut asset_paths);
            }
        }

        delta_rows.push(row.clone());
    }

    if delta_rows.is_empty() {
        return Ok(SyncResult {
            commit: None,
            rows_added: 0,
            assets_added: 0,
        });
    }

    // Stage everything inside a temporary workspace at target_branch HEAD so
    // the user's workspace stays untouched until we re-point its base commit.
    let temporary = repositories::workspaces::create_temporary(repo, &target_branch_commit).await?;

    // Validate + plan asset copies: source branch tree → temp workspace at
    // target_branch HEAD.
    let plan = build_asset_plan_from_branch(repo, &source_commit, &temporary, &asset_paths).await?;

    // Index target_path in the temp workspace before adding rows.
    if !repositories::workspaces::data_frames::is_indexed(&temporary, &request.target_path)? {
        repositories::workspaces::data_frames::index(repo, &temporary, &request.target_path)
            .await?;
    }

    // Stage assets first.
    for asset in &plan {
        core::v_latest::workspaces::files::add_version_file(
            &temporary,
            &asset.version_path,
            &asset.target_path,
            &asset.hash,
            false,
        )
        .await?;
    }

    // Append delta rows.
    for row in &delta_rows {
        repositories::workspaces::data_frames::rows::add(
            repo,
            &temporary,
            &request.target_path,
            row,
        )?;
    }

    // Commit.
    let author = request
        .commit_author
        .clone()
        .unwrap_or_else(|| SELECTIVE_MERGE_AUTHOR.to_string());
    let email = request
        .commit_email
        .clone()
        .unwrap_or_else(|| SELECTIVE_MERGE_EMAIL.to_string());
    let body = NewCommitBody {
        message: request.message.clone(),
        author,
        email,
    };
    let commit =
        repositories::workspaces::commit(&temporary, &body, &request.target_branch).await?;

    // Re-point the user's workspace base commit. Their staged content (files
    // in staged_db, any rows in DuckDB) stays where it is.
    repositories::workspaces::update_commit(target_workspace, &commit.id)?;

    Ok(SyncResult {
        commit: Some(commit),
        rows_added: delta_rows.len(),
        assets_added: plan.len(),
    })
}

async fn collect_target_view_uuids(
    repo: &LocalRepository,
    target_workspace: &Workspace,
    target_path: &Path,
    target_branch_file: &FileNode,
    uuid_column: &str,
) -> Result<HashSet<String>, OxenError> {
    let version_store = repo.version_store()?;

    // (1) uuids on the target branch's HEAD version of the file.
    let branch_version_path = version_store
        .get_version_path(&target_branch_file.hash().to_string())
        .await?
        .to_pathbuf();
    let branch_rows = read_rows_as_json(
        branch_version_path.clone(),
        target_branch_file.extension().to_string(),
    )
    .await?;

    log::debug!(
        "sync_from_branch target_branch read {} rows; first-row keys: {:?}",
        branch_rows.len(),
        branch_rows
            .first()
            .and_then(|v| v.as_object())
            .map(|o| o.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
    );

    if !branch_rows.is_empty() && !rows_have_key(&branch_rows, uuid_column) {
        return Err(OxenError::UuidColumnMissing {
            side: "target_branch".to_string(),
            schema: collect_observed_keys(&branch_rows),
        });
    }

    let mut uuids: HashSet<String> = branch_rows
        .iter()
        .filter_map(|row| {
            row.get(uuid_column)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
        .collect();

    // (2) uuids the user has staged-added in the target workspace (only relevant
    // if the dataframe is indexed there — staging rows requires indexing first).
    if repositories::workspaces::data_frames::is_indexed(target_workspace, target_path)? {
        let db_path =
            repositories::workspaces::data_frames::duckdb_path(target_workspace, target_path);

        let columns = duckdb_table_columns(&db_path, TABLE_NAME)?;
        log::debug!(
            "sync_from_branch target_workspace_staged DuckDB columns: {:?}",
            columns
        );
        if !columns.iter().any(|c| c == uuid_column) {
            return Err(OxenError::UuidColumnMissing {
                side: "target_workspace_staged".to_string(),
                schema: columns,
            });
        }

        let staged_df = with_df_db_manager(&db_path, |manager| {
            manager.with_conn(|conn| {
                let query = Select::new()
                    .select(&format!("\"{uuid_column}\""))
                    .from(TABLE_NAME)
                    .where_clause(&format!("\"{DIFF_STATUS_COL}\" = 'added'"));
                df_db::select(conn, &query, None)
            })
        })?;
        for v in collect_string_column_values(&staged_df, uuid_column) {
            uuids.insert(v);
        }
    }

    Ok(uuids)
}

/// Read tabular rows as a vec of JSON values without going through Polars's
/// JSONL schema inference (which caps at `infer_schema_length` and can drop
/// columns that appear later in the file or whose types vary). For
/// `jsonl`/`ndjson` we parse line-by-line; for other formats we still go
/// through Polars (CSV/Parquet have explicit schemas and round-trip cleanly).
async fn read_rows_as_json(
    path: PathBuf,
    extension: String,
) -> Result<Vec<serde_json::Value>, OxenError> {
    match extension.as_str() {
        "jsonl" | "ndjson" => {
            tokio::task::spawn_blocking(move || -> Result<Vec<serde_json::Value>, OxenError> {
                use std::io::BufRead;
                let file = std::fs::File::open(&path)
                    .map_err(|e| OxenError::basic_str(format!("Could not open {path:?}: {e}")))?;
                let reader = std::io::BufReader::new(file);
                let mut rows = Vec::new();
                for (idx, line) in reader.lines().enumerate() {
                    let line = line.map_err(|e| {
                        OxenError::basic_str(format!("read error at line {}: {e}", idx + 1))
                    })?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let value: serde_json::Value = serde_json::from_str(&line).map_err(|e| {
                        OxenError::basic_str(format!("failed to parse JSONL line {}: {e}", idx + 1))
                    })?;
                    rows.push(value);
                }
                Ok(rows)
            })
            .await
            .map_err(|e| OxenError::basic_str(format!("JSONL read task panicked: {e}")))?
        }
        _ => {
            let df = tabular::read_df_with_extension(path, &extension, &DFOpts::empty()).await?;
            let mut clone = df.clone();
            let json = JsonDataFrameView::json_from_df(&mut clone);
            match json {
                serde_json::Value::Array(arr) => Ok(arr),
                _ => Err(OxenError::basic_str(
                    "dataframe produced no json rows array",
                )),
            }
        }
    }
}

fn rows_have_key(rows: &[serde_json::Value], key: &str) -> bool {
    rows.iter()
        .any(|row| row.get(key).map(|v| !v.is_null()).unwrap_or(false))
}

fn collect_observed_keys(rows: &[serde_json::Value]) -> Vec<String> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for k in obj.keys() {
                keys.insert(k.clone());
            }
        }
    }
    keys.into_iter().collect()
}

fn duckdb_table_columns(db_path: &Path, table_name: &str) -> Result<Vec<String>, OxenError> {
    with_df_db_manager(db_path, |manager| {
        manager.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!("PRAGMA table_info('{table_name}')"))?;
            let mut rows = stmt.query([])?;
            let mut cols = Vec::new();
            while let Some(row) = rows.next()? {
                let name: String = row.get(1)?;
                cols.push(name);
            }
            Ok(cols)
        })
    })
}

fn collect_string_column_values(df: &DataFrame, col_name: &str) -> HashSet<String> {
    let mut values = HashSet::new();
    let Ok(col) = df.column(col_name) else {
        return values;
    };
    for i in 0..col.len() {
        if let Ok(any) = col.get(i) {
            match any {
                AnyValue::String(s) => {
                    values.insert(s.to_string());
                }
                AnyValue::StringOwned(s) => {
                    values.insert(s.to_string());
                }
                _ => {}
            }
        }
    }
    values
}

async fn build_asset_plan_from_branch(
    repo: &LocalRepository,
    source_commit: &Commit,
    target_workspace: &Workspace,
    asset_paths: &BTreeSet<PathBuf>,
) -> Result<Vec<AssetCopy>, OxenError> {
    let version_store = repo.version_store()?;
    let mut plan = Vec::new();
    for path in asset_paths {
        let source_node = repositories::tree::get_file_by_path(repo, source_commit, path)?
            .ok_or_else(|| {
                OxenError::AssetNotFoundInSource(path.to_string_lossy().into_owned().into())
            })?;
        let source_hash = source_node.hash().to_string();

        if let Some(target_node) = find_workspace_file_node(target_workspace, path)? {
            let target_hash = target_node.hash().to_string();
            if target_hash == source_hash {
                continue;
            }
            return Err(OxenError::AssetConflictOnTarget(
                path.to_string_lossy().into_owned().into(),
            ));
        }

        let version_path = version_store
            .get_version_path(&source_hash)
            .await?
            .to_pathbuf();
        plan.push(AssetCopy {
            target_path: path.clone(),
            hash: source_hash,
            version_path,
        });
    }
    Ok(plan)
}

/// Like [`extract_workspace_url_path`] but doesn't require knowing the source
/// workspace id in advance. Used by `sync_from_branch`, where the URLs in
/// committed `request_data` blobs point at whichever workspace originally
/// staged them — their id is no longer relevant; we just want the relative
/// path on the right side of `/files/`.
fn extract_workspace_url_path_wildcard(s: &str) -> Option<PathBuf> {
    let needle = "/workspaces/";
    let idx = s.find(needle)?;
    let after_marker = &s[idx + needle.len()..];
    let slash_after_id = after_marker.find('/')?;
    let id = &after_marker[..slash_after_id];
    if id.is_empty() {
        return None;
    }
    let after_id = &after_marker[slash_after_id..];
    let files_marker = "/files/";
    if !after_id.starts_with(files_marker) {
        return None;
    }
    let rel = &after_id[files_marker.len()..];
    let rel = rel.split('?').next().unwrap_or(rel);
    let rel = rel.split('#').next().unwrap_or(rel);
    if rel.is_empty() {
        return None;
    }
    Some(PathBuf::from(rel))
}

fn walk_for_workspace_paths_wildcard(value: &serde_json::Value, out: &mut BTreeSet<PathBuf>) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(p) = extract_workspace_url_path_wildcard(s) {
                out.insert(p);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                walk_for_workspace_paths_wildcard(v, out);
            }
        }
        serde_json::Value::Object(obj) => {
            for v in obj.values() {
                walk_for_workspace_paths_wildcard(v, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_workspace_url_path_matches_workspace_local() {
        let s = "https://example.test/api/repos/ns/repo/workspaces/abc-123/files/uploads/cat.jpg";
        let got = extract_workspace_url_path(s, "abc-123").unwrap();
        assert_eq!(got, PathBuf::from("uploads/cat.jpg"));
    }

    #[test]
    fn extract_workspace_url_path_strips_oxen_query_params() {
        let s = "https://example.test/api/repos/ns/repo/workspaces/abc-123/files/uploads/cat.jpg?oxen_signature=xyz&oxen_expires=1";
        let got = extract_workspace_url_path(s, "abc-123").unwrap();
        assert_eq!(got, PathBuf::from("uploads/cat.jpg"));
    }

    #[test]
    fn extract_workspace_url_path_strips_fragment() {
        let s =
            "https://example.test/api/repos/ns/repo/workspaces/abc-123/files/uploads/cat.jpg#thumb";
        let got = extract_workspace_url_path(s, "abc-123").unwrap();
        assert_eq!(got, PathBuf::from("uploads/cat.jpg"));
    }

    #[test]
    fn extract_workspace_url_path_ignores_external_url() {
        let s = "https://example.com/uploads/cat.jpg";
        assert!(extract_workspace_url_path(s, "abc-123").is_none());
    }

    #[test]
    fn extract_workspace_url_path_ignores_other_workspace() {
        let s =
            "https://example.test/api/repos/ns/repo/workspaces/some-other-id/files/uploads/cat.jpg";
        assert!(extract_workspace_url_path(s, "abc-123").is_none());
    }

    #[test]
    fn walk_collects_only_workspace_local_strings() {
        let value = serde_json::json!({
            "input": {
                "image_url": "https://example.test/api/repos/ns/repo/workspaces/abc-123/files/uploads/cat.jpg",
                "mask_url":  "https://example.test/api/repos/ns/repo/workspaces/abc-123/files/uploads/mask.png?oxen_signature=ignored",
                "external":  "https://example.com/external.png",
                "nested":    [
                    "not a url",
                    "https://example.test/api/repos/ns/repo/workspaces/abc-123/files/inputs/ref.jpg"
                ],
                "scalar":    42
            }
        });
        let mut out = Vec::new();
        walk_for_workspace_paths(&value, "abc-123", &mut out);
        out.sort();
        assert_eq!(
            out,
            vec![
                PathBuf::from("inputs/ref.jpg"),
                PathBuf::from("uploads/cat.jpg"),
                PathBuf::from("uploads/mask.png"),
            ]
        );
    }

    #[test]
    fn clean_asset_column_value_strips_query_and_fragment() {
        assert_eq!(
            clean_asset_column_value("uploads/cat.jpg?oxen_signature=abc"),
            Some(PathBuf::from("uploads/cat.jpg"))
        );
        assert_eq!(
            clean_asset_column_value("uploads/cat.jpg#thumb"),
            Some(PathBuf::from("uploads/cat.jpg"))
        );
        assert_eq!(clean_asset_column_value(""), None);
    }

    #[test]
    fn extract_workspace_url_path_wildcard_matches_any_workspace() {
        let s =
            "https://example.test/api/repos/ns/repo/workspaces/some-uuid-123/files/uploads/cat.jpg";
        assert_eq!(
            extract_workspace_url_path_wildcard(s),
            Some(PathBuf::from("uploads/cat.jpg"))
        );
        let s = "https://example.test/api/repos/ns/repo/workspaces/another-uuid/files/sub/dir/file.png?oxen_sig=xyz";
        assert_eq!(
            extract_workspace_url_path_wildcard(s),
            Some(PathBuf::from("sub/dir/file.png"))
        );
    }

    #[test]
    fn extract_workspace_url_path_wildcard_ignores_external() {
        assert_eq!(
            extract_workspace_url_path_wildcard("https://example.com/uploads/cat.jpg"),
            None
        );
        assert_eq!(
            extract_workspace_url_path_wildcard(
                "https://example.test/api/repos/ns/repo/workspaces//files/foo"
            ),
            None,
        );
    }

    #[test]
    fn walk_wildcard_collects_paths_across_workspace_ids() {
        let value = serde_json::json!({
            "input": {
                "from_workspace_a": "https://example.test/api/repos/ns/repo/workspaces/aaa/files/uploads/cat.jpg",
                "from_workspace_b": "https://example.test/api/repos/ns/repo/workspaces/bbb/files/uploads/dog.jpg?oxen_sig=1",
                "external": "https://example.com/external.png",
                "scalar": 42,
            }
        });
        let mut out = BTreeSet::new();
        walk_for_workspace_paths_wildcard(&value, &mut out);
        let collected: Vec<PathBuf> = out.into_iter().collect();
        assert_eq!(
            collected,
            vec![
                PathBuf::from("uploads/cat.jpg"),
                PathBuf::from("uploads/dog.jpg"),
            ]
        );
    }

    #[tokio::test]
    async fn read_rows_as_json_preserves_keys_appearing_only_later_in_jsonl() {
        // Repro for the bug that prompted the diagnostic work: Polars's
        // LazyJsonLineReader infers schema from the first N rows; columns
        // that show up only later can be silently dropped. Our streaming
        // reader avoids that — we keep every row as raw JSON.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let mut content = String::new();
        for i in 0..50 {
            content.push_str(&format!("{{\"a\": {i}}}\n"));
        }
        // The `uuid` field shows up only after 50 rows of `a`-only rows.
        content.push_str("{\"a\": 50, \"uuid\": \"abc-123\"}\n");
        content.push_str("{\"a\": 51, \"uuid\": \"def-456\"}\n");
        std::fs::write(&path, content).unwrap();

        let rows = read_rows_as_json(path, "jsonl".to_string()).await.unwrap();
        assert_eq!(rows.len(), 52);
        // The streaming reader should see all keys union'd across the file.
        let keys = collect_observed_keys(&rows);
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"uuid".to_string()));
        assert!(rows_have_key(&rows, "uuid"));
        // And the uuids actually came through.
        let mut uuids: Vec<String> = rows
            .iter()
            .filter_map(|r| {
                r.get("uuid")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        uuids.sort();
        assert_eq!(uuids, vec!["abc-123".to_string(), "def-456".to_string()]);
    }

    #[tokio::test]
    async fn read_rows_as_json_skips_blank_lines_in_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.jsonl");
        let content = "{\"a\": 1}\n\n{\"a\": 2}\n   \n{\"a\": 3}\n";
        std::fs::write(&path, content).unwrap();
        let rows = read_rows_as_json(path, "jsonl".to_string()).await.unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn rows_have_key_only_counts_non_null() {
        let rows = vec![
            serde_json::json!({"uuid": null}),
            serde_json::json!({"other": "x"}),
        ];
        assert!(!rows_have_key(&rows, "uuid"));

        let rows = vec![
            serde_json::json!({"uuid": null}),
            serde_json::json!({"uuid": "abc"}),
        ];
        assert!(rows_have_key(&rows, "uuid"));
    }

    #[test]
    fn collect_observed_keys_unions_across_rows() {
        let rows = vec![
            serde_json::json!({"a": 1, "b": 2}),
            serde_json::json!({"b": 3, "c": 4}),
            serde_json::json!({"a": 5, "uuid": "x"}),
        ];
        assert_eq!(
            collect_observed_keys(&rows),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "uuid".to_string(),
            ]
        );
    }
}
