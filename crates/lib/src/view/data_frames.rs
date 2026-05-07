use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::Commit;
use crate::view::StatusMessage;
use crate::view::data_frames::columns::NewColumn;
use utoipa::ToSchema;

pub mod columns;
pub mod embeddings;

#[derive(Deserialize, Serialize, Debug)]
pub struct DataFramePayload {
    pub is_indexed: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataFrameColumnChange {
    pub operation: String,
    pub column_before: Option<ColumnChange>,
    pub column_after: Option<ColumnChange>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ColumnChange {
    pub column_name: String,
    pub column_data_type: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataFrameRowChange {
    pub row_id: String,
    pub operation: String,
    pub value: Value,
    pub new_value: Option<Value>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct FromDirectoryRequest {
    pub output_path: Option<String>,
    pub extra_columns: Option<Vec<NewColumn>>,
    pub commit_message: Option<String>,
    pub user_name: Option<String>,
    pub user_email: Option<String>,
    pub recursive: Option<bool>,
}

/// Cherry-pick selected rows from a source workspace dataframe and apply them to a
/// target (branch HEAD or another workspace), producing a commit on the target branch.
/// The source workspace id is taken from the URL path; only the source dataframe path,
/// target descriptor, selections, and commit message live in the body.
///
/// `commit_author` / `commit_email` are optional. When provided, they override the
/// fallback `Oxen Selective Merge` / `selective-merge@oxen.ai` attribution so an
/// intermediate server (e.g. OxenHub) can attribute the commit to the originating user.
/// When `None`, the fallback constants are used so direct callers behave as before.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SelectiveRowMergeRequest {
    pub source_path: PathBuf,
    pub target: SelectiveMergeTarget,
    pub selections: Vec<RowSelection>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_email: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SelectiveMergeTarget {
    Branch {
        name: String,
        path: PathBuf,
    },
    Workspace {
        workspace_id: String,
        path: PathBuf,
        branch: String,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RowSelection {
    pub row_id: String,
    pub op: RowMergeOp,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RowMergeOp {
    Add,
    Delete,
    Modify,
}

/// Pull a committed branch's rows + assets into a workspace. Append-only sync:
/// finds rows in the source branch's dataframe whose value in `uuid_column`
/// isn't already present in the target workspace's view of `target_path`, copies
/// them into the workspace's branch as a single commit, and copies every asset
/// they reference along the way.
///
/// The URL workspace is the **target**. `target_branch` says which branch the
/// resulting commit lands on (OxenHub stamps this on the body since the
/// server's workspace model doesn't carry a branch name).
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SyncFromBranchRequest {
    pub source: SyncSource,
    pub target_path: PathBuf,
    pub target_branch: String,
    pub uuid_column: String,
    #[serde(default)]
    pub asset_columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_data_column: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_email: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SyncSource {
    pub branch: String,
    pub path: PathBuf,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SyncFromBranchResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub already_up_to_date: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<Commit>,
    pub rows_added: usize,
    pub assets_added: usize,
}

/// Same as [`SelectiveRowMergeRequest`] but also copies any binary assets the
/// selected rows reference into the target commit. Two column hints control
/// asset resolution:
///
/// * `asset_columns` — each named column on the row is treated as a string
///   path to a file in the source workspace (resolved as
///   `/workspaces/<source_workspace_id>/files/<value>`). Empty / missing
///   values are skipped.
/// * `request_data_column` — optional. The named column holds a JSON-encoded
///   blob; the server walks it recursively and copies any string that looks
///   like a media URL served from this workspace's `/files/` route.
///   Presigned `oxen_*` query params are stripped. Anything that isn't
///   workspace-local (e.g. an external `https://example.com/...`) is ignored.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SelectiveRowMergeWithAssetsRequest {
    pub source_path: PathBuf,
    pub target: SelectiveMergeTarget,
    pub selections: Vec<RowSelection>,
    #[serde(default)]
    pub asset_columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_data_column: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_email: Option<String>,
}
