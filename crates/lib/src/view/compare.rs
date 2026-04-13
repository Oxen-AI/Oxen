use std::collections::HashSet;

use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::constants::DIFF_STATUS_COL;
use crate::error::OxenError;
use crate::model::diff::tabular_diff::{TabularDiffDupes, TabularSchemaDiff};
use crate::model::diff::{AddRemoveModifyCounts, TabularDiff};
use crate::model::{Commit, DiffEntry, Schema};
use crate::view::Pagination;
use crate::view::message::{MessageLevel, OxenMessage};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct CompareCommits {
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub commits: Vec<Commit>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct CompareCommitsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub pagination: Pagination,
    // Wrap everything else in a compare object
    pub compare: CompareCommits,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct CompareEntries {
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub counts: AddRemoveModifyCounts,
    pub entries: Vec<DiffEntry>,
    #[serde(rename = "self")]
    pub self_diff: Option<DiffEntry>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct CompareEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub compare: DiffEntry,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct CompareEntriesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub pagination: Pagination,
    pub compare: CompareEntries,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct CompareTabularResponse {
    pub dfs: CompareTabular,
    #[serde(flatten)]
    pub status: StatusMessage,
    pub messages: Vec<OxenMessage>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct CompareSchemaDiff {
    pub added_cols: Vec<CompareSchemaColumn>,
    pub removed_cols: Vec<CompareSchemaColumn>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct CompareSummary {
    pub modifications: CompareTabularMods,
    pub schema: Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, ToSchema)]
pub struct CompareTabularMods {
    pub added_rows: usize,
    pub removed_rows: usize,
    pub modified_rows: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct CompareDupes {
    pub left: u64,
    pub right: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct CompareSchemaColumn {
    pub name: String,
    pub key: String,
    pub dtype: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct CompareTabular {
    pub dupes: CompareDupes,
    pub summary: Option<CompareSummary>,
    pub schema_diff: Option<CompareSchemaDiff>,
    pub source_schemas: CompareSourceSchemas,
    pub keys: Option<Vec<TabularCompareFieldBody>>,
    pub targets: Option<Vec<TabularCompareTargetBody>>,
    pub display: Option<Vec<TabularCompareTargetBody>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct CompareSourceSchemas {
    pub left: Schema,
    pub right: Schema,
}

impl CompareDupes {
    pub fn from_tabular_diff_dupes(diff_dupes: &TabularDiffDupes) -> CompareDupes {
        CompareDupes {
            left: diff_dupes.left,
            right: diff_dupes.right,
        }
    }

    pub fn to_message(&self) -> OxenMessage {
        OxenMessage {
            level: MessageLevel::Warning,
            title: "Duplicate keys".to_owned(),
            description: format!(
                "This compare contains rows with duplicate keys. Results may be unexpected if keys are intended to be unique.\nLeft df duplicates: {}\nRight df duplicates: {}\n",
                self.left, self.right
            ),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct TabularCompareBody {
    pub compare_id: String,
    pub left: TabularCompareResourceBody,
    pub right: TabularCompareResourceBody,
    pub keys: Vec<TabularCompareFieldBody>,
    pub compare: Vec<TabularCompareTargetBody>,
    pub display: Vec<TabularCompareTargetBody>,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct TabularCompareResourceBody {
    pub path: String,
    pub version: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct TabularCompareFieldBody {
    pub left: String,
    pub right: String,
    pub alias_as: Option<String>,
    pub compare_method: Option<String>,
}

impl TabularCompareFieldBody {
    pub fn as_string(&self) -> String {
        self.left.clone()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareFields {
    pub keys: Vec<TabularCompareFieldBody>,
    pub targets: Vec<TabularCompareTargetBody>,
    pub display: Vec<TabularCompareTargetBody>,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct TabularCompareTargetBody {
    pub left: Option<String>,
    pub right: Option<String>,
    pub compare_method: Option<String>,
}
impl TabularCompareTargetBody {
    pub fn to_string(&self) -> Result<String, OxenError> {
        self.left
            .clone()
            .or_else(|| self.right.clone())
            .ok_or_else(|| OxenError::basic_str("Both 'left' and 'right' fields are None"))
    }
}

impl TabularCompareFields {
    pub fn from_lists_and_schema_diff(
        schema_diff: &TabularSchemaDiff,
        keys: Vec<&str>,
        targets: Vec<&str>,
        display: Vec<&str>,
    ) -> Self {
        let res_keys = keys
            .iter()
            .map(|key| TabularCompareFieldBody {
                left: key.to_string(),
                right: key.to_string(),
                alias_as: None,
                compare_method: None,
            })
            .collect::<Vec<TabularCompareFieldBody>>();

        let mut res_targets: Vec<TabularCompareTargetBody> = vec![];

        // Get added and removed as sets of strings
        let added_set: HashSet<&str> = schema_diff.added.iter().map(|f| f.name.as_str()).collect();
        let removed_set: HashSet<&str> = schema_diff
            .removed
            .iter()
            .map(|f| f.name.as_str())
            .collect();

        for target in targets.iter() {
            if added_set.contains(target) {
                res_targets.push(TabularCompareTargetBody {
                    left: None,
                    right: Some(target.to_string()),
                    compare_method: None,
                });
            } else if removed_set.contains(target) {
                res_targets.push(TabularCompareTargetBody {
                    left: Some(target.to_string()),
                    right: None,
                    compare_method: None,
                });
            } else {
                res_targets.push(TabularCompareTargetBody {
                    left: Some(target.to_string()),
                    right: Some(target.to_string()),
                    compare_method: None,
                });
            }
        }

        let mut res_display: Vec<TabularCompareTargetBody> = vec![];
        for disp in display.iter() {
            if added_set.contains(disp) {
                res_display.push(TabularCompareTargetBody {
                    left: None,
                    right: Some(disp.to_string()),
                    compare_method: None,
                });
            } else if removed_set.contains(disp) {
                res_display.push(TabularCompareTargetBody {
                    left: Some(disp.to_string()),
                    right: None,
                    compare_method: None,
                });
            } else {
                res_display.push(TabularCompareTargetBody {
                    left: Some(disp.to_string()),
                    right: Some(disp.to_string()),
                    compare_method: None,
                });
            }
        }

        TabularCompareFields {
            keys: res_keys,
            targets: res_targets,
            display: res_display,
        }
    }
}

// impl CompareSourceDF {
//     pub fn from_name_df_entry_schema(
//         name: &str,
//         df: DataFrame,
//         entry: &CommitEntry,
//         schema: Schema,
//     ) -> CompareSourceDF {
//         CompareSourceDF {
//             name: name.to_owned(),
//             path: entry.path.clone(),
//             version: entry.commit_id.clone(),
//             schema,
//             size: DataFrameSize {
//                 height: df.height(),
//                 width: df.width(),
//             },
//         }
//     }
// }

impl CompareSummary {
    pub fn from_diff_df(df: &DataFrame) -> Result<CompareSummary, OxenError> {
        // TODO optimization: can this be done in one pass?
        let added_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "added").unwrap_or(false))
            .count();

        let removed_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "removed").unwrap_or(false))
            .count();

        let modified_rows = df
            .column(DIFF_STATUS_COL)?
            .str()?
            .into_iter()
            .filter(|opt| opt.as_ref().map(|s| *s == "modified").unwrap_or(false))
            .count();

        Ok(CompareSummary {
            modifications: CompareTabularMods {
                added_rows,
                removed_rows,
                modified_rows,
            },
            schema: Schema::from_polars(df.schema()),
        })
    }
}

impl From<TabularDiff> for CompareTabular {
    fn from(diff: TabularDiff) -> Self {
        let fields = TabularCompareFields::from_lists_and_schema_diff(
            &diff.summary.modifications.col_changes,
            diff.parameters.keys.iter().map(|k| k.as_str()).collect(),
            diff.parameters.targets.iter().map(|t| t.as_str()).collect(),
            diff.parameters.display.iter().map(|d| d.as_str()).collect(),
        );

        CompareTabular {
            dupes: CompareDupes::from_tabular_diff_dupes(&diff.summary.dupes),
            schema_diff: Some(CompareSchemaDiff {
                added_cols: diff
                    .summary
                    .modifications
                    .col_changes
                    .added
                    .iter()
                    .map(|field| CompareSchemaColumn {
                        name: field.name.clone(),
                        key: format!("{}.{}", field.name, "added"),
                        dtype: field.dtype.to_string(),
                    })
                    .collect(),
                removed_cols: diff
                    .summary
                    .modifications
                    .col_changes
                    .removed
                    .iter()
                    .map(|field| CompareSchemaColumn {
                        name: field.name.clone(),
                        key: format!("{}.{}", field.name, "removed"),
                        dtype: field.dtype.to_string(),
                    })
                    .collect(),
            }),
            summary: Some(CompareSummary {
                modifications: CompareTabularMods {
                    added_rows: diff.summary.modifications.row_counts.added,
                    removed_rows: diff.summary.modifications.row_counts.removed,
                    modified_rows: diff.summary.modifications.row_counts.modified,
                },
                schema: diff.summary.schemas.diff.clone(),
            }),
            keys: Some(fields.keys),
            targets: Some(fields.targets),
            display: Some(fields.display),
            source_schemas: CompareSourceSchemas {
                left: diff.summary.schemas.left.clone(),
                right: diff.summary.schemas.right.clone(),
            },
        }
    }
}
