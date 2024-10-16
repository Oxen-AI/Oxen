use std::collections::HashSet;

use polars::frame::DataFrame;
use polars::prelude::SchemaExt;
use serde::{Deserialize, Serialize};

use crate::constants::DIFF_STATUS_COL;
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::diff::tabular_diff::{TabularDiffDupes, TabularSchemaDiff};
use crate::model::diff::text_diff::TextDiff;
use crate::model::diff::{AddRemoveModifyCounts, TabularDiff};
use crate::model::{Commit, DiffEntry, Schema};
use crate::view::message::{MessageLevel, OxenMessage};
use crate::view::Pagination;

use super::schema::SchemaWithPath;
use super::{JsonDataFrame, JsonDataFrameViews, StatusMessage};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareCommits {
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub commits: Vec<Commit>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareSummary {
    pub num_left_only_rows: usize,
    pub num_right_only_rows: usize,
    pub num_diff_rows: usize,
    pub num_match_rows: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareCommitsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub pagination: Pagination,
    // Wrap everything else in a compare object
    pub compare: CompareCommits,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareEntries {
    pub base_commit: Commit,
    pub head_commit: Commit,
    pub counts: AddRemoveModifyCounts,
    pub entries: Vec<DiffEntry>,
    #[serde(rename = "self")]
    pub self_diff: Option<DiffEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub compare: DiffEntry,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareEntriesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub pagination: Pagination,
    pub compare: CompareEntries,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareTabularResponse {
    pub dfs: CompareTabular,
    #[serde(flatten)]
    pub status: StatusMessage,
    pub messages: Vec<OxenMessage>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareTabularResponseWithDF {
    pub dfs: CompareTabular,
    pub data: JsonDataFrameViews,
    #[serde(flatten)]
    pub status: StatusMessage,
    pub messages: Vec<OxenMessage>,
}

#[derive(Debug)]
pub struct CompareTabularWithDF {
    pub diff_df: DataFrame,
    pub dupes: CompareDupes,
    pub schema_diff: Option<CompareSchemaDiff>,
    pub summary: Option<CompareSummary>,
    pub keys: Vec<TabularCompareFieldBody>,
    pub targets: Vec<TabularCompareTargetBody>,
    pub display: Vec<TabularCompareTargetBody>,
    pub source_schemas: CompareSourceSchemas,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSchemaDiff {
    pub added_cols: Vec<CompareSchemaColumn>,
    pub removed_cols: Vec<CompareSchemaColumn>,
}

impl CompareSchemaDiff {
    pub fn to_tabular_schema_diff(self) -> TabularSchemaDiff {
        TabularSchemaDiff {
            added: self
                .added_cols
                .into_iter()
                .map(|col| col.to_field())
                .collect(),
            removed: self
                .removed_cols
                .into_iter()
                .map(|col| col.to_field())
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSummary {
    pub modifications: CompareTabularMods,
    pub schema: Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CompareTabularMods {
    pub added_rows: usize,
    pub removed_rows: usize,
    pub modified_rows: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareDisplayFields {
    pub left: Vec<String>,
    pub right: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareDupes {
    pub left: u64,
    pub right: u64,
}

impl CompareDupes {
    pub fn to_tabular_diff_dupes(&self) -> TabularDiffDupes {
        TabularDiffDupes {
            left: self.left,
            right: self.right,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSchemaColumn {
    pub name: String,
    pub key: String,
    pub dtype: String,
}

impl CompareSchemaColumn {
    fn to_field(&self) -> Field {
        Field::new(&self.name, &self.dtype)
    }
}

// TODONOW these should maybe be moved to a model

#[derive(Debug)]
pub enum CompareResult {
    Tabular((CompareTabular, DataFrame)),
    Text(TextDiff),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareTabular {
    pub dupes: CompareDupes,
    pub summary: Option<CompareSummary>,
    pub schema_diff: Option<CompareSchemaDiff>,
    pub source_schemas: CompareSourceSchemas,
    pub keys: Option<Vec<TabularCompareFieldBody>>,
    pub targets: Option<Vec<TabularCompareTargetBody>>,
    pub display: Option<Vec<TabularCompareTargetBody>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareVirtualResource {
    // TODO: Maybe this should be common to all v resource types - diffs, queries, etc.
    pub path: String,
    pub base: String,
    pub head: String,
    pub resource: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompareSourceDFs {
    pub left: CompareSourceDF,
    pub right: CompareSourceDF,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct CompareSourceDF {
    pub path: String,
    pub version: Option<String>, // Needs to be option for py / CLI
    pub schema: Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompareSourceSchemas {
    pub left: Schema,
    pub right: Schema,
}

impl CompareDupes {
    pub fn empty() -> CompareDupes {
        CompareDupes { left: 0, right: 0 }
    }

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
            description: format!("This compare contains rows with duplicate keys. Results may be unexpected if keys are intended to be unique.\nLeft df duplicates: {}\nRight df duplicates: {}\n", self.left, self.right),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TabularCompare {
    pub summary: TabularCompareSummary,

    pub schema_left: Option<SchemaWithPath>,
    pub schema_right: Option<SchemaWithPath>,

    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub match_rows: Option<JsonDataFrame>,
    pub diff_rows: Option<JsonDataFrame>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareBody {
    pub compare_id: String,
    pub left: TabularCompareResourceBody,
    pub right: TabularCompareResourceBody,
    pub keys: Vec<TabularCompareFieldBody>,
    pub compare: Vec<TabularCompareTargetBody>,
    pub display: Vec<TabularCompareTargetBody>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularCompareResourceBody {
    pub path: String,
    pub version: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
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

#[derive(Deserialize, Serialize, Debug, Clone)]
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
            .ok_or(OxenError::basic_str(
                "Both 'left' and 'right' fields are None",
            ))
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
            schema: Schema::from_polars(&df.schema()),
        })
    }
}

impl CompareSchemaDiff {
    pub fn from_dfs(df1: &DataFrame, df2: &DataFrame) -> Result<CompareSchemaDiff, OxenError> {
        // Assuming CompareSchemaColumn and CompareSchemaDiff are defined elsewhere
        // and OxenError is a placeholder for error handling in your application.

        // Get added columns
        let added_cols = df2
            .schema()
            .iter_fields()
            .filter(|field| !df1.schema().contains(field.name()))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name().to_owned().to_string(),
                    key: format!("{}.right", field.name()),
                    dtype: format!("{:?}", field.dtype()),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        // Get removed columns
        let removed_cols = df1
            .schema()
            .iter_fields()
            .filter(|field| !df2.schema().contains(field.name()))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name().to_string(),
                    key: format!("{}.left", field.name()),
                    dtype: format!("{:?}", field.dtype()),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        Ok(CompareSchemaDiff {
            added_cols,
            removed_cols,
        })
    }

    pub fn from_schemas(s1: &Schema, s2: &Schema) -> Result<CompareSchemaDiff, OxenError> {
        let added_cols = s2
            .fields
            .iter()
            .filter(|field| !s1.fields.contains(field))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name.to_owned().to_string(),
                    key: format!("{}.right", field.name),
                    dtype: field.dtype.to_owned(),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        let removed_cols = s1
            .fields
            .iter()
            .filter(|field| !s2.fields.contains(field))
            .map(|field| {
                Ok(CompareSchemaColumn {
                    name: field.name.to_string(),
                    key: format!("{}.left", field.name),
                    dtype: field.dtype.to_string(),
                })
            })
            .collect::<Result<Vec<CompareSchemaColumn>, OxenError>>()?;

        Ok(CompareSchemaDiff {
            added_cols,
            removed_cols,
        })
    }
}

impl CompareTabular {
    pub fn from_with_df_and_source_schemas(
        with_df: &CompareTabularWithDF,
        source_schemas: CompareSourceSchemas,
    ) -> CompareTabular {
        CompareTabular {
            dupes: with_df.dupes.clone(),
            schema_diff: with_df.schema_diff.clone(),
            summary: with_df.summary.clone(),
            keys: Some(with_df.keys.clone()),
            targets: Some(with_df.targets.clone()),
            display: Some(with_df.display.clone()),
            source_schemas,
        }
    }
    pub fn from_with_df(with_df: &CompareTabularWithDF) -> CompareTabular {
        CompareTabular {
            dupes: with_df.dupes.clone(),
            schema_diff: with_df.schema_diff.clone(),
            summary: with_df.summary.clone(),
            keys: Some(with_df.keys.clone()),
            targets: Some(with_df.targets.clone()),
            display: Some(with_df.display.clone()),
            source_schemas: with_df.source_schemas.clone(),
        }
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
