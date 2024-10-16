use crate::{
    error::OxenError,
    model::data_frame::schema::{Field, Schema},
};
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

use super::AddRemoveModifyCounts;

#[derive(Debug, Clone, Default)]
pub struct TabularSchemaDiff {
    pub added: Vec<Field>,
    pub removed: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct TabularDiffMods {
    pub row_counts: AddRemoveModifyCounts,
    pub col_changes: TabularSchemaDiff,
}

#[derive(Debug, Clone)]
pub struct TabularDiffSummary {
    pub modifications: TabularDiffMods,
    pub schemas: TabularDiffSchemas,
    pub dupes: TabularDiffDupes,
}

#[derive(Debug, Clone)]
pub struct TabularDiff {
    pub summary: TabularDiffSummary,
    pub parameters: TabularDiffParameters,
    pub contents: DataFrame,
}

#[derive(Debug, Clone)]
pub struct TabularDiffSchemas {
    pub left: Schema,
    pub right: Schema,
    pub diff: Schema,
}

#[derive(Debug, Clone)]
pub struct TabularDiffParameters {
    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub display: Vec<String>,
}

// Need to serialize here because we directly write this to disk to cache compares
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularDiffDupes {
    pub left: u64,
    pub right: u64,
}
impl TabularDiffDupes {
    pub fn empty() -> Self {
        TabularDiffDupes { left: 0, right: 0 }
    }
}

impl TabularDiffMods {
    pub fn empty() -> Self {
        TabularDiffMods {
            row_counts: AddRemoveModifyCounts {
                added: 0,
                removed: 0,
                modified: 0,
            },
            col_changes: TabularSchemaDiff::empty(),
        }
    }
}

impl TabularDiffSchemas {
    pub fn empty() -> Self {
        TabularDiffSchemas {
            left: Schema::empty(),
            right: Schema::empty(),
            diff: Schema::empty(),
        }
    }
}

impl TabularDiffParameters {
    pub fn empty() -> Self {
        TabularDiffParameters {
            keys: Vec::new(),
            targets: Vec::new(),
            display: Vec::new(),
        }
    }
}

impl TabularSchemaDiff {
    pub fn from_schemas(s1: &Schema, s2: &Schema) -> Result<TabularSchemaDiff, OxenError> {
        let added = s2
            .fields
            .iter()
            .filter(|field| !s1.fields.contains(field))
            .cloned()
            .collect::<Vec<Field>>();

        let removed = s1
            .fields
            .iter()
            .filter(|field| !s2.fields.contains(field))
            .cloned()
            .collect::<Vec<Field>>();

        Ok(TabularSchemaDiff { added, removed })
    }

    pub fn empty() -> Self {
        TabularSchemaDiff {
            added: vec![],
            removed: vec![],
        }
    }
}

impl TabularDiff {
    pub fn has_changes(&self) -> bool {
        self.summary.modifications.row_counts.added > 0
            || self.summary.modifications.row_counts.removed > 0
            || self.summary.modifications.row_counts.modified > 0
            || !self.summary.modifications.col_changes.added.is_empty()
            || !self.summary.modifications.col_changes.removed.is_empty()
    }

    pub fn empty() -> Self {
        TabularDiff {
            summary: TabularDiffSummary::empty(),
            parameters: TabularDiffParameters::empty(),
            contents: DataFrame::empty(),
        }
    }
}

impl TabularDiffSummary {
    pub fn empty() -> Self {
        TabularDiffSummary {
            modifications: TabularDiffMods::empty(),
            schemas: TabularDiffSchemas::empty(),
            dupes: TabularDiffDupes::empty(),
        }
    }
}
