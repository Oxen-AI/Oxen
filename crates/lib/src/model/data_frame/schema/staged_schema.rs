use crate::model::StagedEntryStatus;
use serde::{Deserialize, Serialize};

use super::Schema;

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub enum StagedSchemaStatus {
    Added,
    Modified,
    Removed,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StagedSchema {
    pub schema: Schema,
    pub status: StagedEntryStatus,
}
