use std::fmt::Display;

use crate::error::OxenError;

pub enum StagedRowStatus {
    Added,
    Modified,
    Removed,
    Unchanged,
}

impl StagedRowStatus {
    pub fn from_string(s: &str) -> Result<StagedRowStatus, OxenError> {
        match s {
            "added" => Ok(StagedRowStatus::Added),
            "modified" => Ok(StagedRowStatus::Modified),
            "removed" => Ok(StagedRowStatus::Removed),
            "unchanged" => Ok(StagedRowStatus::Unchanged),
            _ => Err(OxenError::basic_str("Invalid row status")),
        }
    }
}

impl Display for StagedRowStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StagedRowStatus::Added => write!(f, "added"),
            StagedRowStatus::Modified => write!(f, "modified"),
            StagedRowStatus::Removed => write!(f, "removed"),
            StagedRowStatus::Unchanged => write!(f, "unchanged"),
        }
    }
}
