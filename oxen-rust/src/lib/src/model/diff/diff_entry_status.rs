use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiffEntryStatus {
    Added,
    Modified,
    Removed,
}

// Downcase the status
impl std::fmt::Display for DiffEntryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            DiffEntryStatus::Added => "added",
            DiffEntryStatus::Modified => "modified",
            DiffEntryStatus::Removed => "removed",
        };
        write!(f, "{status}")
    }
}

// implement from_str for DiffEntryStatus
impl std::str::FromStr for DiffEntryStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "added" => Ok(DiffEntryStatus::Added),
            "modified" => Ok(DiffEntryStatus::Modified),
            "removed" => Ok(DiffEntryStatus::Removed),
            _ => Err(format!("Could not parse {s} as a DiffEntryStatus")),
        }
    }
}
