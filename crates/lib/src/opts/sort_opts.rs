use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortBy {
    Name,
    Date,
}

impl fmt::Display for SortBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortBy::Name => write!(f, "name"),
            SortBy::Date => write!(f, "date"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SortOpts {
    pub sort_by: SortBy,
    pub reverse: bool,
}

impl Default for SortOpts {
    fn default() -> Self {
        SortOpts {
            sort_by: SortBy::Name,
            reverse: false,
        }
    }
}
