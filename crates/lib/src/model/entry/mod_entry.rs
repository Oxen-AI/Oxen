use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::model::Schema;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum ModType {
    Append,
    Delete,
    Modify,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ModEntry {
    pub uuid: String,
    pub modification_type: ModType, // append, delete, modify
    pub schema: Option<Schema>,
    pub data: String,
    pub path: PathBuf,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}

impl fmt::Display for ModType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModType::Append => write!(f, "Append"),
            ModType::Delete => write!(f, "Delete"),
            ModType::Modify => write!(f, "Modify"),
        }
    }
}
