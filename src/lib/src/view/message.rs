//! User-facing messages resulting from Oxen operations

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct OxenMessage {
    pub level: MessageLevel,
    pub title: String,
    pub description: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MessageLevel {
    Info,
    Warning,
    Error,
}
