//! User-facing messages resulting from Oxen operations

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct OxenMessage {
    pub level: MessageLevel,
    pub title: String,
    pub description: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub enum MessageLevel {
    Info,
    Warning,
    Error,
}
