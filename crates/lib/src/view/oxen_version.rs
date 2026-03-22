use super::StatusMessage;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct OxenVersionResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub version: String,
}
