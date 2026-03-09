use super::StatusMessage;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
pub struct VersionResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub version: String,
}
