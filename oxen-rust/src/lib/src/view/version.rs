use super::StatusMessage;
use serde::{Serialize, Deserialize};
#[derive(Serialize, Deserialize, Debug)]
pub struct VersionResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub version: String,
}