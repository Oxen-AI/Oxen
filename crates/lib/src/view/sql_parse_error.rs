use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct SQLParseError {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub sql: String,
}

impl SQLParseError {
    pub fn new(sql: String) -> Self {
        Self {
            status: StatusMessage::error(format!("Error running SQL query '{sql}'")),
            sql,
        }
    }
}
