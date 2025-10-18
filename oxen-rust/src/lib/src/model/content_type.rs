use serde::{Deserialize, Serialize};

use crate::error::OxenError;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum ContentType {
    Json,
    Csv,
    Text,
}

impl std::str::FromStr for ContentType {
    type Err = String;

    fn from_str(s: &str) -> Result<ContentType, String> {
        match s {
            "json" => Ok(ContentType::Json),
            "csv" => Ok(ContentType::Csv),
            "text" => Ok(ContentType::Text),
            _ => Err(format!("Invalid data type: {s}")),
        }
    }
}

impl ContentType {
    pub fn to_http_content_type(&self) -> String {
        match self {
            ContentType::Json => "application/json".to_string(),
            ContentType::Csv => "text/csv".to_string(),
            ContentType::Text => "text/plain".to_string(),
        }
    }

    pub fn from_http_content_type(s: &str) -> Result<ContentType, OxenError> {
        match s {
            "application/json" => Ok(ContentType::Json),
            "text/csv" => Ok(ContentType::Csv),
            "text/plain" => Ok(ContentType::Text),
            _ => Err(OxenError::basic_str(format!(
                "Unsupported content-type: {s}"
            ))),
        }
    }
}
