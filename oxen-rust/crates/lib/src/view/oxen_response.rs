use serde::{Deserialize, Serialize};

// This are the minimum fields we need to check if an oxen response is valid
#[derive(Serialize, Deserialize, Debug)]
pub struct OxenResponse {
    pub status: String,
    pub status_message: String,
    pub status_description: Option<String>,
    pub error: Option<ErrorResponse>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ErrorResponse {
    title: String,
    #[serde(rename = "type")]
    error_type: String,
    detail: Option<String>,
}

impl OxenResponse {
    pub fn desc_or_msg(&self) -> String {
        match self.status_description.to_owned() {
            Some(desc) => desc,
            None => self.status_message.to_owned(),
        }
    }

    pub fn full_err_msg(&self) -> String {
        match self.error.to_owned() {
            Some(err) => format!("{}\n{}", err.title, err.detail.unwrap_or("".to_string())),
            None => self.desc_or_msg(),
        }
    }

    pub fn error_or_msg(&self) -> String {
        match self.error.to_owned() {
            Some(err) => err.title,
            None => self.status_message.to_owned(),
        }
    }
}
