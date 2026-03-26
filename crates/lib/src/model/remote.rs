use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct Remote {
    pub name: String,
    pub url: String,
}

impl std::fmt::Display for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name.as_str() {
            "" => write!(f, "{}", self.url),
            _ => write!(f, "[{}] '{}'", self.name, self.url),
        }
    }
}

impl std::error::Error for Remote {}
