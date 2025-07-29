use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Remote {
    pub name: String,
    pub url: String,
}

impl std::fmt::Display for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] '{}'", self.name, self.url)
    }
}

impl std::error::Error for Remote {}
