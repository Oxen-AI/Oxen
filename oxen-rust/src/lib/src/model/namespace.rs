use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct Namespace {
    pub name: String,
    pub storage_usage_gb: f64,
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({} GB)", self.name, self.storage_usage_gb)
    }
}

impl std::error::Error for Namespace {}
