use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HealthResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub disk_usage: DiskUsage,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiskUsage {
    pub total_gb: f64,
    pub used_gb: f64,
    pub free_gb: f64,
    pub percent_used: f64,
}
