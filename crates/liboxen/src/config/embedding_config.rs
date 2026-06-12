use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const EMBEDDING_CONFIG_FILENAME: &str = "embeddings.toml";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingStatus {
    NotIndexed,
    Started,
    InProgress,
    Complete,
    Failed,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmbeddingColumn {
    pub name: String,
    pub vector_length: usize,
    pub status: EmbeddingStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct EmbeddingConfig {
    // Map of column name to embedding vector length
    pub columns: HashMap<String, EmbeddingColumn>,
}
