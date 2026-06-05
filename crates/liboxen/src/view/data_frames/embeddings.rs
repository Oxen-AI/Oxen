use crate::config::embedding_config::EmbeddingColumn;
use serde::{Deserialize, Serialize};

use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct EmbeddingColumnsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub columns: Vec<EmbeddingColumn>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IndexEmbeddingRequest {
    pub column: String,
    pub use_background_thread: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EmbeddingQuery {
    pub column: String,
    pub embedding: Vec<f32>,
    pub page_size: usize,
    pub page_num: usize,
}
