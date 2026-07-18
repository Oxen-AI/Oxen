use serde::{Deserialize, Serialize};

use crate::view::data_frames::columns::NewColumn;
use utoipa::ToSchema;

pub mod columns;
pub mod embeddings;

#[derive(Deserialize, Serialize, Debug)]
pub struct DataFramePayload {
    pub is_indexed: bool,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct FromDirectoryRequest {
    pub output_path: Option<String>,
    pub extra_columns: Option<Vec<NewColumn>>,
    pub commit_message: Option<String>,
    pub user_name: Option<String>,
    pub user_email: Option<String>,
    pub recursive: Option<bool>,
}
