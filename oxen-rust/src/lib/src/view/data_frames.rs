use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::view::data_frames::columns::NewColumn;
use utoipa::ToSchema;

pub mod columns;
pub mod embeddings;

#[derive(Deserialize, Serialize, Debug)]
pub struct DataFramePayload {
    pub is_indexed: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataFrameColumnChange {
    pub operation: String,
    pub column_before: Option<ColumnChange>,
    pub column_after: Option<ColumnChange>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ColumnChange {
    pub column_name: String,
    pub column_data_type: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataFrameRowChange {
    pub row_id: String,
    pub operation: String,
    pub value: Value,
    pub new_value: Option<Value>,
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
