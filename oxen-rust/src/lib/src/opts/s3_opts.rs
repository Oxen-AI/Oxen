use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct S3Opts {
    pub bucket: String,
    pub prefix: Option<String>,
}
