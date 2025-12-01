use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Opts {
    pub bucket: String,
    pub prefix: Option<String>,
}
