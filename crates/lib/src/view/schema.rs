use serde::{Deserialize, Serialize};

use super::entries::ResourceVersion;
use crate::model::{Commit, Schema};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaWithPath {
    pub path: String,
    #[serde(flatten)]
    pub schema: Schema,
}

impl SchemaWithPath {
    pub fn new(path: String, schema: Schema) -> SchemaWithPath {
        SchemaWithPath { path, schema }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListSchemaResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub schemas: Vec<SchemaWithPath>,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
}

/* For getting schemas directly by hash - no path associated */
#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaResponse {
    pub status: StatusMessage,
    pub schema: Schema,
}
