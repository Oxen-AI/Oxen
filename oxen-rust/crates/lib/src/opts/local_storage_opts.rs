use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use utoipa::ToSchema;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct LocalStorageOpts {
    #[schema(value_type = Option<String>)]
    pub path: Option<PathBuf>,
}
