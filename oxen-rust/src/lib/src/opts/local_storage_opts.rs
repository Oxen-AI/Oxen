use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LocalStorageOpts {
    pub path: Option<PathBuf>,
}
