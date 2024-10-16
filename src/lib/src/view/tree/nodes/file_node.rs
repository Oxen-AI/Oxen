use serde::{Deserialize, Serialize};

use crate::model::merkle_tree::node::FileNode;
use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct FileNodeResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub node: FileNode,
}
