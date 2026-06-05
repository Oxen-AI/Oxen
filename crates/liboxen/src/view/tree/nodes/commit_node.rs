use serde::{Deserialize, Serialize};

use crate::model::merkle_tree::node::CommitNode;
use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct CommitNodeResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub node: CommitNode,
}
