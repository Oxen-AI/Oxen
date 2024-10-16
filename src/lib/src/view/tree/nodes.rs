use serde::{Deserialize, Serialize};

pub mod commit_node;
pub mod dir_node;
pub mod file_node;
pub mod vnode;

pub use commit_node::CommitNodeResponse;
pub use dir_node::DirNodeResponse;
pub use file_node::FileNodeResponse;
pub use vnode::VNodeResponse;

pub use crate::view::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeView {
    pub dtype: String,
    pub hash: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeViewResponse {
    pub status: StatusMessage,
    pub node: NodeView,
}
