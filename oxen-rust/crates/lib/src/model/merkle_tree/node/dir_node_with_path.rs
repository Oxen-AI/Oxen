use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::model::merkle_tree::node::dir_node::DirNode;

#[derive(Debug, Clone)]
pub struct DirNodeWithPath {
    pub dir_node: DirNode,
    pub path: PathBuf,
}

impl PartialEq for DirNodeWithPath {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for DirNodeWithPath {}

impl Hash for DirNodeWithPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}
