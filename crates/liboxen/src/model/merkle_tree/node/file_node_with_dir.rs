use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::model::merkle_tree::node::file_node::FileNode;

#[derive(Debug, Clone)]
pub struct FileNodeWithDir {
    pub file_node: FileNode,
    pub dir: PathBuf,
}

impl PartialEq for FileNodeWithDir {
    fn eq(&self, other: &Self) -> bool {
        self.dir.join(self.file_node.name()) == other.dir.join(other.file_node.name())
    }
}

impl Eq for FileNodeWithDir {}

impl Hash for FileNodeWithDir {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dir.join(self.file_node.name()).hash(state);
    }
}
