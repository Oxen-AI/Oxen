use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::model::merkle_tree::node::file_node::FileNode;
use crate::model::MerkleHash;

#[derive(Debug, Clone)]
pub struct FileNodeWithDir {
    pub file_node: FileNode,
    pub dir: PathBuf,
}

impl PartialEq for FileNodeWithDir {
    fn eq(&self, other: &Self) -> bool {
        self.file_node.hash() == other.file_node.hash()
    }
}

impl Eq for FileNodeWithDir {}

impl Hash for FileNodeWithDir {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_node.hash().hash(state);
    }
}

impl Borrow<MerkleHash> for FileNodeWithDir {
    fn borrow(&self) -> &MerkleHash {
        &self.file_node.hash()
    }
}
