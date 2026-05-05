use crate::error::OxenError;
use crate::model::{
    MerkleHash, MerkleTreeNodeType,
    merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode},
};

/// Interface for read-only access to Merkle tree nodes.
///
/// Dyn-compatible: callers can store this as `Box<dyn MerkleReader + '_>`
/// or `&dyn MerkleReader`.
pub trait MerkleReader: Send + Sync {
    /// True if there is some node with the given hash. False otherwise.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError>;

    /// Retrieve the node record for the given hash, if it exists. None means no such node exists.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, OxenError>;

    /// Retrieve the children of the node for the given hash, if it exists and if it is a directory node.
    /// If the node represents a file, then an empty list is always returned.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError>;
}

/// Metadata returned when reading a single node.
pub struct MerkleNodeRecord {
    hash: MerkleHash,
    dtype: MerkleTreeNodeType,
    parent_id: Option<MerkleHash>,
    node: EMerkleTreeNode,
}

impl MerkleNodeRecord {
    pub fn new(
        hash: MerkleHash,
        dtype: MerkleTreeNodeType,
        parent_id: Option<MerkleHash>,
        node: EMerkleTreeNode,
    ) -> Self {
        Self {
            hash,
            dtype,
            parent_id,
            node,
        }
    }

    pub fn hash(&self) -> &MerkleHash {
        &self.hash
    }

    pub fn dtype(&self) -> &MerkleTreeNodeType {
        &self.dtype
    }

    pub fn parent_id(&self) -> Option<&MerkleHash> {
        self.parent_id.as_ref()
    }

    pub fn node(&self) -> &EMerkleTreeNode {
        &self.node
    }

    /// Consume this record and return its `EMerkleTreeNode`, avoiding a clone
    /// for callers that only need the owned node value.
    pub fn into_node(self) -> EMerkleTreeNode {
        self.node
    }
}
