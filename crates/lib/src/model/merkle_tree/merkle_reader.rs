use crate::error::OxenError;
use crate::model::{
    MerkleHash, MerkleTreeNodeType,
    merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode},
};

/// Interface for read-only access to Merkle tree nodes.
pub trait MerkleReader: Send + Sync
where
    OxenError: From<Self::Error>,
{
    /// The error type for the Merkle tree's underlying storage layer.
    ///
    /// Backends may use whichever error type is natural for their storage
    /// (e.g. `MerkleDbError` for the file backend). The `Into<OxenError>`
    /// bound lets callers that return `Result<_, OxenError>` use `?` directly.
    type Error: std::error::Error;

    /// True if there is some node with the given hash. False otherwise.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn exists(&self, hash: &MerkleHash) -> Result<bool, Self::Error>;

    /// Retrieve the node record for the given hash, if it exists. None means no such node exists.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, Self::Error>;

    /// Retrieve the children of the node for the given hash, if it exists and if it is a directory node.
    /// If the node represents a file, then an empty list is always returned.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, Self::Error>;
}

/// Metadata returned when reading a single node.
pub struct MerkleNodeRecord {
    hash: MerkleHash,
    dtype: MerkleTreeNodeType,
    parent_id: Option<MerkleHash>,
    node: EMerkleTreeNode,
    num_children: u64,
}

impl MerkleNodeRecord {
    pub fn new(
        hash: MerkleHash,
        dtype: MerkleTreeNodeType,
        parent_id: Option<MerkleHash>,
        node: EMerkleTreeNode,
        num_children: u64,
    ) -> Self {
        Self {
            hash,
            dtype,
            parent_id,
            node,
            num_children,
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

    pub fn num_children(&self) -> u64 {
        self.num_children
    }

    /// Consume this record and return its `EMerkleTreeNode`, avoiding a clone
    /// for callers that only need the owned node value.
    pub fn into_node(self) -> EMerkleTreeNode {
        self.node
    }
}
