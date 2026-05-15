use crate::error::OxenError;
use crate::model::{
    MerkleHash,
    merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode},
};

/// Interface for read-only access to Merkle tree nodes representing commits & directories.
///
/// The `exists`, `get_node`, and `get_children` methods only work on [`MerkleHash`] values
/// that map to commits or directories (and by extension, virtual directory nodes).
///
/// A file node is _always_ stored in the `children` of some virtual node. To access a Merkle
/// tree node for a file, one needs to map from the repository relative filepath and a commit
/// hash to the right virtual node hash, call [`get_children`] on it, then iterate to the
/// file's corresponding [`MerkleTreeNode`].
///
/// Dyn-compatible: callers can store use this as `dyn MerkleReader`.
pub trait MerkleReader: Send + Sync {
    /// True if there is some node with the given hash. False otherwise.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError>;

    /// Retrieve the node record for the given hash, if it exists. None means no such node exists.
    /// Note that a file node's [`MerkleHash`] will result in None: file nodes are stored as _children_.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleEntry>, OxenError>;

    /// Retrieve the children of the node for the given hash, if it exists and if it is a directory node.
    /// If the node represents a file, then an empty list is always returned.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError>;

    /// Load a [`MerkleTreeNode`] with full node info and 1-level (aka direct) children for any non-file node.
    /// Note that this method must return `None` if [`get_node`] on the same hash would return `None`.
    /// An error is returned if there is some other failure in the Merkle tree's underlying storage layer.
    fn read_full_node(&self, hash: &MerkleHash) -> Result<Option<MerkleTreeNode>, OxenError> {
        let Some(node) = self.get_node(hash)? else {
            return Ok(None);
        };
        let children = self.get_children(hash)?;
        Ok(Some(MerkleTreeNode {
            hash: *hash,
            node: node.node,
            parent_id: node.parent_id,
            children: children.into_iter().map(|(_, c)| c).collect(),
        }))
    }
}

/// Data returned when reading a single node.
/// Always corresponds to either a commit, directory, or virtual directory Merkle tree node.
pub struct MerkleEntry {
    /// The node content.
    pub node: EMerkleTreeNode,
    /// The parent of this node. Commit nodes are the only nodes that do not have parents.
    pub parent_id: Option<MerkleHash>,
}
