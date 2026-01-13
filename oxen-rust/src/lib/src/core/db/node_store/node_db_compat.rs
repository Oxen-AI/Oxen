/*!
 * Compatibility layer for migrating from MerkleNodeDB to NodeStore.
 *
 * This module provides a `NodeDB` struct that has the same API as `MerkleNodeDB`
 * but uses the `NodeStore` trait underneath. This allows for incremental migration
 * of the codebase without changing all call sites at once.
 */

use std::path::PathBuf;

use rmp_serde::Serializer;

use crate::core::db::node_store::{ChildNodeData, NodeData, NodeStore, NodeTransaction};
use crate::error::OxenError;
use crate::model::merkle_tree::node::{MerkleTreeNodeType, TMerkleTreeNode};
use crate::model::{LocalRepository, MerkleHash};

use crate::core::db::node_store::file_store::FileNodeStore;

/// Compatibility wrapper that mimics MerkleNodeDB API but uses NodeStore underneath.
///
/// This allows gradual migration from MerkleNodeDB to NodeStore without rewriting
/// all calling code at once. Currently uses FileNodeStore for backward compatibility.
pub struct NodeDB {
    // Store these to lazily open the store when needed
    repo_path: PathBuf,
    /// Public for compatibility with MerkleNodeDB::node_id
    pub node_id: MerkleHash,
    /// Public for compatibility with MerkleNodeDB::dtype
    pub dtype: MerkleTreeNodeType,
    /// Public for compatibility with MerkleNodeDB::parent_id
    pub parent_id: Option<MerkleHash>,
    node_data: Vec<u8>,
    // Buffer children until close/drop
    children: Vec<ChildNodeData>,
}

impl NodeDB {
    /// Check if a node exists (equivalent to MerkleNodeDB::exists).
    pub fn exists(repo: &LocalRepository, hash: &MerkleHash) -> bool {
        let store = match FileNodeStore::open(&repo.path) {
            Ok(s) => s,
            Err(_) => return false,
        };
        store.exists(hash).unwrap_or(false)
    }

    /// Open a node for reading (equivalent to MerkleNodeDB::open_read_only).
    ///
    /// This loads the node metadata (type, parent_id) from storage.
    pub fn open_read_only(repo: &LocalRepository, hash: &MerkleHash) -> Result<Self, OxenError> {
        let store = FileNodeStore::open(&repo.path)?;

        // Get the node data to read its type and parent_id
        let node_data = store.get_node(hash)?
            .ok_or_else(|| OxenError::basic_str(format!("Node {} does not exist", hash)))?;

        Ok(Self {
            repo_path: repo.path.clone(),
            node_id: *hash,
            dtype: node_data.node_type,
            parent_id: node_data.parent_id,
            node_data: Vec::new(),
            children: Vec::new(),
        })
    }

    /// Open a node for writing (equivalent to MerkleNodeDB::open_read_write).
    ///
    /// Creates a new node and prepares it for writing children.
    pub fn open_read_write(
        repo: &LocalRepository,
        node: &impl TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self, OxenError> {
        // Serialize the node data
        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf))
            .map_err(|e| OxenError::basic_str(format!("Failed to serialize node: {}", e)))?;

        let node_id = node.hash();
        let dtype = node.node_type();

        Ok(Self {
            repo_path: repo.path.clone(),
            node_id,
            dtype,
            parent_id,
            node_data: buf,
            children: Vec::new(),
        })
    }

    /// Add a child to this node (equivalent to MerkleNodeDB::add_child).
    pub fn add_child(&mut self, child: &impl TMerkleTreeNode) -> Result<(), OxenError> {
        // Serialize the child data
        let mut buf = Vec::new();
        child
            .serialize(&mut Serializer::new(&mut buf))
            .map_err(|e| OxenError::basic_str(format!("Failed to serialize child: {}", e)))?;

        let child_data = ChildNodeData::new(child.hash(), child.node_type(), buf);
        self.children.push(child_data);

        Ok(())
    }

    /// Close the node DB and flush data to storage (equivalent to MerkleNodeDB::close).
    ///
    /// This writes the node and all its children to the store in a single transaction.
    pub fn close(&mut self) -> Result<(), OxenError> {
        // Open the store for this operation
        let mut store = FileNodeStore::open(&self.repo_path)?;

        // Begin a write transaction
        let mut txn = store.begin_write_txn()?;

        // Create the node data
        let node = NodeData::new(
            self.node_id,
            self.dtype,
            self.parent_id,
            self.node_data.clone(),
        );

        // Write the node
        txn.put_node(&node)?;

        // Write all children
        for child in &self.children {
            txn.add_child(&self.node_id, child)?;
        }

        // Commit the transaction
        txn.commit()?;

        // Clear the buffer
        self.children.clear();

        Ok(())
    }

    /// Get the node hash.
    pub fn hash(&self) -> MerkleHash {
        self.node_id
    }

    /// Get the node type.
    pub fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    /// Get the parent ID.
    pub fn parent_id(&self) -> Option<MerkleHash> {
        self.parent_id
    }

    /// Get the node as EMerkleTreeNode (equivalent to MerkleNodeDB::node).
    pub fn node(&self) -> Result<crate::model::merkle_tree::node::EMerkleTreeNode, OxenError> {
        use crate::model::merkle_tree::node::{
            CommitNode, DirNode, FileChunkNode, FileNode, VNode, EMerkleTreeNode,
        };

        if self.node_data.is_empty() {
            // For read-only nodes, fetch from store
            let store = FileNodeStore::open(&self.repo_path)?;
            let node_data = store.get_node(&self.node_id)?
                .ok_or_else(|| OxenError::basic_str(format!("Node {} not found", self.node_id)))?;

            // Deserialize based on type
            match node_data.node_type {
                MerkleTreeNodeType::Commit => {
                    Ok(EMerkleTreeNode::Commit(CommitNode::deserialize(&node_data.data)?))
                }
                MerkleTreeNodeType::Dir => {
                    Ok(EMerkleTreeNode::Directory(DirNode::deserialize(&node_data.data)?))
                }
                MerkleTreeNodeType::File => {
                    Ok(EMerkleTreeNode::File(FileNode::deserialize(&node_data.data)?))
                }
                MerkleTreeNodeType::VNode => {
                    Ok(EMerkleTreeNode::VNode(VNode::deserialize(&node_data.data)?))
                }
                MerkleTreeNodeType::FileChunk => {
                    Ok(EMerkleTreeNode::FileChunk(FileChunkNode::deserialize(&node_data.data)?))
                }
            }
        } else {
            // For write nodes, deserialize from our buffer
            match self.dtype {
                MerkleTreeNodeType::Commit => {
                    Ok(EMerkleTreeNode::Commit(CommitNode::deserialize(&self.node_data)?))
                }
                MerkleTreeNodeType::Dir => {
                    Ok(EMerkleTreeNode::Directory(DirNode::deserialize(&self.node_data)?))
                }
                MerkleTreeNodeType::File => {
                    Ok(EMerkleTreeNode::File(FileNode::deserialize(&self.node_data)?))
                }
                MerkleTreeNodeType::VNode => {
                    Ok(EMerkleTreeNode::VNode(VNode::deserialize(&self.node_data)?))
                }
                MerkleTreeNodeType::FileChunk => {
                    Ok(EMerkleTreeNode::FileChunk(FileChunkNode::deserialize(&self.node_data)?))
                }
            }
        }
    }

    /// Get children as a map (equivalent to MerkleNodeDB::map).
    ///
    /// This returns a Vec of (hash, MerkleTreeNode) pairs, which was the format
    /// returned by the old MerkleNodeDB::map() method.
    pub fn map(&mut self) -> Result<Vec<(MerkleHash, crate::model::merkle_tree::node::MerkleTreeNode)>, OxenError> {
        use crate::model::merkle_tree::node::{
            CommitNode, DirNode, FileChunkNode, FileNode, VNode, EMerkleTreeNode, MerkleTreeNode,
        };

        let store = FileNodeStore::open(&self.repo_path)?;
        let children_data = store.get_children(&self.node_id)?;

        let mut result = Vec::new();
        for child_data in children_data {
            // Deserialize the child based on type
            let node = match child_data.node_type {
                MerkleTreeNodeType::Commit => {
                    EMerkleTreeNode::Commit(CommitNode::deserialize(&child_data.data)?)
                }
                MerkleTreeNodeType::Dir => {
                    EMerkleTreeNode::Directory(DirNode::deserialize(&child_data.data)?)
                }
                MerkleTreeNodeType::File => {
                    EMerkleTreeNode::File(FileNode::deserialize(&child_data.data)?)
                }
                MerkleTreeNodeType::VNode => {
                    EMerkleTreeNode::VNode(VNode::deserialize(&child_data.data)?)
                }
                MerkleTreeNodeType::FileChunk => {
                    EMerkleTreeNode::FileChunk(FileChunkNode::deserialize(&child_data.data)?)
                }
            };

            // Wrap in MerkleTreeNode
            let merkle_tree_node = MerkleTreeNode {
                hash: child_data.hash,
                node,
                parent_id: Some(self.node_id),
                children: Vec::new(), // Children are not loaded recursively
            };

            result.push((child_data.hash, merkle_tree_node));
        }

        Ok(result)
    }
}

impl Drop for NodeDB {
    fn drop(&mut self) {
        // Auto-close on drop, just like MerkleNodeDB
        if !self.children.is_empty() {
            if let Err(e) = self.close() {
                log::error!("Failed to close NodeDB on drop: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::merkle_tree::node::{DirNode, DirNodeOpts};
    use crate::util::hasher;
    use std::collections::HashMap;
    use std::str::FromStr;
    use tempfile::TempDir;

    #[test]
    fn test_node_db_compat_basic() {
        let temp_dir = TempDir::new().unwrap();
        let repo = LocalRepository::new(temp_dir.path(), None).unwrap();

        // Create a parent node
        let parent_hash = MerkleHash::from_str(&hasher::hash_str("parent")).unwrap();
        let parent_opts = DirNodeOpts {
            name: "".into(),
            hash: parent_hash,
            num_entries: 0,
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts: HashMap::new(),
            data_type_sizes: HashMap::new(),
        };
        let parent = DirNode::new(&repo, parent_opts).unwrap();
        let mut node_db = NodeDB::open_read_write(&repo, &parent, None).unwrap();

        // Add children
        let child1_hash = MerkleHash::from_str(&hasher::hash_str("child1")).unwrap();
        let child1_opts = DirNodeOpts {
            name: "child1".into(),
            hash: child1_hash,
            num_entries: 0,
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts: HashMap::new(),
            data_type_sizes: HashMap::new(),
        };
        let child1 = DirNode::new(&repo, child1_opts).unwrap();

        let child2_hash = MerkleHash::from_str(&hasher::hash_str("child2")).unwrap();
        let child2_opts = DirNodeOpts {
            name: "child2".into(),
            hash: child2_hash,
            num_entries: 0,
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts: HashMap::new(),
            data_type_sizes: HashMap::new(),
        };
        let child2 = DirNode::new(&repo, child2_opts).unwrap();

        node_db.add_child(&child1).unwrap();
        node_db.add_child(&child2).unwrap();

        // Close (writes to store)
        node_db.close().unwrap();

        // Verify the node exists
        let store = FileNodeStore::open(temp_dir.path()).unwrap();
        assert!(store.exists(&parent.hash()).unwrap());

        // Verify children were written
        let children = store.get_children(&parent.hash()).unwrap();
        assert_eq!(children.len(), 2);
    }
}
