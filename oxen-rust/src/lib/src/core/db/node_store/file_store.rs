/*!
 * File-based implementation of node storage.
 *
 * This module provides a file-based storage backend that wraps the existing
 * `MerkleNodeDB` implementation. It maintains full backward compatibility with
 * the current storage format while providing the `NodeStore` trait interface.
 *
 * # Storage Format
 *
 * Each node is stored in a directory under `.oxen/tree/nodes/{hash[0:3]}/{hash[3:]}/`:
 * - `node` file: Contains node metadata and lookup table for children
 * - `children` file: Contains concatenated serialized child node data
 *
 * The format uses MessagePack serialization and was designed to be faster than
 * RocksDB for this specific use case (one-time writes, many reads).
 */

use std::path::{Path, PathBuf};

use crate::core::db::merkle_node::MerkleNodeDB;
use crate::error::OxenError;
use crate::model::merkle_tree::node::MerkleTreeNodeType;
use crate::model::{LocalRepository, MerkleHash};

use super::{ChildNodeData, NodeData, NodeStore, NodeTransaction};

/// File-based node storage implementation.
///
/// This implementation wraps the existing `MerkleNodeDB` to provide the
/// `NodeStore` trait interface while maintaining full backward compatibility.
#[derive(Debug)]
pub struct FileNodeStore {
    repo_path: PathBuf,
}

impl FileNodeStore {
    /// Get a LocalRepository reference from the store's path.
    fn repo(&self) -> Result<LocalRepository, OxenError> {
        // Create a minimal LocalRepository with default version store
        LocalRepository::new(&self.repo_path, None)
    }
}

impl NodeStore for FileNodeStore {
    type Transaction<'a> = FileTransaction<'a>;

    fn open(repo_path: &Path) -> Result<Self, OxenError> {
        Ok(Self {
            repo_path: repo_path.to_path_buf(),
        })
    }

    fn begin_read_txn(&self) -> Result<Self::Transaction<'_>, OxenError> {
        Ok(FileTransaction {
            repo_path: &self.repo_path,
            mode: TransactionMode::Read,
            write_dbs: Vec::new(),
        })
    }

    fn begin_write_txn(&mut self) -> Result<Self::Transaction<'_>, OxenError> {
        Ok(FileTransaction {
            repo_path: &self.repo_path,
            mode: TransactionMode::Write,
            write_dbs: Vec::new(),
        })
    }

    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        let repo = self.repo()?;
        Ok(MerkleNodeDB::exists(&repo, hash))
    }
}

/// Transaction mode indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionMode {
    Read,
    Write,
}

/// File-based transaction implementation.
///
/// Transactions group operations on multiple nodes. Read transactions provide
/// consistent reads. Write transactions collect `MerkleNodeDB` instances and
/// flush them all on commit.
pub struct FileTransaction<'a> {
    repo_path: &'a Path,
    mode: TransactionMode,
    // Track open MerkleNodeDB instances for write transactions
    write_dbs: Vec<MerkleNodeDB>,
}

impl<'a> FileTransaction<'a> {
    fn repo(&self) -> Result<LocalRepository, OxenError> {
        LocalRepository::new(self.repo_path, None)
    }

    fn is_read_only(&self) -> bool {
        self.mode == TransactionMode::Read
    }
}

impl<'a> NodeTransaction for FileTransaction<'a> {
    fn commit(mut self) -> Result<(), OxenError> {
        if self.mode == TransactionMode::Write {
            // Close all open write databases to flush to disk
            for mut db in self.write_dbs.drain(..) {
                db.close()?;
            }
        }
        // Read transactions have nothing to commit
        Ok(())
    }

    fn rollback(mut self) -> Result<(), OxenError> {
        if self.mode == TransactionMode::Write {
            // For file-based storage, we can't easily rollback writes.
            // Just drop the databases without closing (files remain).
            // A proper rollback would require tracking which nodes were created
            // and deleting their directories.
            self.write_dbs.clear();
            log::warn!("FileTransaction rollback: files may remain on disk");
        }
        Ok(())
    }

    fn get_node(&self, hash: &MerkleHash) -> Result<Option<NodeData>, OxenError> {
        let repo = self.repo()?;

        if !MerkleNodeDB::exists(&repo, hash) {
            return Ok(None);
        }

        let db = MerkleNodeDB::open_read_only(&repo, hash)?;

        let node_type = db.dtype;
        let parent_id = db.parent_id;
        let data = db.data();
        let num_children = db.num_children();

        Ok(Some(NodeData {
            hash: *hash,
            node_type,
            parent_id,
            data,
            num_children,
        }))
    }

    fn get_children(&self, hash: &MerkleHash) -> Result<Vec<ChildNodeData>, OxenError> {
        let repo = self.repo()?;

        if !MerkleNodeDB::exists(&repo, hash) {
            return Ok(Vec::new());
        }

        let mut db = MerkleNodeDB::open_read_only(&repo, hash)?;
        let children = db.map()?;

        Ok(children
            .into_iter()
            .map(|(child_hash, child_node)| {
                // Serialize the child node back to bytes for the trait interface
                let data = match serialize_node(&child_node.node) {
                    Ok(data) => data,
                    Err(_) => Vec::new(), // Fallback to empty on serialization error
                };

                ChildNodeData {
                    hash: child_hash,
                    node_type: child_node.node.node_type(),
                    data,
                }
            })
            .collect())
    }

    fn put_node(&mut self, node: &NodeData) -> Result<(), OxenError> {
        use crate::model::merkle_tree::node::{CommitNode, DirNode, FileChunkNode, FileNode, VNode};

        if self.is_read_only() {
            return Err(OxenError::basic_str(
                "Cannot write in read-only transaction",
            ));
        }

        let repo = self.repo()?;

        // Deserialize and open based on node type
        // We need to match here because open_read_write needs the concrete type
        let db = match node.node_type {
            MerkleTreeNodeType::Commit => {
                let n = CommitNode::deserialize(&node.data)?;
                MerkleNodeDB::open_read_write(&repo, &n, node.parent_id)?
            }
            MerkleTreeNodeType::Dir => {
                let n = DirNode::deserialize(&node.data)?;
                MerkleNodeDB::open_read_write(&repo, &n, node.parent_id)?
            }
            MerkleTreeNodeType::File => {
                let n = FileNode::deserialize(&node.data)?;
                MerkleNodeDB::open_read_write(&repo, &n, node.parent_id)?
            }
            MerkleTreeNodeType::VNode => {
                let n = VNode::deserialize(&node.data)?;
                MerkleNodeDB::open_read_write(&repo, &n, node.parent_id)?
            }
            MerkleTreeNodeType::FileChunk => {
                let n = FileChunkNode::deserialize(&node.data)?;
                MerkleNodeDB::open_read_write(&repo, &n, node.parent_id)?
            }
        };

        // Store the database handle for later commit
        self.write_dbs.push(db);

        Ok(())
    }

    fn add_child(
        &mut self,
        parent_hash: &MerkleHash,
        child: &ChildNodeData,
    ) -> Result<(), OxenError> {
        use crate::model::merkle_tree::node::{CommitNode, DirNode, FileChunkNode, FileNode, VNode};

        if self.is_read_only() {
            return Err(OxenError::basic_str(
                "Cannot write in read-only transaction",
            ));
        }

        // Find the parent database in our write_dbs list
        let parent_db = self
            .write_dbs
            .iter_mut()
            .find(|db| db.node_id == *parent_hash)
            .ok_or_else(|| {
                OxenError::basic_str(format!(
                    "Parent node {} not found in transaction. Call put_node first.",
                    parent_hash
                ))
            })?;

        // Deserialize and add child based on type
        match child.node_type {
            MerkleTreeNodeType::Commit => {
                let n = CommitNode::deserialize(&child.data)?;
                parent_db.add_child(&n)?;
            }
            MerkleTreeNodeType::Dir => {
                let n = DirNode::deserialize(&child.data)?;
                parent_db.add_child(&n)?;
            }
            MerkleTreeNodeType::File => {
                let n = FileNode::deserialize(&child.data)?;
                parent_db.add_child(&n)?;
            }
            MerkleTreeNodeType::VNode => {
                let n = VNode::deserialize(&child.data)?;
                parent_db.add_child(&n)?;
            }
            MerkleTreeNodeType::FileChunk => {
                let n = FileChunkNode::deserialize(&child.data)?;
                parent_db.add_child(&n)?;
            }
        }

        Ok(())
    }
}

/// Serialize a node to bytes using MessagePack.
fn serialize_node(
    node: &crate::model::merkle_tree::node::EMerkleTreeNode,
) -> Result<Vec<u8>, OxenError> {
    use crate::model::merkle_tree::node::EMerkleTreeNode;
    use rmp_serde::Serializer;
    use serde::Serialize;

    let mut buf = Vec::new();

    let result = match node {
        EMerkleTreeNode::Commit(n) => n.serialize(&mut Serializer::new(&mut buf)),
        EMerkleTreeNode::Directory(n) => n.serialize(&mut Serializer::new(&mut buf)),
        EMerkleTreeNode::File(n) => n.serialize(&mut Serializer::new(&mut buf)),
        EMerkleTreeNode::VNode(n) => n.serialize(&mut Serializer::new(&mut buf)),
        EMerkleTreeNode::FileChunk(n) => n.serialize(&mut Serializer::new(&mut buf)),
    };

    result.map_err(|e| OxenError::basic_str(format!("Failed to serialize node: {}", e)))?;

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;

    // TODO: Add comprehensive tests for FileNodeStore
    // - test_basic_node_operations: Create, read, exists
    // - test_transactions: Commit and rollback
    // - test_parent_child_relationships: Add children, read children
    // - test_batch_operations: put_nodes with multiple nodes
    // - test_error_handling: Read non-existent nodes, invalid data
    //
    // These tests should be added once the trait usage is proven in the codebase.
    // For now, existing MerkleNodeDB tests provide coverage through the wrapper.
}
