/*!
 * Storage abstraction for Merkle tree nodes.
 *
 * This module defines traits for node storage backends, providing a clean
 * interface for reading and writing nodes to persistent storage. The trait
 * design allows swapping between different storage implementations (file-based,
 * LMDB, etc.) without changing client code.
 *
 * # Design Principles
 *
 * - **Raw bytes interface**: The trait works with `Vec<u8>` for node data,
 *   leaving serialization/deserialization to the caller. This decouples the
 *   storage layer from node type definitions and serialization format.
 *
 * - **Explicit relationships**: Parent-child relationships are explicitly
 *   managed through the trait interface, enabling efficient storage in
 *   key-value stores and relational structures.
 *
 * - **Transaction support**: Batch operations are grouped in transactions
 *   for atomicity and performance. Critical for LMDB and other transactional
 *   storage backends.
 *
 * - **Synchronous API**: Matches existing codebase patterns and LMDB's
 *   synchronous nature. Simpler to integrate than async.
 *
 * # Example Usage
 *
 * ```no_run
 * use oxen_rust::core::db::node_store::{NodeStore, NodeData, ChildNodeData};
 * use oxen_rust::model::{MerkleHash, merkle_tree::node::MerkleTreeNodeType};
 *
 * # fn example(repo_path: &std::path::Path) -> Result<(), oxen_rust::error::OxenError> {
 * // Open a store
 * let mut store = FileNodeStore::open(repo_path)?;
 *
 * // Begin a write transaction
 * let mut txn = store.begin_write_txn()?;
 *
 * // Create and write a node
 * let node_data = NodeData::new(
 *     hash,
 *     MerkleTreeNodeType::Dir,
 *     Some(parent_hash),
 *     serialized_bytes,
 * );
 * txn.put_node(&node_data)?;
 *
 * // Add children
 * txn.add_child(&hash, &child_data1)?;
 * txn.add_child(&hash, &child_data2)?;
 *
 * // Commit atomically
 * txn.commit()?;
 * # Ok(())
 * # }
 * ```
 */

use std::fmt::Debug;
use std::path::Path;

use crate::error::OxenError;
use crate::model::merkle_tree::node::MerkleTreeNodeType;
use crate::model::MerkleHash;

pub mod file_store;

/// Main trait for node storage backends.
///
/// This trait defines the interface for persistent storage of Merkle tree nodes.
/// Implementations handle the physical storage layer (files, LMDB, etc.) while
/// working with raw bytes for node data.
///
/// The trait uses associated types for transactions, allowing each implementation
/// to provide its own transaction type with appropriate lifetime management.
pub trait NodeStore: Debug + Send + Sync {
    /// Transaction handle for batch operations.
    ///
    /// Transactions provide atomicity and improved performance for multi-operation
    /// workflows. The lifetime parameter ensures transactions don't outlive the store.
    type Transaction<'a>: NodeTransaction
    where
        Self: 'a;

    /// Open the store for a repository.
    ///
    /// # Arguments
    ///
    /// * `repo_path` - Path to the repository root directory
    ///
    /// # Returns
    ///
    /// A new store instance, or an error if the store cannot be opened.
    fn open(repo_path: &Path) -> Result<Self, OxenError>
    where
        Self: Sized;

    /// Begin a read-only transaction.
    ///
    /// Read transactions allow consistent reads across multiple operations
    /// without blocking writers.
    ///
    /// # Returns
    ///
    /// A read transaction, or an error if the transaction cannot be started.
    fn begin_read_txn(&self) -> Result<Self::Transaction<'_>, OxenError>;

    /// Begin a read-write transaction.
    ///
    /// Write transactions group multiple write operations for atomic commit.
    /// Most storage backends allow only one write transaction at a time.
    ///
    /// # Returns
    ///
    /// A write transaction, or an error if the transaction cannot be started.
    fn begin_write_txn(&mut self) -> Result<Self::Transaction<'_>, OxenError>;

    /// Check if a node exists in storage.
    ///
    /// # Arguments
    ///
    /// * `hash` - The node's hash identifier
    ///
    /// # Returns
    ///
    /// `true` if the node exists, `false` otherwise, or an error.
    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError>;

    /// Convenience method: Read node without explicit transaction.
    ///
    /// Creates an implicit read transaction for a single read operation.
    ///
    /// # Arguments
    ///
    /// * `hash` - The node's hash identifier
    ///
    /// # Returns
    ///
    /// The node data if found, or `None` if the node doesn't exist.
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<NodeData>, OxenError> {
        let txn = self.begin_read_txn()?;
        txn.get_node(hash)
    }

    /// Convenience method: Read children without explicit transaction.
    ///
    /// Creates an implicit read transaction for a single read operation.
    ///
    /// # Arguments
    ///
    /// * `hash` - The parent node's hash identifier
    ///
    /// # Returns
    ///
    /// A vector of child node data, or an error.
    fn get_children(&self, hash: &MerkleHash) -> Result<Vec<ChildNodeData>, OxenError> {
        let txn = self.begin_read_txn()?;
        txn.get_children(hash)
    }
}

/// Transaction interface for node operations.
///
/// Transactions group multiple operations for atomic execution. Read transactions
/// provide consistent snapshots. Write transactions ensure all operations succeed
/// or fail together.
///
/// Transactions should be explicitly committed or rolled back. Some implementations
/// may auto-rollback on drop if neither commit nor rollback was called.
pub trait NodeTransaction {
    /// Commit the transaction.
    ///
    /// For write transactions, this atomically persists all operations to storage.
    /// For read transactions, this releases resources.
    ///
    /// Consumes the transaction to prevent further use.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the commit fails.
    fn commit(self) -> Result<(), OxenError>
    where
        Self: Sized;

    /// Rollback the transaction.
    ///
    /// Discards all operations performed in this transaction. For write transactions,
    /// no changes will be persisted. For read transactions, this releases resources.
    ///
    /// Consumes the transaction to prevent further use.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the rollback fails.
    fn rollback(self) -> Result<(), OxenError>
    where
        Self: Sized;

    /// Read a node by hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The node's hash identifier
    ///
    /// # Returns
    ///
    /// The node data if found, or `None` if the node doesn't exist.
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<NodeData>, OxenError>;

    /// Read all children of a node.
    ///
    /// Returns children in storage order (typically insertion order).
    ///
    /// # Arguments
    ///
    /// * `hash` - The parent node's hash identifier
    ///
    /// # Returns
    ///
    /// A vector of child node data. Empty vector if node has no children or doesn't exist.
    fn get_children(&self, hash: &MerkleHash) -> Result<Vec<ChildNodeData>, OxenError>;

    /// Write a new node (write transactions only).
    ///
    /// Creates or overwrites a node in storage. The node's hash serves as its identifier.
    ///
    /// # Arguments
    ///
    /// * `node` - The node data to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the write fails.
    fn put_node(&mut self, node: &NodeData) -> Result<(), OxenError>;

    /// Add a child relationship (write transactions only).
    ///
    /// Records that a node has a specific child. The child data includes the child's
    /// hash, type, and serialized content.
    ///
    /// # Arguments
    ///
    /// * `parent_hash` - The parent node's hash identifier
    /// * `child` - The child node data
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the operation fails.
    fn add_child(&mut self, parent_hash: &MerkleHash, child: &ChildNodeData)
        -> Result<(), OxenError>;

    /// Batch write multiple nodes (write transactions only).
    ///
    /// Convenience method for writing multiple nodes in one call. Default implementation
    /// calls `put_node` for each node, but implementations may provide optimized batch writes.
    ///
    /// # Arguments
    ///
    /// * `nodes` - Slice of node data to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if any write fails.
    fn put_nodes(&mut self, nodes: &[NodeData]) -> Result<(), OxenError> {
        for node in nodes {
            self.put_node(node)?;
        }
        Ok(())
    }
}

/// Raw node data returned from storage.
///
/// This structure contains the node's metadata and serialized content as raw bytes.
/// The caller is responsible for deserializing the data into the appropriate node type.
#[derive(Debug, Clone)]
pub struct NodeData {
    /// Node hash (unique identifier).
    pub hash: MerkleHash,

    /// Node type (Commit, Dir, VNode, File, FileChunk).
    pub node_type: MerkleTreeNodeType,

    /// Parent node hash, if any.
    ///
    /// `None` for root nodes (typically commit nodes without parents).
    pub parent_id: Option<MerkleHash>,

    /// Serialized node data (format determined by caller).
    ///
    /// Typically MessagePack-serialized node structure, but the storage
    /// layer doesn't interpret this data.
    pub data: Vec<u8>,

    /// Number of children this node has.
    ///
    /// Cached for quick access without loading children.
    pub num_children: u64,
}

/// Child node metadata.
///
/// Contains the essential information about a child node: its hash, type, and serialized data.
#[derive(Debug, Clone)]
pub struct ChildNodeData {
    /// Child node hash.
    pub hash: MerkleHash,

    /// Child node type.
    pub node_type: MerkleTreeNodeType,

    /// Serialized child data.
    pub data: Vec<u8>,
}

impl NodeData {
    /// Create new node data.
    ///
    /// # Arguments
    ///
    /// * `hash` - Node's hash identifier
    /// * `node_type` - Type of node (Commit, Dir, etc.)
    /// * `parent_id` - Optional parent node hash
    /// * `data` - Serialized node content
    ///
    /// # Returns
    ///
    /// A new `NodeData` instance with `num_children` initialized to 0.
    pub fn new(
        hash: MerkleHash,
        node_type: MerkleTreeNodeType,
        parent_id: Option<MerkleHash>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            hash,
            node_type,
            parent_id,
            data,
            num_children: 0,
        }
    }

    /// Create node data with a specific child count.
    ///
    /// # Arguments
    ///
    /// * `hash` - Node's hash identifier
    /// * `node_type` - Type of node
    /// * `parent_id` - Optional parent node hash
    /// * `data` - Serialized node content
    /// * `num_children` - Number of children
    ///
    /// # Returns
    ///
    /// A new `NodeData` instance with the specified child count.
    pub fn with_children(
        hash: MerkleHash,
        node_type: MerkleTreeNodeType,
        parent_id: Option<MerkleHash>,
        data: Vec<u8>,
        num_children: u64,
    ) -> Self {
        Self {
            hash,
            node_type,
            parent_id,
            data,
            num_children,
        }
    }
}

impl ChildNodeData {
    /// Create new child node data.
    ///
    /// # Arguments
    ///
    /// * `hash` - Child's hash identifier
    /// * `node_type` - Type of child node
    /// * `data` - Serialized child content
    ///
    /// # Returns
    ///
    /// A new `ChildNodeData` instance.
    pub fn new(hash: MerkleHash, node_type: MerkleTreeNodeType, data: Vec<u8>) -> Self {
        Self {
            hash,
            node_type,
            data,
        }
    }
}
