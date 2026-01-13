/*!
 * LMDB-based implementation of node storage using heed and rkyv.
 *
 * This module provides a high-performance storage backend using LMDB (via heed)
 * and zero-copy serialization (via rkyv). It implements the same `NodeStore` trait
 * as the file-based implementation but with significantly improved performance.
 *
 * # Storage Format
 *
 * LMDB Environment: `.oxen/tree/nodes.mdb/`
 * - `nodes` database: MerkleHash (u128) → NodeStorageData (rkyv-serialized)
 * - `children` database: (parent_hash: u128, child_index: u64) → ChildStorageData (rkyv-serialized)
 *
 * # Performance Benefits
 *
 * - **No file-open overhead**: Single mmap for all nodes
 * - **Zero-copy reads**: rkyv allows direct memory access without deserialization
 * - **Atomic batch writes**: LMDB transactions group operations efficiently
 * - **Better memory locality**: Sequential storage improves cache performance
 */

use std::path::Path;
use std::sync::Arc;

use heed::{Database, Env, EnvOpenOptions, byteorder};
use heed::types::{Bytes, U128};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer as RkyvSerializer;

use crate::error::OxenError;
use crate::model::merkle_tree::node::MerkleTreeNodeType;
use crate::model::MerkleHash;

use super::{ChildNodeData, NodeData, NodeStore, NodeTransaction};

/// Storage structure for node data with rkyv serialization.
///
/// This mirrors `NodeData` but with rkyv's Archive, Serialize, and Deserialize traits.
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
#[archive(check_bytes)]
struct NodeStorageData {
    hash: u128,
    node_type: u8,  // MerkleTreeNodeType as u8
    parent_id: Option<u128>,
    data: Vec<u8>,
    num_children: u64,
}

/// Storage structure for child node data with rkyv serialization.
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
#[archive(check_bytes)]
struct ChildStorageData {
    hash: u128,
    node_type: u8,
    data: Vec<u8>,
}

/// Composite key for children database: (parent_hash, child_index).
///
/// This allows efficient lookups of all children for a given parent
/// by using LMDB's range queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
struct ChildKey {
    parent_hash: u128,
    child_index: u64,
}

impl ChildKey {
    fn new(parent_hash: u128, child_index: u64) -> Self {
        Self {
            parent_hash,
            child_index,
        }
    }

    fn to_bytes(&self) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[0..16].copy_from_slice(&self.parent_hash.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.child_index.to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, OxenError> {
        if bytes.len() != 24 {
            return Err(OxenError::basic_str("Invalid ChildKey bytes length"));
        }
        let parent_hash = u128::from_be_bytes(bytes[0..16].try_into().unwrap());
        let child_index = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        Ok(Self {
            parent_hash,
            child_index,
        })
    }
}

/// LMDB-based node storage implementation.
///
/// Uses heed for safe LMDB access and rkyv for zero-copy serialization.
/// The environment is shared across threads using Arc<Mutex<>>.
#[derive(Debug, Clone)]
pub struct LmdbNodeStore {
    env: Arc<Env>,
    nodes_db: Database<U128<byteorder::BE>, Bytes>,
    children_db: Database<Bytes, Bytes>,
}

impl LmdbNodeStore {
    /// Helper to serialize node data with rkyv.
    fn serialize_node_data(node: &NodeData) -> Result<Vec<u8>, OxenError> {
        let storage_data = NodeStorageData {
            hash: node.hash.to_u128(),
            node_type: node_type_to_u8(node.node_type),
            parent_id: node.parent_id.map(|h| h.to_u128()),
            data: node.data.clone(),
            num_children: node.num_children,
        };

        let mut serializer = AllocSerializer::<256>::default();
        serializer
            .serialize_value(&storage_data)
            .map_err(|e| OxenError::basic_str(format!("Failed to serialize node: {}", e)))?;

        Ok(serializer.into_serializer().into_inner().to_vec())
    }

    /// Helper to deserialize node data with rkyv.
    fn deserialize_node_data(bytes: &[u8]) -> Result<NodeData, OxenError> {
        let archived = unsafe {
            rkyv::archived_root::<NodeStorageData>(bytes)
        };

        // Validate the archived data
        rkyv::check_archived_root::<NodeStorageData>(bytes)
            .map_err(|e| OxenError::basic_str(format!("Invalid node data: {}", e)))?;

        let parent_id = match &archived.parent_id {
            rkyv::option::ArchivedOption::Some(h) => Some(MerkleHash::new(*h)),
            rkyv::option::ArchivedOption::None => None,
        };

        Ok(NodeData {
            hash: MerkleHash::new(archived.hash),
            node_type: u8_to_node_type(archived.node_type)?,
            parent_id,
            data: archived.data.to_vec(),
            num_children: archived.num_children,
        })
    }

    /// Helper to serialize child data with rkyv.
    fn serialize_child_data(child: &ChildNodeData) -> Result<Vec<u8>, OxenError> {
        let storage_data = ChildStorageData {
            hash: child.hash.to_u128(),
            node_type: node_type_to_u8(child.node_type),
            data: child.data.clone(),
        };

        let mut serializer = AllocSerializer::<256>::default();
        serializer
            .serialize_value(&storage_data)
            .map_err(|e| OxenError::basic_str(format!("Failed to serialize child: {}", e)))?;

        Ok(serializer.into_serializer().into_inner().to_vec())
    }

    /// Helper to deserialize child data with rkyv.
    fn deserialize_child_data(bytes: &[u8]) -> Result<ChildNodeData, OxenError> {
        let archived = unsafe {
            rkyv::archived_root::<ChildStorageData>(bytes)
        };

        // Validate the archived data
        rkyv::check_archived_root::<ChildStorageData>(bytes)
            .map_err(|e| OxenError::basic_str(format!("Invalid child data: {}", e)))?;

        Ok(ChildNodeData {
            hash: MerkleHash::new(archived.hash),
            node_type: u8_to_node_type(archived.node_type)?,
            data: archived.data.to_vec(),
        })
    }

    /// Count the number of children for a node.
    fn count_children(&self, parent_hash: &MerkleHash) -> Result<u64, OxenError> {
        let rtxn = self.env.read_txn()
            .map_err(|e| OxenError::basic_str(format!("Failed to begin read txn: {}", e)))?;

        let parent_u128 = parent_hash.to_u128();
        let prefix = parent_u128.to_be_bytes();

        let count = self.children_db
            .prefix_iter(&rtxn, &prefix)
            .map_err(|e| OxenError::basic_str(format!("Failed to count children: {}", e)))?
            .count();

        Ok(count as u64)
    }
}

impl NodeStore for LmdbNodeStore {
    type Transaction<'a> = LmdbTransaction<'a>;

    fn open(repo_path: &Path) -> Result<Self, OxenError>
    where
        Self: Sized,
    {
        let db_path = repo_path.join(".oxen").join("tree").join("nodes.mdb");

        // Create directory if it doesn't exist
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)
                .map_err(|e| OxenError::basic_str(format!("Failed to create db directory: {}", e)))?;
        }

        // Open LMDB environment with 1TB map size (sparse file)
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024 * 1024) // 1TB
                .max_dbs(2) // nodes and children databases
                .open(&db_path)
                .map_err(|e| OxenError::basic_str(format!("Failed to open LMDB env: {}", e)))?
        };

        // Open/create the two databases
        let mut wtxn = env.write_txn()
            .map_err(|e| OxenError::basic_str(format!("Failed to begin write txn: {}", e)))?;

        let nodes_db: Database<U128<byteorder::BE>, Bytes> = env
            .create_database(&mut wtxn, Some("nodes"))
            .map_err(|e| OxenError::basic_str(format!("Failed to create nodes db: {}", e)))?;

        let children_db: Database<Bytes, Bytes> = env
            .create_database(&mut wtxn, Some("children"))
            .map_err(|e| OxenError::basic_str(format!("Failed to create children db: {}", e)))?;

        wtxn.commit()
            .map_err(|e| OxenError::basic_str(format!("Failed to commit db creation: {}", e)))?;

        Ok(Self {
            env: Arc::new(env),
            nodes_db,
            children_db,
        })
    }

    fn begin_read_txn(&self) -> Result<Self::Transaction<'_>, OxenError> {
        let rtxn = self.env.read_txn()
            .map_err(|e| OxenError::basic_str(format!("Failed to begin read txn: {}", e)))?;

        Ok(LmdbTransaction::Read(LmdbReadTransaction {
            store: self,
            rtxn: Some(rtxn),
        }))
    }

    fn begin_write_txn(&mut self) -> Result<Self::Transaction<'_>, OxenError> {
        let wtxn = self.env.write_txn()
            .map_err(|e| OxenError::basic_str(format!("Failed to begin write txn: {}", e)))?;

        Ok(LmdbTransaction::Write(LmdbWriteTransaction {
            store: self,
            wtxn: Some(wtxn),
            child_counts: std::collections::HashMap::new(),
        }))
    }

    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        let rtxn = self.env.read_txn()
            .map_err(|e| OxenError::basic_str(format!("Failed to begin read txn: {}", e)))?;

        let hash_u128 = hash.to_u128();
        let exists = self.nodes_db.get(&rtxn, &hash_u128)
            .map_err(|e| OxenError::basic_str(format!("Failed to check existence: {}", e)))?
            .is_some();

        Ok(exists)
    }
}

/// LMDB transaction enum wrapping both read and write transactions.
pub enum LmdbTransaction<'a> {
    Read(LmdbReadTransaction<'a>),
    Write(LmdbWriteTransaction<'a>),
}

/// LMDB read-only transaction.
pub struct LmdbReadTransaction<'a> {
    store: &'a LmdbNodeStore,
    rtxn: Option<heed::RoTxn<'a>>,
}

/// LMDB read-write transaction.
pub struct LmdbWriteTransaction<'a> {
    store: &'a LmdbNodeStore,
    wtxn: Option<heed::RwTxn<'a>>,
    // Track number of children added per parent for num_children updates
    child_counts: std::collections::HashMap<u128, u64>,
}

impl<'a> NodeTransaction for LmdbTransaction<'a> {
    fn commit(self) -> Result<(), OxenError> {
        match self {
            LmdbTransaction::Read(mut txn) => {
                // Read transactions don't need commit, just drop
                txn.rtxn.take();
                Ok(())
            }
            LmdbTransaction::Write(mut txn) => {
                if let Some(mut wtxn) = txn.wtxn.take() {
                    // Update num_children for all parents that had children added
                    for (parent_hash, _count) in &txn.child_counts {
                        // Recalculate actual child count from database
                        let prefix = parent_hash.to_be_bytes();

                        let count = txn.store.children_db
                            .prefix_iter(&wtxn, &prefix)
                            .map_err(|e| OxenError::basic_str(format!("Failed to count children: {}", e)))?
                            .count() as u64;

                        // Update the node's num_children field
                        if let Some(bytes) = txn.store.nodes_db.get(&wtxn, parent_hash)
                            .map_err(|e| OxenError::basic_str(format!("Failed to get node: {}", e)))?
                        {
                            let mut node = LmdbNodeStore::deserialize_node_data(bytes)?;
                            node.num_children = count;
                            let updated_bytes = LmdbNodeStore::serialize_node_data(&node)?;
                            txn.store.nodes_db.put(&mut wtxn, parent_hash, &updated_bytes)
                                .map_err(|e| OxenError::basic_str(format!("Failed to update node: {}", e)))?;
                        }
                    }

                    wtxn.commit()
                        .map_err(|e| OxenError::basic_str(format!("Failed to commit write txn: {}", e)))?;
                }
                Ok(())
            }
        }
    }

    fn rollback(self) -> Result<(), OxenError> {
        match self {
            LmdbTransaction::Read(mut txn) => {
                txn.rtxn.take();
                Ok(())
            }
            LmdbTransaction::Write(mut txn) => {
                if let Some(wtxn) = txn.wtxn.take() {
                    wtxn.abort();
                }
                Ok(())
            }
        }
    }

    fn get_node(&self, hash: &MerkleHash) -> Result<Option<NodeData>, OxenError> {
        let hash_u128 = hash.to_u128();

        let bytes = match self {
            LmdbTransaction::Read(txn) => {
                let rtxn = txn.rtxn.as_ref()
                    .ok_or_else(|| OxenError::basic_str("Transaction already completed"))?;
                txn.store.nodes_db.get(rtxn, &hash_u128)
                    .map_err(|e| OxenError::basic_str(format!("Failed to get node: {}", e)))?
            }
            LmdbTransaction::Write(txn) => {
                let wtxn = txn.wtxn.as_ref()
                    .ok_or_else(|| OxenError::basic_str("Transaction already completed"))?;
                txn.store.nodes_db.get(wtxn, &hash_u128)
                    .map_err(|e| OxenError::basic_str(format!("Failed to get node: {}", e)))?
            }
        };

        match bytes {
            Some(bytes) => Ok(Some(LmdbNodeStore::deserialize_node_data(bytes)?)),
            None => Ok(None),
        }
    }

    fn get_children(&self, hash: &MerkleHash) -> Result<Vec<ChildNodeData>, OxenError> {
        let parent_u128 = hash.to_u128();
        let prefix = parent_u128.to_be_bytes();

        let iter = match self {
            LmdbTransaction::Read(txn) => {
                let rtxn = txn.rtxn.as_ref()
                    .ok_or_else(|| OxenError::basic_str("Transaction already completed"))?;
                txn.store.children_db.prefix_iter(rtxn, &prefix)
                    .map_err(|e| OxenError::basic_str(format!("Failed to get children: {}", e)))?
            }
            LmdbTransaction::Write(txn) => {
                let wtxn = txn.wtxn.as_ref()
                    .ok_or_else(|| OxenError::basic_str("Transaction already completed"))?;
                txn.store.children_db.prefix_iter(wtxn, &prefix)
                    .map_err(|e| OxenError::basic_str(format!("Failed to get children: {}", e)))?
            }
        };

        let mut children = Vec::new();
        for result in iter {
            let (_key, value) = result
                .map_err(|e| OxenError::basic_str(format!("Failed to iterate children: {}", e)))?;
            children.push(LmdbNodeStore::deserialize_child_data(value)?);
        }

        Ok(children)
    }

    fn put_node(&mut self, node: &NodeData) -> Result<(), OxenError> {
        match self {
            LmdbTransaction::Read(_) => {
                Err(OxenError::basic_str("Cannot write in read-only transaction"))
            }
            LmdbTransaction::Write(txn) => {
                let wtxn = txn.wtxn.as_mut()
                    .ok_or_else(|| OxenError::basic_str("Transaction already completed"))?;

                let hash_u128 = node.hash.to_u128();
                let bytes = LmdbNodeStore::serialize_node_data(node)?;

                txn.store.nodes_db.put(wtxn, &hash_u128, &bytes)
                    .map_err(|e| OxenError::basic_str(format!("Failed to put node: {}", e)))?;

                Ok(())
            }
        }
    }

    fn add_child(
        &mut self,
        parent_hash: &MerkleHash,
        child: &ChildNodeData,
    ) -> Result<(), OxenError> {
        match self {
            LmdbTransaction::Read(_) => {
                Err(OxenError::basic_str("Cannot write in read-only transaction"))
            }
            LmdbTransaction::Write(txn) => {
                let wtxn = txn.wtxn.as_mut()
                    .ok_or_else(|| OxenError::basic_str("Transaction already completed"))?;

                let parent_u128 = parent_hash.to_u128();

                // Find the next child index by counting existing children
                let prefix = parent_u128.to_be_bytes();

                let count = txn.store.children_db
                    .prefix_iter(wtxn, &prefix)
                    .map_err(|e| OxenError::basic_str(format!("Failed to count children: {}", e)))?
                    .count() as u64;

                let child_key = ChildKey::new(parent_u128, count);
                let key_bytes = child_key.to_bytes();
                let value_bytes = LmdbNodeStore::serialize_child_data(child)?;

                txn.store.children_db.put(wtxn, &key_bytes, &value_bytes)
                    .map_err(|e| OxenError::basic_str(format!("Failed to add child: {}", e)))?;

                // Track that this parent has children for num_children update on commit
                *txn.child_counts.entry(parent_u128).or_insert(0) += 1;

                Ok(())
            }
        }
    }
}

/// Convert MerkleTreeNodeType to u8 for storage.
fn node_type_to_u8(node_type: MerkleTreeNodeType) -> u8 {
    match node_type {
        MerkleTreeNodeType::Commit => 0,
        MerkleTreeNodeType::Dir => 1,
        MerkleTreeNodeType::File => 2,
        MerkleTreeNodeType::VNode => 3,
        MerkleTreeNodeType::FileChunk => 4,
    }
}

/// Convert u8 to MerkleTreeNodeType.
fn u8_to_node_type(value: u8) -> Result<MerkleTreeNodeType, OxenError> {
    match value {
        0 => Ok(MerkleTreeNodeType::Commit),
        1 => Ok(MerkleTreeNodeType::Dir),
        2 => Ok(MerkleTreeNodeType::File),
        3 => Ok(MerkleTreeNodeType::VNode),
        4 => Ok(MerkleTreeNodeType::FileChunk),
        _ => Err(OxenError::basic_str(format!("Invalid node type: {}", value))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_lmdb_store_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();

        // Open store
        let mut store = LmdbNodeStore::open(repo_path).unwrap();

        // Create a test node
        let hash = MerkleHash::new(12345);
        let node = NodeData::new(
            hash,
            MerkleTreeNodeType::Dir,
            None,
            vec![1, 2, 3, 4],
        );

        // Write node
        let mut wtxn = store.begin_write_txn().unwrap();
        wtxn.put_node(&node).unwrap();
        wtxn.commit().unwrap();

        // Check exists
        assert!(store.exists(&hash).unwrap());

        // Read node
        let read_node = store.get_node(&hash).unwrap().unwrap();
        assert_eq!(read_node.hash, hash);
        assert_eq!(read_node.data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_lmdb_store_children() {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();

        let mut store = LmdbNodeStore::open(repo_path).unwrap();

        // Create parent node
        let parent_hash = MerkleHash::new(100);
        let parent_node = NodeData::new(
            parent_hash,
            MerkleTreeNodeType::Dir,
            None,
            vec![],
        );

        // Create children
        let child1 = ChildNodeData::new(
            MerkleHash::new(101),
            MerkleTreeNodeType::File,
            vec![1, 2],
        );
        let child2 = ChildNodeData::new(
            MerkleHash::new(102),
            MerkleTreeNodeType::File,
            vec![3, 4],
        );

        // Write parent and children
        let mut wtxn = store.begin_write_txn().unwrap();
        wtxn.put_node(&parent_node).unwrap();
        wtxn.add_child(&parent_hash, &child1).unwrap();
        wtxn.add_child(&parent_hash, &child2).unwrap();
        wtxn.commit().unwrap();

        // Read children
        let children = store.get_children(&parent_hash).unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].hash, MerkleHash::new(101));
        assert_eq!(children[1].hash, MerkleHash::new(102));

        // Check num_children was updated
        let parent = store.get_node(&parent_hash).unwrap().unwrap();
        assert_eq!(parent.num_children, 2);
    }
}
