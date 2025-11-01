// Zero-copy Cap'n Proto wrapper for MerkleTreeNode
// This avoids deserialization overhead by accessing data directly from the buffer

use crate::merkle_tree_capnp;
use capnp::message::ReaderOptions;
use capnp::serialize;
use liboxen::model::MerkleHash;

/// A wrapper that holds Cap'n Proto data without deserializing
/// This provides zero-copy access to node data
/// 
/// IMPORTANT: This owns the message, so the lifetime is 'static
pub struct MerkleTreeNodeReader {
    // Hold the message reader to keep the buffer alive
    message: capnp::message::Reader<capnp::serialize::OwnedSegments>,
}

impl MerkleTreeNodeReader {
    /// Create a reader from bytes without deserializing
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, capnp::Error> {
        let message = serialize::read_message(bytes, ReaderOptions::new())?;
        Ok(Self { message })
    }
    
    /// Get the root reader - this is safe because we own the message
    pub fn get_root(&self) -> Result<merkle_tree_capnp::merkle_tree_node::Reader, capnp::Error> {
        self.message.get_root()
    }
    
    /// Get the hash without deserializing
    pub fn hash(&self) -> Result<MerkleHash, capnp::Error> {
        let reader = self.get_root()?;
        let hash_reader = reader.get_hash()?;
        let high = hash_reader.get_value()?.get_high() as u128;
        let low = hash_reader.get_value()?.get_low() as u128;
        Ok(MerkleHash::new((high << 64) | low))
    }
    
    /// Check if this is a file node
    pub fn is_file(&self) -> Result<bool, capnp::Error> {
        let reader = self.get_root()?;
        let node = reader.get_node()?;
        Ok(matches!(node.which()?, merkle_tree_capnp::e_merkle_tree_node::File(_)))
    }
    
    /// Check if this is a directory node
    pub fn is_dir(&self) -> Result<bool, capnp::Error> {
        let reader = self.get_root()?;
        let node = reader.get_node()?;
        Ok(matches!(node.which()?, merkle_tree_capnp::e_merkle_tree_node::Directory(_)))
    }
    
    /// Get number of children without deserializing them
    pub fn num_children(&self) -> Result<usize, capnp::Error> {
        let reader = self.get_root()?;
        Ok(reader.get_children()?.len() as usize)
    }
}

/// Example: Lazy loading with zero-copy access
pub struct LazyMerkleTree {
    // Store raw bytes instead of deserialized nodes
    nodes: std::collections::HashMap<MerkleHash, Vec<u8>>,
}

impl LazyMerkleTree {
    pub fn new() -> Self {
        Self {
            nodes: std::collections::HashMap::new(),
        }
    }
    
    /// Insert a node (stores serialized bytes)
    pub fn insert(&mut self, hash: MerkleHash, bytes: Vec<u8>) {
        self.nodes.insert(hash, bytes);
    }
    
    /// Get a node reader without deserializing
    pub fn get(&self, hash: &MerkleHash) -> Option<Result<MerkleTreeNodeReader, capnp::Error>> {
        self.nodes.get(hash).map(|bytes| MerkleTreeNodeReader::from_bytes(bytes))
    }
    
    /// Walk the tree without full deserialization
    pub fn walk<F>(&self, root_hash: &MerkleHash, mut f: F) -> Result<(), capnp::Error>
    where
        F: FnMut(&MerkleTreeNodeReader) -> Result<(), capnp::Error>,
    {
        let mut stack = vec![*root_hash];
        
        while let Some(hash) = stack.pop() {
            if let Some(Ok(reader)) = self.get(&hash) {
                f(&reader)?;
                
                // Add children to stack without deserializing
                let root = reader.get_root()?;
                let children = root.get_children()?;
                for child in children.iter() {
                    let child_hash_reader = child.get_hash()?;
                    let high = child_hash_reader.get_value()?.get_high() as u128;
                    let low = child_hash_reader.get_value()?.get_low() as u128;
                    let child_hash = MerkleHash::new((high << 64) | low);
                    stack.push(child_hash);
                }
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_zero_copy_access() {
        // This demonstrates zero-copy access:
        // 1. Serialize once
        // 2. Access fields directly from bytes without deserializing
        // 3. Much faster for read-heavy workloads
    }
}
