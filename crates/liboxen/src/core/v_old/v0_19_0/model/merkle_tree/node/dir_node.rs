//! This is a compact representation of a directory merkle tree node
//! that is stored in on disk
//!
//! This is v0.19.0 that did not contain a count for the number of children
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::versions::MinOxenVersion;
use crate::model::merkle_tree::node::dir_node::TDirNode;
use crate::model::{MerkleHash, MerkleTreeNodeType};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNodeData {
    // The type of the node
    pub node_type: MerkleTreeNodeType,

    // The name of the directory
    pub name: String,

    // Hash of all the children
    pub hash: MerkleHash,
    // Recursive size of the directory
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: MerkleHash,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    // Recursive file counts in the directory
    pub data_type_counts: HashMap<String, u64>,
    pub data_type_sizes: HashMap<String, u64>,
}

impl TDirNode for DirNodeData {
    fn version(&self) -> MinOxenVersion {
        MinOxenVersion::V0_19_0
    }

    fn node_type(&self) -> &MerkleTreeNodeType {
        &self.node_type
    }

    fn hash(&self) -> &MerkleHash {
        &self.hash
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    fn num_files(&self) -> u64 {
        // Old implementation did not have a count for the number of entries
        self.data_type_counts.values().sum()
    }

    fn num_entries(&self) -> u64 {
        // log::warn!("num_entries is not supported for v0.19.0");
        0
    }

    fn set_num_entries(&mut self, _: u64) {
        // log::warn!("set_num_entries is not supported for v0.19.0");
    }

    fn num_bytes(&self) -> u64 {
        self.num_bytes
    }

    fn last_commit_id(&self) -> &MerkleHash {
        &self.last_commit_id
    }

    fn set_last_commit_id(&mut self, last_commit_id: &MerkleHash) {
        self.last_commit_id = *last_commit_id;
    }

    fn last_modified_seconds(&self) -> i64 {
        self.last_modified_seconds
    }

    fn last_modified_nanoseconds(&self) -> u32 {
        self.last_modified_nanoseconds
    }

    fn data_type_counts(&self) -> &HashMap<String, u64> {
        &self.data_type_counts
    }

    fn data_type_sizes(&self) -> &HashMap<String, u64> {
        &self.data_type_sizes
    }

    fn set_data_type_counts(&mut self, data_type_counts: HashMap<String, u64>) {
        self.data_type_counts = data_type_counts;
    }

    fn set_data_type_sizes(&mut self, data_type_sizes: HashMap<String, u64>) {
        self.data_type_sizes = data_type_sizes;
    }
}
