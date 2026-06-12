//! This is a compact representation of a merkle tree file node
//! that is stored in on disk
//!

use crate::core::versions::MinOxenVersion;
use crate::model::merkle_tree::node::file_node::TFileNode;
use crate::model::merkle_tree::node::file_node_types::{FileChunkType, FileStorageType};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{EntryDataType, MerkleHash, MerkleTreeNodeType};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct FileNodeData {
    pub node_type: MerkleTreeNodeType,

    // The name of the file
    pub name: String,

    // Full file hash
    pub metadata_hash: Option<MerkleHash>, // hash of the metadata
    pub hash: MerkleHash,
    pub combined_hash: MerkleHash, //hash of the content_hash and metadata_hash
    // Number of bytes in the file
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: MerkleHash,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,

    // Data Type
    pub data_type: EntryDataType,

    // Metadata
    pub metadata: Option<GenericMetadata>,

    // Mime Type
    pub mime_type: String,
    // Extension
    pub extension: String,

    // File chunks, for single chunk files, this will be empty (and we can just use the hash)
    pub chunk_hashes: Vec<u128>,

    pub chunk_type: FileChunkType, // How the data is stored on disk
    pub storage_backend: FileStorageType, // Where the file is stored in the backend
}

impl TFileNode for FileNodeData {
    fn version(&self) -> MinOxenVersion {
        MinOxenVersion::LATEST
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

    fn combined_hash(&self) -> &MerkleHash {
        &self.combined_hash
    }

    fn set_combined_hash(&mut self, combined_hash: &MerkleHash) {
        self.combined_hash = *combined_hash;
    }

    fn metadata_hash(&self) -> Option<&MerkleHash> {
        self.metadata_hash.as_ref()
    }

    fn set_metadata_hash(&mut self, metadata_hash: Option<MerkleHash>) {
        self.metadata_hash = metadata_hash;
    }

    fn metadata(&self) -> Option<GenericMetadata> {
        self.metadata.clone()
    }

    fn get_mut_metadata(&mut self) -> &mut Option<GenericMetadata> {
        &mut self.metadata
    }

    fn set_metadata(&mut self, metadata: Option<GenericMetadata>) {
        self.metadata = metadata;
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

    fn data_type(&self) -> &EntryDataType {
        &self.data_type
    }

    fn mime_type(&self) -> &str {
        &self.mime_type
    }

    fn extension(&self) -> &str {
        &self.extension
    }

    fn chunk_hashes(&self) -> &Vec<u128> {
        &self.chunk_hashes
    }

    fn set_chunk_hashes(&mut self, chunk_hashes: Vec<u128>) {
        self.chunk_hashes = chunk_hashes;
    }

    fn chunk_type(&self) -> &FileChunkType {
        &self.chunk_type
    }

    fn storage_backend(&self) -> &FileStorageType {
        &self.storage_backend
    }
}
