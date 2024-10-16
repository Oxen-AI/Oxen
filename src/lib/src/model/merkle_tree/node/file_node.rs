//! This is a compact representation of a merkle tree file node
//! that is stored in on disk
//!

use super::file_node_types::{FileChunkType, FileStorageType};
use crate::error::OxenError;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{
    EntryDataType, MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Deserialize, Serialize, Clone)]
pub struct FileNode {
    pub dtype: MerkleTreeNodeType,

    // The name of the file
    pub name: String,

    // Full file hash
    pub metadata_hash: Option<MerkleHash>, //hash of the metadata
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

impl FileNode {
    pub fn deserialize(data: &[u8]) -> Result<FileNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing file node: {e}")))
    }
}

impl Default for FileNode {
    fn default() -> Self {
        FileNode {
            dtype: MerkleTreeNodeType::File,
            name: "".to_string(),
            hash: MerkleHash::new(0),
            combined_hash: MerkleHash::new(0),
            metadata_hash: None,
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type: EntryDataType::Binary,
            metadata: None,
            mime_type: "".to_string(),
            extension: "".to_string(),
            chunk_hashes: vec![],
            chunk_type: FileChunkType::SingleFile,
            storage_backend: FileStorageType::Disk,
        }
    }
}

impl MerkleTreeNodeIdType for FileNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn hash(&self) -> MerkleHash {
        self.hash
    }
}

impl Hash for FileNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.num_bytes.hash(state);
        self.last_modified_seconds.hash(state);
        self.last_modified_nanoseconds.hash(state);
        self.hash.hash(state);
    }
}

impl TMerkleTreeNode for FileNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for FileNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "FileNode")?;
        writeln!(f, "\thash: {}", self.hash)?;
        writeln!(f, "\tname: {}", self.name)?;
        writeln!(f, "\tnum_bytes: {}", bytesize::ByteSize::b(self.num_bytes))?;
        writeln!(f, "\tdata_type: {:?}", self.data_type)?;
        writeln!(f, "\tmetadata: {:?}", self.metadata)?;
        writeln!(f, "\tmime_type: {}", self.mime_type)?;
        writeln!(f, "\textension: {}", self.extension)?;
        writeln!(f, "\tchunk_hashes: {:?}", self.chunk_hashes)?;
        writeln!(f, "\tchunk_type: {:?}", self.chunk_type)?;
        writeln!(f, "\tstorage_backend: {:?}", self.storage_backend)?;
        writeln!(f, "\tlast_commit_id: {}", self.last_commit_id)?;
        writeln!(f, "\tlast_modified_seconds: {}", self.last_modified_seconds)?;
        writeln!(
            f,
            "\tlast_modified_nanoseconds: {}",
            self.last_modified_nanoseconds
        )?;
        writeln!(f, "\tmetadata: {:?}", self.metadata)?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for FileNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\"{}\" ({}) [{}] (commit {})",
            self.name,
            bytesize::ByteSize::b(self.num_bytes),
            self.hash.to_short_str(),
            self.last_commit_id.to_short_str()
        )?;
        if let Some(metadata) = &self.metadata {
            write!(f, " {}", metadata)?;
        }
        Ok(())
    }
}

impl PartialEq for FileNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for FileNode {}
