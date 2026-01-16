//! Wrapper around the FileNodeData struct to support old versions of the file node

use crate::core::v_latest::model::merkle_tree::node::file_node::FileNodeData as FileNodeDataV0_25_0;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merkle_tree::node::file_node_types::{FileChunkType, FileStorageType};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{
    EntryDataType, LocalRepository, MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType,
    TMerkleTreeNode,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FileNodeOpts {
    pub name: String,
    pub hash: MerkleHash,
    pub combined_hash: MerkleHash,
    pub metadata_hash: Option<MerkleHash>,
    pub num_bytes: u64,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    pub data_type: EntryDataType,
    pub metadata: Option<GenericMetadata>,
    pub mime_type: String,
    pub extension: String,
}

pub trait TFileNode {
    fn version(&self) -> MinOxenVersion;
    fn node_type(&self) -> &MerkleTreeNodeType;
    fn hash(&self) -> &MerkleHash;
    fn name(&self) -> &str;
    fn set_name(&mut self, name: &str);
    fn combined_hash(&self) -> &MerkleHash;
    fn set_combined_hash(&mut self, combined_hash: &MerkleHash);
    fn metadata_hash(&self) -> Option<&MerkleHash>;
    fn set_metadata_hash(&mut self, metadata_hash: Option<MerkleHash>);
    fn num_bytes(&self) -> u64;
    fn last_commit_id(&self) -> &MerkleHash;
    fn set_last_commit_id(&mut self, last_commit_id: &MerkleHash);
    fn last_modified_seconds(&self) -> i64;
    fn last_modified_nanoseconds(&self) -> u32;
    fn data_type(&self) -> &EntryDataType;
    fn set_data_type(&mut self, data_type: EntryDataType);
    fn metadata(&self) -> Option<GenericMetadata>;
    fn get_mut_metadata(&mut self) -> &mut Option<GenericMetadata>;
    fn set_metadata(&mut self, metadata: Option<GenericMetadata>);
    fn mime_type(&self) -> &str;
    fn extension(&self) -> &str;
    fn chunk_hashes(&self) -> &Vec<u128>;
    fn set_chunk_hashes(&mut self, chunk_hashes: Vec<u128>);
    fn chunk_type(&self) -> &FileChunkType;
    fn storage_backend(&self) -> &FileStorageType;
}

#[derive(Deserialize, Serialize, Clone)]
pub enum EFileNode {
    V0_25_0(FileNodeDataV0_25_0),
}

#[derive(Deserialize, Serialize, Clone)]
pub struct FileNode {
    pub node: EFileNode,
}

impl FileNode {
    pub fn new(repo: &LocalRepository, opts: FileNodeOpts) -> Result<Self, OxenError> {
        match repo.min_version() {
            MinOxenVersion::LATEST | MinOxenVersion::V0_25_0 | MinOxenVersion::V0_19_0 => {
                Ok(Self {
                    node: EFileNode::V0_25_0(FileNodeDataV0_25_0 {
                        node_type: MerkleTreeNodeType::File,
                        name: opts.name,
                        hash: opts.hash,
                        combined_hash: opts.combined_hash,
                        metadata_hash: opts.metadata_hash,
                        num_bytes: opts.num_bytes,
                        last_commit_id: MerkleHash::new(0),
                        last_modified_seconds: opts.last_modified_seconds,
                        last_modified_nanoseconds: opts.last_modified_nanoseconds,
                        data_type: opts.data_type,
                        metadata: opts.metadata,
                        mime_type: opts.mime_type,
                        extension: opts.extension,
                        chunk_hashes: vec![],
                        chunk_type: FileChunkType::SingleFile,
                        storage_backend: FileStorageType::Disk,
                    }),
                })
            }
            _ => Err(OxenError::basic_str(
                "FileNode not supported in this version",
            )),
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<FileNode, OxenError> {
        let file_node: FileNode = match rmp_serde::from_slice(data) {
            Ok(file_node) => file_node,
            Err(_) => {
                // This is a fallback for old versions of the file node
                let file_node: FileNodeDataV0_25_0 = rmp_serde::from_slice(data)?;
                Self {
                    node: EFileNode::V0_25_0(file_node),
                }
            }
        };
        Ok(file_node)
    }

    fn mut_node(&mut self) -> &mut dyn TFileNode {
        match self.node {
            EFileNode::V0_25_0(ref mut file_node) => file_node,
        }
    }

    fn node(&self) -> &dyn TFileNode {
        match self.node {
            EFileNode::V0_25_0(ref file_node) => file_node,
        }
    }

    pub fn node_type(&self) -> &MerkleTreeNodeType {
        self.node().node_type()
    }

    pub fn version(&self) -> MinOxenVersion {
        self.node().version()
    }

    pub fn hash(&self) -> &MerkleHash {
        self.node().hash()
    }

    pub fn name(&self) -> &str {
        self.node().name()
    }

    pub fn set_name(&mut self, name: &str) {
        self.mut_node().set_name(name);
    }

    pub fn combined_hash(&self) -> &MerkleHash {
        self.node().combined_hash()
    }

    pub fn set_combined_hash(&mut self, combined_hash: &MerkleHash) {
        self.mut_node().set_combined_hash(combined_hash);
    }

    pub fn metadata_hash(&self) -> Option<&MerkleHash> {
        self.node().metadata_hash()
    }

    pub fn set_metadata_hash(&mut self, metadata_hash: Option<MerkleHash>) {
        self.mut_node().set_metadata_hash(metadata_hash);
    }

    pub fn num_bytes(&self) -> u64 {
        self.node().num_bytes()
    }

    pub fn last_commit_id(&self) -> &MerkleHash {
        self.node().last_commit_id()
    }

    pub fn set_last_commit_id(&mut self, last_commit_id: &MerkleHash) {
        self.mut_node().set_last_commit_id(last_commit_id);
    }

    pub fn last_modified_seconds(&self) -> i64 {
        self.node().last_modified_seconds()
    }

    pub fn last_modified_nanoseconds(&self) -> u32 {
        self.node().last_modified_nanoseconds()
    }

    pub fn data_type(&self) -> &EntryDataType {
        self.node().data_type()
    }

    pub fn set_data_type(&mut self, data_type: EntryDataType) {
        self.mut_node().set_data_type(data_type);
    }

    pub fn metadata(&self) -> Option<GenericMetadata> {
        self.node().metadata()
    }

    pub fn get_mut_metadata(&mut self) -> &mut Option<GenericMetadata> {
        self.mut_node().get_mut_metadata()
    }

    pub fn set_metadata(&mut self, metadata: Option<GenericMetadata>) {
        self.mut_node().set_metadata(metadata);
    }

    pub fn mime_type(&self) -> &str {
        self.node().mime_type()
    }

    pub fn extension(&self) -> &str {
        self.node().extension()
    }

    pub fn chunk_hashes(&self) -> &Vec<u128> {
        self.node().chunk_hashes()
    }

    pub fn set_chunk_hashes(&mut self, chunk_hashes: Vec<u128>) {
        self.mut_node().set_chunk_hashes(chunk_hashes);
    }

    pub fn chunk_type(&self) -> &FileChunkType {
        self.node().chunk_type()
    }

    pub fn storage_backend(&self) -> &FileStorageType {
        self.node().storage_backend()
    }
}

impl Default for FileNode {
    fn default() -> Self {
        Self {
            node: EFileNode::V0_25_0(FileNodeDataV0_25_0 {
                node_type: MerkleTreeNodeType::File,
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
            }),
        }
    }
}

impl MerkleTreeNodeIdType for FileNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        *self.node_type()
    }

    fn hash(&self) -> MerkleHash {
        *self.hash()
    }
}

impl Hash for FileNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.num_bytes().hash(state);
        self.last_modified_seconds().hash(state);
        self.last_modified_nanoseconds().hash(state);
        self.hash().hash(state);
    }
}

impl TMerkleTreeNode for FileNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for FileNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "FileNode({})", self.version())?;
        writeln!(f, "\thash: {}", self.hash())?;
        writeln!(f, "\tname: {}", self.name())?;
        writeln!(
            f,
            "\tnum_bytes: {}",
            bytesize::ByteSize::b(self.num_bytes())
        )?;
        writeln!(f, "\tdata_type: {:?}", self.data_type())?;
        writeln!(f, "\tmetadata: {:?}", self.metadata())?;
        writeln!(f, "\tmime_type: {}", self.mime_type())?;
        writeln!(f, "\textension: {}", self.extension())?;
        writeln!(f, "\tchunk_hashes: {:?}", self.chunk_hashes())?;
        writeln!(f, "\tchunk_type: {:?}", self.chunk_type())?;
        writeln!(f, "\tstorage_backend: {:?}", self.storage_backend())?;
        writeln!(f, "\tlast_commit_id: {}", self.last_commit_id())?;
        writeln!(
            f,
            "\tlast_modified_seconds: {}",
            self.last_modified_seconds()
        )?;
        writeln!(
            f,
            "\tlast_modified_nanoseconds: {}",
            self.last_modified_nanoseconds()
        )?;
        writeln!(f, "\tmetadata: {:?}", self.metadata())?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for FileNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\"{}\" ({}) {} [{}] (commit {})",
            self.name(),
            self.mime_type(),
            bytesize::ByteSize::b(self.num_bytes()),
            self.hash().to_short_str(),
            self.last_commit_id().to_short_str()
        )?;
        if let Some(metadata) = self.metadata() {
            write!(f, " {metadata}")?;
        }
        Ok(())
    }
}

impl PartialEq for FileNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash() == other.hash()
    }
}

impl Eq for FileNode {}
