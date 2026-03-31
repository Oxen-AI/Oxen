//! Wrapper around the DirNodeData struct to support old versions of the dir node

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use crate::core::v_latest::model::merkle_tree::node::dir_node::DirNodeData as DirNodeDataV0_25_0;
use crate::core::v_old::v0_19_0::model::merkle_tree::node::dir_node::DirNodeData as DirNodeDataV0_19_0;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{
    LocalRepository, MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode,
};
use crate::view::DataTypeCount;

pub trait TDirNode {
    fn version(&self) -> MinOxenVersion;
    fn node_type(&self) -> &MerkleTreeNodeType;
    fn hash(&self) -> &MerkleHash;
    fn name(&self) -> &str;
    fn set_name(&mut self, name: &str);
    fn num_files(&self) -> u64; // This us just the number of files
    fn num_entries(&self) -> u64; // This is the number of files and directories and vnodes
    fn set_num_entries(&mut self, num_entries: u64);
    fn num_bytes(&self) -> u64;
    fn last_commit_id(&self) -> &MerkleHash;
    fn set_last_commit_id(&mut self, last_commit_id: &MerkleHash);
    fn last_modified_seconds(&self) -> i64;
    fn last_modified_nanoseconds(&self) -> u32;
    fn data_type_counts(&self) -> &HashMap<String, u64>;
    fn data_type_sizes(&self) -> &HashMap<String, u64>;
    fn set_data_type_counts(&mut self, data_type_counts: HashMap<String, u64>);
    fn set_data_type_sizes(&mut self, data_type_sizes: HashMap<String, u64>);
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum EDirNode {
    V0_25_0(DirNodeDataV0_25_0),
    V0_19_0(DirNodeDataV0_19_0),
}

pub struct DirNodeOpts {
    pub name: String,
    pub hash: MerkleHash,
    pub num_entries: u64,
    pub num_bytes: u64,
    pub last_commit_id: MerkleHash,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    pub data_type_counts: HashMap<String, u64>,
    pub data_type_sizes: HashMap<String, u64>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNode {
    pub node: EDirNode,
}

impl DirNode {
    pub fn new(repo: &LocalRepository, opts: DirNodeOpts) -> Result<Self, OxenError> {
        match repo.min_version() {
            MinOxenVersion::LATEST | MinOxenVersion::V0_25_0 => Ok(Self {
                node: EDirNode::V0_25_0(DirNodeDataV0_25_0 {
                    node_type: MerkleTreeNodeType::Dir,
                    name: opts.name,
                    hash: opts.hash,
                    num_entries: opts.num_entries,
                    num_bytes: opts.num_bytes,
                    last_commit_id: opts.last_commit_id,
                    last_modified_seconds: opts.last_modified_seconds,
                    last_modified_nanoseconds: opts.last_modified_nanoseconds,
                    data_type_counts: opts.data_type_counts,
                    data_type_sizes: opts.data_type_sizes,
                }),
            }),
            MinOxenVersion::V0_19_0 => Ok(Self {
                node: EDirNode::V0_19_0(DirNodeDataV0_19_0 {
                    node_type: MerkleTreeNodeType::Dir,
                    name: opts.name,
                    hash: opts.hash,
                    num_bytes: opts.num_bytes,
                    last_commit_id: opts.last_commit_id,
                    last_modified_seconds: opts.last_modified_seconds,
                    last_modified_nanoseconds: opts.last_modified_nanoseconds,
                    data_type_counts: opts.data_type_counts,
                    data_type_sizes: opts.data_type_sizes,
                }),
            }),
            _ => Err(OxenError::basic_str(format!(
                "Unsupported DirNode version: {}",
                repo.min_version()
            ))),
        }
    }

    pub fn get_opts(&self) -> DirNodeOpts {
        match &self.node {
            EDirNode::V0_25_0(data) => DirNodeOpts {
                name: data.name.clone(),
                hash: data.hash,
                num_entries: data.num_entries,
                num_bytes: data.num_bytes,
                last_commit_id: data.last_commit_id,
                last_modified_seconds: data.last_modified_seconds,
                last_modified_nanoseconds: data.last_modified_nanoseconds,
                data_type_counts: data.data_type_counts.clone(),
                data_type_sizes: data.data_type_sizes.clone(),
            },
            EDirNode::V0_19_0(data) => DirNodeOpts {
                name: data.name.clone(),
                hash: data.hash,
                num_entries: 0, // not supported in v0.19.0
                num_bytes: data.num_bytes,
                last_commit_id: data.last_commit_id,
                last_modified_seconds: data.last_modified_seconds,
                last_modified_nanoseconds: data.last_modified_nanoseconds,
                data_type_counts: data.data_type_counts.clone(),
                data_type_sizes: data.data_type_sizes.clone(),
            },
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<DirNode, rmp_serde::decode::Error> {
        // In order to support versions that didn't have the enum,
        // if it fails we will fall back to the old struct, then populate the enum
        let dir_node: DirNode = match rmp_serde::from_slice(data) {
            Ok(dir_node) => dir_node,
            Err(_) => {
                // This is a fallback for old versions of the dir node
                let dir_node: DirNodeDataV0_19_0 = rmp_serde::from_slice(data)?;
                Self {
                    node: EDirNode::V0_19_0(dir_node),
                }
            }
        };
        Ok(dir_node)
    }

    fn node(&self) -> &dyn TDirNode {
        match &self.node {
            EDirNode::V0_25_0(data) => data,
            EDirNode::V0_19_0(data) => data,
        }
    }

    fn mut_node(&mut self) -> &mut dyn TDirNode {
        match &mut self.node {
            EDirNode::V0_25_0(data) => data,
            EDirNode::V0_19_0(data) => data,
        }
    }

    pub fn hash(&self) -> &MerkleHash {
        self.node().hash()
    }

    pub fn version(&self) -> MinOxenVersion {
        self.node().version()
    }

    pub fn node_type(&self) -> &MerkleTreeNodeType {
        self.node().node_type()
    }

    pub fn data_types(&self) -> Vec<DataTypeCount> {
        self.data_type_counts()
            .iter()
            .map(|(k, v)| DataTypeCount {
                data_type: k.clone(),
                count: *v as usize,
            })
            .collect()
    }

    pub fn name(&self) -> &str {
        self.node().name()
    }

    pub fn set_name(&mut self, name: impl AsRef<str>) {
        self.mut_node().set_name(name.as_ref());
    }

    /// /// Number of files (not including directories)
    pub fn num_files(&self) -> u64 {
        // The data type counts are the number of files per data type,
        // so we can sum them to get the total number of files
        self.data_type_counts().values().sum()
    }

    /// Number of files and directories and vnodes
    pub fn num_entries(&self) -> u64 {
        self.node().num_entries()
    }

    pub fn set_num_entries(&mut self, num_entries: u64) {
        self.mut_node().set_num_entries(num_entries);
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

    pub fn data_type_counts(&self) -> &HashMap<String, u64> {
        self.node().data_type_counts()
    }

    pub fn data_type_sizes(&self) -> &HashMap<String, u64> {
        self.node().data_type_sizes()
    }

    pub fn set_data_type_counts(&mut self, data_type_counts: HashMap<String, u64>) {
        self.mut_node().set_data_type_counts(data_type_counts);
    }

    pub fn set_data_type_sizes(&mut self, data_type_sizes: HashMap<String, u64>) {
        self.mut_node().set_data_type_sizes(data_type_sizes);
    }
}

impl Default for DirNode {
    fn default() -> Self {
        Self {
            node: EDirNode::V0_25_0(DirNodeDataV0_25_0 {
                node_type: MerkleTreeNodeType::Dir,
                name: "".to_string(),
                hash: MerkleHash::new(0),
                num_bytes: 0,
                num_entries: 0,
                last_commit_id: MerkleHash::new(0),
                last_modified_seconds: 0,
                last_modified_nanoseconds: 0,
                data_type_counts: HashMap::new(),
                data_type_sizes: HashMap::new(),
            }),
        }
    }
}

impl MerkleTreeNodeIdType for DirNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        *self.node().node_type()
    }

    fn hash(&self) -> MerkleHash {
        *self.node().hash()
    }
}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for DirNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DirNode({})", self.version())?;
        writeln!(f, "\thash: {}", self.hash())?;
        writeln!(f, "\tname: {}", self.name())?;
        writeln!(
            f,
            "\tnum_bytes: {}",
            bytesize::ByteSize::b(self.num_bytes())
        )?;
        writeln!(f, "\tnum_entries: {}", self.num_entries())?;
        writeln!(f, "\tnum_files: {}", self.num_files())?;
        writeln!(f, "\tdata_type_counts: {:?}", self.data_type_counts())?;
        writeln!(f, "\tdata_type_sizes: {:?}", self.data_type_sizes())?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for DirNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\"{}\" ({}) ({} files) (commit {}) ",
            self.name(),
            bytesize::ByteSize::b(self.num_bytes()),
            self.num_files(),
            self.last_commit_id().to_short_str()
        )
    }
}
