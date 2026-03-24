//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::core::v_latest::model::merkle_tree::node::vnode::VNodeData as VNodeImplV0_25_0;
use crate::core::v_old::v0_19_0::model::merkle_tree::node::vnode::VNodeData as VNodeImplV0_19_0;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{
    LocalRepository, MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode,
};

pub trait TVNode {
    fn node_type(&self) -> &MerkleTreeNodeType;
    fn hash(&self) -> &MerkleHash;
    fn version(&self) -> MinOxenVersion;
    fn num_entries(&self) -> u64;
    fn set_num_entries(&mut self, _: u64);
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum EVNode {
    V0_19_0(VNodeImplV0_19_0),
    V0_25_0(VNodeImplV0_25_0),
}

pub struct VNodeOpts {
    pub hash: MerkleHash,
    pub num_entries: u64,
}

#[derive(Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct VNode {
    pub node: EVNode,
}

impl VNode {
    pub fn new(repo: &LocalRepository, vnode_opts: VNodeOpts) -> Result<VNode, OxenError> {
        match repo.min_version() {
            MinOxenVersion::V0_19_0 => Ok(Self {
                node: EVNode::V0_19_0(VNodeImplV0_19_0 {
                    hash: vnode_opts.hash,
                    node_type: MerkleTreeNodeType::VNode,
                }),
            }),
            MinOxenVersion::LATEST | MinOxenVersion::V0_25_0 => Ok(Self {
                node: EVNode::V0_25_0(VNodeImplV0_25_0 {
                    hash: vnode_opts.hash,
                    node_type: MerkleTreeNodeType::VNode,
                    num_entries: vnode_opts.num_entries,
                }),
            }),
            _ => Err(OxenError::basic_str("VNode not supported in this version")),
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<VNode, OxenError> {
        // In order to support versions that didn't have the enum,
        // if it fails we will fall back to the old struct, then populate the enum
        let vnode: VNode = match rmp_serde::from_slice(data) {
            Ok(vnode) => vnode,
            Err(_) => {
                // This is a fallback for old versions of the vnode
                let vnode: VNodeImplV0_19_0 = rmp_serde::from_slice(data)?;
                Self {
                    node: EVNode::V0_19_0(vnode),
                }
            }
        };
        Ok(vnode)
    }

    pub fn get_opts(&self) -> VNodeOpts {
        match &self.node {
            EVNode::V0_25_0(vnode) => VNodeOpts {
                hash: vnode.hash,
                num_entries: vnode.num_entries,
            },
            EVNode::V0_19_0(vnode) => VNodeOpts {
                hash: vnode.hash,
                num_entries: 0,
            },
        }
    }

    fn node(&self) -> &dyn TVNode {
        match self.node {
            EVNode::V0_25_0(ref vnode) => vnode,
            EVNode::V0_19_0(ref vnode) => vnode,
        }
    }

    fn mut_node(&mut self) -> &mut dyn TVNode {
        match self.node {
            EVNode::V0_25_0(ref mut vnode) => vnode,
            EVNode::V0_19_0(ref mut vnode) => vnode,
        }
    }

    pub fn version(&self) -> MinOxenVersion {
        self.node().version()
    }

    pub fn hash(&self) -> &MerkleHash {
        self.node().hash()
    }

    pub fn num_entries(&self) -> u64 {
        self.node().num_entries()
    }

    pub fn set_num_entries(&mut self, num_entries: u64) {
        self.mut_node().set_num_entries(num_entries);
    }
}

impl Default for VNode {
    fn default() -> Self {
        VNode {
            node: EVNode::V0_25_0(VNodeImplV0_25_0 {
                node_type: MerkleTreeNodeType::VNode,
                hash: MerkleHash::new(0),
                num_entries: 0,
            }),
        }
    }
}

impl MerkleTreeNodeIdType for VNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        *self.node().node_type()
    }

    fn hash(&self) -> MerkleHash {
        *self.node().hash()
    }
}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VNode({})", self.hash())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // id and dtype already get printed by the node.rs println!("{:?}", node)
        write!(f, "")
    }
}

impl TMerkleTreeNode for VNode {}
