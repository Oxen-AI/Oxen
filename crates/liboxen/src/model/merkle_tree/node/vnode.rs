//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::core::v_latest::model::merkle_tree::node::vnode::VNodeData as VNodeImplV0_25_0;
use crate::error::OxenError;
use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

pub trait TVNode {
    fn node_type(&self) -> &MerkleTreeNodeType;
    fn hash(&self) -> &MerkleHash;
    fn num_entries(&self) -> u64;
    fn set_num_entries(&mut self, _: u64);
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum EVNode {
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
    pub fn new(vnode_opts: VNodeOpts) -> Result<VNode, OxenError> {
        Ok(Self {
            node: EVNode::V0_25_0(VNodeImplV0_25_0 {
                hash: vnode_opts.hash,
                node_type: MerkleTreeNodeType::VNode,
                num_entries: vnode_opts.num_entries,
            }),
        })
    }

    #[inline(always)]
    pub fn deserialize(data: &[u8]) -> Result<VNode, rmp_serde::decode::Error> {
        rmp_serde::from_slice(data)
    }

    pub fn get_opts(&self) -> VNodeOpts {
        match &self.node {
            EVNode::V0_25_0(vnode) => VNodeOpts {
                hash: vnode.hash,
                num_entries: vnode.num_entries,
            },
        }
    }

    fn node(&self) -> &dyn TVNode {
        match self.node {
            EVNode::V0_25_0(ref vnode) => vnode,
        }
    }

    fn mut_node(&mut self) -> &mut dyn TVNode {
        match self.node {
            EVNode::V0_25_0(ref mut vnode) => vnode,
        }
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

#[cfg(test)]
mod on_disk_format {
    use super::*;
    use crate::model::merkle_tree::node::commit_node::{CommitNode, ECommitNode};
    use crate::model::merkle_tree::node::dir_node::{DirNode, EDirNode};

    // Merkle nodes are serialized with `rmp_serde::to_vec`, which tags enum variants by
    // *name* (`{"V0_25_0": ...}`), not by ordinal position. That name tag is what lets us drop
    // the earlier `V0_19_0` variant without rewriting existing repos: a LATEST node keeps the
    // exact same on-disk bytes even though `V0_25_0` is the second variant of `EVNode` but the
    // first of `EDirNode`. If this regressed to ordinal tagging, the name would vanish from the
    // bytes and nodes written by older Oxen versions would stop decoding.
    #[test]
    fn latest_nodes_are_name_tagged_and_round_trip() {
        let vnode_bytes = rmp_serde::to_vec(&VNode::default()).unwrap();
        let dir_bytes = rmp_serde::to_vec(&DirNode::default()).unwrap();
        let commit_bytes = rmp_serde::to_vec(&CommitNode::default()).unwrap();

        for bytes in [&vnode_bytes, &dir_bytes, &commit_bytes] {
            assert!(
                bytes.windows(7).any(|w| w == b"V0_25_0"),
                "node must be tagged by variant name, not ordinal: {bytes:02x?}"
            );
        }

        assert!(matches!(
            VNode::deserialize(&vnode_bytes).unwrap().node,
            EVNode::V0_25_0(_)
        ));
        assert!(matches!(
            DirNode::deserialize(&dir_bytes).unwrap().node,
            EDirNode::V0_25_0(_)
        ));
        assert!(matches!(
            CommitNode::deserialize(&commit_bytes).unwrap().node,
            ECommitNode::V0_25_0(_)
        ));
    }
}
