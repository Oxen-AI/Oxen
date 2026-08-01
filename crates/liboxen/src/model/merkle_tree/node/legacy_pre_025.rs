//! Node payload shapes from before v0.25.0, kept so repositories written then stay readable.
//!
//! Harvested verbatim from `aa8f564b8^:crates/liboxen/src/core/v_old/v0_19_0/model/merkle_tree/`,
//! the commit that deleted the old read path. Only the two shapes that actually changed live here:
//! `CommitNodeData` kept the same seven fields in the same order and decodes unchanged, and
//! `FileNode` already carries its own fallback.
//!
//! The difference from the current structs is one field. Both gained `num_entries`, which these
//! payloads simply do not contain, so a conversion has to supply it. Reads do not need it — a
//! directory listing counts the entries it actually returns — so the conversions below leave it
//! zero, matching what the pre-removal code did. Deriving the real value belongs to the migration
//! that rewrites these nodes, where the child counts are being walked anyway.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::v_latest::model::merkle_tree::node::dir_node::DirNodeData;
use crate::core::v_latest::model::merkle_tree::node::vnode::VNodeData;
use crate::model::{MerkleHash, MerkleTreeNodeType};

/// A vnode as written before v0.25.0: no `num_entries`.
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNodeDataPre025 {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
}

impl From<VNodeDataPre025> for VNodeData {
    fn from(legacy: VNodeDataPre025) -> Self {
        VNodeData {
            hash: legacy.hash,
            node_type: legacy.node_type,
            num_entries: 0,
        }
    }
}

/// A directory node as written before v0.25.0: no `num_entries`.
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNodeDataPre025 {
    pub node_type: MerkleTreeNodeType,
    pub name: String,
    pub hash: MerkleHash,
    pub num_bytes: u64,
    pub last_commit_id: MerkleHash,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    pub data_type_counts: HashMap<String, u64>,
    pub data_type_sizes: HashMap<String, u64>,
}

impl From<DirNodeDataPre025> for DirNodeData {
    fn from(legacy: DirNodeDataPre025) -> Self {
        DirNodeData {
            node_type: legacy.node_type,
            name: legacy.name,
            hash: legacy.hash,
            num_entries: 0,
            num_bytes: legacy.num_bytes,
            last_commit_id: legacy.last_commit_id,
            last_modified_seconds: legacy.last_modified_seconds,
            last_modified_nanoseconds: legacy.last_modified_nanoseconds,
            data_type_counts: legacy.data_type_counts,
            data_type_sizes: legacy.data_type_sizes,
        }
    }
}
