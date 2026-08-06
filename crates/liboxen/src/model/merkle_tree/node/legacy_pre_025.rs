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

use crate::core::v_latest::model::merkle_tree::node::commit_node::CommitNodeData;
use crate::core::v_latest::model::merkle_tree::node::dir_node::DirNodeData;
use crate::core::v_latest::model::merkle_tree::node::file_node::FileNodeData;
use crate::core::v_latest::model::merkle_tree::node::vnode::VNodeData;
use crate::model::{MerkleHash, MerkleTreeNodeType};

/// Whether these bytes decode as the payload shape written before v0.25.0 for `dtype`.
///
/// Answers "is this a retired-format node" by decoding it as one, so bytes that merely lack the
/// current envelope are not taken for a retired format on that basis alone. Damage to a payload's
/// leading bytes destroys the envelope, which makes prefix inspection alone unable to tell a
/// legacy node from a corrupt one.
///
/// `Commit` and `File` kept their field lists across the change, so the current data struct reads
/// the older bytes. `FileChunk` never had an envelope, so the question does not apply to it.
pub(crate) fn decodes_as_pre_v025(dtype: MerkleTreeNodeType, data: &[u8]) -> bool {
    match dtype {
        MerkleTreeNodeType::Commit => rmp_serde::from_slice::<CommitNodeData>(data).is_ok(),
        MerkleTreeNodeType::Dir => rmp_serde::from_slice::<DirNodeDataPre025>(data).is_ok(),
        MerkleTreeNodeType::File => rmp_serde::from_slice::<FileNodeData>(data).is_ok(),
        MerkleTreeNodeType::VNode => rmp_serde::from_slice::<VNodeDataPre025>(data).is_ok(),
        MerkleTreeNodeType::FileChunk => false,
    }
}

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
