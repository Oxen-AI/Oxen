//! This is the type of node that we are storing in the merkle tree
//!
//! There are only 5 node types as of now, so can store in a u8, and would
//! need a migration to change anyways.
//!
//! This value is stored at the top of a merkle tree db file
//! to know how to deserialize the node type
//!

use crate::model::merkle_tree::merkle_hash::MerkleHash;

use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
};
use thiserror::Error;

/// The type of node that we are storing in the merkle tree.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Copy)]
pub enum MerkleTreeNodeType {
    /// A commit in the repository. From this commit node, we can traverse the tree to recover the
    /// entire repository state at the commit.
    Commit,

    /// A file in the repository.
    File,

    /// A directory in the repository.
    Dir,

    /// Directories can be very large, so we split their contents into (potentially) multiple
    /// virtual directory nodes: a `VNode`. A `VNode` is like a directory: it contains some
    /// number of files and directories. It is always a subset of a real directory. If a
    /// directory has multiple `VNode` children, these children form a strict paritioning.
    VNode,

    /// A chunk of a file in the repository. (TODO: unused at this point, but planned)
    FileChunk,
}

impl Hash for MerkleTreeNodeType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u8(self.to_u8());
    }
}

#[derive(Debug, Error)]
#[error("Deserialization failure: Invalid MerkleTreeNodeType: {0}")]
/// Failed to deserialize a `MerkleTreeNodeType` due to unknown `u8` value.
pub struct InvalidMerkleTreeNodeType(u8);

impl MerkleTreeNodeType {
    /*
     **********************************************************************************************

    NOTE: The mapping of u8 -> node type is important! DO NOT MODIFY THIS!!!!

    It is stored on disk / in a database as a `u8` and this value is used to deserialize nodes into
    their correct type. Changing this will **BREAK REPOSITORIES**.

    IF it is absolutely necessary to change it, then a migration **MUST** be implemented and
    operations **MUST** be gated behind a breaking version update to force this migration on
    outdated repositories.

    That being said, it's difficult to imagine any valid reason why this mapping would need to be
    changed for existing `MerkleTreeNode` variants.

    **NEW** variants can be added without a migration. New variants must have an incremented `u8`
    value to not conflcit with any existing variants. Older repositories will still be able to be
    read by newer variants: they will simply not contain nodes of the new variant.

    If there is some **OTHER** backwards-incompatible change with how the repository data is stored
    that requires a new variant, then a migration **MUST** be implemented and operations **MUST**
    be gated behind a breaking version update to force this migration on outdated repositories.

    **********************************************************************************************
    */

    /// Serialize the node type into a stable `u8` value.
    /// This function is 1:1 with `from_u8`.
    pub fn to_u8(&self) -> u8 {
        match self {
            MerkleTreeNodeType::Commit => 0u8,
            MerkleTreeNodeType::Dir => 1u8,
            MerkleTreeNodeType::VNode => 2u8,
            MerkleTreeNodeType::File => 3u8,
            MerkleTreeNodeType::FileChunk => 4u8,
        }
    }

    /// Deserialize a `u8` value into a `MerkleTreeNodeType`.
    /// Panics if the `u8` value is not a valid `MerkleTreeNodeType`.
    pub fn from_u8_unwrap(val: u8) -> MerkleTreeNodeType {
        Self::from_u8(val).expect("Invalid MerkleTreeNodeType: {val}")
    }

    /// Deserialize a `u8` value into a `MerkleTreeNodeType`.
    /// This function is 1:1 with `to_u8`: all outputs from `to_u8` result in an `Ok`.
    pub fn from_u8(val: u8) -> Result<MerkleTreeNodeType, InvalidMerkleTreeNodeType> {
        match val {
            0u8 => Ok(MerkleTreeNodeType::Commit),
            1u8 => Ok(MerkleTreeNodeType::Dir),
            2u8 => Ok(MerkleTreeNodeType::VNode),
            3u8 => Ok(MerkleTreeNodeType::File),
            4u8 => Ok(MerkleTreeNodeType::FileChunk),
            _ => Err(InvalidMerkleTreeNodeType(val)),
        }
    }
}

/// Allows for types to identify themselves as a specific kind of merkle tree node via the
/// `MerkleTreeNodeType` enum. This is critical for defining the node's stable on-disk
/// byte representation.
pub trait MerkleTreeNodeIdType {
    /// The type of merkle tree node this node can serialize & deserialize into.
    fn node_type(&self) -> MerkleTreeNodeType;

    /// The stable merkle hash of this node.
    fn hash(&self) -> MerkleHash;
}

/// Trait for things that are Merkle tree nodes.
///
/// Critical functionality is to be able to serialize node, compute a stable merkle hash, and
/// identify the node as a `MerkleTreeNodeType`. The Debug & Display constraints are for
/// convenience.
///
/// Since this trait is used as a parameter for generic functions, it must be object-safe. This
/// means that it cannot extend `Serialize` since that has a `<S: Serializer>` parameter on the
/// `serialize` method, which causes the type to not be implementable as a vtable lookup.
///
/// This is why the trait contains a manual serialization (`to_msgpack_bytes`) method. For types
/// that do implement `Serialize`, there's a blanket implementation that delegates the `serialize`
/// call to the `to_msgpack_bytes` method.
pub trait TMerkleTreeNode: MerkleTreeNodeIdType + Debug + Display {
    /// Serialize this node to a MsgPack byte array.
    fn to_msgpack_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error>;
}

/// Blanket implementation for Merkle tree nodes that implement serde's `Serialize` trait.
impl<T: Serialize + MerkleTreeNodeIdType + Debug + Display> TMerkleTreeNode for T {
    /// Uses serde to serialize this node.
    fn to_msgpack_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        let mut buf = Vec::new();
        let x = self.serialize(&mut rmp_serde::Serializer::new(&mut buf));
        x.map(|_| buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::merkle_tree::node::{CommitNode, DirNode, FileChunkNode, FileNode, VNode};

    use super::*;

    #[test]
    fn test_nodes_implement_trait() {
        /// this only exists so we can check that all node types implement `TMerkleTreeNode`
        /// it will be a compile-time error if a node type does not implement the trait
        fn is_tmerkletreenode<T: TMerkleTreeNode>(_: T) {}

        is_tmerkletreenode(CommitNode::default());
        is_tmerkletreenode(DirNode::default());
        is_tmerkletreenode(VNode::default());
        is_tmerkletreenode(FileNode::default());
        is_tmerkletreenode(FileChunkNode::default());
    }
}
