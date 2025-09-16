use std::fmt::Debug;
use std::fmt::Display;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;

use crate::{
    error::OxenError,
    model::{
        merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode},
        LocalRepository, MerkleHash, MerkleTreeNodeType, TMerkleTreeNode,
    },
};

// pub fn get_node_db(repo: &LocalRepository, hash: &MerkleHash) -> Box<dyn NodeDb> {}

pub struct MerkleNode {
    pub dtype: MerkleTreeNodeType,
    pub node_id: MerkleHash,
    pub parent_id: Option<MerkleHash>,
}

pub trait NodeDb {
    fn num_children(&self) -> u64;
    fn data(&self) -> Vec<u8>;
    fn node(&self) -> Result<EMerkleTreeNode, OxenError>;
    fn to_node(&self, dtype: MerkleTreeNodeType, data: &[u8])
        -> Result<EMerkleTreeNode, OxenError>;
    fn path(&self) -> PathBuf;
    fn map(&mut self) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError>;
    fn close(&mut self) -> Result<(), OxenError>;
}



pub trait NodeDbWriter: NodeDb {
    fn exists(repo: &LocalRepository, hash: &MerkleHash) -> bool;
    fn write_node<N: TMerkleTreeNode + Serialize + Debug + Display>(
        &mut self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<(), OxenError>;
    fn open_read_only(
        repo: &LocalRepository,
        hash: &MerkleHash,
    ) -> Result<Box<dyn NodeDb>, OxenError>
    where
        Self: Sized; // Add Sized bound for functions returning a trait object

    fn open_read_write_if_not_exists(
        repo: &LocalRepository,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Option<Box<dyn NodeDb>>, OxenError>
    where
        Self: Sized;

    fn open_read_write(
        repo: &LocalRepository,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Box<dyn NodeDb>, OxenError>
    where
        Self: Sized;

    fn open(path: impl AsRef<Path>, read_only: bool) -> Result<Box<dyn NodeDb>, OxenError>
    where
        Self: Sized;
    fn add_child(&mut self, item: &dyn TMerkleTreeNode) -> Result<(), OxenError>;
}
