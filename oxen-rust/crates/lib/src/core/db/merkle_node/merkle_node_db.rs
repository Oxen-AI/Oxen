/*
LMDB-backed merkle tree node database.

All nodes are stored in a single LMDB environment at .oxen/tree/lmdb/
with two named databases:
  - "nodes":    hash -> NodeRecord (dtype, parent_id, msgpack data)
  - "children": hash -> ChildEntry[] (dtype, hash, msgpack data per child)

Writing happens once at commit (accumulated in memory, flushed on close/drop).
Reading is via LMDB read transactions (concurrent, zero-copy).
*/

use rmp_serde::Serializer;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::constants;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::MerkleHash;

use crate::model::merkle_tree::node::{
    CommitNode, DirNode, EMerkleTreeNode, FileChunkNode, FileNode, MerkleTreeNode,
    MerkleTreeNodeType, TMerkleTreeNode, VNode,
};

use super::node_record::{ChildEntry, NodeRecord};
use super::tree_store::{get_tree_store, TreeStore};

// Keep these around for the network sync wire format (compress/unpack).
pub fn node_db_prefix(hash: &MerkleHash) -> PathBuf {
    let hash_str = hash.to_string();
    let dir_prefix_len = 3;
    let dir_prefix = hash_str.chars().take(dir_prefix_len).collect::<String>();
    let dir_suffix = hash_str.chars().skip(dir_prefix_len).collect::<String>();
    Path::new(&dir_prefix).join(&dir_suffix)
}

pub fn node_db_path(repo: &LocalRepository, hash: &MerkleHash) -> PathBuf {
    let dir_prefix = node_db_prefix(hash);
    repo.path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(constants::NODES_DIR)
        .join(dir_prefix)
}

pub struct MerkleNodeDB {
    pub dtype: MerkleTreeNodeType,
    pub node_id: MerkleHash,
    pub parent_id: Option<MerkleHash>,
    read_only: bool,
    store: Arc<TreeStore>,
    // Node data (msgpack bytes, either read from LMDB or serialized for writing)
    data: Vec<u8>,
    // Write mode: accumulated children, flushed on close/drop
    pending_children: Vec<ChildEntry>,
    flushed: bool,
}

impl MerkleNodeDB {
    pub fn num_children(&self) -> u64 {
        if self.read_only {
            // For read mode, we need to load children to know the count.
            // This is rarely called in read mode outside of map(), so
            // we do a quick LMDB lookup.
            self.store
                .get_children(&self.node_id)
                .ok()
                .flatten()
                .map(|c| c.len() as u64)
                .unwrap_or(0)
        } else {
            self.pending_children.len() as u64
        }
    }

    pub fn data(&self) -> Vec<u8> {
        self.data.clone()
    }

    pub fn node(&self) -> Result<EMerkleTreeNode, OxenError> {
        Self::to_node(self.dtype, &self.data)
    }

    pub fn to_node(dtype: MerkleTreeNodeType, data: &[u8]) -> Result<EMerkleTreeNode, OxenError> {
        match dtype {
            MerkleTreeNodeType::Commit => {
                Ok(EMerkleTreeNode::Commit(CommitNode::deserialize(data)?))
            }
            MerkleTreeNodeType::Dir => Ok(EMerkleTreeNode::Directory(DirNode::deserialize(data)?)),
            MerkleTreeNodeType::File => Ok(EMerkleTreeNode::File(FileNode::deserialize(data)?)),
            MerkleTreeNodeType::VNode => Ok(EMerkleTreeNode::VNode(VNode::deserialize(data)?)),
            MerkleTreeNodeType::FileChunk => Ok(EMerkleTreeNode::FileChunk(
                FileChunkNode::deserialize(data)?,
            )),
        }
    }

    pub fn exists(repo: &LocalRepository, hash: &MerkleHash) -> bool {
        get_tree_store(repo)
            .and_then(|store| store.node_exists(hash))
            .unwrap_or(false)
    }

    pub fn open_read_only(repo: &LocalRepository, hash: &MerkleHash) -> Result<Self, OxenError> {
        let store = get_tree_store(repo)?;
        let record = store
            .get_node(hash)?
            .ok_or_else(|| OxenError::basic_str(format!("Node not found in LMDB: {hash}")))?;

        Ok(Self {
            dtype: MerkleTreeNodeType::from_u8(record.dtype),
            node_id: *hash,
            parent_id: Some(MerkleHash::new(record.parent_id)),
            read_only: true,
            store,
            data: record.data,
            pending_children: vec![],
            flushed: true,
        })
    }

    pub fn open_read_write_if_not_exists(
        repo: &LocalRepository,
        node: &impl TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Option<Self>, OxenError> {
        if Self::exists(repo, &node.hash()) {
            log::debug!(
                "open_read_write_if_not_exists skipping existing node {}",
                node.hash()
            );
            Ok(None)
        } else {
            Ok(Some(Self::open_read_write(repo, node, parent_id)?))
        }
    }

    pub fn open_read_write(
        repo: &LocalRepository,
        node: &impl TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self, OxenError> {
        let store = get_tree_store(repo)?;

        // Serialize node data via msgpack (same format as before)
        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf)).unwrap();

        log::debug!("open_read_write merkle node {} in LMDB", node.hash());

        Ok(Self {
            dtype: node.node_type(),
            node_id: node.hash(),
            parent_id,
            read_only: false,
            store,
            data: buf,
            pending_children: vec![],
            flushed: false,
        })
    }

    pub fn add_child<N: TMerkleTreeNode>(&mut self, item: &N) -> Result<(), OxenError> {
        if self.read_only {
            return Err(OxenError::basic_str("Cannot write to read-only db"));
        }

        let mut buf = Vec::new();
        item.serialize(&mut Serializer::new(&mut buf)).unwrap();

        self.pending_children.push(ChildEntry {
            dtype: item.node_type().to_u8(),
            hash: item.hash().to_u128(),
            data: buf,
        });

        Ok(())
    }

    pub fn map(&self) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError> {
        let children = self
            .store
            .get_children(&self.node_id)?
            .unwrap_or_default();

        // Compute parent_id from this node's data
        let parent_id = MerkleTreeNode::deserialize_id(&self.data, self.dtype)?;

        let mut ret = Vec::with_capacity(children.len());
        for child in children {
            let dtype = MerkleTreeNodeType::from_u8(child.dtype);
            let node = MerkleTreeNode {
                parent_id: Some(parent_id),
                hash: MerkleHash::new(child.hash),
                node: Self::to_node(dtype, &child.data)?,
                children: Vec::new(),
            };
            ret.push((MerkleHash::new(child.hash), node));
        }

        Ok(ret)
    }

    fn flush_to_lmdb(&mut self) -> Result<(), OxenError> {
        if self.flushed {
            return Ok(());
        }

        let node_record = NodeRecord {
            dtype: self.dtype.to_u8(),
            parent_id: self.parent_id.map(|h| h.to_u128()).unwrap_or(0),
            data: self.data.clone(),
        };

        self.store.put_node_with_children(
            &self.node_id,
            &node_record,
            &self.pending_children,
        )?;

        self.flushed = true;
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), OxenError> {
        if !self.read_only {
            self.flush_to_lmdb()?;
        }
        Ok(())
    }
}

impl Drop for MerkleNodeDB {
    fn drop(&mut self) {
        if !self.read_only
            && !self.flushed
            && let Err(e) = self.flush_to_lmdb()
        {
            log::error!("Failed to flush MerkleNodeDB to LMDB on drop: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test;

    #[test]
    fn test_merkle_node_db() -> Result<(), OxenError> {
        test::run_empty_dir_test(|_dir| {
            // TODO: update test for LMDB-backed MerkleNodeDB
            Ok(())
        })
    }
}
