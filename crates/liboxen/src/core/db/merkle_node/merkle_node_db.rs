/*
Write a db that is optimized for opening, finding by hash, listing.

Rocks db is too slow. It was taking ~100ms to open a db, and if we have > 10 vnodes,
that means we are taking > 1 second to open before doing any operations.

We can make this faster by using a simple file format.

Writing happens once at commit, then we read many times from the server and status.

Is also already sharded and optimized in the tree structure.
Reading, find by hash, listing is high throughput.

On Disk Format:

All nodes are stored in .oxen/tree/{NODE_HASH} and contain two files:
- node: the metadata for the node and a lookup table for all the children
- data: the serialized nodes

node file format:
- node data
- data-type,hash-int,data-offset,data-length

children file format:
- data blobs


For example, data for a vnode of hash 1234 with two children:

.oxen/tree/1234/node
    0 # data length
    4 # data

    0 # file data type
    1235 # hash
    0 # data offset
    100 # data length

    1 # dir data type
    1236 # hash
    100 # data offset
    100 # data length

.oxen/tree/1234/children
    {file data node}
    {dir data node}
*/

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::constants;
use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::model::merkle_tree::node_type::InvalidMerkleTreeNodeType;

use crate::model::merkle_tree::node::{
    EMerkleTreeNode, MerkleTreeNode, MerkleTreeNodeType, TMerkleTreeNode,
};

use super::fs_merkle_node_store::FsMerkleNodeStore;
use super::merkle_node_store::MerkleNodeStore;

pub(crate) const NODE_FILE: &str = "node";
pub(crate) const CHILDREN_FILE: &str = "children";

/// An absolute path to the directory for the Merkle node's `node` and `children` files.
pub(crate) fn node_db_path(repo_path: &Path, hash: &MerkleHash) -> PathBuf {
    let dir_prefix = hash.to_hex_hash().node_db_prefix();
    repo_path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(constants::NODES_DIR)
        .join(dir_prefix)
}

/// Errors that the Merkle node database can encounter when reading and writing nodes.
#[derive(Debug, thiserror::Error)]
pub enum MerkleDbError {
    // Errors encountered in the operation of the custom file format based Merkle tree store.
    #[error("Must call open before closing")]
    CloseBeforeOpen,
    #[error("Cannot write to read-only db")]
    ReadOnly,
    #[error("Cannot write size after writing data")]
    IllegalOperationWriteSizeFirst,
    #[error("Must call open before writing")]
    WriteBeforeOpen,
    #[error("Must call open before reading")]
    ReadBeforeOpen,
    // wrappers
    #[error("Error writing to a node or children file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Cannot encode a Merkle node: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("Cannot decode a Merkle node: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("{0}")]
    TypeMismatch(#[from] InvalidMerkleTreeNodeType),
    #[error("Failed to create directory: {0}")]
    DirCreate(Box<OxenError>), // TODO: replace with FsError from upcoming refactoring PR
    #[error("Failed to open file: {0}")]
    Open(Box<OxenError>), // TODO: replace with FsError from upcoming refactoring PR
    #[error("Filesystem operation failed during merkle transport: {0}")]
    FsTransport(Box<OxenError>), // TODO: replace with FsError from upcoming refactoring PR
    #[error("Could not read entries from merkle tree tar archive: {0}")]
    CannotReadMerkle(std::io::Error),
    #[error(
        "Unsupported tar entry type for {path}: only regular files and directories are allowed"
    )]
    UnsupportedTarEntry { path: String },
    #[error("Path traversal detected in merkle tar entry: {0}")]
    PathTraversal(String),
    /// The merkle tarball entry's path doesn't have the expected
    /// `tree/nodes/{prefix}/{suffix}/[node|children]` shape. Either the path is
    /// shorter or longer than expected, or the leaf file isn't `node`/`children`,
    /// or one of the path components isn't valid UTF-8.
    #[error("Invalid merkle tar archive structure at {entry_path:?}: {reason}")]
    InvalidTarStructure { entry_path: String, reason: String },
    /// A `{prefix}/{suffix}` directory entry was found, but the concatenated
    /// `{prefix}{suffix}` string doesn't parse as a hexadecimal `u128` node id.
    #[error("Invalid merkle node id {id:?} in tar archive (not a hex u128): {source}")]
    InvalidNodeIdHex {
        id: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("Missing node dir for hash {0}")]
    MissingNodeDir(MerkleHash),
    #[error("Missing oxen tree/nodes dir in this repository")]
    MissingTreeNodesDir,
}

impl MerkleDbError {
    pub(crate) fn dir_create(err: OxenError) -> Self {
        Self::DirCreate(Box::new(err))
    }

    pub(crate) fn fs_transport(err: OxenError) -> Self {
        Self::FsTransport(Box::new(err))
    }
}

struct MerkleNodeLookup {
    data_type: u8,
    parent_id: u128,
    data: Vec<u8>,
    num_children: u64,
    /// hash -> (dtype, offset, length)
    offsets: Vec<(u128, (u8, u64, u64))>,
}

impl MerkleNodeLookup {
    /// Takes the bytes of a node's `node` blob and deserializes it into a [`MerkleNodeLookup`].
    #[inline(always)]
    fn deserialize(file_data: Bytes) -> Result<Self, MerkleDbError> {
        // Create a cursor to iterate over data
        let mut cursor = std::io::Cursor::new(file_data);

        // Read the data type
        let mut buffer = [0u8; 1]; // u8 is 1 byte
        cursor.read_exact(&mut buffer)?;
        let node_data_type = u8::from_le_bytes(buffer);
        // log::debug!(
        //     "MerkleNodeLookup.deserialize() data_type: {:?}",
        //     MerkleTreeNodeType::from_u8(node_data_type)
        // );

        // Read the parent id
        let mut buffer = [0u8; 16]; // u128 is 16 bytes
        cursor.read_exact(&mut buffer)?;
        let parent_id = u128::from_le_bytes(buffer);
        // log::debug!("MerkleNodeLookup.deserialize() parent_id: {:x}", parent_id);

        // Read the length of the node data
        let mut buffer = [0u8; 4]; // u32 is 4 bytes
        cursor.read_exact(&mut buffer)?;
        let data_len = u32::from_le_bytes(buffer);
        // log::debug!("MerkleNodeLookup.deserialize() data_len: {}", data_len);

        // Read the length of the data and save buffer
        let mut buffer = vec![0u8; data_len as usize];
        cursor.read_exact(&mut buffer)?;
        let data = buffer;
        // log::debug!("MerkleNodeLookup.deserialize() read data: {}", data.len());

        // Read the map of offsets
        let mut offsets: Vec<(u128, (u8, u64, u64))> = Vec::new();
        let mut dtype_buffer = [0u8; 1]; // data-type u8 is 1 byte
        let mut hash_buffer = [0u8; 16]; // hash u128 is 16 bytes
        let mut offset_buffer = [0u8; 8]; // data-offset u64 is 8 bytes
        let mut len_buffer = [0u8; 8]; // data-length u64 is 8 bytes

        // Will loop until we hit an EOF error
        // let mut i = 0;
        while cursor.read_exact(&mut dtype_buffer).is_ok() {
            // log::debug!("MerkleNodeLookup.deserialize() --reading-- {}", i);

            let data_type = u8::from_le_bytes(dtype_buffer);
            // log::debug!(
            //     "MerkleNodeLookup.deserialize() got data_type {:?}",
            //     MerkleTreeNodeType::from_u8(data_type)
            // );

            // Read the hash
            cursor.read_exact(&mut hash_buffer)?;
            let hash = u128::from_le_bytes(hash_buffer);
            // log::debug!("MerkleNodeLookup.deserialize() got hash {:x}", hash);

            // Read the offset
            cursor.read_exact(&mut offset_buffer)?;
            let data_offset = u64::from_le_bytes(offset_buffer);
            // log::debug!("MerkleNodeLookup.deserialize() got data_offset {}", data_offset);

            // Read the length
            cursor.read_exact(&mut len_buffer)?;
            let data_len = u64::from_le_bytes(len_buffer);
            // log::debug!("MerkleNodeLookup.deserialize() got data_len {}", data_len);

            offsets.push((hash, (data_type, data_offset, data_len)));
            // i += 1;
        }

        let num_children = offsets.len() as u64;
        // log::debug!(
        //     "MerkleNodeLookup.deserialize() parent_id {:x} num_children {}",
        //     parent_id,
        //     num_children
        // );
        Ok(Self {
            data_type: node_data_type,
            parent_id,
            data,
            num_children,
            offsets,
        })
    }
}

/// Reads and writes a single Merkle tree node and its children list.
///
/// The on-engine format is owned here (the `node` blob header + child lookup table, and the
/// concatenated `children` blob); *where* those two blobs are persisted is delegated to a
/// [`MerkleNodeStore`]. In read-write mode the blobs are accumulated in memory and persisted as one
/// unit by [`close`](Self::close) (or by `Drop` as a fallback) — there is no incremental flushing,
/// so a node is written exactly once and atomically.
///
/// The constructors take a `repo_path` and resolve the file backend internally; a later change
/// hands the store in from the repo so the backend (file vs. another engine) is selected once at
/// repository construction.
pub(crate) struct MerkleNodeDB {
    pub dtype: MerkleTreeNodeType,
    pub node_id: MerkleHash,
    pub parent_id: Option<MerkleHash>,
    read_only: bool,
    store: Arc<dyn MerkleNodeStore>,
    /// `node` blob accumulator (header + child lookup entries). `Some` in read-write mode until the
    /// node is persisted, then `None`.
    node_buf: Option<BytesMut>,
    /// `children` blob accumulator (concatenated child node data). `Some`/`None` in lockstep with
    /// `node_buf`.
    children_buf: Option<BytesMut>,
    /// True once the write buffers have been persisted, so `Drop` does not flush again.
    flushed: bool,
    /// Decoded `node` blob; `Some` only in read-only mode.
    lookup: Option<MerkleNodeLookup>,
    /// Running length of `children_buf`, written into each child's lookup entry.
    data_offset: u64,
}

impl MerkleNodeDB {
    pub fn data(&self) -> Vec<u8> {
        self.lookup
            .as_ref()
            .map(|lookup| lookup.data.to_owned())
            .unwrap_or_default()
    }

    pub fn node(&self) -> Result<EMerkleTreeNode, MerkleDbError> {
        let node = Self::to_node(self.dtype, &self.data())?;
        Ok(node)
    }

    fn to_node(
        dtype: MerkleTreeNodeType,
        data: &[u8],
    ) -> Result<EMerkleTreeNode, rmp_serde::decode::Error> {
        EMerkleTreeNode::from_type_and_bytes(dtype, data)
    }

    /// Whether a node has been written for `hash`. A store error is reported as `false`, mirroring
    /// `Path::exists()` (which the file backend is built on).
    pub(crate) fn exists(repo_path: &Path, hash: &MerkleHash) -> bool {
        Self::fs_store(repo_path).exists(hash).unwrap_or(false)
    }

    /// The file backend for the repo at `repo_path`. Resolved per-open for now; a later change has
    /// the repo own a single store and hand it in.
    fn fs_store(repo_path: &Path) -> Arc<dyn MerkleNodeStore> {
        Arc::new(FsMerkleNodeStore::new(repo_path))
    }

    pub(crate) fn open_read_only(
        repo_path: &Path,
        hash: &MerkleHash,
    ) -> Result<Self, MerkleDbError> {
        let store = Self::fs_store(repo_path);
        let node_bytes = store.read_node(hash)?;
        let lookup = MerkleNodeLookup::deserialize(node_bytes)?;
        let dtype = MerkleTreeNodeType::from_u8(lookup.data_type)?;
        // A zero parent id round-trips as `Some(MerkleHash::new(0))`, matching the historical
        // behavior that callers (e.g. `MerkleTreeNode::from_hash_uncached`) read directly.
        let parent_id = Some(MerkleHash::new(lookup.parent_id));
        Ok(Self {
            dtype,
            node_id: *hash,
            parent_id,
            read_only: true,
            store,
            node_buf: None,
            children_buf: None,
            flushed: true,
            lookup: Some(lookup),
            data_offset: 0,
        })
    }

    pub(crate) fn open_read_write(
        repo_path: &Path,
        node: &impl TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self, MerkleDbError> {
        let mut db = Self {
            dtype: node.node_type(),
            node_id: node.hash(),
            parent_id,
            read_only: false,
            store: Self::fs_store(repo_path),
            node_buf: Some(BytesMut::new()),
            children_buf: Some(BytesMut::new()),
            flushed: false,
            lookup: None,
            data_offset: 0,
        };
        db.write_node(node, parent_id)?;
        Ok(db)
    }

    /// Persist the buffered node and children blobs through the store. Call before the node is read
    /// back. A second call after a successful flush errors with [`MerkleDbError::CloseBeforeOpen`];
    /// `Drop` flushes as a fallback if this is never called.
    pub(crate) fn close(&mut self) -> Result<(), MerkleDbError> {
        if self.read_only {
            return Ok(());
        }
        self.flush()
    }

    fn flush(&mut self) -> Result<(), MerkleDbError> {
        let (Some(node_buf), Some(children_buf)) = (self.node_buf.take(), self.children_buf.take())
        else {
            return Err(MerkleDbError::CloseBeforeOpen);
        };
        self.store
            .write_node(&self.node_id, node_buf.freeze(), children_buf.freeze())?;
        self.flushed = true;
        Ok(())
    }

    /// Writes the node header (type, parent id, data) into the `node` blob buffer.
    /// WARNING: Sets the internal dtype, node_id, parent_id of `self` to the values from `node`.
    fn write_node(
        &mut self,
        node: &impl TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<(), MerkleDbError> {
        if self.read_only {
            return Err(MerkleDbError::ReadOnly);
        }

        if self.data_offset > 0 {
            return Err(MerkleDbError::IllegalOperationWriteSizeFirst);
        }

        let Some(node_buf) = self.node_buf.as_mut() else {
            return Err(MerkleDbError::WriteBeforeOpen);
        };

        log::trace!("write_node node: {}", node);

        node_buf.extend_from_slice(&node.node_type().to_u8().to_le_bytes());

        // Write parent id
        if let Some(parent_id) = parent_id {
            node_buf.extend_from_slice(&parent_id.to_le_bytes());
        } else {
            // write 16 bytes, each is zero => write a 0_u128
            node_buf.extend_from_slice(&[0u8; 16]);
        }

        // Write data length
        let buf = rmp_serde::to_vec(node)?;
        let data_len = buf.len() as u32;
        node_buf.extend_from_slice(&data_len.to_le_bytes());
        log::trace!("write_node Wrote data length {}", data_len);

        // Write data
        node_buf.extend_from_slice(&buf);

        self.dtype = node.node_type();
        self.node_id = node.hash();
        self.parent_id = parent_id;
        Ok(())
    }

    /// Appends a child: its lookup entry to the `node` blob and its data to the `children` blob.
    pub(crate) fn add_child(&mut self, item: &impl TMerkleTreeNode) -> Result<(), MerkleDbError> {
        if self.read_only {
            return Err(MerkleDbError::ReadOnly);
        }

        let data_offset = self.data_offset;
        let Some(node_buf) = self.node_buf.as_mut() else {
            return Err(MerkleDbError::WriteBeforeOpen);
        };
        let Some(children_buf) = self.children_buf.as_mut() else {
            return Err(MerkleDbError::WriteBeforeOpen);
        };

        let buf = rmp_serde::to_vec(item)?;
        let data_len = buf.len() as u64;

        node_buf.extend_from_slice(&item.node_type().to_u8().to_le_bytes());
        node_buf.extend_from_slice(&item.hash().to_le_bytes()); // id of child
        node_buf.extend_from_slice(&data_offset.to_le_bytes());
        node_buf.extend_from_slice(&data_len.to_le_bytes());

        children_buf.extend_from_slice(&buf);

        self.data_offset += data_len;

        Ok(())
    }

    pub(crate) fn map(&mut self) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, MerkleDbError> {
        // log::debug!("Loading merkle node db map");
        let Some(lookup) = self.lookup.as_ref() else {
            return Err(MerkleDbError::ReadBeforeOpen);
        };

        // Parse the node parent id
        let data_type = MerkleTreeNodeType::from_u8(lookup.data_type)?;
        let parent_id = MerkleTreeNode::deserialize_id(&lookup.data, data_type)?;

        let children_bytes = self.store.read_children(&self.node_id)?;
        // log::debug!("Loading merkle node db map got {} bytes", children_bytes.len());

        let mut ret: Vec<(MerkleHash, MerkleTreeNode)> =
            Vec::with_capacity(lookup.num_children as usize);

        let mut cursor = std::io::Cursor::new(children_bytes);
        // Iterate over offsets and read the data
        for (hash, (dtype, offset, len)) in lookup.offsets.iter() {
            // log::debug!("Loading dtype {:?}", MerkleTreeNodeType::from_u8(*dtype));
            // log::debug!("Loading offset {}", offset);
            // log::debug!("Loading len {}", len);
            cursor.seek(SeekFrom::Start(*offset))?;
            let mut data = vec![0; *len as usize];
            cursor.read_exact(&mut data)?;
            let dtype = MerkleTreeNodeType::from_u8(*dtype)?;
            let node = MerkleTreeNode {
                parent_id: Some(parent_id),
                hash: MerkleHash::new(*hash),
                node: Self::to_node(dtype, &data)?,
                children: Vec::new(),
            };
            // log::debug!("Loaded node {:?}", node);
            ret.push((MerkleHash::new(*hash), node));
        }

        Ok(ret)
    }
}

impl Drop for MerkleNodeDB {
    /// Fallback flush for write-mode dbs that were never explicitly `close`d. Callers should prefer
    /// `close()` so the persist error can propagate; here it can only be logged.
    fn drop(&mut self) {
        if self.read_only || self.flushed || self.node_buf.is_none() {
            return;
        }
        if let Err(err) = self.flush() {
            log::error!(
                "MerkleNodeDB: failed to flush node {} on drop: {err}",
                self.node_id
            );
        }
    }
}
