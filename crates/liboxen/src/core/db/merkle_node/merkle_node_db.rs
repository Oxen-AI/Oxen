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

use std::cell::Cell;
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

use crate::model::merkle_tree::node::legacy_pre_025::decodes_as_pre_v025;
use crate::model::merkle_tree::node::{
    EMerkleTreeNode, MerkleTreeNode, MerkleTreeNodeType, TMerkleTreeNode,
};

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

/// Prefix every current node payload starts with: `rmp_serde` encodes the one-field wrapper
/// struct as a 1-element array holding an externally-tagged enum, so the variant name lands in
/// the bytes. Payloads written before v0.25.0 are the bare data struct and carry no tag.
/// `FileChunk` is the exception — it has no enum wrapper at all, so the tag says nothing about it.
const CURRENT_NODE_PAYLOAD_TAG: &[u8] = b"\x91\x81\xa7V0_25_0";

/// The envelope alone, without committing to which variant it names. Distinguishes "written
/// before the envelope existed" from "wrapped, but naming a variant this build does not know" —
/// a payload tagged by some future variant also fails the [`CURRENT_NODE_PAYLOAD_TAG`] check, and
/// calling that one *older* than v0.25.0 would be exactly backwards.
const NODE_ENVELOPE_PREFIX: &[u8] = b"\x91\x81";

thread_local! {
    /// Set only for the duration of a [`RetiredFormatLogGuard`]. Thread-local rather than global
    /// so one caller's bulk scan cannot silence the warning for concurrent request handlers.
    static RETIRED_FORMAT_LOGGING_SUPPRESSED: Cell<bool> = const { Cell::new(false) };
}

fn retired_format_logging_suppressed() -> bool {
    RETIRED_FORMAT_LOGGING_SUPPRESSED.with(|suppressed| suppressed.get())
}

/// Restores whatever suppression state was in force when this guard was taken, so a guard nested
/// inside another does not un-suppress the outer scope on the way out.
#[must_use = "suppression lasts only as long as the guard is held"]
pub(crate) struct RetiredFormatLogGuard {
    restore_to: bool,
}

impl Drop for RetiredFormatLogGuard {
    fn drop(&mut self) {
        RETIRED_FORMAT_LOGGING_SUPPRESSED.with(|suppressed| suppressed.set(self.restore_to));
    }
}

/// Silence the per-node retired-format warning on this thread until the guard drops.
///
/// For a caller that reads one repository, the warning is the whole point: it surfaces the
/// condition even on paths that discard the error. For one that deliberately decodes every node
/// in a fleet, it is duplication of that caller's own output at a scale — tens of thousands of
/// lines — that buries the result it was meant to explain.
///
/// Only covers the calling thread, so a scan that grows a worker pool has to take the guard on
/// each worker.
pub(crate) fn suppress_retired_format_logging() -> RetiredFormatLogGuard {
    let restore_to = RETIRED_FORMAT_LOGGING_SUPPRESSED.with(|suppressed| suppressed.replace(true));
    RetiredFormatLogGuard { restore_to }
}

/// Whether this payload was written before the node types gained their enum envelope.
///
/// The single definition of "pre-v0.25.0 on disk", used by the classifier below and by anything
/// counting the population still awaiting migration.
///
/// Missing the envelope is necessary but not sufficient: damage to a payload's leading bytes
/// destroys the envelope too, so the payload must also decode as the shape that release wrote.
/// Without that second half, a corrupt node is indistinguishable from a legacy one, and the two
/// call for opposite remedies.
///
/// `FileChunk` never had an envelope, so the tag says nothing about it. A payload carrying an
/// envelope that names some variant this build does not know is not *older* than v0.25.0, so it
/// is excluded too.
pub(crate) fn is_pre_v025_payload(dtype: MerkleTreeNodeType, data: &[u8]) -> bool {
    dtype != MerkleTreeNodeType::FileChunk
        && !data.starts_with(CURRENT_NODE_PAYLOAD_TAG)
        && !data.starts_with(NODE_ENVELOPE_PREFIX)
        && decodes_as_pre_v025(dtype, data)
}

/// Classify a failure to decode a node payload, separating a retired on-disk format from a
/// damaged one. A retired format is a property of the repository and is answered by migrating it;
/// damage is a property of the bytes and is answered by restoring them.
///
/// Only `Commit` reaches the retired-format arm. `Dir`, `VNode`, and `File` each try the
/// pre-v0.25.0 shape inside their own `deserialize`, so a payload in that shape decodes there and
/// never arrives here, and `FileChunk` never had an envelope to lose. `CommitNode::deserialize`
/// has no such fallback, so this is the only place that shape is recognized for it.
///
/// Every conversion from a decode error into a [`MerkleDbError`] goes through here — the `#[from]`
/// on [`MerkleDbError::Decode`] will silently classify a retired-format node as corruption if a
/// new decode site reaches for `?` instead.
fn classify_decode_failure(
    dtype: MerkleTreeNodeType,
    hash: MerkleHash,
    data: &[u8],
    err: rmp_serde::decode::Error,
) -> MerkleDbError {
    if is_pre_v025_payload(dtype, data) {
        // Logged where the condition is detected rather than where it surfaces: callers that
        // discard the error still leave a trace, and a repository read from a warm node cache
        // reports the first time it actually reaches disk.
        if !retired_format_logging_suppressed() {
            log::warn!("Merkle node {hash} ({dtype:?}) predates Oxen v0.25.0 and cannot be read");
        }
        MerkleDbError::PreV025Node { dtype, hash }
    } else {
        MerkleDbError::Decode(err)
    }
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
    #[error("LMDB merkle node store error: {0}")]
    Lmdb(#[from] crate::lmdb::LmdbLayerError),
    #[error("Cannot encode a Merkle node: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("Cannot decode a Merkle node: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("Merkle node {hash} ({dtype:?}) predates Oxen v0.25.0 and cannot be read")]
    PreV025Node {
        dtype: MerkleTreeNodeType,
        hash: MerkleHash,
    },
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
    #[error(
        "Merkle tar entry {path} declares {size} bytes, exceeding the {max}-byte per-entry limit"
    )]
    OversizedTarEntry { path: String, size: u64, max: u64 },
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
    /// The tar archive ended while a node still had only one of its two
    /// (`node`, `children`) blobs, so the archive is truncated or malformed.
    #[error("Incomplete merkle node {hash} in tar archive: missing {missing} blob")]
    IncompleteNode { hash: MerkleHash, missing: String },
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
/// unit by [`close`](Self::close) — there is no incremental flushing, so a node is written exactly
/// once and atomically. A write-mode db dropped without `close` discards its buffers unwritten (see
/// the `Drop` impl) rather than risk persisting a partial node.
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
    /// True once the write buffers have been persisted by `close`. A write-mode db must reach this
    /// state before being dropped; `Drop` checks it to detect (and assert on) a forgotten `close`.
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
        let node = Self::to_node(self.dtype, self.node_id, &self.data())?;
        Ok(node)
    }

    fn to_node(
        dtype: MerkleTreeNodeType,
        hash: MerkleHash,
        data: &[u8],
    ) -> Result<EMerkleTreeNode, MerkleDbError> {
        EMerkleTreeNode::from_type_and_bytes(dtype, data)
            .map_err(|err| classify_decode_failure(dtype, hash, data, err))
    }

    pub(crate) fn open_read_only(
        store: Arc<dyn MerkleNodeStore>,
        hash: &MerkleHash,
    ) -> Result<Self, MerkleDbError> {
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
        store: Arc<dyn MerkleNodeStore>,
        node: &impl TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self, MerkleDbError> {
        let mut db = Self {
            dtype: node.node_type(),
            node_id: node.hash(),
            parent_id,
            read_only: false,
            store,
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
    /// back. A second call after a successful flush errors with [`MerkleDbError::CloseBeforeOpen`].
    /// A write-mode db dropped without calling this discards its buffers unwritten rather than
    /// persisting a partial node.
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
        let node_id = self.node_id;
        let Some(lookup) = self.lookup.as_ref() else {
            return Err(MerkleDbError::ReadBeforeOpen);
        };

        // Parse the node parent id
        let data_type = MerkleTreeNodeType::from_u8(lookup.data_type)?;
        let parent_id = MerkleTreeNode::deserialize_id(&lookup.data, data_type)
            .map_err(|err| classify_decode_failure(data_type, node_id, &lookup.data, err))?;

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
                node: Self::to_node(dtype, MerkleHash::new(*hash), &data)?,
                children: Vec::new(),
            };
            // log::debug!("Loaded node {:?}", node);
            ret.push((MerkleHash::new(*hash), node));
        }

        Ok(ret)
    }
}

impl Drop for MerkleNodeDB {
    /// A write-mode db must be explicitly `close`d so its buffered node is persisted as one unit and
    /// any persist error can propagate. `Drop` deliberately does **not** flush: a flush here could
    /// only ever persist whatever children happened to be buffered before an early return — a
    /// silently truncated node. Instead the buffers are dropped unwritten (no write is issued), so a
    /// forgotten `close()` trips a debug assertion (failing the test) rather than corrupting the
    /// store; in release it is logged and the node is simply never written, surfacing downstream as
    /// a missing node.
    fn drop(&mut self) {
        // Normal drop: read-only, already flushed, or buffers already taken — nothing to do.
        if self.read_only || self.flushed || self.node_buf.is_none() {
            return;
        }
        // Reached Drop in write mode without close(): a bug. Don't mask an in-flight panic by
        // panicking again while unwinding.
        if !std::thread::panicking() {
            debug_assert!(
                false,
                "MerkleNodeDB for node {} dropped without close(); buffered node discarded unwritten",
                self.node_id
            );
        }
        log::error!(
            "MerkleNodeDB for node {} dropped without close(); buffered node discarded unwritten",
            self.node_id
        );
    }
}

#[cfg(test)]
mod to_node_tests {
    use super::*;
    use crate::model::merkle_tree::node::commit_node::ECommitNode;
    use crate::model::merkle_tree::node::{CommitNode, DirNode, FileChunkNode, VNode};

    const HASH_VALUE: u128 = 42;

    fn hash() -> MerkleHash {
        MerkleHash::new(HASH_VALUE)
    }

    // `CURRENT_NODE_PAYLOAD_TAG` is ten bytes of hand-written assumption about what `rmp_serde`
    // emits: a one-field wrapper struct, an externally-tagged enum, a seven-character variant
    // name. Nothing else checks it. If any of those shifts — a field added to a *wrapper* struct
    // turns 0x91 into 0x92 — the tag silently stops matching and every damaged node starts being
    // reported as a retired format, with no test failing. Compare it against real writer output.
    #[test]
    fn the_tag_matches_what_the_writer_emits() {
        let wrapped = [
            ("vnode", rmp_serde::to_vec(&VNode::default())),
            ("dir", rmp_serde::to_vec(&DirNode::default())),
            ("commit", rmp_serde::to_vec(&CommitNode::default())),
        ];
        for (label, bytes) in wrapped {
            let bytes = bytes.expect("node should serialize");
            assert!(
                bytes.starts_with(CURRENT_NODE_PAYLOAD_TAG),
                "{label} no longer starts with the tag the classifier matches: {bytes:02x?}"
            );
        }

        // The exception the classifier special-cases: FileChunk has no enum wrapper, so its
        // payloads never carry the tag. If it ever gains one, that special case goes stale.
        let chunk = rmp_serde::to_vec(&FileChunkNode::default()).expect("chunk should serialize");
        assert!(
            !chunk.starts_with(CURRENT_NODE_PAYLOAD_TAG),
            "FileChunk gained an envelope; the dtype special case is now wrong"
        );
    }

    #[test]
    fn a_pre_v025_payload_reads_through_the_legacy_fallback() {
        // The pre-0.25 shape: the bare data struct, with no enum envelope around it.
        // Built positionally rather than from a struct so the test pins the bytes, not a type
        // that could drift alongside the code under test.
        // A hash inside the msgpack payload is a `u128` encoded big-endian, which is the opposite
        // of the container header around it — that one writes hashes with `to_le_bytes`.
        let mut legacy = vec![0x92, 0xc4, 0x10];
        legacy.extend_from_slice(&HASH_VALUE.to_be_bytes());
        legacy.extend_from_slice(b"\xa5VNode");

        let node = MerkleNodeDB::to_node(MerkleTreeNodeType::VNode, hash(), &legacy)
            .expect("a pre-0.25 payload must read");

        let EMerkleTreeNode::VNode(vnode) = node else {
            panic!("expected a vnode, got {node:?}");
        };
        assert_eq!(*vnode.hash(), hash());
        // The old shape carries no entry count and a listing does not need one — it counts what
        // it returns. Deriving the real value belongs to the migration that rewrites these nodes.
        assert_eq!(vnode.num_entries(), 0);
    }

    #[test]
    fn untagged_bytes_of_no_known_shape_report_a_decode_error() {
        // No envelope, and not readable as the pre-0.25 struct either. Nothing about these bytes
        // is a retired format; naming one sends an operator to run a migration, which cannot
        // repair a payload that decodes as nothing.
        let untagged = vec![0x92, 0xc1, 0xc1];

        let err = MerkleNodeDB::to_node(MerkleTreeNodeType::VNode, hash(), &untagged)
            .expect_err("bytes of no known shape must not decode");

        assert!(
            matches!(err, MerkleDbError::Decode(_)),
            "expected Decode, got {err:?}"
        );
    }

    #[test]
    fn a_genuine_pre_v025_commit_still_reports_the_retired_format() {
        // Commit is the one type whose `deserialize` has no legacy fallback, so a retired-format
        // commit reaches the classifier and the classifier's own decode attempt is what keeps it
        // from being called corrupt. For the others the fallback has already run, and reaching
        // the classifier at all means neither shape decoded.
        //
        // The payload is the current data struct serialized bare, without the enum envelope,
        // which is exactly what pre-0.25.0 wrote: that release's field list is unchanged.
        let ECommitNode::V0_25_0(ref data) = CommitNode::default().node;
        let legacy = rmp_serde::to_vec(data).expect("commit data should serialize");

        let err = MerkleNodeDB::to_node(MerkleTreeNodeType::Commit, hash(), &legacy)
            .expect_err("a bare commit payload does not decode as the enveloped shape");

        assert!(
            matches!(err, MerkleDbError::PreV025Node { .. }),
            "expected PreV025Node, got {err:?}"
        );
    }

    #[test]
    fn damage_to_the_leading_bytes_is_not_mistaken_for_the_retired_format() {
        // The case that motivated separating the two: corruption anywhere in the first bytes
        // takes the envelope with it, leaving a payload that looks untagged. Judging by prefix
        // alone called this a retired format, which both named the wrong remedy and inflated
        // every count of the population still awaiting migration.
        //
        // `Commit` is the case that carries this test. It is the only type whose `deserialize`
        // has no legacy fallback, so the classifier's own decode attempt is the only thing
        // telling damage from a retired format. For the other two that attempt can only agree
        // with the fallback that already failed, so they are here as regression cover rather
        // than as the thing being proven.
        let payloads = [
            (
                MerkleTreeNodeType::Commit,
                rmp_serde::to_vec(&CommitNode::default()),
            ),
            (
                MerkleTreeNodeType::Dir,
                rmp_serde::to_vec(&DirNode::default()),
            ),
            (
                MerkleTreeNodeType::VNode,
                rmp_serde::to_vec(&VNode::default()),
            ),
        ];

        for (dtype, payload) in payloads {
            let mut damaged = payload.expect("node should serialize");
            damaged[..3].fill(0xc1); // never a valid msgpack byte

            let err = MerkleNodeDB::to_node(dtype, hash(), &damaged)
                .expect_err("a damaged payload must not decode");

            assert!(
                matches!(err, MerkleDbError::Decode(_)),
                "expected Decode for {dtype:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn damaged_current_payload_still_reports_a_decode_error() {
        // Carries the variant tag, so it is a current-format node that is simply broken.
        // Misreporting this as a retired format would send the reader chasing the wrong problem.
        let mut damaged = CURRENT_NODE_PAYLOAD_TAG.to_vec();
        damaged.extend_from_slice(&[0xc1]); // never a valid msgpack byte

        let err = MerkleNodeDB::to_node(MerkleTreeNodeType::VNode, hash(), &damaged)
            .expect_err("a damaged payload must not decode");

        assert!(
            matches!(err, MerkleDbError::Decode(_)),
            "expected Decode, got {err:?}"
        );
    }

    #[test]
    fn the_log_guard_restores_the_warning_when_it_drops() {
        // A leaked suppression would silence the condition for everything that runs afterwards on
        // this thread, which is worse than the noise it exists to prevent.
        assert!(!retired_format_logging_suppressed());
        {
            let _quiet = suppress_retired_format_logging();
            assert!(retired_format_logging_suppressed());
            {
                let _nested = suppress_retired_format_logging();
                assert!(retired_format_logging_suppressed());
            }
            assert!(
                retired_format_logging_suppressed(),
                "an inner guard dropping must not un-suppress the scope still holding one"
            );
        }
        assert!(!retired_format_logging_suppressed());
    }

    #[test]
    fn a_variant_this_build_does_not_know_is_not_called_old() {
        // Name tagging exists so a node keeps its bytes across variant changes, which means a
        // payload written by a *newer* build also fails the V0_25_0 check. Reporting that as
        // predating v0.25.0 would point the reader in precisely the wrong direction.
        let mut future = b"\x91\x81\xa7V0_99_0".to_vec();
        future.extend_from_slice(&[0xc1]);

        let err = MerkleNodeDB::to_node(MerkleTreeNodeType::VNode, hash(), &future)
            .expect_err("a variant this build has no arm for must not decode");

        assert!(
            matches!(err, MerkleDbError::Decode(_)),
            "expected Decode for an unknown-but-tagged payload, got {err:?}"
        );
    }

    #[test]
    fn file_chunk_is_never_reported_as_the_retired_format() {
        // FileChunk has no enum wrapper by design, so its payloads never carry the tag. Judging
        // it by the tag would label every damaged chunk a retired-format node.
        let err = MerkleNodeDB::to_node(MerkleTreeNodeType::FileChunk, hash(), &[0xc1])
            .expect_err("a damaged payload must not decode");

        assert!(
            matches!(err, MerkleDbError::Decode(_)),
            "expected Decode, got {err:?}"
        );
    }

    #[test]
    fn current_payload_still_decodes() {
        let bytes = rmp_serde::to_vec(&VNode::default()).expect("VNode should serialize");

        MerkleNodeDB::to_node(MerkleTreeNodeType::VNode, hash(), &bytes)
            .expect("a current-format payload must decode");
    }
}
