//! [`MerkleUnpacker`] implementation for the [`LmdbBackend`].
//!
//! The wire format consumed is identical to the one [`super::super::file_backend::FileBackend`]
//! emits: tar-gz of `tree/nodes/{prefix}/{suffix}/{node,children}` entries, where
//! `node` & `children` follow the [`super::super::merkle_node_db::MerkleNodeDB`]
//! on-disk byte layout.
//!
//! Unpack is **streaming**: we drive [`crate::util::tar_stream::stream_unpack`] and
//! pair `node` / `children` entries by hash on the fly. As soon as both members of
//! a pair are available we parse and commit the parent's node + link rows; the
//! raw bytes get released immediately. Embedded children whose own `{prefix}/
//! {suffix}/` pair hasn't shown up are deferred and seeded at end-of-stream — same
//! semantics as the old buffered loop, but without holding every blob in RAM
//! simultaneously.
//!
//! Memory profile: O(currently-unpaired entries + embedded-children-without-own-
//! pair). For a well-formed archive whose `node` / `children` entries are
//! contiguous per hash (file backend's `append_dir_all` and LMDB's pack both
//! emit them this way), the unpaired set is ~1 hash at any time. Compare with
//! the old approach which buffered the full archive.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::constants::{NODES_DIR, TREE_DIR};
use crate::core::db::merkle_node::file_backend::{TarEntryKind, classify_tar_entry_path};
use crate::core::db::merkle_node::lmdb::LmdbError;
use crate::core::db::merkle_node::lmdb::lmdb_backend::LmdbBackend;
use crate::core::db::merkle_node::lmdb::value_structs::{LmdbLink, LmdbNode};
use crate::core::db::merkle_node::merkle_node_db::MerkleDbError;
use crate::error::OxenError;
use crate::model::merkle_tree::merkle_transport::{MerkleUnpacker, UnpackOptions};
use crate::model::{MerkleHash, MerkleTreeNodeType};
use crate::util::tar_stream::{StreamedEntry, stream_unpack};

impl MerkleUnpacker for LmdbBackend {
    /// Unpack a tar-gz wire stream into the LMDB store.
    ///
    /// Tolerates both the server-canonical (`tree/nodes/{prefix}/{suffix}/...`)
    /// and the legacy client-push (`{prefix}/{suffix}/...`) layouts — same
    /// behaviour as [`super::super::file_backend::FileBackend::unpack`].
    ///
    /// `UnpackOptions::SkipExisting` doesn't overwrite entries whose hashes are
    /// already in the LMDB store; `UnpackOptions::Overwrite` writes everything.
    fn unpack(
        &self,
        reader: &mut dyn Read,
        opts: UnpackOptions,
    ) -> Result<HashSet<MerkleHash>, OxenError> {
        let wtxn = LmdbBackend::write_txn(&self.lmdb_env)?;
        let state = UnpackState {
            backend: self,
            wtxn,
            pending_nodes: HashMap::new(),
            pending_children: HashMap::new(),
            own_pair_committed: HashSet::new(),
            deferred_embedded: HashMap::new(),
            seen: HashSet::new(),
            overwrite: matches!(opts, UnpackOptions::Overwrite),
        };

        let mut state =
            stream_unpack::<_, _, _, OxenError>(reader, state, |entry, st| on_entry(entry, st))?;
        flush_partials(&mut state)?;
        state.wtxn.commit().map_err(LmdbError::Write)?;
        Ok(state.seen)
    }
}

/// Streaming state for [`MerkleUnpacker::unpack`].
///
/// Carries the open write transaction, currently-unpaired raw bytes, and the
/// deferred set of embedded children seen so far without their own pair.
struct UnpackState<'env> {
    backend: &'env LmdbBackend,
    wtxn: heed::RwTxn<'env>,
    /// `{prefix}/{suffix}/node` raw bytes for hashes whose matching
    /// `children` entry hasn't yet arrived in the tar stream.
    pending_nodes: HashMap<MerkleHash, Vec<u8>>,
    /// `{prefix}/{suffix}/children` raw bytes for hashes whose matching
    /// `node` entry hasn't yet arrived in the tar stream.
    pending_children: HashMap<MerkleHash, Vec<u8>>,
    /// Hashes whose `(node, children)` pair has been parsed and committed to
    /// LMDB. Embedded-child seeding skips any hash already in this set, since
    /// the own-pair commit wrote its node + full link.
    own_pair_committed: HashSet<MerkleHash>,
    /// Embedded children observed under some parent's `children` blob whose
    /// own `{prefix}/{suffix}/` pair we haven't (yet) seen. At end-of-stream,
    /// children still in this map get seeded with a minimal link row pointing
    /// back at their first-observed parent — matching the file backend's
    /// "node present, link minimal" tolerance.
    deferred_embedded: HashMap<MerkleHash, DeferredEmbedded>,
    /// Hashes parsed out of the tarball — returned to the caller verbatim,
    /// mirroring [`super::super::file_backend::FileBackend::unpack`]'s contract.
    seen: HashSet<MerkleHash>,
    /// `true` for [`UnpackOptions::Overwrite`], `false` for
    /// [`UnpackOptions::SkipExisting`].
    overwrite: bool,
}

/// One embedded child waiting to be seeded if its own pair never shows up.
struct DeferredEmbedded {
    parent_hash: MerkleHash,
    kind: MerkleTreeNodeType,
    data: Vec<u8>,
}

/// Drive a single tar entry through the streaming state machine.
fn on_entry(entry: StreamedEntry<'_>, st: &mut UnpackState<'_>) -> Result<(), OxenError> {
    let classified = classify_streaming_entry(entry.path)?;
    match classified {
        StreamingKind::Intermediate => {
            // `tree/nodes` / `tree/nodes/{prefix}` directory entries — no body
            // to read, no hash to record.
        }
        StreamingKind::HashDir(hash) => {
            st.seen.insert(hash);
        }
        StreamingKind::Node(hash) => {
            st.seen.insert(hash);
            let mut buf = Vec::with_capacity(entry.size as usize);
            entry.body.read_to_end(&mut buf)?;
            st.pending_nodes.insert(hash, buf);
            if st.pending_children.contains_key(&hash) {
                commit_pair(st, hash)?;
            }
        }
        StreamingKind::Children(hash) => {
            st.seen.insert(hash);
            let mut buf = Vec::with_capacity(entry.size as usize);
            entry.body.read_to_end(&mut buf)?;
            st.pending_children.insert(hash, buf);
            if st.pending_nodes.contains_key(&hash) {
                commit_pair(st, hash)?;
            }
        }
    }
    Ok(())
}

/// Both pieces of a hash's `(node, children)` pair are present in the pending
/// buffers — parse them, commit the parent row, and record any embedded
/// children that don't yet have their own pair as deferred.
fn commit_pair(st: &mut UnpackState<'_>, hash: MerkleHash) -> Result<(), OxenError> {
    let node_bytes = st
        .pending_nodes
        .remove(&hash)
        .expect("commit_pair: pending node bytes must exist");
    let children_bytes = st
        .pending_children
        .remove(&hash)
        .expect("commit_pair: pending children bytes must exist");
    let parsed = parse_pair(&node_bytes, &children_bytes)?;
    finalize_parsed(st, hash, parsed)
}

/// Write the parent's node + link rows, then update the embedded-children
/// tracking. Used by both [`commit_pair`] (full pair) and [`flush_partials`]
/// (node-only pair with empty children blob).
fn finalize_parsed(
    st: &mut UnpackState<'_>,
    hash: MerkleHash,
    parsed: ParsedEntry,
) -> Result<(), OxenError> {
    put_node(st, &hash, parsed.parent_kind, parsed.parent_data)?;
    let child_hashes: Vec<MerkleHash> = parsed.children.iter().map(|c| c.hash).collect();
    put_link(st, &hash, parsed.parent_id, child_hashes)?;
    st.own_pair_committed.insert(hash);
    // A child whose own pair just arrived (this call) overrides any earlier
    // deferred-embedded record — drop the stale one so we don't seed twice.
    st.deferred_embedded.remove(&hash);

    for child in parsed.children {
        if st.own_pair_committed.contains(&child.hash) {
            // Own pair already processed for this child — nothing to defer.
            continue;
        }
        // First observation wins: if another parent already deferred this
        // child, keep that earlier record. Matches the buffered loop's
        // "key_present guard" behaviour for minimal-link seeding.
        st.deferred_embedded
            .entry(child.hash)
            .or_insert(DeferredEmbedded {
                parent_hash: hash,
                kind: child.kind,
                data: child.data,
            });
    }
    Ok(())
}

/// End-of-stream housekeeping:
///   1. Any `node` entry with no matching `children` becomes
///      `parse_pair(node, &[])` — matches the file backend's "node present,
///      children absent" tolerance.
///   2. Any `children` entry with no matching `node` is dropped (can't parse
///      without the node's lookup table). The hash is still in `seen`.
///   3. Deferred embedded children that never got an own pair get their
///      minimal link row seeded.
fn flush_partials(st: &mut UnpackState<'_>) -> Result<(), OxenError> {
    let pending_nodes = std::mem::take(&mut st.pending_nodes);
    for (hash, node_bytes) in pending_nodes {
        let parsed = parse_pair(&node_bytes, &[])?;
        finalize_parsed(st, hash, parsed)?;
    }
    // `children`-only entries aren't decodable without a node; matches the
    // buffered loop's behaviour (it built `parsed` keyed on node-present hashes
    // and ignored orphan `children`).
    st.pending_children.clear();

    let deferred = std::mem::take(&mut st.deferred_embedded);
    for (child_hash, dc) in deferred {
        if st.own_pair_committed.contains(&child_hash) {
            // Child's own pair already wrote node + link.
            continue;
        }
        put_node(st, &child_hash, dc.kind, dc.data)?;
        // Minimal-link seeding: only if the link row isn't already there.
        // Matches the buffered loop's unconditional key-present guard for
        // embedded children regardless of `opts`.
        if !LmdbBackend::key_present(&st.wtxn, &st.backend.merkle_links, &child_hash)? {
            LmdbBackend::put_serialized(
                &mut st.wtxn,
                &st.backend.merkle_links,
                &child_hash,
                LmdbLink::encode,
                LmdbLink {
                    parent_id: Some(dc.parent_hash),
                    children: Vec::new(),
                },
            )?;
        }
    }
    Ok(())
}

/// Put a node row into `merkle_tree_nodes`, honouring the `overwrite` flag.
fn put_node(
    st: &mut UnpackState<'_>,
    hash: &MerkleHash,
    kind: MerkleTreeNodeType,
    data: Vec<u8>,
) -> Result<(), LmdbError> {
    if !st.overwrite && LmdbBackend::key_present(&st.wtxn, &st.backend.merkle_tree_nodes, hash)? {
        return Ok(());
    }
    LmdbBackend::put_serialized(
        &mut st.wtxn,
        &st.backend.merkle_tree_nodes,
        hash,
        LmdbNode::encode,
        LmdbNode { kind, data },
    )
}

/// Put a link row into `merkle_links`, honouring the `overwrite` flag.
fn put_link(
    st: &mut UnpackState<'_>,
    hash: &MerkleHash,
    parent_id: Option<MerkleHash>,
    children: Vec<MerkleHash>,
) -> Result<(), LmdbError> {
    if !st.overwrite && LmdbBackend::key_present(&st.wtxn, &st.backend.merkle_links, hash)? {
        return Ok(());
    }
    LmdbBackend::put_serialized(
        &mut st.wtxn,
        &st.backend.merkle_links,
        hash,
        LmdbLink::encode,
        LmdbLink {
            parent_id,
            children,
        },
    )
}

/// Classification of a tar entry's path under either of the two wire layouts.
enum StreamingKind {
    /// `tree/nodes` itself, or `tree/nodes/{prefix}` — intermediate dirs.
    Intermediate,
    /// `tree/nodes/{prefix}/{suffix}` — the hash-bearing dir.
    HashDir(MerkleHash),
    /// `tree/nodes/{prefix}/{suffix}/node`.
    Node(MerkleHash),
    /// `tree/nodes/{prefix}/{suffix}/children`.
    Children(MerkleHash),
}

/// Wrap [`super::super::file_backend::classify_tar_entry_path`] so it works for
/// both the server-canonical (`tree/nodes/...`) and the legacy client-push
/// (`{prefix}/{suffix}/...`) layouts. The classifier is path-shape-driven, so
/// we just synthesize a virtual `.oxen/` prefix and pick a layout based on
/// whether the entry starts with `tree/nodes/`.
fn classify_streaming_entry(path: &Path) -> Result<StreamingKind, OxenError> {
    let virtual_oxen_hidden = PathBuf::from(".");
    let tree_nodes_prefix = Path::new(TREE_DIR).join(NODES_DIR);
    let synth = if path.starts_with(&tree_nodes_prefix) {
        virtual_oxen_hidden.join(path)
    } else {
        virtual_oxen_hidden.join(&tree_nodes_prefix).join(path)
    };
    Ok(
        match classify_tar_entry_path(&synth, &virtual_oxen_hidden)? {
            TarEntryKind::Intermediate => StreamingKind::Intermediate,
            TarEntryKind::HashDir(h) => StreamingKind::HashDir(h),
            TarEntryKind::NodeFile(h) => StreamingKind::Node(h),
            TarEntryKind::ChildrenFile(h) => StreamingKind::Children(h),
        },
    )
}

/// Parsed contents of one `tree/nodes/{prefix}/{suffix}/{node,children}` pair.
struct ParsedEntry {
    /// Kind discriminant from the parent `node` file's header.
    parent_kind: MerkleTreeNodeType,
    /// Parent ID from the parent `node` file's header. `None` if the on-disk slot was 0.
    parent_id: Option<MerkleHash>,
    /// Parent's msgpack tail.
    parent_data: Vec<u8>,
    /// One entry per child (already paired with its data slice from the children file).
    children: Vec<ParsedChild>,
}

/// One child as recovered from a parent's `node` lookup + `children` blob.
struct ParsedChild {
    hash: MerkleHash,
    kind: MerkleTreeNodeType,
    data: Vec<u8>,
}

/// Decode a `tree/nodes/{prefix}/{suffix}/node` file payload + its paired
/// `children` blob into the (parent, children) values used by the writer.
fn parse_pair(node_bytes: &[u8], children_bytes: &[u8]) -> Result<ParsedEntry, OxenError> {
    // node_bytes layout (file backend):
    //   1 byte: kind
    //   16 bytes: parent_id (u128 LE; 0 means "no parent")
    //   4 bytes: data_len (u32 LE)
    //   data_len bytes: msgpack tail
    //   then [(1 byte kind, 16 byte hash LE, 8 byte offset LE, 8 byte len LE)] children entries
    const HEADER_LEN: usize = 1 + 16 + 4;
    if node_bytes.len() < HEADER_LEN {
        return Err(LmdbError::NodeHeaderTruncated {
            len: node_bytes.len(),
        }
        .into());
    }
    let parent_kind = MerkleTreeNodeType::from_u8(node_bytes[0]).map_err(LmdbError::from)?;
    let mut parent_bytes_arr = [0u8; 16];
    parent_bytes_arr.copy_from_slice(&node_bytes[1..17]);
    let parent_value = u128::from_le_bytes(parent_bytes_arr);
    let parent_id = if parent_value == 0 {
        None
    } else {
        Some(MerkleHash::new(parent_value))
    };
    let mut data_len_arr = [0u8; 4];
    data_len_arr.copy_from_slice(&node_bytes[17..21]);
    let data_len = u32::from_le_bytes(data_len_arr) as usize;
    let data_end = HEADER_LEN + data_len;
    if node_bytes.len() < data_end {
        return Err(LmdbError::NodeHeaderTruncated {
            len: node_bytes.len(),
        }
        .into());
    }
    let parent_data = node_bytes[HEADER_LEN..data_end].to_vec();

    let mut children: Vec<ParsedChild> = Vec::new();
    let lookup_tail = &node_bytes[data_end..];
    const LOOKUP_ENTRY_SIZE: usize = 1 + 16 + 8 + 8;
    if !lookup_tail.len().is_multiple_of(LOOKUP_ENTRY_SIZE) {
        return Err(MerkleDbError::InvalidTarStructure {
            entry_path: format!("MerkleHash({})", MerkleHash::new(parent_value)),
            reason: format!(
                "child-lookup tail size {} is not a multiple of {LOOKUP_ENTRY_SIZE}",
                lookup_tail.len(),
            ),
        }
        .into());
    }
    for chunk in lookup_tail.chunks_exact(LOOKUP_ENTRY_SIZE) {
        let child_kind = MerkleTreeNodeType::from_u8(chunk[0]).map_err(LmdbError::from)?;
        let mut hash_buf = [0u8; 16];
        hash_buf.copy_from_slice(&chunk[1..17]);
        let child_hash = MerkleHash::new(u128::from_le_bytes(hash_buf));
        let mut offset_buf = [0u8; 8];
        offset_buf.copy_from_slice(&chunk[17..25]);
        let child_offset = u64::from_le_bytes(offset_buf) as usize;
        let mut len_buf = [0u8; 8];
        len_buf.copy_from_slice(&chunk[25..33]);
        let child_len = u64::from_le_bytes(len_buf) as usize;

        if child_offset.saturating_add(child_len) > children_bytes.len() {
            return Err(MerkleDbError::InvalidTarStructure {
                entry_path: format!("MerkleHash({child_hash})"),
                reason: format!(
                    "child slice [{child_offset}..{}] exceeds children blob length {}",
                    child_offset + child_len,
                    children_bytes.len(),
                ),
            }
            .into());
        }
        let child_data = children_bytes[child_offset..child_offset + child_len].to_vec();
        children.push(ParsedChild {
            hash: child_hash,
            kind: child_kind,
            data: child_data,
        });
    }

    Ok(ParsedEntry {
        parent_kind,
        parent_id,
        parent_data,
        children,
    })
}

// ────────────────────────────────────────────────────────────────────────────
// |                                                                          |
// |  **NOTE**           All tests live under pack.rs !                       |
// |                                                                          |
// ────────────────────────────────────────────────────────────────────────────
