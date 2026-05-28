//! [`MerklePacker`] + [`MerkleUnpacker`] implementations for the [`LmdbBackend`].
//!
//! The wire format produced/consumed is identical to the one [`super::super::file_backend::FileBackend`]
//! emits: tar-gz of `tree/nodes/{prefix}/{suffix}/{node,children}` entries, where
//! `node` & `children` follow the [`super::super::merkle_node_db::MerkleNodeDB`]
//! on-disk byte layout. Cross-backend interop is the requirement — an LMDB-backed
//! local repo must be able to push to a File-backed server and pull from one.
//!
//! Pack: for each requested hash (or all hashes in [`MerklePacker::pack_all`]),
//! the LMDB-side node + link rows are recombined into the file backend's
//! `node` + `children` byte format and appended to the tar archive.
//!
//! Unpack: tar entries are first buffered in-memory and paired up by hash, then
//! the parent's `node` byte format is decoded to recover {kind, parent_id,
//! data, children-lookup}; the corresponding `children` blob is sliced via the
//! lookup to recover each child's full node data; everything is committed in
//! one [`heed::RwTxn`].

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;

use crate::constants::{NODES_DIR, TREE_DIR};
use crate::core::db::merkle_node::file_backend::{TarEntryKind, classify_tar_entry_path};
use crate::core::db::merkle_node::lmdb::LmdbError;
use crate::core::db::merkle_node::lmdb::hash_content_name::Filename;
use crate::core::db::merkle_node::lmdb::hash_content_name::HashCN;
use crate::core::db::merkle_node::lmdb::hash_content_name::hash_cn_from;
use crate::core::db::merkle_node::lmdb::lmdb_backend::LmdbBackend;
use crate::core::db::merkle_node::lmdb::lmdb_backend::LmdbTables;
use crate::core::db::merkle_node::lmdb::value_structs::LmdbDupes;
use crate::core::db::merkle_node::lmdb::value_structs::{LmdbLink, LmdbNode};
use crate::core::db::merkle_node::merkle_node_db::MerkleDbError;
use crate::error::OxenError;
use crate::model::merkle_tree::merkle_transport::{MerkleUnpacker, UnpackOptions};
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::{MerkleHash, MerkleTreeNodeType};

impl MerkleUnpacker for LmdbBackend {
    /// Unpack a tar-gz wire stream into the LMDB store.
    ///
    /// Tolerates both the server-canonical (`tree/nodes/{prefix}/{suffix}/...`)
    /// and the legacy client-push (`{prefix}/{suffix}/...`) layouts — same
    /// behaviour as [`super::super::file_backend::FileBackend::unpack`].
    ///
    /// `UnpackOptions::SkipExisting` doesn't write entries whose hashes are
    /// already in the LMDB store; `UnpackOptions::Overwrite` writes everything.
    fn unpack(
        &self,
        reader: &mut dyn Read,
        opts: UnpackOptions,
    ) -> Result<HashSet<MerkleHash>, OxenError> {
        let buffered = collect_tar_entries(reader)?;
        write_unpacked_into_lmdb(self, buffered, opts)
    }
}

/// Hashes whose `node` + `children` payload bytes have been read out of the tar.
/// Either side may be absent if the tar archive is incomplete — handled when
/// we pair them up.
#[derive(Default)]
struct BufferedTarPayloads {
    node_files: HashMap<MerkleHash, Vec<u8>>,
    children_files: HashMap<MerkleHash, Vec<u8>>,
    /// Hashes we observed at any path component. Returned to the caller even
    /// if the entry's bytes were skipped (mirrors the file backend's reporting
    /// of every parsed hash from the tarball).
    ///
    /// **NOTE**: This **NEVER** includes file content hashes! Each
    ///
    /// dir_1/
    ///     - file_a.txt (C)
    /// dir_2/
    ///     - file_a.txt (C)
    ///
    /// lookup (C)
    ///     => OK file node type
    ///     => OK content
    ///     => FAIL metadata different
    ///
    /// => I think it just needs a hash of _ALL_ of the `FileNode` content
    ///     => => HashCN = XXH3( [ MerkleHash, hash(self) ] ) <= <=
    seen_hashes: HashSet<MerkleHash>,
}

/// Read every `node` / `children` entry out of the tar archive into memory,
/// keyed by hash. Path validation (path-traversal, allowed entry types,
/// well-formed hash dirs) matches [`super::super::file_backend::FileBackend::unpack`].
fn collect_tar_entries(reader: &mut dyn Read) -> Result<BufferedTarPayloads, OxenError> {
    let decoder = GzDecoder::new(reader);
    let mut archive = Archive::new(decoder);
    let entries = archive.entries().map_err(MerkleDbError::CannotReadMerkle)?;

    let mut buffered = BufferedTarPayloads::default();

    // We classify with the synthetic oxen-hidden root + tree-nodes prefix; the
    // helper already strips both off. Two virtual roots cover the two layouts
    // a tar archive can arrive in.
    let virtual_oxen_hidden = PathBuf::from(".");
    let server_canonical_root = virtual_oxen_hidden.clone();
    let legacy_client_push_root = virtual_oxen_hidden.join(TREE_DIR).join(NODES_DIR);

    for entry in entries {
        let mut entry = entry.map_err(MerkleDbError::CannotReadMerkle)?;
        let path = entry
            .path()
            .map_err(MerkleDbError::CannotReadMerkle)?
            .into_owned();

        // Mirror file_backend's path-traversal guard.
        if path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(MerkleDbError::PathTraversal(path.display().to_string()).into());
        }
        let entry_type = entry.header().entry_type();
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(MerkleDbError::UnsupportedTarEntry {
                path: path.display().to_string(),
            }
            .into());
        }

        // Classify under both layouts; whichever path validates wins.
        let classified = if path.starts_with(Path::new(TREE_DIR).join(NODES_DIR)) {
            classify_tar_entry_path(&server_canonical_root.join(&path), &virtual_oxen_hidden)?
        } else {
            classify_tar_entry_path(&legacy_client_push_root.join(&path), &virtual_oxen_hidden)?
        };

        match classified {
            TarEntryKind::Intermediate => continue,
            TarEntryKind::HashDir(hash) => {
                buffered.seen_hashes.insert(hash);
            }
            TarEntryKind::NodeFile(hash) => {
                buffered.seen_hashes.insert(hash);
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).map_err(MerkleDbError::Io)?;
                // bytes is the node file
                buffered.node_files.insert(hash, bytes);
            }
            TarEntryKind::ChildrenFile(hash) => {
                buffered.seen_hashes.insert(hash);
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).map_err(MerkleDbError::Io)?;
                // bytes is the children file
                buffered.children_files.insert(hash, bytes);
            }
        }
    }
    Ok(buffered)
}

/// Parsed contents of a pair of `node` and `children` files under some `tree/nodes/{prefix}/{suffix}/` directory.
struct ParsedEntry {
    /// Kind discriminant from this `node` file's header.
    kind: MerkleTreeNodeType,
    /// Parent ID from the parent `node` file's header. `None` if the on-disk slot was 0.
    parent_id: Option<MerkleHash>,
    /// The name of this node, if it is a directory.
    name: Option<String>,
    /// Parent's msgpack tail.
    data: Vec<u8>,
    /// One entry per child (already paired with its data slice from the children file).
    children: Vec<ParsedChild>,
}

/// One child as recovered from a parent's `node` lookup + `children` blob.
struct ParsedChild {
    hash: MerkleHash,
    kind: MerkleTreeNodeType,
    data: Vec<u8>,
    hash_cn: HashCN,
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
    let kind = MerkleTreeNodeType::from_u8(node_bytes[0]).map_err(LmdbError::from)?;
    let parent_value = {
        let mut parent_bytes_arr = [0u8; 16];
        parent_bytes_arr.copy_from_slice(&node_bytes[1..17]);
        u128::from_le_bytes(parent_bytes_arr)
    };
    let parent_id = if parent_value == 0 {
        None
    } else {
        Some(MerkleHash::new(parent_value))
    };
    let data_len = {
        let mut data_len_arr = [0u8; 4];
        data_len_arr.copy_from_slice(&node_bytes[17..21]);
        u32::from_le_bytes(data_len_arr) as usize
    };
    let data_end = HEADER_LEN + data_len;
    if node_bytes.len() < data_end {
        return Err(LmdbError::NodeHeaderTruncated {
            len: node_bytes.len(),
        }
        .into());
    }
    let data = node_bytes[HEADER_LEN..data_end].to_vec();

    let name = EMerkleTreeNode::from_type_and_bytes(kind, &data)?
        .as_t_node()
        .name()
        .map(|x| x.to_string());

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

    let children: Vec<ParsedChild> = {
        let mut children = Vec::new();
        for chunk in lookup_tail.chunks_exact(LOOKUP_ENTRY_SIZE) {
            let child_kind = MerkleTreeNodeType::from_u8(chunk[0]).map_err(LmdbError::from)?;
            let child_hash = {
                let mut hash_buf = [0u8; 16];
                hash_buf.copy_from_slice(&chunk[1..17]);
                MerkleHash::new(u128::from_le_bytes(hash_buf))
            };
            let child_offset = {
                let mut offset_buf = [0u8; 8];
                offset_buf.copy_from_slice(&chunk[17..25]);
                u64::from_le_bytes(offset_buf) as usize
            };
            let child_len = {
                let mut len_buf = [0u8; 8];
                len_buf.copy_from_slice(&chunk[25..33]);
                u64::from_le_bytes(len_buf) as usize
            };
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
            let child_data = &children_bytes[child_offset..child_offset + child_len];
            let hash_cn = hash_cn_from(
                EMerkleTreeNode::from_type_and_bytes(child_kind, child_data)?.as_t_node(),
            );

            children.push(ParsedChild {
                hash: child_hash,
                kind: child_kind,
                data: child_data.to_vec(),
                hash_cn,
            });
        }
        children
    };

    Ok(ParsedEntry {
        kind,
        parent_id,
        name,
        data,
        children,
    })
}

/// Drive the LMDB write side of unpack.
///
/// For every `{prefix}/{suffix}` pair we have both a `node` and `children`
/// file for, the parent's row in `merkle_tree_nodes` and `merkle_links` is
/// written. For every embedded child of such a parent that doesn't itself
/// have its own pair in the tar, a node row is written and a minimal link
/// row (parent_id = embedding parent, no children of its own) is written —
/// so [`super::super::lmdb::lmdb_backend::LmdbBackend::get_node`]'s
/// "node + link must both exist" invariant is preserved.
///
/// `UnpackOptions::SkipExisting` causes hashes already present in the
/// `merkle_tree_nodes` table to be left untouched (matches the
/// `repositories::tree::unpack_nodes` use-case in the file backend).
fn write_unpacked_into_lmdb(
    backend: &LmdbBackend,
    buffered: BufferedTarPayloads,
    opts: UnpackOptions,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let BufferedTarPayloads {
        node_files,
        children_files,
        seen_hashes,
    } = buffered;
    let overwrite = matches!(opts, UnpackOptions::Overwrite);

    // Pair up hashes that have both a node and children file.
    // The node is the parent of the children.
    //      - The key (MerkleHash) identifies the parent.
    //      - The value (ParsedEntry) is its children.
    let node_and_children: HashMap<(MerkleHash, HashCN), ParsedEntry> = {
        let mut parsed = HashMap::with_capacity(node_files.len());
        for (node_hash, node_bytes) in &node_files {
            let empty_children: Vec<u8> = Vec::new();
            let children_bytes = children_files.get(node_hash).unwrap_or(&empty_children);
            let entry = parse_pair(node_bytes, children_bytes)?;
            let node_hash_cn = HashCN::new(
                node_hash,
                entry
                    .name
                    .and_then(Filename::new_assume_invariants)
                    .as_ref(),
            );
            parsed.insert((*node_hash, node_hash_cn), entry);
        }
        parsed
    };

    let h = Helper {
        overwrite,
        lmdb: &backend,
    };

    let mut wtxn = LmdbBackend::write_txn(&backend.lmdb_env)?;

    for ((node_hash, node_hash_cn), entry) in &node_and_children {
        h.put_node(
            &mut wtxn,
            node_hash,
            node_hash_cn,
            entry.kind,
            entry.data.clone(),
        )?;
        let child_hashes = entry.children.iter().map(|c| c.hash_cn).collect::<Vec<_>>();
        h.put_link(&mut wtxn, node_hash_cn, entry.parent_id, child_hashes)?;

        // Embedded children: their `node` row is observable through this parent
        // entry alone. If they don't have a full entry of their own in the tar,
        // write a node + minimal-link pair so `get_node` doesn't trip its
        // "node present but no link" integrity check.
        for child in &entry.children {
            if node_and_children.contains_key(&child.hash) {
                // Will be handled by its own iteration of this loop.
                continue;
            }
            h.put_node(
                &mut wtxn,
                &child.hash,
                &child.hash_cn,
                child.kind,
                child.data.clone(),
            )?;
            // Only seed a minimal link if the link row doesn't already exist —
            // skip-existing semantics for embedded-only children regardless of
            // `opts`, so a child observed via two different parents doesn't
            // get its link clobbered.
            if !LmdbBackend::key_present(
                &wtxn,
                &backend.tables.merkle_links,
                &child.hash.to_u128(),
            )? {
                LmdbBackend::put_serialized(
                    &mut wtxn,
                    &backend.tables.merkle_links,
                    &child.hash.to_u128(),
                    LmdbLink::encode,
                    LmdbLink {
                        parent_id: Some(*parent_hash),
                        children: Vec::new(),
                    },
                )?;
            }
        }
    }
    wtxn.commit().map_err(LmdbError::Write)?;

    // Honour the file backend's contract: return every hash parsed out of the
    // tarball, even ones whose payloads we skipped via SkipExisting.
    Ok(seen_hashes)
}

/// Helper implementing functionality used in `unpack`.
struct Helper<'a> {
    overwrite: bool,
    lmdb: &'a LmdbBackend,
}

impl<'a> Helper<'a> {
    fn put_node(
        &self,
        wtxn: &mut heed::RwTxn<'_>,
        hash: &MerkleHash,
        hash_cn: &HashCN,
        kind: MerkleTreeNodeType,
        data: Vec<u8>,
    ) -> Result<(), LmdbError> {
        if !self.overwrite
            && LmdbBackend::key_present(wtxn, &self.lmdb.tables.merkle_node_dupes, &hash.to_u128())?
        {
            return Ok(());
        }
        self.lmdb
            .put_node(wtxn, *hash, *hash_cn, LmdbNode { kind, data })
    }

    fn put_link(
        &self,
        wtxn: &mut heed::RwTxn<'_>,
        hash: &MerkleHash,
        parent_id: Option<MerkleHash>,
        children_and_data: Vec<(MerkleHash, Option<Filename>, LmdbNode)>,
    ) -> Result<(), LmdbError> {
        if !self.overwrite
            && LmdbBackend::key_present(wtxn, &self.lmdb.tables.merkle_links, &hash.to_u128())?
        {
            return Ok(());
        }
        self.lmdb.put_links(wtxn, parent_id, children_and_data)
    }
}

// ────────────────────────────────────────────────────────────────────────────
// |                                                                          |
// |  **NOTE**           All tests live under pack.rs !                       |
// |                                                                          |
// ────────────────────────────────────────────────────────────────────────────
