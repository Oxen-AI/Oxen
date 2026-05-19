//! [`MerklePacker`] implementation for the [`LmdbBackend`].
//!
//! The wire format produced/consumed is identical to the one [`super::super::file_backend::FileBackend`]
//! emits: tar-gz of `tree/nodes/{prefix}/{suffix}/{node,children}` entries, where
//! `node` & `children` follow the [`super::super::merkle_node_db::MerkleNodeDB`]
//! on-disk byte layout. Cross-backend interop is the requirement — an LMDB-backed
//! local repo must be able to push to a File-backed server and pull from one.
//!
//! Pack: for each requested hash (or all hashes in [`MerklePacker::pack_all`]),
//! the LMDB-side node + link rows are recombined into the file backend's
//! `node` + `children` byte format and streamed through the shared
//! [`crate::util::tar_stream::stream_pack`] helper. Per-hash work still buffers
//! that single hash's `node` and `children` blobs (the lookup table's child
//! offsets need to be known up front), but the archive as a whole is never
//! materialized — each `PackEntry` flows through gzip+tar one body chunk at a
//! time.

use std::collections::HashSet;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use flate2::Compression;

use crate::constants::{NODES_DIR, TREE_DIR};
use crate::core::db::merkle_node::lmdb::LmdbError;
use crate::core::db::merkle_node::lmdb::lmdb_backend::LmdbBackend;
use crate::error::OxenError;
use crate::model::merkle_tree::merkle_transport::{MerklePacker, PackOptions};
use crate::model::{MerkleHash, MerkleTreeNodeType};
use crate::util::tar_stream::{EntryKind, PackEntry, stream_pack};

impl MerklePacker for LmdbBackend {
    /// Pack the given node `hashes` into `out`. Layout (and gzip level) follow
    /// [`PackOptions`]. Hashes that aren't in LMDB are silently skipped — same
    /// behaviour as the file backend.
    fn pack_nodes(
        &self,
        hashes: &HashSet<MerkleHash>,
        opts: PackOptions,
        out: &mut dyn Write,
    ) -> Result<(), OxenError> {
        let entries = hashes
            .iter()
            .copied()
            .flat_map(|hash| entries_for_hash(self, hash, opts));
        stream_pack::<_, _, OxenError>(out, pack_options_compression(opts), entries)
    }

    /// Pack every node currently in the LMDB store into `out` using the
    /// server-canonical layout — same as [`super::super::file_backend::FileBackend::pack_all`].
    fn pack_all(&self, out: &mut dyn Write) -> Result<(), OxenError> {
        let opts = PackOptions::ServerCanonical;
        let hashes = all_node_hashes(self)?;
        let entries = hashes
            .into_iter()
            .flat_map(|hash| entries_for_hash(self, hash, opts));
        stream_pack::<_, _, OxenError>(out, pack_options_compression(opts), entries)
    }

    /// Estimate the **uncompressed** packed node tar payload for the LMDB backend.
    ///
    /// Mirrors [`super::super::file_backend::FileBackend::raw_byte_count`]: returns
    /// a tight upper bound on the post-gzip bytes that will flow over the wire,
    /// since `node` and `children` blobs are hash-dense and compress to ~1.0×.
    ///
    /// Hashes not present in LMDB contribute 0, matching `pack_nodes`'s silent-skip
    /// behaviour. File / FileChunk nodes also contribute 0 because [`entries_for_hash`]
    /// skips them — they ride inside a parent's `children` blob, not their own dir.
    fn raw_byte_count(&self, hashes: &HashSet<MerkleHash>) -> u64 {
        const TAR_HEADER_BYTES: u64 = 512;
        const TAR_BLOCK_SIZE: u64 = 512;
        fn padded(len: u64) -> u64 {
            len.div_ceil(TAR_BLOCK_SIZE).saturating_mul(TAR_BLOCK_SIZE)
        }

        let mut total: u64 = 0;
        for hash in hashes {
            let Ok(Some(stored_node)) = self.full_get_node(hash) else {
                continue;
            };
            if matches!(
                stored_node.kind(),
                MerkleTreeNodeType::File | MerkleTreeNodeType::FileChunk
            ) {
                continue;
            }
            let Ok(Some(link)) = self.get_links(hash) else {
                continue;
            };

            // children blob: concatenated `data()` of every present child.
            let mut children_len: u64 = 0;
            let mut lookup_entries: u64 = 0;
            for child_hash in link.children() {
                let Ok(Some(child_node)) = self.full_get_node(child_hash) else {
                    continue;
                };
                children_len = children_len.saturating_add(child_node.data().len() as u64);
                lookup_entries = lookup_entries.saturating_add(1);
            }
            // node blob layout: kind(1) | parent_id(16) | data_len(4) | data | (33 bytes per lookup entry).
            // See `encode_node_file` above.
            let node_len: u64 = 21u64
                .saturating_add(stored_node.data().len() as u64)
                .saturating_add(lookup_entries.saturating_mul(33));

            // One tar dir entry + node file entry + children file entry, each padded.
            let entry_total = TAR_HEADER_BYTES
                .saturating_add(TAR_HEADER_BYTES.saturating_add(padded(node_len)))
                .saturating_add(TAR_HEADER_BYTES.saturating_add(padded(children_len)));
            total = total.saturating_add(entry_total);
        }
        total
    }
}

/// Match the per-layout gzip compression level used by [`super::super::file_backend::FileBackend`]
/// so the tar-gz wire payload is interchangeable across backends.
fn pack_options_compression(opts: PackOptions) -> Compression {
    match opts {
        PackOptions::ServerCanonical => Compression::fast(),
        PackOptions::LegacyClientPush => Compression::default(),
    }
}

/// Iterate every key in the `merkle_tree_nodes` table. Used by
/// [`MerklePacker::pack_all`] to enumerate the store. The borrow on
/// `rtxn` is released before we return the collected `Vec`, so callers
/// don't tie up a read transaction.
fn all_node_hashes(lmdb: &LmdbBackend) -> Result<Vec<MerkleHash>, LmdbError> {
    let rtxn = lmdb.read_txn()?;
    let mut hashes: Vec<MerkleHash> = Vec::new();
    for entry in lmdb
        .merkle_tree_nodes
        .iter(&rtxn)
        .map_err(LmdbError::Access)?
    {
        let (key, _value) = entry.map_err(LmdbError::Retrieve)?;
        hashes.push(MerkleHash::new(key));
    }
    Ok(hashes)
}

/// Produce the lazy per-hash entry sequence (dir + node-file + children-file)
/// for the streaming pack pipeline.
///
/// Returns an empty Vec for hashes that should be skipped silently (missing
/// from LMDB, or file-level kinds that don't get their own `{prefix}/{suffix}/`
/// directory). On any LMDB read error, returns a single `Err` item so the error
/// surfaces through `stream_pack` without aborting the iterator at panic
/// boundaries.
fn entries_for_hash(
    lmdb: &LmdbBackend,
    hash: MerkleHash,
    opts: PackOptions,
) -> Vec<Result<PackEntry, OxenError>> {
    match build_entries_for_hash(lmdb, hash, opts) {
        Ok(entries) => entries.into_iter().map(Ok).collect(),
        Err(err) => vec![Err(err.into())],
    }
}

/// Build the (dir, node, children) tar entries for one hash. Returns an empty
/// Vec for the silent-skip cases (missing node, file/file-chunk kinds, missing
/// link row).
fn build_entries_for_hash(
    lmdb: &LmdbBackend,
    hash: MerkleHash,
    opts: PackOptions,
) -> Result<Vec<PackEntry>, LmdbError> {
    let Some(stored_node) = lmdb.full_get_node(&hash)? else {
        return Ok(Vec::new());
    };
    // File-level nodes don't get their own dir in the file backend's wire
    // format; they only appear embedded in a parent's `children` blob.
    if matches!(
        stored_node.kind(),
        MerkleTreeNodeType::File | MerkleTreeNodeType::FileChunk
    ) {
        return Ok(Vec::new());
    }
    let Some(link) = lmdb.get_links(&hash)? else {
        // Node row exists but link row doesn't — should be impossible per
        // the writer's invariants. Skip rather than fail the whole pack.
        return Ok(Vec::new());
    };

    let mut children_blob: Vec<u8> = Vec::new();
    let mut lookup_entries: Vec<LookupEntry> = Vec::with_capacity(link.children().len());
    for child_hash in link.children() {
        let Some(child_node) = lmdb.full_get_node(child_hash)? else {
            // Missing child node — emit an empty placeholder so the parent
            // still round-trips. Same silent-skip behaviour as the file
            // backend when its child data is missing on disk.
            continue;
        };
        let child_data = child_node.data();
        let offset = children_blob.len() as u64;
        let len = child_data.len() as u64;
        children_blob.extend_from_slice(child_data);
        lookup_entries.push(LookupEntry {
            kind: child_node.kind(),
            hash: *child_hash,
            offset,
            len,
        });
    }

    let node_blob = encode_node_file(
        stored_node.kind(),
        link.parent_id().copied(),
        stored_node.data(),
        &lookup_entries,
    );

    // Tar entry paths follow the layout selected by `opts`.
    let dir_prefix = hash.to_hex_hash().node_db_prefix();
    let tar_subdir: PathBuf = match opts {
        PackOptions::ServerCanonical => Path::new(TREE_DIR).join(NODES_DIR).join(&dir_prefix),
        PackOptions::LegacyClientPush => PathBuf::from(&dir_prefix),
    };
    let node_path = tar_subdir.join("node");
    let children_path = tar_subdir.join("children");

    Ok(vec![
        PackEntry {
            path: tar_subdir,
            kind: EntryKind::Directory,
            size: 0,
            mode: 0o755,
            body: Box::new(Cursor::new(Vec::new())),
        },
        PackEntry {
            path: node_path,
            kind: EntryKind::File,
            size: node_blob.len() as u64,
            mode: 0o644,
            body: Box::new(Cursor::new(node_blob)),
        },
        PackEntry {
            path: children_path,
            kind: EntryKind::File,
            size: children_blob.len() as u64,
            mode: 0o644,
            body: Box::new(Cursor::new(children_blob)),
        },
    ])
}

/// One entry of the parent's `node`-file child lookup table — mirrors the
/// `(dtype, hash, offset, len)` quad that
/// [`super::super::merkle_node_db::MerkleNodeDB::add_child`] writes.
struct LookupEntry {
    kind: MerkleTreeNodeType,
    hash: MerkleHash,
    offset: u64,
    len: u64,
}

/// Build the byte content of a `tree/nodes/{prefix}/{suffix}/node` file:
/// `kind(1) | parent_id(16 LE) | data_len(4 LE) | data | [child lookup entries]`.
/// Format is fixed by [`super::super::merkle_node_db::MerkleNodeLookup::deserialize`].
/// Note that here `data` is the msgpack-encoded bytes of the associated [`EMerkleTreeNode`].
fn encode_node_file(
    kind: MerkleTreeNodeType,
    parent_id: Option<MerkleHash>,
    data: &[u8],
    lookup: &[LookupEntry],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 16 + 4 + data.len() + lookup.len() * (1 + 16 + 8 + 8));
    buf.push(kind.to_u8());
    let parent_bytes = parent_id.map(|p| p.to_le_bytes()).unwrap_or([0u8; 16]);
    buf.extend_from_slice(&parent_bytes);
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
    for entry in lookup {
        buf.push(entry.kind.to_u8());
        buf.extend_from_slice(&entry.hash.to_le_bytes());
        buf.extend_from_slice(&entry.offset.to_le_bytes());
        buf.extend_from_slice(&entry.len.to_le_bytes());
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::repository_config::MerkleStoreKind;
    use crate::core::db::merkle_node::lmdb::tests::commit_with_hash;
    use crate::core::db::merkle_node::lmdb::tests::h;
    use crate::core::db::merkle_node::lmdb::tests::with_test_backend;
    use crate::core::db::merkle_node::merkle_node_db::MerkleDbError;
    use crate::model::merkle_tree::UnpackOptions;
    use crate::model::merkle_tree::merkle_writer::MerkleWriter;
    use crate::repositories;
    use crate::test;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::collections::HashSet;

    /// Pack just a parent commit (with one embedded child commit), unpack into
    /// a fresh LMDB-backed repo, and verify both nodes round-trip back through
    /// the reader interface.
    #[test]
    fn test_lmdb_transport_roundtrip_via_lmdb_unpack() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let parent_h = h("11111111111111111111111111111111");
            let child_h = h("22222222222222222222222222222222");
            let parent = commit_with_hash(repo, parent_h);
            let child = commit_with_hash(repo, child_h);

            let session = backend.begin()?;
            {
                let mut parent_ns = session.create_node(&parent, None)?;
                parent_ns.add_child(&child)?;
                parent_ns.finish()?;
            }
            session.finish()?;

            let mut buf = Vec::new();
            backend.pack_nodes(
                &HashSet::from_iter([parent_h]),
                PackOptions::ServerCanonical,
                &mut buf,
            )?;
            assert!(!buf.is_empty(), "pack should produce some bytes");

            // Fresh target repo backed by LMDB; unpack the bytes into it and
            // verify both the parent (with full link) and the child (with
            // minimal link) are observable.
            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let target_store = target_repo.merkle_store()?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &buf[..], UnpackOptions::Overwrite)?;
            assert!(
                installed.contains(&parent_h),
                "unpack must report parent hash; got {installed:?}"
            );
            // get_node on parent → Some(node) with parent_id == None.
            let parent_entry = target_store
                .get_node(&parent_h)?
                .expect("parent must be readable through target store");
            assert!(parent_entry.parent_id.is_none());

            // Embedded child: minimal link points back at parent, no
            // children of its own.
            let child_entry = target_store
                .get_node(&child_h)?
                .expect("embedded child must be readable through target store");
            assert_eq!(child_entry.parent_id, Some(parent_h));
            Ok(())
        })
    }

    /// LMDB → tar → file-backed target. Confirms wire-format interop with the
    /// FileBackend (i.e., a future server using LMDB could still serve File-backed
    /// clients, and an LMDB client can push to a File-backed server today).
    #[tokio::test]
    async fn test_lmdb_pack_unpacks_into_file_backend() -> Result<(), OxenError> {
        let source_repo =
            test::init_test_repo_merkle_init_version_store_async(MerkleStoreKind::Lmdb).await?;
        let parent_h = h("11111111111111111111111111111111");
        let child_h = h("22222222222222222222222222222222");
        let parent = commit_with_hash(&source_repo, parent_h);
        let child = commit_with_hash(&source_repo, child_h);

        {
            let store = source_repo.merkle_store()?;
            let session = store.begin()?;
            {
                let mut parent_ns = session.create_node(&parent, None)?;
                parent_ns.add_child(&child)?;
                parent_ns.finish()?;
            }
            session.finish()?;
        }

        let mut buf = Vec::new();
        source_repo.merkle_transport()?.pack_nodes(
            &HashSet::from_iter([parent_h]),
            PackOptions::ServerCanonical,
            &mut buf,
        )?;

        // File-backed target.
        test::run_empty_local_repo_test_async(|target_repo| async move {
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &buf[..], UnpackOptions::Overwrite)?;
            assert!(
                installed.contains(&parent_h),
                "unpack reported {installed:?}"
            );
            let store = target_repo.merkle_store()?;
            assert!(store.exists(&parent_h)?);
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// File-backed source → tar → LMDB target. Confirms the reverse direction:
    /// the LMDB unpacker accepts the canonical file backend wire format.
    #[tokio::test]
    async fn test_file_pack_unpacks_into_lmdb() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|source_repo| async move {
            // Pack everything that's in the file-backed source.
            let mut buf = Vec::new();
            source_repo
                .merkle_transport()?
                .pack_all(&mut buf)
                .expect("pack_all on file backend");
            assert!(!buf.is_empty(), "expected pack_all to produce bytes");

            let target_repo =
                test::init_test_repo_merkle_init_version_store_async(MerkleStoreKind::Lmdb).await?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &buf[..], UnpackOptions::Overwrite)?;
            assert!(
                !installed.is_empty(),
                "expected at least one installed hash"
            );

            let store = target_repo.merkle_store()?;
            // The head commit's hash should be readable in the LMDB target.
            let head_hash = repositories::commits::head_commit(&source_repo)
                .expect("source has a head commit")
                .hash()
                .expect("head commit's hash");
            assert!(
                store.exists(&head_hash)?,
                "head commit {head_hash} not readable through LMDB target store"
            );
            Ok(())
        })
        .await
    }

    /// `pack_nodes` on an LMDB store that doesn't contain the requested hashes
    /// produces a valid empty tarball (silent skip semantics match the file
    /// backend).
    #[test]
    fn test_lmdb_pack_silently_skips_absent_hashes() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let absent = h("deadbeefdeadbeefdeadbeefdeadbeef");
            let mut buf = Vec::new();
            backend.pack_nodes(
                &HashSet::from_iter([absent]),
                PackOptions::ServerCanonical,
                &mut buf,
            )?;
            // A valid gzip wrapper around an empty tar payload — non-empty bytes
            // overall but no tar entries beyond the gzip terminator.
            // Easiest invariant to check: unpack into a fresh repo and assert
            // the reported hash set is empty.
            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &buf[..], UnpackOptions::Overwrite)?;
            assert!(
                installed.is_empty(),
                "expected no hashes, got {installed:?}"
            );
            Ok(())
        })
    }

    /// `pack_all` over an empty LMDB store: produces a valid (effectively
    /// empty) tarball that round-trips cleanly.
    #[test]
    fn test_lmdb_pack_all_on_empty_store_is_noop() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let mut buf = Vec::new();
            backend.pack_all(&mut buf)?;
            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &buf[..], UnpackOptions::Overwrite)?;
            assert!(installed.is_empty());
            Ok(())
        })
    }

    /// `pack_all` over a non-empty LMDB store visits every node and round-trips
    /// each one through unpack into a fresh LMDB store.
    #[test]
    fn test_lmdb_pack_all_round_trip() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            // Two unrelated commit nodes — no parent link between them.
            let a_h = h("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            let b_h = h("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            let a = commit_with_hash(repo, a_h);
            let b = commit_with_hash(repo, b_h);
            let session = backend.begin()?;
            session.create_node(&a, None)?.finish()?;
            session.create_node(&b, None)?.finish()?;
            session.finish()?;

            let mut buf = Vec::new();
            backend.pack_all(&mut buf)?;

            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &buf[..], UnpackOptions::Overwrite)?;
            assert!(installed.contains(&a_h));
            assert!(installed.contains(&b_h));
            let store = target_repo.merkle_store()?;
            assert!(store.exists(&a_h)?);
            assert!(store.exists(&b_h)?);
            Ok(())
        })
    }

    /// Rebuild a tar-gz archive with its entries in a caller-controlled order.
    ///
    /// Used to construct streaming-edge-case tarballs (out-of-order pair
    /// arrival, lone-node, lone-children) without having to hand-build the
    /// LMDB-encoded node/children blobs from scratch.
    fn rebuild_tar_with_filter<F>(original: &[u8], keep_and_order: F) -> Vec<u8>
    where
        F: Fn(&std::path::Path) -> Option<u32>,
    {
        use flate2::read::GzDecoder;
        use std::io::Read;
        use tar::Archive;

        let decoder = GzDecoder::new(original);
        let mut archive = Archive::new(decoder);
        let mut entries: Vec<(std::path::PathBuf, tar::EntryType, Vec<u8>, u32)> = Vec::new();
        for entry in archive
            .entries()
            .expect("rebuild: cannot iterate archive entries")
        {
            let mut entry = entry.expect("rebuild: cannot read entry");
            let path = entry
                .path()
                .expect("rebuild: cannot read path")
                .into_owned();
            let entry_type = entry.header().entry_type();
            let mut body = Vec::new();
            entry
                .read_to_end(&mut body)
                .expect("rebuild: cannot read body");
            if let Some(rank) = keep_and_order(&path) {
                entries.push((path, entry_type, body, rank));
            }
        }
        entries.sort_by_key(|(_, _, _, rank)| *rank);

        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            for (path, entry_type, body, _) in &entries {
                let mut header = tar::Header::new_gnu();
                header.set_size(body.len() as u64);
                header.set_mode(if entry_type.is_dir() { 0o755 } else { 0o644 });
                header.set_entry_type(*entry_type);
                header.set_cksum();
                tar.append_data(&mut header, path, std::io::Cursor::new(body.clone()))
                    .expect("rebuild: cannot append entry");
            }
            tar.finish().expect("rebuild: tar finish");
            tar.into_inner()
                .expect("rebuild: tar inner")
                .finish()
                .expect("rebuild: gz finish");
        }
        buf
    }

    /// Streaming unpack must handle pairs whose `node` and `children` entries
    /// are interleaved with other pairs' entries in the tar (the natural
    /// outcome when iteration order varies across packers). We hand-build an
    /// archive with `a.children, b.node, a.node, b.children` and verify both
    /// hashes round-trip correctly.
    #[test]
    fn test_lmdb_unpack_handles_out_of_order_pairs() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let a_h = h("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            let b_h = h("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            let a = commit_with_hash(repo, a_h);
            let b = commit_with_hash(repo, b_h);
            let session = backend.begin()?;
            session.create_node(&a, None)?.finish()?;
            session.create_node(&b, None)?.finish()?;
            session.finish()?;

            let mut buf = Vec::new();
            backend.pack_nodes(
                &HashSet::from_iter([a_h, b_h]),
                PackOptions::ServerCanonical,
                &mut buf,
            )?;

            // Score entries so a-children comes first, then b-node, then
            // a-node, then b-children, with dirs and anything else trailing.
            let a_prefix = a_h.to_hex_hash().node_db_prefix();
            let b_prefix = b_h.to_hex_hash().node_db_prefix();
            let a_prefix_str = a_prefix.to_string_lossy().replace('\\', "/");
            let b_prefix_str = b_prefix.to_string_lossy().replace('\\', "/");
            let reordered = rebuild_tar_with_filter(&buf, |path| {
                let p = path.to_string_lossy().replace('\\', "/");
                let in_a = p.contains(&a_prefix_str);
                let in_b = p.contains(&b_prefix_str);
                if in_a && p.ends_with("/children") {
                    Some(0)
                } else if in_b && p.ends_with("/node") {
                    Some(1)
                } else if in_a && p.ends_with("/node") {
                    Some(2)
                } else if in_b && p.ends_with("/children") {
                    Some(3)
                } else {
                    Some(4)
                }
            });

            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &reordered[..], UnpackOptions::Overwrite)?;
            assert!(installed.contains(&a_h));
            assert!(installed.contains(&b_h));
            let store = target_repo.merkle_store()?;
            assert!(store.exists(&a_h)?);
            assert!(store.exists(&b_h)?);
            Ok(())
        })
    }

    /// A tarball with only a `node` entry for a hash (no matching `children`)
    /// is valid per the file-backend contract: `parse_pair(node, &[])` decodes
    /// it as a leaf node. The streaming unpack flushes lone-node partials at
    /// end-of-stream.
    #[test]
    fn test_lmdb_unpack_tolerates_node_without_children() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let parent_h = h("11111111111111111111111111111111");
            let parent = commit_with_hash(repo, parent_h);
            let session = backend.begin()?;
            session.create_node(&parent, None)?.finish()?;
            session.finish()?;

            let mut buf = Vec::new();
            backend.pack_nodes(
                &HashSet::from_iter([parent_h]),
                PackOptions::ServerCanonical,
                &mut buf,
            )?;

            // Strip the `children` entry — keep everything else.
            let stripped = rebuild_tar_with_filter(&buf, |path| {
                let p = path.to_string_lossy().replace('\\', "/");
                if p.ends_with("/children") {
                    None
                } else {
                    Some(0)
                }
            });

            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &stripped[..], UnpackOptions::Overwrite)?;
            assert!(
                installed.contains(&parent_h),
                "lone-node entry's hash must still be reported: {installed:?}"
            );
            let store = target_repo.merkle_store()?;
            assert!(
                store.exists(&parent_h)?,
                "parent commit must be readable after lone-node unpack"
            );
            Ok(())
        })
    }

    /// A tarball with only a `children` entry for a hash (no matching `node`)
    /// can't be decoded — `parse_pair` needs the node's lookup table to slice
    /// the children blob. The streaming unpack drops orphan-children bytes
    /// silently but still reports the hash through `seen` (matches the
    /// buffered loop's contract).
    #[test]
    fn test_lmdb_unpack_drops_orphan_children_but_reports_hash() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let parent_h = h("22222222222222222222222222222222");
            let parent = commit_with_hash(repo, parent_h);
            let session = backend.begin()?;
            session.create_node(&parent, None)?.finish()?;
            session.finish()?;

            let mut buf = Vec::new();
            backend.pack_nodes(
                &HashSet::from_iter([parent_h]),
                PackOptions::ServerCanonical,
                &mut buf,
            )?;

            // Strip the `node` entry; keep `children` and dirs.
            let stripped = rebuild_tar_with_filter(&buf, |path| {
                let p = path.to_string_lossy().replace('\\', "/");
                if p.ends_with("/node") { None } else { Some(0) }
            });

            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &stripped[..], UnpackOptions::Overwrite)?;
            assert!(
                installed.contains(&parent_h),
                "orphan-children entry's hash must still be reported: {installed:?}"
            );
            // The parent's row was NOT written (no node bytes to decode).
            let store = target_repo.merkle_store()?;
            assert!(
                !store.exists(&parent_h)?,
                "no row should be written for an orphan-children-only hash"
            );
            Ok(())
        })
    }

    /// `UnpackOptions::SkipExisting` must not clobber a pre-existing row when
    /// the same hash arrives in the tarball.
    #[test]
    fn test_lmdb_unpack_skip_existing_preserves_existing_row() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let hash = h("33333333333333333333333333333333");
            let commit = commit_with_hash(repo, hash);
            let session = backend.begin()?;
            session.create_node(&commit, None)?.finish()?;
            session.finish()?;

            // Pack the source.
            let mut buf = Vec::new();
            backend.pack_nodes(
                &HashSet::from_iter([hash]),
                PackOptions::ServerCanonical,
                &mut buf,
            )?;

            // Set up a target with a pre-existing row for the same hash.
            let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
            let target_store = target_repo.merkle_store()?;
            {
                let session = target_store.begin()?;
                session.create_node(&commit, None)?.finish()?;
                session.finish()?;
            }
            let pre_existing = target_store
                .get_node(&hash)?
                .expect("pre-existing row should be readable");

            // SkipExisting unpack — must not overwrite.
            let installed = target_repo
                .merkle_transport()?
                .unpack(&mut &buf[..], UnpackOptions::SkipExisting)?;
            assert!(installed.contains(&hash));
            let after = target_store
                .get_node(&hash)?
                .expect("hash should still exist");
            assert_eq!(pre_existing.parent_id, after.parent_id);
            Ok(())
        })
    }

    /// Path-traversal entries in the tar are rejected — same posture as the
    /// file backend's unpack.
    #[test]
    fn test_lmdb_unpack_rejects_path_traversal() -> Result<(), OxenError> {
        // Hand-build a malicious tarball whose entry name contains `..`.
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_old();
            header.set_size(0);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            let name_bytes = b"tree/nodes/../escape";
            let old = header.as_old_mut();
            old.name[..name_bytes.len()].copy_from_slice(name_bytes);
            header.set_cksum();
            tar.append(&header, std::io::Cursor::new(Vec::new()))
                .map_err(MerkleDbError::Io)?;
            tar.finish().map_err(MerkleDbError::Io)?;
            tar.into_inner()
                .map_err(MerkleDbError::Io)?
                .finish()
                .map_err(MerkleDbError::Io)?;
        }
        let target_repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        let err = target_repo
            .merkle_transport()?
            .unpack(&mut &buf[..], UnpackOptions::Overwrite)
            .expect_err("path traversal must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Path traversal"),
            "unexpected error message: {msg}"
        );
        Ok(())
    }
}
