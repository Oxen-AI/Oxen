use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use tar::Archive;
use tempfile::TempDir;

use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::db::merkle_node::merkle_node_db::{MerkleDbError, MerkleNodeDB, node_db_path};
use crate::model::LocalRepository;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_transport::{
    MerklePacker, MerkleUnpacker, PackOptions, UnpackOptions,
};
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{MerkleHash, TMerkleTreeNode};
use crate::util;

/// File-based Merkle node store backend. Implements the [`MerkleStore`] trait.
///
/// Holds a borrowed `&LocalRepository` so it can delegate straight to
/// [`MerkleNodeDB`]'s existing repository-based methods without any modification.
/// Construction is O(1); feel free to call `LocalRepository::merkle_store` on
/// each operation.
///
/// Wire-format and existing-file policy are per-call concerns and live on the
/// transport methods themselves — see [`PackOptions`] and [`UnpackOptions`].
pub struct FileBackend<'repo> {
    repo: &'repo LocalRepository,
}

impl<'repo> FileBackend<'repo> {
    pub fn new(repo: &'repo LocalRepository) -> Self {
        Self { repo }
    }
}

/// Merkle reader implementation for the [`FileBackend`].
impl<'repo> MerkleReader for FileBackend<'repo> {
    type Error = MerkleDbError;

    /// Checks if a node with the given `hash` exists in the store.
    ///
    /// Alias for [`MerkleNodeDB::exists`].
    fn exists(&self, hash: &MerkleHash) -> Result<bool, MerkleDbError> {
        Ok(MerkleNodeDB::exists(self.repo, hash))
    }

    /// Retrieves the node with the given `hash` from the store. `None` means no such node exists.
    ///
    /// Alias for [`MerkleNodeDB::open_read_only`].
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, MerkleDbError> {
        if !MerkleNodeDB::exists(self.repo, hash) {
            return Ok(None);
        }
        let db = MerkleNodeDB::open_read_only(self.repo, hash)?;
        let node = db.node()?;
        Ok(Some(MerkleNodeRecord::new(
            db.node_id,
            db.dtype,
            db.parent_id,
            node,
            db.num_children(),
        )))
    }

    /// Retrieves the children of the node with the given `hash` from the store.
    /// An empty vec means that either the node is a not a directory or virtual node or it is one
    /// but has no files.
    ///
    /// Alias for [`MerkleNodeDB::open_read_only`] & a `.map()` call..
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, MerkleDbError> {
        let mut db = MerkleNodeDB::open_read_only(self.repo, hash)?;
        db.map()
    }
}

/// Merkle writer implementation for the [`FileBackend`].
impl<'repo> MerkleWriter for FileBackend<'repo> {
    type Error = MerkleDbError;
    #[rustfmt::skip]
    type Session<'a> = FileWriteSession<'repo> where Self: 'a;

    fn begin(&self) -> Result<FileWriteSession<'repo>, MerkleDbError> {
        Ok(FileWriteSession { repo: self.repo })
    }
}

/// Write session for the file backend. Used to write multiple nodes & their children.
///
/// Writes happen eagerly through each [`FileNodeSession`]; this session's
/// [`finish`] is a no-op.
pub struct FileWriteSession<'repo> {
    repo: &'repo LocalRepository,
}

/// Merkle write session implementation that the [`FileBackend`] uses.
impl<'repo> MerkleWriteSession for FileWriteSession<'repo> {
    type Error = MerkleDbError;
    #[rustfmt::skip]
    type NodeSession<'b> = FileNodeSession where Self: 'b;

    /// Creates a new session for writing a `node` and `children` file.
    /// Calls [`MerkleNodeDb::open_read_write`] internally.
    fn create_node<N: TMerkleTreeNode>(
        &self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<FileNodeSession, MerkleDbError> {
        FileNodeSession::new(self.repo, node, parent_id)
    }

    /// A no-op -- the node write session from [`create_node`] eagerly writes its files.
    /// The [`FileNodeSession::finish`] method flushes and closes open file handles.
    fn finish(self) -> Result<(), MerkleDbError> {
        Ok(())
    }
}

/// Per-node write handle for the file backend. Writes exactly 1 `node` and 1 `children` file.
///
/// Acts as a newtype around [`MerkleNodeDB`] with a `finished` sentinel that guards [`Drop`]
/// against double-closing the underlying file handles. When required, the drop implementation
/// will call [`FileNodeSession::finish`].
pub struct FileNodeSession {
    db: MerkleNodeDB,
    finished: bool,
}

impl FileNodeSession {
    /// Opens a new [`MerkleNodeDB`] in read-write mode.
    fn new<N: TMerkleTreeNode>(
        repo: &LocalRepository,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self, MerkleDbError> {
        Ok(Self {
            db: MerkleNodeDB::open_read_write(repo, node, parent_id)?,
            finished: false,
        })
    }

    /// The `finish` implementation, but using `&mut self` so that it can be used in `Drop`.
    fn idempotent_finish(&mut self) -> Result<(), MerkleDbError> {
        if self.finished {
            Ok(())
        } else {
            self.finished = true;
            MerkleNodeDB::close(&mut self.db)
        }
    }
}

/// Ensure that the `node` and `children` file handles are flushed and closed when dropped.
impl Drop for FileNodeSession {
    fn drop(&mut self) {
        self.idempotent_finish()
            .expect("Did not explicitly call finish() and encountered an error.");
    }
}

/// Merkle node write session that the [`FileBackend`] uses.
impl NodeWriteSession for FileNodeSession {
    type Error = MerkleDbError;

    /// The node currently being written.
    fn node_id(&self) -> &MerkleHash {
        &self.db.node_id
    }

    /// Adds an entry to the `children` file for the current node. Alias for [`MerkleNodeDB::add_child`].
    fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), MerkleDbError> {
        MerkleNodeDB::add_child(&mut self.db, child)
    }

    /// Flushes the open `node` and `children` file handles, closes them, then calls `fsync` on them.
    /// Consumes the session; [`Drop`] becomes a no-op after this returns `Ok` because the
    /// `finished` sentinel guards `idempotent_finish`.
    fn finish(mut self) -> Result<(), MerkleDbError> {
        self.idempotent_finish()
    }
}

/// Estimate the **uncompressed** tar payload size for [`MerklePacker::pack_nodes`]
/// over `hashes`, in bytes. Used to extend an upload progress bar's total length
/// before the pack/upload kicks off, so the bar has a known end and a meaningful ETA.
///
/// The estimate sums each present node directory's file content sizes plus tar's
/// fixed-size overhead (one 512-byte header per file/directory entry, file content
/// padded to 512-byte multiples). It ignores the gzip ratio because the merkle
/// `node` and `children` files contain mostly random-looking hash bytes, which
/// compress to ~1.0× — so this uncompressed total is a tight upper bound on the
/// post-gzip bytes that will flow over the wire.
///
/// Hashes whose node directory is missing on disk contribute 0, matching the
/// silent-skip semantics of [`MerklePacker::pack_nodes`].
pub fn pack_nodes_byte_estimate(repo: &LocalRepository, hashes: &HashSet<MerkleHash>) -> u64 {
    const TAR_HEADER_BYTES: u64 = 512;
    const TAR_BLOCK_SIZE: u64 = 512;

    let mut total: u64 = 0;
    for hash in hashes {
        let node_dir = node_db_path(repo, hash);
        if !node_dir.exists() {
            continue;
        }
        // The directory entry itself.
        total = total.saturating_add(TAR_HEADER_BYTES);
        let Ok(entries) = std::fs::read_dir(&node_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_file() {
                let len = meta.len();
                let padded = len.div_ceil(TAR_BLOCK_SIZE).saturating_mul(TAR_BLOCK_SIZE);
                total = total.saturating_add(TAR_HEADER_BYTES.saturating_add(padded));
            } else if meta.is_dir() {
                total = total.saturating_add(TAR_HEADER_BYTES);
            }
        }
    }
    total
}

/// Pack the tar-gz wire format for a set of merkle hashes into `out`, using the
/// in-tar layout and gzip compression level selected by `opts`.
///
/// Hashes whose node directory is missing on disk are silently skipped iff
/// `skip_missing_node_dir` is `true`: this matches the existing oxen behavior
/// of `compress_nodes` / `compress_node` / `compress_commits`. If `false`,
/// missing node directories result in an `Err(MerkleDbError::MissingNodeDir)`.
fn write_hashes_tar<W: Write>(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
    opts: PackOptions,
    out: W,
    skip_missing_node_dir: bool,
) -> Result<(), MerkleDbError> {
    let enc = GzEncoder::new(out, pack_options_compression(opts));
    let mut tar = tar::Builder::new(enc);
    for hash in hashes {
        let dir_prefix = hash.to_hex_hash().node_db_prefix();
        let tar_subdir: PathBuf = match opts {
            PackOptions::ServerCanonical => Path::new(TREE_DIR).join(NODES_DIR).join(&dir_prefix),
            PackOptions::LegacyClientPush => PathBuf::from(&dir_prefix),
        };
        let node_dir = node_db_path(repo, hash);
        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        } else if !skip_missing_node_dir {
            return Err(MerkleDbError::MissingNodeDir(*hash));
        } else {
            log::warn!("Skipping missing node dir for hash {}", hash.to_hex_hash());
        }
    }
    tar.finish()?;
    tar.into_inner()?.finish()?;
    Ok(())
}

/// Map a [`PackOptions`] to the gzip compression level historically used for that
/// layout. `Compression::fast` for the server-canonical download bytes;
/// `Compression::default` for the legacy client-push upload bytes.
fn pack_options_compression(opts: PackOptions) -> Compression {
    match opts {
        PackOptions::ServerCanonical => Compression::fast(),
        PackOptions::LegacyClientPush => Compression::default(),
    }
}

/// Pack the tar-gz wire format for every node in the store into `out`.
/// If `skip_missing_node_dir` is `false`, missing node directories result in
/// an `Err(MerkleDbError::MissingTreeNodesDir)`. Otherwise, missing node dirs
/// are skipped and logged as a warning.
fn write_all_tar<W: Write>(
    repo: &LocalRepository,
    out: W,
    skip_missing_node_dir: bool,
) -> Result<(), MerkleDbError> {
    let enc = GzEncoder::new(out, Compression::fast());
    let mut tar = tar::Builder::new(enc);
    let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR);
    let nodes_dir = repo
        .path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);
    if nodes_dir.exists() {
        tar.append_dir_all(&tar_subdir, nodes_dir)?;
    } else if !skip_missing_node_dir {
        return Err(MerkleDbError::MissingTreeNodesDir);
    } else {
        log::warn!("Missing oxen tree/nodes dir in this repository: resulting in empty tarball");
    }
    tar.finish()?;
    tar.into_inner()?.finish()?;
    Ok(())
}

/// Unpack a tar-gz wire stream into `oxen_hidden` (the repository's `.oxen/` directory).
///
/// Tolerates two historical tarball layouts so that either a new or legacy client can talk
/// to a store built with this trait:
///   - **Server-style** (emitted by [`pack_nodes`] / [`pack_all`] and the old `compress_*`
///     helpers): entries carry the full `tree/nodes/{prefix}/{suffix}/{node,children}`
///     prefix. Joined directly under `oxen_hidden`.
///   - **Legacy client-push style** (emitted by the old `api::client::tree::create_nodes`):
///     entries start at `{prefix}/{suffix}/{node,children}` with no `tree/nodes/` prefix.
///     Prepended under `oxen_hidden/tree/nodes/`.
///
/// Returns the set of hashes parsed from the tarball.
///
/// Behaviour controls (to provide a backwards-compatible format that older clients & servers speak):
/// - `overwrite_existing == true` overwrites entries whose destination already exists;
///   matches `util::fs::unpack_async_tar_archive`'s download-path behaviour.
/// - `overwrite_existing == false` skips them; matches
///   `repositories::tree::unpack_nodes`'s upload-consumer behaviour.
///
/// Errors:
/// - Entries that aren't regular files or directories return [`MerkleDbError::UnsupportedTarEntry`].
/// - Entries whose path contains a `..` component return [`MerkleDbError::PathTraversal`].
///
/// Both checks mirror `util::fs::unpack_async_tar_archive`.
fn extract_tar_under<R: Read>(
    reader: R,
    oxen_hidden: &Path,
    overwrite_existing: bool,
) -> Result<HashSet<MerkleHash>, MerkleDbError> {
    let mut hashes: HashSet<MerkleHash> = HashSet::new();
    let decoder = GzDecoder::new(reader);
    let mut archive = Archive::new(decoder);
    let entries = archive.entries().map_err(MerkleDbError::CannotReadMerkle)?;

    let tree_nodes_prefix = Path::new(TREE_DIR).join(NODES_DIR);

    for entry in entries {
        // Propagate per-entry read errors (e.g. truncated/corrupted gzip stream surfacing
        // as the iterator yields `Err`). Matches `main`'s `unpack_async_tar_archive`
        // behavior, which `?`'d the entry directly. Silent-skip would otherwise return
        // `Ok` with an empty hash set on a corrupted body, hiding the failure from the
        // download path's `JoinHandle`-error surface in `api::client::tree`.
        let mut file = entry.map_err(MerkleDbError::CannotReadMerkle)?;
        let path = file.path()?.into_owned();
        // Path-traversal guard: refuse any entry whose path resolves above its container.
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(MerkleDbError::PathTraversal(path.display().to_string()));
        }
        // Entry-type validation: only regular files and directories are allowed.
        let entry_type = file.header().entry_type();
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(MerkleDbError::UnsupportedTarEntry {
                path: path.display().to_string(),
            });
        }
        // Server-style entries already contain `tree/nodes/...`; join directly.
        // Legacy client-push entries begin at `{prefix}/{suffix}/...`; prepend `tree/nodes/`.
        let dst_path = if path.starts_with(&tree_nodes_prefix) {
            oxen_hidden.join(&path)
        } else {
            oxen_hidden.join(&tree_nodes_prefix).join(&path)
        };
        if let Some(parent) = dst_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if dst_path.exists() && !overwrite_existing {
            log::info!("Node already exists at {dst_path:?}, skipping");
            continue;
        }
        file.unpack(&dst_path)?;

        // Extract the merkle hash from this entry's path, if it identifies one.
        //
        // After the path-resolution above, `dst_path` is of the form
        // `<oxen_hidden>/tree/nodes/<rest>`. We classify entries by the SHAPE
        // of `<rest>`, never by whether components happen to be hex. We assume that
        // we have the hex-encoded hash as the `{prefix}/{suffix}` dirs.
        if let Some(hash) = extract_hash_from_entry_path(&dst_path, oxen_hidden)? {
            hashes.insert(hash);
        } else {
            log::warn!(
                "Skipping non-merkle entry in tarball: {}",
                dst_path.display()
            );
        }
    }
    Ok(hashes)
}

/// Inspect a fully-resolved tar entry destination path and, if it identifies a Merkle
/// node, return that node's hash.
///
/// `dst_path` must be inside `<oxen_hidden>/tree/nodes/`. The path's segments after
/// that prefix determine the entry kind:
///
/// | Segments after `tree/nodes/` | Entry kind | Result |
/// |---|---|---|
/// | 0 (the `tree/nodes` dir itself) | intermediate dir | `Ok(None)` |
/// | 1 (the `{prefix}` dir) | intermediate dir | `Ok(None)` |
/// | 2 (`{prefix}/{suffix}` dir) | hash-bearing dir | `Ok(Some(hash))` (hash parsed from `{prefix}{suffix}` as hex u128) |
/// | 3 (`{prefix}/{suffix}/node` or `.../children`) | leaf file | `Ok(None)` (hash already produced from the parent dir entry) |
///
/// Anything else returns [`MerkleDbError::InvalidTarStructure`]. A `{prefix}/{suffix}`
/// dir whose `{prefix}` & `{suffix}` don't hex-parse as a `u128` value returns a Err of
/// [`MerkleDbError::InvalidNodeIdHex`]. Any non-UTF-8 path components in the tarball return
/// an Err of [`MerkleDbError::InvalidTarStructure`]: the merkle layout should never produce
/// a non-UTF-8 segment name.
fn extract_hash_from_entry_path(
    dst_path: &Path,
    oxen_hidden: &Path,
) -> Result<Option<MerkleHash>, MerkleDbError> {
    let tree_nodes_prefix = Path::new(TREE_DIR).join(NODES_DIR);

    // make a new InvalidTarStructure MerkleDbError instance
    let invalid_structure = |reason: &str| MerkleDbError::InvalidTarStructure {
        entry_path: dst_path.display().to_string(),
        reason: reason.to_string(),
    };

    let rel = dst_path.strip_prefix(oxen_hidden).map_err(|_| {
        invalid_structure("entry resolved outside of the repo's `.oxen/` directory")
    })?;
    let under_tree_nodes = rel
        .strip_prefix(&tree_nodes_prefix)
        .map_err(|_| invalid_structure("entry path is not under `tree/nodes/`"))?;

    // Collect normal path components as `&str`. Reject non-UTF-8 components
    // up front (they can't appear in a well-formed merkle tar archive).
    let mut components: Vec<&str> = Vec::new();
    for component in under_tree_nodes.components() {
        let Component::Normal(segment) = component else {
            return Err(invalid_structure(
                "entry path contains a non-`Normal` component",
            ));
        };
        let Some(s) = segment.to_str() else {
            return Err(invalid_structure("entry path contains a non-UTF-8 segment"));
        };
        components.push(s);
    }

    match components.as_slice() {
        // `tree/nodes` itself, or `tree/nodes/{prefix}` — intermediate dirs
        // produced by `pack_all`. No hash to record.
        [] | [_] => Ok(None),
        // `tree/nodes/{prefix}/{suffix}` — the hash-bearing dir. Parse
        // unconditionally; failure is a structured error.
        [prefix, suffix] => {
            let id = format!("{prefix}{suffix}");
            let hash_value = u128::from_str_radix(&id, 16)
                .map_err(|source| MerkleDbError::InvalidNodeIdHex { id, source })?;
            Ok(Some(MerkleHash::new(hash_value)))
        }
        // `tree/nodes/{prefix}/{suffix}/{node|children}` — leaf files. The
        // hash is recorded when the parent dir entry is processed.
        [_, _, leaf] if *leaf == "node" || *leaf == "children" => Ok(None),
        [_, _, other] => Err(invalid_structure(&format!(
            "leaf file under `tree/nodes/{{prefix}}/{{suffix}}/` must be `node` or `children`, got `{other}`"
        ))),
        _ => Err(invalid_structure(
            "entry has more components than `tree/nodes/{prefix}/{suffix}/[node|children]`",
        )),
    }
}

/// Merkle tree node packing implementation for the [`FileBackend`].
impl<'repo> MerklePacker for FileBackend<'repo> {
    type Error = MerkleDbError;

    /// Pack the given node hashes into `out` as a tar-gz stream, using the layout
    /// and compression level selected by `opts`. Hashes absent from the store are
    /// silently skipped.
    fn pack_nodes<W: Write>(
        &self,
        hashes: &HashSet<MerkleHash>,
        opts: PackOptions,
        out: W,
    ) -> Result<(), MerkleDbError> {
        write_hashes_tar(self.repo, hashes, opts, out, true)
    }

    /// Pack every node the store holds into `out` as a tar-gz stream.
    /// Always emits the server-canonical layout.
    fn pack_all<W: Write>(&self, out: W) -> Result<(), MerkleDbError> {
        write_all_tar(self.repo, out, true)
    }
}

/// Merkle tree node unpacking implementation for the [`FileBackend`].
impl<'repo> MerkleUnpacker for FileBackend<'repo> {
    type Error = MerkleDbError;

    /// Unpack a tar-gz wire stream into the store, applying `opts`'s existing-file policy.
    ///
    /// If the repository sits on a virtual filesystem ([`LocalRepository::is_vfs`] is true),
    /// unpack into a tempdir first and `copy_dir_all` the result through the VFS. Some
    /// VFS implementations don't tolerate tar's streaming many-small-files pattern, so the
    /// staging hop is needed for correctness. Otherwise, unpack directly to `.oxen/`.
    fn unpack<R: Read>(
        &self,
        reader: R,
        opts: UnpackOptions,
    ) -> Result<HashSet<MerkleHash>, MerkleDbError> {
        let overwrite_existing = matches!(opts, UnpackOptions::Overwrite);
        let oxen_hidden = self.repo.path.join(OXEN_HIDDEN_DIR);
        if self.repo.is_vfs() {
            let tmp = TempDir::new()?;
            let hashes = extract_tar_under(reader, tmp.path(), overwrite_existing)?;
            util::fs::copy_dir_all(tmp.path(), &oxen_hidden)
                .map_err(|e| MerkleDbError::FsTransport(Box::new(e)))?;
            Ok(hashes)
        } else {
            extract_tar_under(reader, &oxen_hidden, overwrite_existing)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use bytesize::ByteSize;

    use super::*;
    use crate::error::OxenError;
    use crate::model::Commit;
    use crate::model::merkle_tree::node::{CommitNode, DirNode};
    use crate::{repositories, test};

    /// Dropping a `FileNodeSession` without calling `finish()` must still
    /// flush+sync the underlying files. This is to match the implicit-drop semantics
    /// that `MerkleNodeDB` has before these `MerkleStore` traits were introduced.
    #[test]
    fn test_drop_finishes_file_node_session() -> Result<(), crate::error::OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let commit = CommitNode::default();
            let dir = DirNode::default();
            let commit_hash = commit.hash();

            // Scope the session so Drop runs at its end.
            {
                let store = repo.merkle_store();
                let session = store.begin().expect("Could not begin session");
                let mut ns = session
                    .create_node(&commit, None)
                    .expect("Could not begin node session");
                ns.add_child(&dir)
                    .expect("Could not add a child to the node session");
                // Deliberately DO NOT call ns.finish() or session.finish().
            }

            let store = repo.merkle_store();
            assert!(
                store
                    .exists(commit_hash)
                    .expect("commit to exist after being written")
            );
            let children = store
                .get_children(commit_hash)
                .expect("children to exist after being written");
            assert_eq!(children.len(), 1, "expected the single dir child");
            Ok(())
        })
    }

    /// Gunzip + collect tar entries into a deterministic map for byte-compat comparison.
    /// Gzip-compressed bytes aren't stable (mtime field varies), but the tar entry set is.
    /// Results in a panic! if internal errors are encountered.
    fn list_tar_entries(buffer: &[u8]) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut out = BTreeMap::new();
        let decoder = GzDecoder::new(buffer);
        let mut archive = Archive::new(decoder);
        for entry in archive.entries().expect("tar entries failed") {
            let mut entry = entry.expect("tar entry failed");
            let path = entry.path().expect("tar path failed").into_owned();
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut bytes).expect("tar read failed");
            out.insert(path, bytes);
        }
        out
    }

    // --------------------------------------------------------------------------------------------
    //
    // [START] Old Implementation
    //
    // Retained for reference during review; never called at runtime.
    // These functions preserve the existing Merkle tree operations (both reading, writing,
    // and tree packing & unpacking) **before** the refactor to use these MerkleStore and
    // MerkleTransport traits.
    //
    // Tests use these here to ensure that we're backwards compatible and that older clients
    // and servers are inoperable with their newer counterparts.
    //
    // --------------------------------------------------------------------------------------------

    fn compress_tree(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);
        compress_full_tree(repository, &mut tar)?;
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);

        log::debug!("Compressed entire tree size is {}", ByteSize::b(total_size));

        Ok(buffer)
    }

    fn compress_full_tree(
        repository: &LocalRepository,
        tar: &mut tar::Builder<GzEncoder<Vec<u8>>>,
    ) -> Result<(), OxenError> {
        // This will be the subdir within the tarball,
        // so when we untar it, all the subdirs will be extracted to
        // tree/nodes/...
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR);
        let nodes_dir = repository
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(TREE_DIR)
            .join(NODES_DIR);

        log::debug!("Compressing tree in dir {nodes_dir:?}");

        if nodes_dir.exists() {
            tar.append_dir_all(&tar_subdir, nodes_dir)?;
        }

        Ok(())
    }

    fn compress_nodes(
        repository: &LocalRepository,
        hashes: &HashSet<MerkleHash>,
    ) -> Result<Vec<u8>, OxenError> {
        // zip up the node directories for each commit tree
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);

        log::debug!("Compressing {} unique nodes...", hashes.len());
        for hash in hashes {
            // This will be the subdir within the tarball
            // so when we untar it, all the subdirs will be extracted to
            // tree/nodes/...
            let dir_prefix = hash.to_hex_hash().node_db_prefix();
            let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

            let node_dir = node_db_path(repository, hash);
            if node_dir.exists() {
                tar.append_dir_all(&tar_subdir, node_dir)?;
            }
        }
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        Ok(buffer)
    }

    fn compress_node(
        repository: &LocalRepository,
        hash: &MerkleHash,
    ) -> Result<Vec<u8>, OxenError> {
        // This will be the subdir within the tarball
        // so when we untar it, all the subdirs will be extracted to
        // tree/nodes/...
        let dir_prefix = hash.to_hex_hash().node_db_prefix();
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

        // zip up the node directory
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);
        let node_dir = node_db_path(repository, hash);

        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        }
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
        log::debug!(
            "Compressed node {} size is {}",
            hash,
            ByteSize::b(total_size)
        );

        Ok(buffer)
    }

    fn compress_commits(
        repository: &LocalRepository,
        commits: &Vec<Commit>,
    ) -> Result<Vec<u8>, OxenError> {
        // zip up the node directory
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);

        for commit in commits {
            let hash = commit.hash()?;
            // This will be the subdir within the tarball
            // so when we untar it, all the subdirs will be extracted to
            // tree/nodes/...
            let dir_prefix = hash.to_hex_hash().node_db_prefix();
            let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

            let node_dir = node_db_path(repository, &hash);
            log::debug!("Compressing commit from dir {node_dir:?}");
            if node_dir.exists() {
                tar.append_dir_all(&tar_subdir, node_dir)?;
            }
        }
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        Ok(buffer)
    }

    /// Synchronous mirror of `main`'s `api::client::tree::node_download_request` unpack
    /// step, sans HTTP/streaming. Allows for comparing bytes-on-disk in the old
    /// `unpack_async_tar_archive` install produced for a given input tarball.
    ///
    /// Wire identity with existing oxen behavior:
    ///   - decompress the gzip stream as-is;
    ///   - construct an `async_tar::Archive`;
    ///   - call `util::fs::unpack_async_tar_archive(archive, <oxen_hidden>)`.
    ///
    /// We bridge a sync `&[u8]` into the async iface via `futures::io::Cursor`. The
    /// install is identical to running the actual HTTP path against a server that hands
    /// back exactly `buffer` as the response body.
    async fn node_download_request_unpack_old(
        repository: &LocalRepository,
        buffer: &[u8],
    ) -> Result<(), OxenError> {
        let cursor = futures::io::Cursor::new(buffer.to_vec());
        let decoder = async_compression::futures::bufread::GzipDecoder::new(
            futures::io::BufReader::new(cursor),
        );
        let archive = async_tar::Archive::new(decoder);
        let dst = repository.path.join(OXEN_HIDDEN_DIR);
        util::fs::create_dir_all(&dst)?;
        util::fs::unpack_async_tar_archive(archive, &dst).await?;
        Ok(())
    }

    /// Exact copy of `api::client::tree::create_nodes`'s pack body pre Merkle packing traits,
    /// but stripped of HTTP / progress concerns. Used as the byte-level source of truth to
    /// ensure that the Merkle packing & unpacking refactor allows for older clients & servers
    /// to interoperate with the refactored code.
    fn create_nodes_pack_old(
        local_repo: &LocalRepository,
        nodes: &HashSet<MerkleHash>,
    ) -> Result<Vec<u8>, OxenError> {
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);
        let node_path = local_repo
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(TREE_DIR)
            .join(NODES_DIR);
        for node_hash in nodes {
            let dir_prefix = node_hash.to_hex_hash().node_db_prefix();
            let node_dir = node_path.join(&dir_prefix);
            tar.append_dir_all(dir_prefix, node_dir)?;
        }
        tar.finish()?;
        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        Ok(buffer)
    }

    fn unpack_nodes(
        repository: &LocalRepository,
        buffer: &[u8],
    ) -> Result<HashSet<MerkleHash>, OxenError> {
        let mut hashes: HashSet<MerkleHash> = HashSet::new();
        log::debug!("Unpacking nodes from buffer...");
        let decoder = GzDecoder::new(buffer);
        log::debug!("Decoder created");
        let mut archive = Archive::new(decoder);
        log::debug!("Archive created");
        let Ok(entries) = archive.entries() else {
            return Err(OxenError::basic_str(
                "Could not unpack tree database from archive",
            ));
        };
        log::debug!("Extracting entries...");
        for file in entries {
            let Ok(mut file) = file else {
                log::error!("Could not unpack file in archive...");
                continue;
            };
            let path = file.path().unwrap();
            let oxen_hidden_path = repository.path.join(OXEN_HIDDEN_DIR);
            let dst_path = oxen_hidden_path.join(TREE_DIR).join(NODES_DIR).join(path);

            if let Some(parent) = dst_path.parent() {
                util::fs::create_dir_all(parent).expect("Could not create parent dir");
            }
            if dst_path.exists() {
                log::debug!("Node already exists at {dst_path:?}");
                continue;
            }
            file.unpack(&dst_path)?;

            // the hash is the last two path components combined
            if !dst_path.ends_with("node") && !dst_path.ends_with("children") {
                let id = dst_path
                    .components()
                    .rev()
                    .take(2)
                    .map(|c| c.as_os_str().to_str().unwrap())
                    .collect::<Vec<&str>>()
                    .into_iter()
                    .rev()
                    .collect::<String>();
                hashes.insert(id.parse()?);
            }
        }
        Ok(hashes)
    }

    // --------------------------------------------------------------------------------------------
    //
    // [END] Old Implementation
    //
    // --------------------------------------------------------------------------------------------

    /// Pack every node in a repo, unpack into a fresh empty repo, and verify every
    /// installed hash is readable from the target store.
    #[tokio::test]
    async fn test_transport_round_trip() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let mut packed = Vec::new();
            repo.merkle_store()
                .pack_all(&mut packed)
                .expect("pack_all failed");
            assert!(!packed.is_empty(), "pack_all produced empty buffer");

            let tmp = tempfile::TempDir::new()?;
            let clone = repositories::init(tmp.path())?;
            let installed = clone
                .merkle_store()
                .unpack(&packed[..], UnpackOptions::Overwrite)
                .expect("unpack failed");
            assert!(!installed.is_empty(), "unpack installed no nodes");

            for hash in &installed {
                assert!(
                    clone.merkle_store().exists(hash).expect("exists failed"),
                    "expected installed hash {hash} to be readable"
                );
            }
            Ok(())
        })
        .await
    }

    /// The tar entry set produced by the legacy `compress_nodes` helper must equal the
    /// one produced by the trait's `pack_nodes`. Gzip bytes differ on mtime, but the
    /// decompressed tar payload must be identical.
    #[tokio::test]
    async fn test_compress_nodes_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hashes = HashSet::from_iter([head.hash().expect("no commit for head")]);

            // prior code for packing Merkle nodes into a .tar.gz
            let old_pack_method =
                compress_nodes(&repo, &hashes).expect("could not compress Merkle tree nodes");

            let new_pack_method = {
                let mut via_trait = Vec::new();
                repo.merkle_store()
                    .pack_nodes(&hashes, PackOptions::ServerCanonical, &mut via_trait)
                    .expect("pack_nodes failed");
                via_trait
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_nodes helper and pack_nodes trait"
            );
            Ok(())
        })
        .await
    }

    /// Same byte-compat check for the whole-tree path.
    #[tokio::test]
    async fn test_compress_tree_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            // prior code for packing an entire Merkle tree into a .tar.gz
            let old_pack_method = compress_tree(&repo)?;

            let new_pack_method = {
                let mut via_trait = Vec::new();
                repo.merkle_store()
                    .pack_all(&mut via_trait)
                    .expect("pack_all failed");
                via_trait
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_tree helper and pack_all trait"
            );
            Ok(())
        })
        .await
    }

    /// Byte-compat for the single-hash pack path (`compress_node` vs
    /// `pack_nodes(&{hash})`).
    #[tokio::test]
    async fn test_compress_node_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hash = head.hash().expect("no commit for head");

            // prior code for packing a single Merkle node into a .tar.gz
            let old_pack_method =
                compress_node(&repo, &hash).expect("could not compress Merkle tree node");

            let new_pack_method = {
                let hashes = HashSet::from_iter([hash]);
                let mut via_trait = Vec::new();
                repo.merkle_store()
                    .pack_nodes(&hashes, PackOptions::ServerCanonical, &mut via_trait)
                    .expect("pack_nodes failed");
                via_trait
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_node helper and pack_nodes trait"
            );
            Ok(())
        })
        .await
    }

    /// Byte-compat for the commit-set pack path (`compress_commits` vs
    /// `pack_nodes(&{commit hashes})`).
    #[tokio::test]
    async fn test_compress_commits_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let commits: Vec<Commit> = vec![head];

            // prior code for packing a set of commit hashes into a .tar.gz
            let old_pack_method =
                compress_commits(&repo, &commits).expect("could not compress Merkle tree commits");

            let new_pack_method = {
                let mut hashes = HashSet::with_capacity(commits.len());
                for c in &commits {
                    hashes.insert(c.hash().expect("no hash for commit"));
                }
                let mut via_trait = Vec::new();
                repo.merkle_store()
                    .pack_nodes(&hashes, PackOptions::ServerCanonical, &mut via_trait)
                    .expect("pack_nodes failed");
                via_trait
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_commits helper and pack_nodes trait"
            );
            Ok(())
        })
        .await
    }

    /// Build a tarball in the legacy client-push layout (entries begin at
    /// `{prefix}/{suffix}/...` with no `tree/nodes/` prefix) — this is the format
    /// the old `api::client::tree::create_nodes` emitted and the one
    /// `unpack_nodes` was designed to consume.
    fn compress_nodes_client_push_format(
        repo: &LocalRepository,
        hashes: &HashSet<MerkleHash>,
    ) -> Result<Vec<u8>, OxenError> {
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);
        let node_path = repo
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(TREE_DIR)
            .join(NODES_DIR);
        for hash in hashes {
            let dir_prefix = hash.to_hex_hash().node_db_prefix();
            let node_dir = node_path.join(&dir_prefix);
            if node_dir.exists() {
                tar.append_dir_all(dir_prefix, node_dir)?;
            }
        }
        tar.finish()?;
        Ok(tar.into_inner()?.finish()?)
    }

    /// Behavioral parity between the legacy `unpack_nodes` and the new trait `unpack`
    /// on a legacy client-push-format tarball: same reported hash set, same readability
    /// through the store in both target repos.
    #[tokio::test]
    async fn test_unpack_nodes_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hashes = HashSet::from_iter([head.hash().expect("no commit for head")]);

            // Produce a legacy client-push-format tarball (the one old `unpack_nodes`
            // was designed to consume).
            let bytes = compress_nodes_client_push_format(&repo, &hashes)
                .expect("client-push-format pack failed");

            // Unpack into two fresh repos: one via the old helper, one via the trait.
            let tmp_old = tempfile::TempDir::new()?;
            let repo_old = repositories::init(tmp_old.path())?;
            let old_hashes = unpack_nodes(&repo_old, &bytes).expect("old unpack_nodes failed");

            let tmp_new = tempfile::TempDir::new()?;
            let repo_new = repositories::init(tmp_new.path())?;
            // Old `unpack_nodes` skipped existing files; mirror that with
            // `UnpackOptions::SkipExisting` so the parity check is semantically faithful.
            let new_hashes = repo_new
                .merkle_store()
                .unpack(&bytes[..], UnpackOptions::SkipExisting)
                .expect("new unpack failed");

            assert_eq!(
                old_hashes, new_hashes,
                "reported hash sets differ between unpack_nodes helper and unpack trait"
            );
            assert!(
                !new_hashes.is_empty(),
                "no hashes were unpacked — test input was empty"
            );

            // Every installed hash must be readable through both stores.
            for h in &new_hashes {
                assert!(
                    repo_old
                        .merkle_store()
                        .exists(h)
                        .expect("old repo exists check failed"),
                    "hash {h} not readable in repo unpacked via legacy unpack_nodes"
                );
                assert!(
                    repo_new
                        .merkle_store()
                        .exists(h)
                        .expect("new repo exists check failed"),
                    "hash {h} not readable in repo unpacked via trait unpack"
                );
            }
            Ok(())
        })
        .await
    }

    /// Byte-for-byte compare two directory trees. Used to assert that the old and new
    /// unpack code paths produce identical on-disk state.
    fn collect_dir_contents(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut out: BTreeMap<PathBuf, Vec<u8>> = BTreeMap::new();
        if !root.exists() {
            return out;
        }
        for entry in walkdir::WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let rel = path.strip_prefix(root).unwrap_or(path).to_path_buf();
            let bytes = std::fs::read(path).expect("failed to read file under root");
            out.insert(rel, bytes);
        }
        out
    }

    /// Behavioural parity for the download-side unpack path. Pack a server-canonical
    /// tarball (the kind a server emits via `compress_tree` on `main` / `pack_all`
    /// today), feed the **same** bytes to:
    ///   - the old client unpack: `node_download_request_unpack_old` (the verbatim
    ///     `unpack_async_tar_archive` install from `main`'s `node_download_request`),
    ///   - the new client unpack: `merkle_store().unpack(...)` (overwrite-existing
    ///     default, matching `unpack_async_tar_archive`'s behaviour).
    /// The on-disk merkle-node tree under `<oxen_hidden>/tree/nodes/` must be identical
    /// in both target repos. The set of hashes the trait reports is also asserted to
    /// cover every installed hash directory.
    #[tokio::test]
    async fn test_node_download_request_unpack_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let mut packed = Vec::new();
            repo.merkle_store()
                .pack_all(&mut packed)
                .expect("pack_all failed");
            assert!(!packed.is_empty(), "pack_all produced empty buffer");

            // Old client install path (mirror of node_download_request on main).
            let tmp_old = tempfile::TempDir::new()?;
            let repo_old = repositories::init(tmp_old.path())?;
            node_download_request_unpack_old(&repo_old, &packed)
                .await
                .expect("old unpack failed");

            // New client install path: trait, with download-path overwrite semantics.
            let tmp_new = tempfile::TempDir::new()?;
            let repo_new = repositories::init(tmp_new.path())?;
            let installed = repo_new
                .merkle_store()
                .unpack(&packed[..], UnpackOptions::Overwrite)
                .expect("new unpack failed");

            // 1. The on-disk node trees must be identical.
            let old_tree = collect_dir_contents(
                &repo_old
                    .path
                    .join(OXEN_HIDDEN_DIR)
                    .join(TREE_DIR)
                    .join(NODES_DIR),
            );
            let new_tree = collect_dir_contents(
                &repo_new
                    .path
                    .join(OXEN_HIDDEN_DIR)
                    .join(TREE_DIR)
                    .join(NODES_DIR),
            );
            assert!(
                !old_tree.is_empty(),
                "no files installed via old node_download_request unpack — test input was empty"
            );
            assert_eq!(
                old_tree, new_tree,
                "on-disk merkle node trees differ between old node_download_request \
                 unpack and new MerkleUnpacker::unpack"
            );

            // 2. Every installed hash must be readable through the new store.
            assert!(!installed.is_empty(), "trait unpack reported no hashes");
            for h in &installed {
                assert!(
                    repo_new.merkle_store().exists(h).expect("exists failed"),
                    "hash {h} not readable in repo unpacked via trait unpack"
                );
            }
            Ok(())
        })
        .await
    }

    /// Path-traversal guard: an entry whose path resolves above its container must be
    /// rejected (matches `main`'s `unpack_async_tar_archive`).
    ///
    /// `tar::Header::set_path` validates and rejects `..` at write time, so we bypass
    /// that by writing directly into the old-style `name` field on the header — which
    /// is exactly the kind of malicious tarball a hostile server could construct.
    #[tokio::test]
    async fn test_unpack_rejects_path_traversal() -> Result<(), OxenError> {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_old();
            header.set_size(0);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            // Bypass `set_path`'s `..` rejection by writing the raw name bytes.
            let name_bytes = b"tree/nodes/../escape";
            let old = header.as_old_mut();
            old.name[..name_bytes.len()].copy_from_slice(name_bytes);
            header.set_cksum();
            tar.append(&header, std::io::Cursor::new(Vec::new()))?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = repo
            .merkle_store()
            .unpack(&buf[..], UnpackOptions::Overwrite)
            .expect_err("path traversal must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Path traversal"),
            "unexpected error message: {msg}"
        );
        Ok(())
    }

    /// Entry-type guard: anything that isn't a regular file or directory must be
    /// rejected (matches `main`'s `unpack_async_tar_archive`).
    #[tokio::test]
    async fn test_unpack_rejects_unsupported_entry_type() -> Result<(), OxenError> {
        // Build a tarball whose single entry is a symlink (entry type "Symlink").
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o777);
            header.set_entry_type(tar::EntryType::Symlink);
            // tar requires the link target to be set on a Symlink entry header.
            header.set_link_name("/etc/passwd")?;
            header.set_cksum();
            tar.append_data(
                &mut header,
                "tree/nodes/some_link",
                std::io::Cursor::new(Vec::new()),
            )?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = repo
            .merkle_store()
            .unpack(&buf[..], UnpackOptions::Overwrite)
            .expect_err("unsupported entry type must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Unsupported tar entry"),
            "unexpected error message: {msg}"
        );
        Ok(())
    }

    /// Regression: a hash whose hex form has stripped leading zeros (so its
    /// `node_db_prefix()` produces a `{prefix}/{suffix}` shorter than 32 chars
    /// total) must still round-trip through pack → unpack and appear in the
    /// returned hash set.
    ///
    /// Before the fix to `extract_hash_from_entry_path`, the unpack side had a
    /// silent `id.len() == 32` gate that dropped these entries.
    #[tokio::test]
    async fn test_unpack_recovers_hash_with_leading_zero_nibbles() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Pick a small `u128` whose hex form is much shorter than 32 chars.
            // `MerkleHash`'s `Display` is `{:x}` (no zero padding) so this is
            // exactly the shape that triggered the bug.
            let stripped_hash = MerkleHash::new(0x1234_u128);
            let hex = stripped_hash.to_string();
            assert!(
                hex.len() < 32,
                "expected hex form < 32 chars to exercise the regression, got {hex:?}"
            );

            // Manually plant a `{prefix}/{suffix}/node` and `.../children`
            // pair on disk so `pack_nodes` will tar them up.
            let prefix = stripped_hash.to_hex_hash().node_db_prefix();
            let nodes_root = repo
                .path
                .join(crate::constants::OXEN_HIDDEN_DIR)
                .join(crate::constants::TREE_DIR)
                .join(crate::constants::NODES_DIR);
            let node_dir = nodes_root.join(prefix);
            std::fs::create_dir_all(&node_dir)?;
            std::fs::write(node_dir.join("node"), b"node-bytes")?;
            std::fs::write(node_dir.join("children"), b"children-bytes")?;

            // Pack just this hash.
            let hashes = HashSet::from_iter([stripped_hash]);
            let mut buf = Vec::new();
            repo.merkle_store()
                .pack_nodes(&hashes, PackOptions::ServerCanonical, &mut buf)
                .expect("pack_nodes failed");

            // Unpack into a fresh repo and confirm the short hash made it out.
            let tmp = tempfile::TempDir::new()?;
            let target = repositories::init(tmp.path())?;
            let installed = target
                .merkle_store()
                .unpack(&buf[..], UnpackOptions::Overwrite)
                .expect("unpack failed");

            assert!(
                installed.contains(&stripped_hash),
                "unpack must report the short-hex hash; got {installed:?}"
            );
            Ok(())
        })
    }

    /// A `{prefix}/{suffix}` dir entry whose name isn't valid hex must produce
    /// the structured `InvalidNodeIdHex` error, not a silent skip and not a
    /// generic `ParseIntError`.
    #[tokio::test]
    async fn test_unpack_rejects_non_hex_node_id() -> Result<(), OxenError> {
        // Build a server-canonical-format tarball with a bogus dir name where
        // the suffix contains non-hex characters.
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            // Three levels of dir entries: tree/nodes/abc/zzzznothex.
            for path in &["tree/nodes", "tree/nodes/abc", "tree/nodes/abc/zzzznothex"] {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(tar::EntryType::Directory);
                header.set_cksum();
                tar.append_data(&mut header, path, std::io::Cursor::new(Vec::new()))?;
            }
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = repo
            .merkle_store()
            .unpack(&buf[..], UnpackOptions::Overwrite)
            .expect_err("non-hex node id must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Invalid merkle node id") && msg.contains("abczzzznothex"),
            "expected InvalidNodeIdHex error mentioning the offending id; got {msg}"
        );
        Ok(())
    }

    /// A tar entry whose path goes deeper than `tree/nodes/{prefix}/{suffix}/{node|children}`
    /// must produce `InvalidTarStructure`.
    #[tokio::test]
    async fn test_unpack_rejects_path_too_deep() -> Result<(), OxenError> {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            // Five levels under tree/nodes — too deep.
            for path in &[
                "tree/nodes",
                "tree/nodes/abc",
                "tree/nodes/abc/def0123",
                "tree/nodes/abc/def0123/extra",
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(tar::EntryType::Directory);
                header.set_cksum();
                tar.append_data(&mut header, path, std::io::Cursor::new(Vec::new()))?;
            }
            // Append a file at the over-deep level.
            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(0);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.set_cksum();
            tar.append_data(
                &mut file_header,
                "tree/nodes/abc/def0123/extra/node",
                std::io::Cursor::new(Vec::new()),
            )?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = repo
            .merkle_store()
            .unpack(&buf[..], UnpackOptions::Overwrite)
            .expect_err("over-deep entry must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Invalid merkle tar archive structure"),
            "expected InvalidTarStructure error; got {msg}"
        );
        Ok(())
    }

    /// A leaf file under `{prefix}/{suffix}/` whose name is neither `node`
    /// nor `children` must be rejected with `InvalidTarStructure`.
    #[tokio::test]
    async fn test_unpack_rejects_unknown_leaf_filename() -> Result<(), OxenError> {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            for path in &["tree/nodes", "tree/nodes/abc", "tree/nodes/abc/def0123"] {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(tar::EntryType::Directory);
                header.set_cksum();
                tar.append_data(&mut header, path, std::io::Cursor::new(Vec::new()))?;
            }
            // Bogus leaf filename `unexpected.txt` instead of `node`/`children`.
            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(0);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.set_cksum();
            tar.append_data(
                &mut file_header,
                "tree/nodes/abc/def0123/unexpected.txt",
                std::io::Cursor::new(Vec::new()),
            )?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = repo
            .merkle_store()
            .unpack(&buf[..], UnpackOptions::Overwrite)
            .expect_err("unknown leaf filename must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Invalid merkle tar archive structure") && msg.contains("unexpected.txt"),
            "expected InvalidTarStructure error mentioning the offending filename; got {msg}"
        );
        Ok(())
    }

    /// Cross-platform sanity check for `extract_hash_from_entry_path`. We
    /// bypass tar entirely and feed the helper paths constructed with
    /// explicit forward-slash literals — the same shape that
    /// `tar::Entry::path()` produces on every platform (tar archives are
    /// canonically `/`-separated, and the `tar` crate's `bytes2path` impl
    /// just feeds those bytes into `Path::new`).
    ///
    /// On Windows, `Path::components()` and `Path::strip_prefix` both treat
    /// `/` and `\` as separators, so the helper's match on segment-name
    /// strings dispatches the same way regardless of host. This test pins
    /// that down without needing CI on every OS.
    #[test]
    fn test_extract_hash_from_entry_path_is_path_separator_agnostic() {
        // Concrete hash whose hex form is < 32 chars (exercising the
        // leading-zero-nibble fix at the same time).
        let hash = MerkleHash::new(0xfeed_face_u128);
        let hex = hash.to_string();
        let prefix: String = hex.chars().take(3).collect();
        let suffix: String = hex.chars().skip(3).collect();

        let oxen_hidden = PathBuf::from("repo").join(".oxen");

        // Helper to build a tar-style path under `tree/nodes/...`.
        let under_tree_nodes = |tail: &[&str]| -> PathBuf {
            let mut s = PathBuf::from("repo")
                .join(".oxen")
                .join("tree")
                .join("nodes");
            for t in tail {
                if !t.is_empty() {
                    s.push(t);
                }
            }
            s
        };

        // tree/nodes itself — intermediate, no hash.
        let p = under_tree_nodes(&[""]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // tree/nodes/{prefix} — intermediate prefix dir, no hash.
        let p = under_tree_nodes(&[prefix.as_str()]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // tree/nodes/{prefix}/{suffix} — hash-bearing dir.
        let p = under_tree_nodes(&[prefix.as_str(), suffix.as_str()]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            Some(hash),
            "{prefix}/{suffix} must classify as the hash dir on every platform"
        );

        // tree/nodes/{prefix}/{suffix}/node — leaf file, no hash from this entry.
        let p = under_tree_nodes(&[prefix.as_str(), suffix.as_str(), "node"]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // tree/nodes/{prefix}/{suffix}/children — leaf file, no hash from this entry.
        let p = under_tree_nodes(&[prefix.as_str(), suffix.as_str(), "children"]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // Mixed-separator construction: `oxen_hidden` via platform-native
        // `Path::join` (uses `\` on Windows, `/` on Unix), but the entry
        // path itself uses forward slashes from a tar archive. `strip_prefix`
        // is documented to be component-aware, so this must still match on
        // every platform.
        let p = oxen_hidden
            .join("tree")
            .join("nodes")
            .join(&prefix)
            .join(&suffix);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden).unwrap(),
            Some(hash),
            "platform-native joins must classify the same as forward-slash literals"
        );
    }

    /// Byte-level parity for the upload-pack path: legacy `create_nodes` body on
    /// `main` vs the new trait-driven `pack_nodes(&hashes, PackOptions::LegacyClientPush, ...)`.
    /// The two tar entry sets must match exactly — that's the proof of zero
    /// externally visible behaviour change for the upload wire format.
    #[tokio::test]
    async fn test_create_nodes_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hashes = HashSet::from_iter([head.hash().expect("no commit for head")]);

            let old_pack =
                create_nodes_pack_old(&repo, &hashes).expect("legacy create_nodes pack failed");

            let new_pack = {
                let mut buf = Vec::new();
                repo.merkle_store()
                    .pack_nodes(&hashes, PackOptions::LegacyClientPush, &mut buf)
                    .expect("new pack failed");
                buf
            };

            assert_eq!(
                list_tar_entries(&old_pack),
                list_tar_entries(&new_pack),
                "tar entry set differs between create_nodes pack body and new upload pack"
            );
            Ok(())
        })
        .await
    }

    /// `exists` on a hash that was never written returns `Ok(false)`.
    #[test]
    fn test_exists_returns_false_for_missing_hash() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let store = repo.merkle_store();
            let missing = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            assert!(
                !store.exists(&missing).expect("exists must not error"),
                "expected exists() to return false for an unwritten hash"
            );
            Ok(())
        })
    }

    /// `get_node` on a hash that was never written returns `Ok(None)`.
    #[test]
    fn test_get_node_returns_none_for_missing_hash() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let store = repo.merkle_store();
            let missing = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            assert!(
                store
                    .get_node(&missing)
                    .expect("get_node must not error")
                    .is_none(),
                "expected get_node() to return None for an unwritten hash"
            );
            Ok(())
        })
    }

    /// `get_children` on a node that was written but never had children added returns
    /// `Ok(empty vec)`. Documents the leaf-children-are-empty contract from
    /// [`MerkleReader::get_children`].
    #[test]
    fn test_get_children_returns_empty_for_node_without_children() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let commit = CommitNode::default();
            let commit_hash = *commit.hash();
            {
                let store = repo.merkle_store();
                let session = store.begin().expect("begin failed");
                let ns = session
                    .create_node(&commit, None)
                    .expect("create_node failed");
                ns.finish().expect("finish node session failed");
                session.finish().expect("finish session failed");
            }
            let store = repo.merkle_store();
            let children = store
                .get_children(&commit_hash)
                .expect("get_children must not error");
            assert!(
                children.is_empty(),
                "expected an empty children list for a node with no add_child calls; got {} entries",
                children.len()
            );
            Ok(())
        })
    }

    /// A write session that begins and finishes without creating any node sessions
    /// should round-trip cleanly with no error.
    #[test]
    fn test_writer_session_with_no_nodes() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let store = repo.merkle_store();
            let session = store.begin().expect("begin failed");
            session
                .finish()
                .expect("finish must not error on empty session");
            Ok(())
        })
    }

    /// `unpack` on an empty tar-gz stream (the result of `pack_nodes` with an empty
    /// hash set) returns `Ok(empty hash set)`.
    #[tokio::test]
    async fn test_unpack_empty_tarball() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let mut buf = Vec::new();
            repo.merkle_store()
                .pack_nodes(&HashSet::new(), PackOptions::ServerCanonical, &mut buf)
                .expect("pack_nodes(empty) must not error");

            let tmp = tempfile::TempDir::new()?;
            let target = repositories::init(tmp.path())?;
            let installed = target
                .merkle_store()
                .unpack(&buf[..], UnpackOptions::Overwrite)
                .expect("unpack of empty tarball must not error");
            assert!(
                installed.is_empty(),
                "expected empty hash set from unpacking an empty tarball, got {} entries",
                installed.len()
            );
            Ok(())
        })
        .await
    }

    /// `pack_nodes` with a mix of valid and absent hashes silently skips the absent
    /// ones (matches `compress_nodes` semantics from `main`). The output tarball must
    /// contain entries only for the valid hash's prefix.
    #[tokio::test]
    async fn test_pack_nodes_skips_absent_hashes() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let head_hash = head.hash().expect("no commit for head");
            let absent = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);

            let mut hashes = HashSet::with_capacity(2);
            hashes.insert(head_hash);
            hashes.insert(absent);

            let mut buf = Vec::new();
            repo.merkle_store()
                .pack_nodes(&hashes, PackOptions::ServerCanonical, &mut buf)
                .expect("pack_nodes failed");

            // Compare via `Path::starts_with` (component-aware) rather than
            // substring on `to_string_lossy()`: tar entry paths use `/` while
            // `node_db_prefix()` joins via `Path::join`, which uses `\` on
            // Windows. The string forms wouldn't match cross-platform; the
            // component sequences do.
            let head_dir = Path::new(TREE_DIR)
                .join(NODES_DIR)
                .join(head_hash.to_hex_hash().node_db_prefix());
            let absent_dir = Path::new(TREE_DIR)
                .join(NODES_DIR)
                .join(absent.to_hex_hash().node_db_prefix());
            let entries = list_tar_entries(&buf);
            let any_head = entries.keys().any(|p| p.starts_with(&head_dir));
            let any_absent = entries.keys().any(|p| p.starts_with(&absent_dir));
            assert!(any_head, "expected entries for the head hash prefix");
            assert!(
                !any_absent,
                "expected no entries for the absent hash prefix; got at least one matching entry"
            );
            Ok(())
        })
        .await
    }

    /// `pack_nodes_byte_estimate` should be a tight upper bound on the actual
    /// uncompressed tar payload size for a given hash set. Verify that:
    /// 1. for a present hash, the estimate is non-zero;
    /// 2. the estimate is >= the actual gzipped output (i.e., usable as a progress total
    ///    that the upload won't blow past).
    /// 3. for an absent hash, the estimate contributes 0 (matches `pack_nodes`'s skip).
    #[tokio::test]
    async fn test_pack_nodes_byte_estimate_is_upper_bound() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let head_hash = head.hash().expect("no commit for head");
            let mut hashes = HashSet::new();
            hashes.insert(head_hash);

            let estimate = pack_nodes_byte_estimate(&repo, &hashes);
            assert!(estimate > 0, "estimate must be non-zero for a present hash");

            let mut buf = Vec::new();
            repo.merkle_store()
                .pack_nodes(&hashes, PackOptions::ServerCanonical, &mut buf)
                .expect("pack_nodes failed");
            assert!(
                estimate >= buf.len() as u64,
                "estimate ({estimate}) should be an upper bound on actual gzipped output \
                 ({} bytes)",
                buf.len()
            );

            // Absent hash: should contribute 0 to the estimate.
            let absent = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            let absent_only: HashSet<_> = HashSet::from_iter([absent]);
            assert_eq!(
                pack_nodes_byte_estimate(&repo, &absent_only),
                0,
                "absent hash should contribute 0 to the estimate"
            );

            // Mixed: head + absent should equal head-only.
            let mut mixed = HashSet::new();
            mixed.insert(head_hash);
            mixed.insert(absent);
            assert_eq!(
                pack_nodes_byte_estimate(&repo, &mixed),
                pack_nodes_byte_estimate(&repo, &hashes),
                "absent hash must not change the estimate when added to a present hash"
            );
            Ok(())
        })
        .await
    }

    /// VFS branch in `FileBackend::unpack`: when `is_vfs()` is true, unpack stages into
    /// a tempdir and copies through `copy_dir_all`. Pack the source repo, flip the
    /// target to vfs=true, unpack, and assert hashes are readable.
    #[tokio::test]
    async fn test_unpack_via_vfs_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let mut packed = Vec::new();
            repo.merkle_store()
                .pack_all(&mut packed)
                .expect("pack_all failed");
            assert!(!packed.is_empty(), "pack_all produced empty buffer");

            let tmp = tempfile::TempDir::new()?;
            let mut clone = repositories::init(tmp.path())?;
            clone.set_vfs(Some(true));
            assert!(clone.is_vfs(), "vfs flag should be on for this test");

            let installed = clone
                .merkle_store()
                .unpack(&packed[..], UnpackOptions::Overwrite)
                .expect("unpack via vfs branch failed");
            assert!(
                !installed.is_empty(),
                "vfs unpack reported no installed hashes"
            );
            for h in &installed {
                assert!(
                    clone.merkle_store().exists(h).expect("exists failed"),
                    "hash {h} not readable in vfs-cloned repo"
                );
            }
            Ok(())
        })
        .await
    }
}
