use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::Path;

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use tar::Archive;
use tempfile::TempDir;

use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::db::merkle_node::merkle_node_db::{MerkleDbError, MerkleNodeDB, node_db_path};
use crate::model::LocalRepository;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_transport::{MerklePacker, MerkleUnpacker};
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

/// Pack the tar-gz wire format for a set of merkle hashes into `out`.
fn write_hashes_tar<W: Write>(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
    out: W,
) -> Result<(), MerkleDbError> {
    let enc = GzEncoder::new(out, Compression::fast());
    let mut tar = tar::Builder::new(enc);
    for hash in hashes {
        let dir_prefix = hash.to_hex_hash().node_db_prefix();
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);
        let node_dir = node_db_path(repo, hash);
        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        }
    }
    tar.finish()?;
    tar.into_inner()?.finish()?;
    Ok(())
}

/// Pack the tar-gz wire format for every node in the store into `out`.
fn write_all_tar<W: Write>(repo: &LocalRepository, out: W) -> Result<(), MerkleDbError> {
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
/// Returns the set of hashes parsed from the tarball. Entries whose destination already
/// exists are skipped.
fn extract_tar_under<R: Read>(
    reader: R,
    oxen_hidden: &Path,
) -> Result<HashSet<MerkleHash>, MerkleDbError> {
    let mut hashes: HashSet<MerkleHash> = HashSet::new();
    let decoder = GzDecoder::new(reader);
    let mut archive = Archive::new(decoder);
    let entries = archive.entries().map_err(MerkleDbError::CannotReadMerkle)?;

    let tree_nodes_prefix = Path::new(TREE_DIR).join(NODES_DIR);

    for entry in entries {
        let Ok(mut file) = entry else {
            log::error!("Could not unpack file in merkle tar archive");
            continue;
        };
        let path = file.path()?.into_owned();
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
        if dst_path.exists() {
            log::debug!("Node already exists at {dst_path:?}");
            continue;
        }
        file.unpack(&dst_path)?;

        // The hash is the concatenation of the last two path components. Entries ending
        // in "node" / "children" are the leaf files; entries that end at the {suffix}
        // directory contribute the hash value. A full-tree tarball (from `pack_all`)
        // also contains intermediate directory entries like `tree/nodes` or
        // `tree/nodes/{prefix}` — those don't form a valid 32-hex-char hash when their
        // last two components are concatenated, so we gate parsing on that.
        if !dst_path.ends_with("node") && !dst_path.ends_with("children") {
            let id: String = dst_path
                .components()
                .rev()
                .take(2)
                .filter_map(|c| c.as_os_str().to_str())
                .collect::<Vec<&str>>()
                .into_iter()
                .rev()
                .collect();
            if id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()) {
                let hash = u128::from_str_radix(&id, 16)?;
                hashes.insert(MerkleHash::new(hash));
            }
        }
    }
    Ok(hashes)
}

/// Merkle packer implementation for the [`FileBackend`].
impl<'repo> MerklePacker for FileBackend<'repo> {
    type Error = MerkleDbError;

    /// Pack the given node hashes into `out` as a tar-gz stream.
    ///
    /// Emits one subtree per hash under `{TREE_DIR}/{NODES_DIR}/{prefix}/{suffix}/...`.
    /// Hashes absent from the store are silently skipped.
    fn pack_nodes<W: Write>(
        &self,
        hashes: &HashSet<MerkleHash>,
        out: W,
    ) -> Result<(), MerkleDbError> {
        write_hashes_tar(self.repo, hashes, out)
    }

    /// Pack every node the store holds into `out` as a tar-gz stream.
    fn pack_all<W: Write>(&self, out: W) -> Result<(), MerkleDbError> {
        write_all_tar(self.repo, out)
    }
}

/// Merkle unpacker implementation for the [`FileBackend`].
impl<'repo> MerkleUnpacker for FileBackend<'repo> {
    type Error = MerkleDbError;

    /// Unpack a tar-gz wire stream into the store.
    ///
    /// If the repository sits on a virtual filesystem ([`LocalRepository::is_vfs`] is true),
    /// unpack into a tempdir first and `copy_dir_all` the result through the VFS. Some
    /// VFS implementations don't tolerate tar's streaming many-small-files pattern, so the
    /// staging hop is needed for correctness. Otherwise, unpack directly to `.oxen/`.
    fn unpack<R: Read>(&self, reader: R) -> Result<HashSet<MerkleHash>, MerkleDbError> {
        let oxen_hidden = self.repo.path.join(OXEN_HIDDEN_DIR);
        if self.repo.is_vfs() {
            let tmp = TempDir::new()?;
            let hashes = extract_tar_under(reader, tmp.path())?;
            util::fs::copy_dir_all(tmp.path(), &oxen_hidden)
                .map_err(|e| MerkleDbError::FsTransport(Box::new(e)))?;
            Ok(hashes)
        } else {
            extract_tar_under(reader, &oxen_hidden)
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
            // log::debug!("Compressing node from dir {:?}", node_dir);
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

        // log::debug!("Compressing node {} from dir {:?}", hash, node_dir);
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
            // log::debug!("create_node writing {:?}", dst_path);
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
                .unpack(&packed[..])
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
                    .pack_nodes(&hashes, &mut via_trait)
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
                    .pack_nodes(&hashes, &mut via_trait)
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
                    .pack_nodes(&hashes, &mut via_trait)
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
            let new_hashes = repo_new
                .merkle_store()
                .unpack(&bytes[..])
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
}
