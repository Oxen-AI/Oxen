/// The [`LmdbBackend`] struct.
mod lmdb_backend;
/// The [`MerklePacker`] implementation.
mod pack;
/// The [`MerkleReader`] implementation.
mod reader;
/// The [`MerkleUnpacker`] implementation.
mod unpack;
/// The structures stored as values.
mod value_structs;
/// The [`MerkleWriter`] implementation.
mod writer;

/// A hash of a file's contents & its name. Constructed from a `MerkleHash` and a filename exactly.
mod hash_content_name;

/// The cache of open `LMDB` environments in the process.
pub(crate) mod cache;

#[cfg(test)]
use std::path::{Path, PathBuf};

pub(in crate::core::db::merkle_node::lmdb) use lmdb_backend::DEFAULT_LMDB_MMAP_SIZE;
pub use lmdb_backend::LmdbBackend;
pub(crate) use lmdb_backend::{lmdb_backend_options, lmdb_dir_location};

use thiserror::Error;

use crate::{
    core::db::merkle_node::lmdb::hash_content_name::HexHashCN,
    error::OxenError,
    model::{
        TMerkleTreeNode,
        merkle_tree::{merkle_hash::HexHash, node_type::InvalidMerkleTreeNodeType},
    },
};
use bytesize::ByteSize;

/// Errors that the LMDB backend's operations can encounter.
///
/// Major categories:
///     - deserializing values from LMDB incorrectly (truncated, bad magic,
///       unsupported version, malformed tail)
///     - underlying LMDB library errors (heed)
///     - violations on the integrity of the specific LMDB tables
#[derive(Debug, Error)]
pub enum LmdbError {
    // ── LmdbNode value (merkle_tree_nodes) decode errors ─────────────────────
    #[error("[LmdbNode] header truncated: only {len} bytes (need at least 12)")]
    NodeHeaderTruncated { len: usize },

    #[error("[LmdbNode] bad magic: got {actual:?}, expected b\"OXNV\"")]
    NodeBadMagic { actual: [u8; 4] },

    #[error("[LmdbNode] unsupported on-disk version: {0}")]
    NodeUnsupportedVersion(u8),

    #[error("[LmdbNode] {0}")]
    InvalidMerkleTreeNodeType(#[from] InvalidMerkleTreeNodeType),

    // ── LmdbLink value (merkle_links) decode errors ──────────────────────────
    #[error("[LmdbLink] header truncated: only {len} bytes (need at least 36)")]
    LinkHeaderTruncated { len: usize },

    #[error("[LmdbLink] bad magic: got {actual:?}, expected b\"OXLN\"")]
    LinkBadMagic { actual: [u8; 4] },

    #[error("[LmdbLink] unsupported on-disk version: {0}")]
    LinkUnsupportedVersion(u8),

    #[error("[LmdbLink] invalid has_parent flag: expected 0 or 1, got {0}")]
    InvalidIsParent(u8),

    #[error("[LmdbLink] children tail length {tail_len} is not a multiple of 16 bytes")]
    ChildrenTailMisaligned { tail_len: usize },

    #[error("[LmdbLink] header claims {claimed} children but tail has {actual}")]
    ChildrenCountMismatch { claimed: usize, actual: usize },

    // ── LmdbDupes value (merkle_node_dupes) decode errors ────────────────────
    #[error("[LmdbDupes] header truncated: only {len} bytes (need at least 16)")]
    DupesHeaderTruncated { len: usize },

    #[error("[LmdbDupes] bad magic: got {actual:?}, expected b\"OXDP\"")]
    DupesBadMagic { actual: [u8; 4] },

    #[error("[LmdbDupes] unsupported on-disk version: {0}")]
    DupesUnsupportedVersion(u8),

    #[error("[LmdbDupes] dupes tail length {tail_len} is not a multiple of 16 bytes")]
    DupesTailMisaligned { tail_len: usize },

    #[error("[LmdbDupes] header claims {claimed} dupes but tail has {actual}")]
    DupesCountMismatch { claimed: usize, actual: usize },

    // ── LMDB / heed transport ────────────────────────────────────────────────
    #[error("Error retrieving: {0}")]
    Retrieve(heed::Error),

    #[error("Error accessing LMDB Merkle store: {0}")]
    Access(heed::Error),

    #[error("Error writing LMDB Merkle store: {0}")]
    Write(heed::Error),

    // ── Cross-table integrity ────────────────────────────────────────────────
    #[error("Missing node, have link for content-name (hex) hash: {0}")]
    IntegrityNoNode(HexHashCN),

    #[error("Missing link, have node for (hex) hash: {0}")]
    IntegrityNoLink(HexHash),

    #[error(
        "Stored a child for (hex) hash of content & name ({0}) but node for hash does not exist."
    )]
    IntegrityNoHash(HexHashCN),

    #[error("Missing a content name hash mappiung for content hash: {0}")]
    IntegrityNoDupe(HexHash),

    // ── Initialization Errors ────────────────────────────────────────────────
    #[error(
        "Cannot create LMDB mmap file of size {0}: it is larger than this system's supported memory."
    )]
    InitMmap(ByteSize),

    #[error("Cannot create an absolute path for the LMDB mmap file: {0}")]
    InitAbs(Box<OxenError>), // TODO: update to FsError when that refactoring PR lands

    #[error("Cannot create directory for LMDB's memory mapped file: {0}")]
    InitDir(Box<OxenError>), // TODO: update to FsError when that refactoring PR lands
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
    };

    use bytesize::ByteSize;
    use time::OffsetDateTime;

    use crate::{
        core::db::merkle_node::{
            LmdbBackend,
            lmdb::{
                hash_content_name::{Filename, HashCN},
                lmdb_backend_options, lmdb_dir_location,
            },
        },
        error::OxenError,
        model::{
            EntryDataType, LocalRepository, MerkleHash, MerkleTreeNodeType, TMerkleTreeNode,
            merkle_tree::{
                merkle_writer::MerkleWriter,
                node::{
                    CommitNode, DirNode, FileChunkNode, FileNode, VNode,
                    commit_node::CommitNodeOpts, dir_node::DirNodeOpts, file_node::FileNodeOpts,
                    vnode::VNodeOpts,
                },
            },
        },
    };

    /// Open the backend keyed by a `repo_root` — used to test persistence across env opens.
    ///
    /// The on-disk env lives under the OS temp dir, not under `repo_root` itself; see
    /// [`lmdb_test_root`] for why. heed (without `NO_SUB_DIR`) treats the env path as a
    /// directory it writes `data.mdb` + `lock.mdb` into, so the leaf must exist, and we
    /// create it here.
    pub(in crate::core::db::merkle_node::lmdb) fn open_lmdb_at(repo_root: &Path) -> LmdbBackend {
        let test_root =
            test_lmdb_repo_root(repo_root).expect("could not create test LMDB location");
        // 10 MiB is plenty for the test workloads and lets dozens of
        // independent test envs coexist without exhausting virtual memory.
        let opts = lmdb_backend_options(ByteSize::mib(10))
            .expect("could not initialize LMDB environment options");
        LmdbBackend::new(test_root, opts).expect("open lmdb backend")
    }

    /// Uses and returns [`lmdb_test_root`], ensuring that the LMDB dir exists under .oxen/.
    pub(crate) fn test_lmdb_repo_root(repo_root: &Path) -> std::io::Result<PathBuf> {
        let test_root = lmdb_test_root(repo_root);
        let env_dir = lmdb_dir_location(&test_root);
        std::fs::create_dir_all(&env_dir)?;
        Ok(test_root)
    }

    /// Map a test repo path to a per-repo directory under the OS temp dir.
    ///
    /// The Windows CI runner mounts an ImDisk RAMDisk at `R:\test` and points
    /// `OXEN_TEST_RUN_DIR` there (see `.github/workflows/ci_test.yml`). ImDisk is a
    /// Win32-level emulation that does not fully implement the NT-level memory-section
    /// APIs LMDB depends on: `NtCreateSection` against a file on the ImDisk volume
    /// returns `STATUS_INVALID_DEVICE_REQUEST`, which `mdb_nt2win32` converts to
    /// `ERROR_INVALID_FUNCTION` (Win32 code 1). It surfaces here as
    /// `Lmdb(Access(Io(Os { code: 1, .. })))` and is reported as "Incorrect function.".
    /// `LmdbBackend` already documents `DO NOT USE ON A VIRTUAL FILE SYSTEM`; an ImDisk
    /// volume is exactly that.
    ///
    /// Routing the env to the OS temp dir keeps it on the host's real filesystem
    /// (NTFS on Windows runners), where the NT memory-section APIs work normally.
    /// The mapping is stable per repo path so that callers that re-open with the
    /// same `repo_root` (e.g. `test_data_persists_across_env_reopen`) hit the same
    /// env on each open. The repo's UUID-named leaf keeps env paths unique across
    /// concurrent tests.
    fn lmdb_test_root(repo_root: &Path) -> PathBuf {
        let leaf = repo_root
            .file_name()
            .expect("test repo_root has a leaf component");
        std::env::temp_dir().join("oxen-lmdb-tests").join(leaf)
    }

    /// Drive a test against a fresh [`LocalRepository`] and an [`LmdbBackend`]
    /// rooted at the same path. Mirrors the writer.rs `with_test_backend` helper.
    pub(in crate::core::db::merkle_node::lmdb) fn with_test_backend<F>(
        test_fn: F,
    ) -> Result<(), OxenError>
    where
        F: FnOnce(&LocalRepository, &LmdbBackend) -> Result<(), OxenError> + std::panic::UnwindSafe,
    {
        // These tests exercise the `LmdbBackend` directly via `open_lmdb_at` (a separate
        // env under the OS temp dir), so the repo's own Merkle store kind is irrelevant —
        // use `File` for the cheap, default setup.
        crate::test::run_empty_local_repo_test(
            crate::config::repository_config::MerkleStoreKind::File,
            |repo| {
                let backend = open_lmdb_at(&repo.path);
                test_fn(&repo, &backend)
            },
        )
    }

    /// Helper: parse a hex-char string into a [`MerkleHash`]. Uses the `FromStr` impl.
    pub(in crate::core::db::merkle_node::lmdb) fn h(hex: &str) -> MerkleHash {
        hex.parse().expect("valid hex hash")
    }

    /// Helper: make a commit node.
    pub(in crate::core::db::merkle_node::lmdb) fn commit_with_hash(
        repo: &LocalRepository,
        hash: MerkleHash,
    ) -> CommitNode {
        CommitNode::new(
            repo,
            CommitNodeOpts {
                hash,
                parent_ids: vec![],
                email: String::new(),
                author: String::new(),
                message: String::new(),
                timestamp: OffsetDateTime::UNIX_EPOCH,
            },
        )
        .expect("CommitNode::new")
    }

    /// Helper: make a directory node.
    pub(in crate::core::db::merkle_node::lmdb) fn dir_with_hash(
        repo: &LocalRepository,
        hash: MerkleHash,
    ) -> DirNode {
        DirNode::new(
            repo,
            DirNodeOpts {
                name: String::new(),
                hash,
                num_entries: 0,
                num_bytes: 0,
                last_commit_id: MerkleHash::new(0),
                last_modified_seconds: 0,
                last_modified_nanoseconds: 0,
                data_type_counts: HashMap::new(),
                data_type_sizes: HashMap::new(),
            },
        )
        .expect("DirNode::new")
    }

    /// Helper: make a virtual directory node.
    pub(in crate::core::db::merkle_node::lmdb) fn vnode_with_hash(
        repo: &LocalRepository,
        hash: MerkleHash,
    ) -> VNode {
        VNode::new(
            repo,
            VNodeOpts {
                hash,
                num_entries: 0,
            },
        )
        .expect("VNode::new")
    }

    /// Helper: make a file node.
    pub(in crate::core::db::merkle_node::lmdb) fn file_node_with_hash(
        repo: &LocalRepository,
        hash: MerkleHash,
    ) -> FileNode {
        FileNode::new(
            repo,
            FileNodeOpts {
                name: String::new(),
                hash,
                combined_hash: MerkleHash::new(0),
                metadata_hash: None,
                num_bytes: 0,
                last_modified_seconds: 0,
                last_modified_nanoseconds: 0,
                data_type: EntryDataType::Binary,
                metadata: None,
                mime_type: String::new(),
                extension: String::new(),
            },
        )
        .expect("FileNode::new")
    }

    /// Helper: make a file chunk node.
    pub(in crate::core::db::merkle_node::lmdb) fn file_chunk_node_with_hash(
        hash: MerkleHash,
    ) -> FileChunkNode {
        FileChunkNode {
            data: vec![],
            node_type: MerkleTreeNodeType::FileChunk,
            hash,
        }
    }

    /// Helper: write a single node into the backend with the given parent_id. Does not write children.
    pub(in crate::core::db::merkle_node::lmdb) fn write_one(
        backend: &LmdbBackend,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<HashCN, OxenError> {
        let session = backend.begin()?;
        let ns = session.create_node(node, parent_id)?;
        ns.finish()?;
        session.finish()?;
        Ok(hash_cn_from(node))
    }
}
