use serde::{Deserialize, Serialize};

use crate::explore::hash::{ContentHash, HasContentHash, HasLocationHash, HexHash, LocationHash};
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::paths::{AbsolutePath, Name};

/// Lazy (i.e. stored-and-reloaded) view of a Merkle tree node. Each record
/// represents **one occurrence** of a file or directory at a specific
/// position in some tree. The LMDB key for the record is its
/// [`LocationHash`] (not stored inside — it's the key, known by the caller
/// that fetched it).
#[derive(Debug, Serialize, Deserialize)]
pub enum MerkleTreeL {
    Dir {
        /// Hash of this directory's content — `hash_of_hashes` over its
        /// children's content hashes. Deduplicable across occurrences.
        content_hash: ContentHash,
        name: Name,
        /// `None` means the parent is the commit root.
        /// This means that _this_ directory is at the root of the repository.
        parent: Option<LazyNode>,
        /// Child records, identified by their own location hashes.
        children: Vec<LazyNode>,
    },
    File {
        /// Hash of this file's bytes.
        content_hash: ContentHash,
        name: Name,
        /// This means that _this_ file is at the root of the repository.
        parent: Option<LazyNode>,
    },
}

pub trait HasName {
    fn name(&self) -> &Name;
}

impl MerkleTreeL {
    pub fn parent(&self) -> Option<&LazyNode> {
        match self {
            MerkleTreeL::Dir { parent, .. } => parent,
            MerkleTreeL::File { parent, .. } => parent,
        }
        .as_ref()
    }
}

impl HasContentHash for MerkleTreeL {
    #[inline]
    fn content_hash(&self) -> ContentHash {
        match self {
            MerkleTreeL::Dir { content_hash, .. } => *content_hash,
            MerkleTreeL::File { content_hash, .. } => *content_hash,
        }
    }
}

impl HasName for MerkleTreeL {
    #[inline(always)]
    fn name(&self) -> &Name {
        match self {
            MerkleTreeL::Dir { name, .. } => name,
            MerkleTreeL::File { name, .. } => name,
        }
    }
}

/// Intrinsic content of a tree node, stored in the dedup-friendly
/// `contents` table keyed by [`ContentHash`]. Unlike `MerkleTreeL`, this
/// has no name / parent / child-location info — just the content itself.
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeContent {
    /// Children's **content** hashes, in the order that `hash_of_hashes`
    /// was computed over.
    Dir { children: Vec<ContentHash> },
    /// Lightweight file metadata. File bytes live on disk; this is enough
    /// to describe the content at a glance.
    File { size: u64 },
}

/// Serializable handle pointing to a stored `MerkleTreeL` node, by its
/// [`LocationHash`]. Guaranteed-by-type to be a *location*, not a content.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LazyNode(LocationHash);

impl LazyNode {
    #[inline(always)]
    pub(crate) fn new(location: LocationHash) -> Self {
        Self(location)
    }
}

impl HasLocationHash for LazyNode {
    #[inline(always)]
    fn location_hash(&self) -> LocationHash {
        self.0
    }
}

/// Load a file's bytes from disk, given its storage handle in `db`.
///
/// Walks the parent chain of `file_location` via `db.path(...)` to
/// reconstruct the file's repo-relative path, then reads the file.
pub async fn load_file_bytes<DB: MerkleReader>(
    db: &DB,
    file_location: LocationHash,
) -> Result<Vec<u8>, LoadError<DB>> {
    let Some(rel_path) = db.path(file_location).map_err(LoadError::Store)? else {
        return Err(LoadError::Path(HexHash::from(file_location)));
    };
    let path = AbsolutePath::from(db.repository(), &rel_path);
    tokio::fs::read(path.as_path())
        .await
        .map_err(LoadError::Read)
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError<DB: MerkleReader> {
    #[error("Error from Merkle tree data store: {0}")]
    Store(DB::Error),

    #[error("Cannot determine relative path for location: {0}")]
    Path(HexHash),

    #[error("{0}")]
    Read(std::io::Error),
}

/// A committed root (a single repository version).
///
/// The commit's identity is its [`ContentHash`] (derived from the children's
/// content hashes via `hash_of_hashes`). Children are referenced by their
/// [`LocationHash`] so each direct child of the commit has a well-defined
/// position even if its content hash is shared with a node elsewhere.
#[derive(Debug, Serialize, Deserialize)]
pub struct Root {
    repository: AbsolutePath,
    hash: ContentHash,
    children: Vec<LazyNode>,
    parent: Option<ContentHash>,
}

impl Root {
    /// Compute the commit hash from the children's content hashes, then pair
    /// with a pre-built list of children locations. Caller computes the
    /// locations with access to the commit hash they drive.
    pub fn new(
        repository: AbsolutePath,
        child_content_hashes: &[ContentHash],
        children_locations: Vec<LazyNode>,
        parent: Option<ContentHash>,
    ) -> Self {
        Self {
            repository,
            hash: ContentHash::hash_of_hashes(child_content_hashes.iter()),
            children: children_locations,
            parent,
        }
    }

    pub fn children(&self) -> impl Iterator<Item = &LazyNode> {
        self.children.iter()
    }

    #[allow(dead_code)] // Public accessor intended for future callers; not consumed yet.
    pub fn repository(&self) -> &AbsolutePath {
        &self.repository
    }

    #[allow(dead_code)] // Parent-commit accessor intended for future callers; not consumed yet.
    pub fn parent(&self) -> Option<ContentHash> {
        self.parent
    }
}

impl HasContentHash for Root {
    #[inline(always)]
    fn content_hash(&self) -> ContentHash {
        self.hash
    }
}

pub struct UncomittedRoot {
    pub parent: Option<ContentHash>,
    pub repository: AbsolutePath,
    pub children: Vec<MerkleTreeB>,
}

/// In-memory tree ("before-committed") used during generation. Each node
/// carries only its content hash; location hashes are derived at write time.
#[derive(Debug)]
pub enum MerkleTreeB {
    Dir {
        content_hash: ContentHash,
        name: Name,
        children: Vec<Self>,
    },
    File {
        content_hash: ContentHash,
        name: Name,
    },
}

impl HasContentHash for MerkleTreeB {
    #[inline(always)]
    fn content_hash(&self) -> ContentHash {
        match self {
            MerkleTreeB::Dir { content_hash, .. } => *content_hash,
            MerkleTreeB::File { content_hash, .. } => *content_hash,
        }
    }
}

impl HasName for MerkleTreeB {
    #[inline(always)]
    fn name(&self) -> &Name {
        match self {
            MerkleTreeB::Dir { name, .. } => name,
            MerkleTreeB::File { name, .. } => name,
        }
    }
}
