use crate::explore::hash::{ContentHash, HasLocationHash, LocationHash};
use crate::explore::lazy_merkle::{HasName, MerkleTreeL, NodeContent, Root};
use crate::explore::paths::{AbsolutePath, RelativePath};

/// Trait for reading Merkle tree data from a durable store.
///
/// Hash parameters are typed — a `LocationHash` goes to `node` / `path` /
/// `location_exists`, a `ContentHash` goes to `content` / `commit` /
/// `content_exists` / `commit_exists`. Mixing them is a compile error.
pub trait MerkleReader: Sized {
    type Error: std::error::Error;

    /// Whether a `MerkleTreeL` record exists at this location.
    fn location_exists(&self, location: LocationHash) -> Result<bool, Self::Error>;

    /// Whether a `NodeContent` record exists for this content.
    fn content_exists(&self, content: ContentHash) -> Result<bool, Self::Error>;

    /// Whether a `Root` record exists for this commit hash.
    #[allow(dead_code)] // Part of the typed-exists trio; provided for API completeness.
    fn commit_exists(&self, commit: ContentHash) -> Result<bool, Self::Error>;

    /// Fetch the per-occurrence node record at a given location.
    fn node(&self, location: LocationHash) -> Result<Option<MerkleTreeL>, Self::Error>;

    /// Fetch the intrinsic content record for a given content hash.
    fn content(&self, content: ContentHash) -> Result<Option<NodeContent>, Self::Error>;

    /// Fetch a commit (`Root`) record by its content hash.
    fn commit(&self, commit: ContentHash) -> Result<Option<Root>, Self::Error>;

    /// The repository root for which this store holds the Merkle tree.
    fn repository(&self) -> &AbsolutePath;

    /// The repository-relative path to the node at `location`, reconstructed
    /// by walking the node's parent chain until a node with `parent = None`
    /// is reached (a commit's root-level child). Returns `None` when the
    /// starting `location` isn't present in the store.
    fn path(&self, location: LocationHash) -> Result<Option<RelativePath>, Self::Error> {
        let mut reverse_path = Vec::new();
        let mut current = location;
        loop {
            let Some(node) = self.node(current)? else {
                return Ok(None);
            };
            reverse_path.push(node.name().clone());
            match node.parent() {
                Some(parent) => current = parent.location_hash(),
                None => break,
            }
        }
        reverse_path.reverse();
        if reverse_path.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RelativePath::from_parts(reverse_path)))
        }
    }
}
