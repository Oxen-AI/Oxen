use std::collections::HashSet;
use std::io::{Read, Write};

use crate::error::IntoOxenError;
use crate::model::MerkleHash;

/// Produce transport-ready bytes from some subset (or all) of the backend's Merkle tree nodes.
///
/// Writes the canonical tar-gz wire format directly into the caller-provided sink. No buffer
/// is materialized inside the trait, so memory use is O(compressor window). Callers can plug
/// in HTTP response bodies, pipes, files, or in-memory `Vec<u8>` sinks as the writer.
pub trait MerklePacker: Send + Sync {
    /// Native backend error. Must be convertible into an [`OxenError`] via [`IntoOxenError`]
    /// so callers returning [`OxenError`] can use `?` directly.
    ///
    /// [`OxenError`]: crate::error::OxenError
    type Error: std::error::Error + IntoOxenError;

    /// Pack the given node `hashes` into `out` as a tar-gz stream.
    ///
    /// Matches [`compress_nodes`] semantics: hashes not present in the store are silently
    /// skipped, and an empty `hashes` produces a valid but empty tarball.
    ///
    /// [`compress_nodes`]: crate::repositories::tree::compress_nodes
    fn pack_nodes<W: Write>(&self, hashes: &HashSet<MerkleHash>, out: W)
    -> Result<(), Self::Error>;

    /// Pack every node the backend currently holds into `out` as a tar-gz stream.
    ///
    /// Matches [`compress_tree`] semantics.
    ///
    /// [`compress_tree`]: crate::repositories::tree::compress_tree
    fn pack_all<W: Write>(&self, out: W) -> Result<(), Self::Error>;
}

/// Consume transport bytes and install the nodes they contain into the backend.
///
/// Reads the canonical tar-gz wire format incrementally from `reader`. Nothing buffers
/// the full payload inside the trait. Async callers bridge a `Stream<Item = Bytes>` to a
/// sync [`Read`] via [`tokio_util::io::SyncIoBridge`] inside a [`tokio::task::spawn_blocking`].
pub trait MerkleUnpacker: Send + Sync {
    /// Native backend error. Must be convertible into an [`OxenError`] via [`IntoOxenError`]
    /// so callers returning [`OxenError`] can use `?` directly.
    ///
    /// [`OxenError`]: crate::error::OxenError
    type Error: std::error::Error + IntoOxenError;

    /// Unpack the tar-gz stream from `reader` into the store.
    ///
    /// Matches [`unpack_nodes`] semantics: nodes already present are skipped rather than
    /// overwritten. Returns the set of hashes parsed from the tarball (not necessarily
    /// only those newly installed).
    ///
    /// [`unpack_nodes`]: crate::repositories::tree::unpack_nodes
    fn unpack<R: Read>(&self, reader: R) -> Result<HashSet<MerkleHash>, Self::Error>;
}

/// Marker super-trait: a backend that can both pack and unpack with a single error type.
///
/// The blanket impl below makes any type that implements [`MerklePacker`] and
/// [`MerkleUnpacker`] with matching error types automatically a [`MerkleTransport`].
pub trait MerkleTransport:
    MerklePacker + MerkleUnpacker<Error = <Self as MerklePacker>::Error>
{
}

impl<T> MerkleTransport for T where
    T: MerklePacker + MerkleUnpacker<Error = <T as MerklePacker>::Error>
{
}
