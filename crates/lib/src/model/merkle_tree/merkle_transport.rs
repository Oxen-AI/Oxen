use std::collections::HashSet;
use std::io::{Read, Write};

use crate::error::OxenError;
use crate::model::MerkleHash;

/// Wire-format selector for [`MerklePacker::pack_nodes`].
///
/// Two on-the-wire tar-gz layouts have coexisted as long as the merkle transport has
/// existed. Each call site must pick the variant that matches the peer it's writing
/// to; the trait makes no claim that a single canonical format exists.
// **No `Default` impl on purpose.** Picking a wire format is a protocol decision and
// must be made explicitly at every call site. **No `#[non_exhaustive]` on purpose.**
// Adding a future variant should be a deliberate breaking change that surfaces at
// every match arm — compile errors are the forcing function.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PackOptions {
    /// Entries appear under `tree/nodes/{prefix}/{suffix}/...`. Compressed with
    /// [`flate2::Compression::fast`]. Used by all `repositories::tree::compress_*`
    /// helpers — the bytes any server download endpoint emits.
    ///
    /// [`flate2::Compression::fast`]: https://docs.rs/flate2/latest/flate2/struct.Compression.html#method.fast
    ServerCanonical,
    /// Entries appear under `{prefix}/{suffix}/...` with no `tree/nodes/` prefix.
    /// Compressed with [`flate2::Compression::default`]. Required by
    /// [`api::client::tree::create_nodes`] so older `oxen-server` deployments
    /// (which pre-pend `tree/nodes/` server-side at install time) install entries at
    /// the right paths.
    ///
    /// [`flate2::Compression::default`]: https://docs.rs/flate2/latest/flate2/struct.Compression.html#method.default
    /// [`api::client::tree::create_nodes`]: crate::api::client::tree::create_nodes
    LegacyClientPush,
}

/// Per-call extraction policy for [`MerkleUnpacker::unpack`].
///
/// **No `Default` impl on purpose.** The choice between overwriting and skipping is
/// path-dependent and must be made explicitly at every call site.
// **No `#[non_exhaustive]` on purpose** for the same reason as [`PackOptions`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum UnpackOptions {
    /// Overwrite files that already exist on disk. Matches `main`'s
    /// `util::fs::unpack_async_tar_archive` — the client download path's behaviour.
    Overwrite,
    /// Skip files that already exist on disk. Matches `main`'s
    /// `repositories::tree::unpack_nodes` — the server-side upload-consumer path.
    SkipExisting,
}

/// Produce transport-ready bytes from some subset (or all) of the backend's Merkle tree nodes.
///
/// Writes a tar-gz wire stream directly into the caller-provided sink. No buffer is
/// materialized inside the trait, so memory use is O(compressor window). Callers can
/// plug in HTTP response bodies, pipes, files, or in-memory `Vec<u8>` sinks as the writer.
///
/// dyn-compatible: callers can store this as `Box<dyn MerklePacker + '_>` or
/// `&dyn MerklePacker`. Methods take `&mut dyn Write` instead of generic `W: Write`
/// so the trait carries no per-call type parameters.
pub trait MerklePacker: Send + Sync {
    /// Pack the given node `hashes` into `out` as a tar-gz stream, in the layout
    /// selected by `opts`.
    ///
    /// Hashes not present in the store are silently skipped, and an empty `hashes`
    /// produces a valid but empty tarball. See [`PackOptions`] for the per-variant
    /// wire-format details.
    fn pack_nodes(
        &self,
        hashes: &HashSet<MerkleHash>,
        opts: PackOptions,
        out: &mut dyn Write,
    ) -> Result<(), OxenError>;

    /// Pack every node the backend currently holds into `out` as a tar-gz stream.
    ///
    /// Single-format: only the server-canonical layout has ever been emitted for a
    /// whole-tree pack on `main`. There is no legacy whole-tree variant, so this
    /// method does not accept [`PackOptions`].
    fn pack_all(&self, out: &mut dyn Write) -> Result<(), OxenError>;
}

/// Consume transport bytes and install the nodes they contain into the backend.
///
/// Reads the tar-gz wire format incrementally from `reader`. Nothing buffers the full
/// payload inside the trait. Async callers bridge a `Stream<Item = Bytes>` to a sync
/// [`Read`] via [`tokio_util::io::SyncIoBridge`] inside a [`tokio::task::spawn_blocking`].
///
/// dyn-compatible: callers can store this as `Box<dyn MerkleUnpacker + '_>` or
/// `&dyn MerkleUnpacker`. The reader is taken as `&mut dyn Read` for the same
/// reason as [`MerklePacker`]'s `&mut dyn Write` argument.
pub trait MerkleUnpacker: Send + Sync {
    /// Unpack the tar-gz stream from `reader` into the store, applying the existing-file
    /// policy in `opts`.
    ///
    /// Returns the set of hashes parsed from the tarball (not necessarily only those
    /// newly installed — entries skipped per [`UnpackOptions::SkipExisting`] still appear
    /// in the result, matching `main`'s `repositories::tree::unpack_nodes` behaviour).
    fn unpack(
        &self,
        reader: &mut dyn Read,
        opts: UnpackOptions,
    ) -> Result<HashSet<MerkleHash>, OxenError>;
}

/// Marker super-trait: a type that can both pack and unpack Merkle tree nodes for transport.
pub trait MerkleTransport: MerklePacker + MerkleUnpacker {}

/// This blanket impl makes any type that implements [`MerklePacker`] and
/// [`MerkleUnpacker`] automatically a [`MerkleTransport`]. The `?Sized` bound lets
/// the marker apply to `dyn MerkleTransport` itself, so the impl works for both
/// concrete backends and trait-object views over them.
impl<T: MerklePacker + MerkleUnpacker + ?Sized> MerkleTransport for T {}
