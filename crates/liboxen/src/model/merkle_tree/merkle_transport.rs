/// Wire-format selector for packing Merkle tree nodes.
///
/// Two on-the-wire tar-gz layouts have coexisted as long as the merkle transport has
/// existed. Each call site must pick the variant that matches the peer it's writing
/// to; there is no claim that a single canonical format exists.
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

/// Per-call extraction policy for unpacking Merkle tree nodes.
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
