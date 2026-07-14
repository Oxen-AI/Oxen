//! Block-level deduplication: content-defined chunking, per-chunk compression,
//! and block-packed storage for large version files.
//!
//! Design reference: `docs/block_level_dedup_plan.md`. The vocabulary is fixed there:
//! a **chunk** is a content-defined slice of a file (~64 KiB target, the unit of
//! deduplication); a **block** is an immutable content-addressed pack of chunk payloads
//! (≤64 MiB, the unit of storage and transfer); a **manifest** is the ordered
//! file→chunk mapping for one file version (pure content — it never records block
//! placement, which lives only in the store-local chunk index).
//!
//! This module is the single place future chunkers, codecs, and transforms are added;
//! see `registry` for the extension contract.

pub mod block;
pub mod chunker;
pub mod compressor;
pub mod error;
pub mod manifest;
pub mod policy;
pub mod registry;

pub use block::{BlockChunk, BlockWriter, SealedBlock, parse_block_footer, verify_block};
pub use chunker::{Chunker, RawChunk};
pub use compressor::{Compressor, EncodedChunk, decode_chunk, encode_chunk};
pub use error::ChunkedError;
pub use manifest::{ChunkEntry, ChunkManifest};
pub use policy::{EncodePolicy, dedup_min_file_size, encode_policy, should_chunk};
pub use registry::{ChunkerId, CodecId, TransformId, chunker, codec};

/// Minimum content-defined chunk size (FastCDC `min_size`).
pub const MIN_CHUNK_SIZE: u32 = 8 * 1024;
/// Target (average) content-defined chunk size (FastCDC `avg_size`).
pub const AVG_CHUNK_SIZE: u32 = 64 * 1024;
/// Maximum content-defined chunk size (FastCDC `max_size`). Also the hard upper
/// bound the block parser accepts for a declared raw chunk length.
pub const MAX_CHUNK_SIZE: u32 = 128 * 1024;
/// A block is sealed once its complete encoded size reaches this many bytes.
pub const MAX_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
