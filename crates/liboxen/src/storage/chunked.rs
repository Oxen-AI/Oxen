//! Block-level deduplication.
//! Files are split into chunks of around 64 KiB, and identical chunks are stored once.
//!
//! Where a chunk ends is decided by the bytes themselves, never by a fixed offset.
//! With fixed offsets, one inserted byte moves every boundary after it,
//! and the edited file shares no chunk with the version before it.

pub mod chunker;
pub mod error;
pub mod registry;

pub use chunker::{Chunker, RawChunk};
pub use error::ChunkedError;
pub use registry::{ChunkerId, chunker};

// FastCDC's min_size, avg_size, and max_size.
// These read like tuning knobs, but ChunkerId::GENERIC_FASTCDC_V1 names this triple,
// and changing any of them belongs under a new ID.
pub const MIN_CHUNK_SIZE: usize = 8 * 1024;
pub const AVG_CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_CHUNK_SIZE: usize = 128 * 1024;
