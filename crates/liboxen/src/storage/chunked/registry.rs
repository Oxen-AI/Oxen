//! Chunker IDs.
//!
//! An ID is a u8 naming one exact boundary function, and a number is never reused.
//! Editing what an ID means would make every chunk already stored unmatchable,
//! and nothing would report it.
//! Writes keep succeeding while storage quietly stops improving.
//! A change to where FastCDC cuts arrives as a new ID and leaves the old one alone.
//!
//! An ID is consulted only when something needs to chunk.
//! Reconstruction walks a manifest's chunk hashes and never re-chunks.
//! A manifest carrying an unknown ID therefore stays readable,
//! and manifest validation should not reject one.
//!
//! A new chunker needs golden boundary fixtures before it belongs here.

use serde::{Deserialize, Serialize};

use super::chunker::{Chunker, FastCdc2020Chunker};
use super::error::ChunkedError;

/// Names the exact boundary function that produced a manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChunkerId(u8);

impl ChunkerId {
    /// FastCDC v2020, normalized, min 8 KiB, target 64 KiB, max 128 KiB.
    ///
    /// A crate release that moved a cut by one byte would keep the same API,
    /// and it would break the format.
    /// The golden boundary test is what catches that,
    /// along with a change to the sizes above or to the chunking code.
    pub const GENERIC_FASTCDC_V1: ChunkerId = ChunkerId(1);

    // 0 stays permanently unassigned.
    // Zeroed or corrupt metadata reads as 0, and it has to fail the lookup below.

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

/// The only place an ID turns into an implementation,
/// which makes it the only place an unrecognized ID gets caught.
pub fn chunker(id: ChunkerId) -> Result<&'static dyn Chunker, ChunkedError> {
    static FASTCDC: FastCdc2020Chunker = FastCdc2020Chunker;
    match id {
        ChunkerId::GENERIC_FASTCDC_V1 => Ok(&FASTCDC),
        ChunkerId(other) => Err(ChunkedError::UnknownChunkerId(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unknown IDs are the case worth testing.
    /// The lookup ends in a catch-all arm,
    /// and an arm returning a default chunker would pass everything else here.
    #[test]
    fn registry_round_trips_ids() -> Result<(), ChunkedError> {
        assert_eq!(
            chunker(ChunkerId::GENERIC_FASTCDC_V1)?.id(),
            ChunkerId::GENERIC_FASTCDC_V1
        );
        assert!(matches!(
            chunker(ChunkerId(0)),
            Err(ChunkedError::UnknownChunkerId(0))
        ));
        assert!(matches!(
            chunker(ChunkerId(200)),
            Err(ChunkedError::UnknownChunkerId(200))
        ));
        Ok(())
    }

    /// The number is what a manifest records.
    /// Renaming the constant is harmless, and renumbering it is destructive.
    #[test]
    fn ids_are_stable() {
        assert_eq!(ChunkerId::GENERIC_FASTCDC_V1.as_u8(), 1);
    }
}
