//! Content-defined chunking: the [`Chunker`] trait and the default FastCDC
//! implementation.
//!
//! Chunkers are sync and streaming with bounded memory (at most the current maximum
//! chunk plus a window); the `spawn_blocking` bridge to the async world lives at the
//! operation boundary, per `docs/async_policy.md`.

use std::io::Read;

use fastcdc::v2020::StreamCDC;

use super::error::ChunkedError;
use super::registry::ChunkerId;
use super::{AVG_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};

/// One content-defined slice of a file: its offset in the file and its raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawChunk {
    /// Byte offset of this chunk in the original file.
    pub offset: u64,
    /// The raw (uncompressed) chunk bytes.
    pub data: Vec<u8>,
}

/// Splits a stream into content-defined chunks.
///
/// The chunks yielded for a given input are part of the format contract: same bytes +
/// same [`ChunkerId`] ⇒ same `(offset, len)` boundaries, always. Implementations must
/// stream with bounded memory, and boundaries must depend only on content so
/// insertions and deletions only perturb neighboring chunks.
pub trait Chunker: Send + Sync {
    /// The registered ID recorded in manifests produced with this chunker.
    fn id(&self) -> ChunkerId;

    /// Yield the chunks of `reader` in file order. Concatenating the yielded chunk
    /// bytes reconstructs the input exactly.
    fn chunk<'r>(
        &self,
        reader: Box<dyn Read + Send + 'r>,
    ) -> Box<dyn Iterator<Item = Result<RawChunk, ChunkedError>> + Send + 'r>;
}

/// FastCDC v2020 (normalized chunking) at min 8 KiB / target 64 KiB / max 128 KiB —
/// the boundary function frozen as [`ChunkerId::GENERIC_FASTCDC_V1`].
///
/// Golden fixtures in this module's tests pin the exact boundaries; if a `fastcdc`
/// crate upgrade shifts them, those tests fail and the change must ship as a new
/// `ChunkerId` instead.
pub struct FastCdc2020Chunker;

impl Chunker for FastCdc2020Chunker {
    fn id(&self) -> ChunkerId {
        ChunkerId::GENERIC_FASTCDC_V1
    }

    fn chunk<'r>(
        &self,
        reader: Box<dyn Read + Send + 'r>,
    ) -> Box<dyn Iterator<Item = Result<RawChunk, ChunkedError>> + Send + 'r> {
        let cdc = StreamCDC::new(reader, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
        Box::new(cdc.map(|result| match result {
            Ok(chunk) => Ok(RawChunk {
                offset: chunk.offset,
                data: chunk.data,
            }),
            Err(fastcdc::v2020::Error::IoError(err)) => Err(ChunkedError::ChunkRead(err)),
            Err(other) => Err(ChunkedError::ChunkRead(std::io::Error::other(other))),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::hasher::hash_buffer_128bit;

    /// Deterministic pseudo-random bytes (xorshift64*) so golden fixtures are
    /// reproducible without checking a multi-MB file into the repo.
    fn deterministic_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed;
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let word = state.wrapping_mul(0x2545F4914F6CDD1D);
            out.extend_from_slice(&word.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    fn chunk_all(data: &[u8]) -> Vec<RawChunk> {
        FastCdc2020Chunker
            .chunk(Box::new(data))
            .collect::<Result<Vec<_>, _>>()
            .expect("chunking in-memory data cannot fail")
    }

    /// Chunks must tile the input exactly: contiguous offsets, lengths within
    /// [min, max] (except the final chunk, which may be short), bytes identical.
    fn assert_tiles_input(chunks: &[RawChunk], data: &[u8]) {
        let mut expected_offset = 0u64;
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.offset, expected_offset, "chunk {i} offset");
            assert!(!chunk.data.is_empty(), "chunk {i} empty");
            assert!(
                chunk.data.len() <= MAX_CHUNK_SIZE as usize,
                "chunk {i} exceeds max size"
            );
            if i + 1 < chunks.len() {
                assert!(
                    chunk.data.len() >= MIN_CHUNK_SIZE as usize,
                    "non-final chunk {i} under min size"
                );
            }
            let start = chunk.offset as usize;
            assert_eq!(
                &data[start..start + chunk.data.len()],
                &chunk.data[..],
                "chunk {i} bytes"
            );
            expected_offset += chunk.data.len() as u64;
        }
        assert_eq!(
            expected_offset,
            data.len() as u64,
            "chunks must cover input"
        );
    }

    /// Golden boundary fixture: pins the exact FastCDC boundary function behind
    /// `ChunkerId::GENERIC_FASTCDC_V1`. If this test fails after a `fastcdc` crate
    /// upgrade, the boundaries moved — that change must ship as a NEW chunker ID,
    /// never an in-place upgrade (it would silently degrade dedup fleet-wide).
    #[test]
    fn golden_fastcdc_boundaries() {
        let data = deterministic_bytes(0x0DEDA7A5EED, 1024 * 1024);
        let chunks = chunk_all(&data);
        assert_tiles_input(&chunks, &data);

        let boundaries: Vec<(u64, usize)> =
            chunks.iter().map(|c| (c.offset, c.data.len())).collect();
        let expected: Vec<(u64, usize)> = vec![
            // GOLDEN: (offset, len) pairs for seed 0x0DEDA7A5EED, 1 MiB.
            (0, 75164),
            (75164, 22447),
            (97611, 74856),
            (172467, 59977),
            (232444, 131072),
            (363516, 47514),
            (411030, 26207),
            (437237, 74500),
            (511737, 71410),
            (583147, 76703),
            (659850, 45931),
            (705781, 131072),
            (836853, 74147),
            (911000, 83126),
            (994126, 54450),
        ];
        assert_eq!(boundaries, expected);
    }

    /// All-zero input has no content-defined cut points, so every chunk is forced
    /// out at exactly the maximum size.
    #[test]
    fn zeros_chunk_at_max_size() {
        let len = 3 * MAX_CHUNK_SIZE as usize + 1000;
        let data = vec![0u8; len];
        let chunks = chunk_all(&data);
        assert_tiles_input(&chunks, &data);
        let lens: Vec<usize> = chunks.iter().map(|c| c.data.len()).collect();
        assert_eq!(
            lens,
            vec![
                MAX_CHUNK_SIZE as usize,
                MAX_CHUNK_SIZE as usize,
                MAX_CHUNK_SIZE as usize,
                1000
            ]
        );
    }

    /// Inputs at and below the minimum chunk size yield a single chunk; empty input
    /// yields no chunks.
    #[test]
    fn small_inputs() {
        assert!(chunk_all(&[]).is_empty());

        let tiny = deterministic_bytes(7, 100);
        let chunks = chunk_all(&tiny);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].data, tiny);
    }

    /// The dedup property this feature exists for: inserting bytes mid-file only
    /// perturbs chunks near the edit; the chunks before and well after the edit
    /// keep their hashes.
    #[test]
    fn middle_insertion_preserves_most_chunks() {
        let original = deterministic_bytes(42, 2 * 1024 * 1024);
        let mut edited = original.clone();
        let insert_at = original.len() / 2;
        edited.splice(
            insert_at..insert_at,
            b"inserted row,1,2,3\n".iter().copied(),
        );

        let original_hashes: std::collections::HashSet<u128> = chunk_all(&original)
            .iter()
            .map(|c| hash_buffer_128bit(&c.data))
            .collect();
        let edited_chunks = chunk_all(&edited);
        assert_tiles_input(&edited_chunks, &edited);

        let shared = edited_chunks
            .iter()
            .filter(|c| original_hashes.contains(&hash_buffer_128bit(&c.data)))
            .count();
        let total = edited_chunks.len();
        assert!(
            shared * 10 >= total * 8,
            "expected ≥80% of chunks shared after a middle insertion, got {shared}/{total}"
        );
    }

    /// IO errors from the reader surface as `ChunkRead`, not a panic.
    #[test]
    fn read_errors_surface() {
        struct FailingReader;
        impl Read for FailingReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("boom"))
            }
        }
        let mut iter = FastCdc2020Chunker.chunk(Box::new(FailingReader));
        assert!(matches!(iter.next(), Some(Err(ChunkedError::ChunkRead(_)))));
    }
}
