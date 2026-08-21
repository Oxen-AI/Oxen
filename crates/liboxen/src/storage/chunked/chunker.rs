//! Chunking is synchronous and streams.
//! Memory stays bounded at one maximum-size chunk plus a hash window,
//! whatever the file size.
//!
//! Callers cross into async at the operation boundary with spawn_blocking,
//! per docs/async_policy.md.

use std::io::Read;

use fastcdc::v2020::StreamCDC;

use super::error::ChunkedError;
use super::registry::ChunkerId;
use super::{AVG_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};

/// One content-defined slice of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawChunk {
    /// Byte offset of this chunk in the original file.
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Splits a stream into content-defined chunks.
///
/// The boundaries an implementation produces are part of the on-disk format.
/// Identical bytes under the same chunker ID must yield identical offsets and lengths,
/// on every machine, forever.
/// An implementation streams with bounded memory,
/// and decides boundaries from the content alone.
pub trait Chunker: Send + Sync {
    /// The registered ID recorded in manifests produced with this chunker.
    fn id(&self) -> ChunkerId;

    /// Yield the chunks in file order.
    /// Concatenating the yielded bytes reconstructs the input exactly.
    fn chunk<'r>(
        &self,
        reader: Box<dyn Read + Send + 'r>,
    ) -> Box<dyn Iterator<Item = Result<RawChunk, ChunkedError>> + Send + 'r>;
}

/// FastCDC v2020 with normalized chunking, at the sizes this module declares.
/// This is the boundary function frozen as ChunkerId::GENERIC_FASTCDC_V1.
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

    /// Pseudo-random bytes from xorshift64*, identical on every run and platform.
    /// Generating the golden input here keeps a binary file out of the repo.
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

    /// Checks that the chunks tile the input exactly:
    /// contiguous offsets from zero, sizes inside the bounds except for the last chunk,
    /// and bytes matching the input.
    /// Every test here runs this.
    fn assert_tiles_input(chunks: &[RawChunk], data: &[u8]) {
        let mut expected_offset = 0u64;
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.offset, expected_offset, "chunk {i} offset");
            assert!(!chunk.data.is_empty(), "chunk {i} empty");
            assert!(
                chunk.data.len() <= MAX_CHUNK_SIZE,
                "chunk {i} exceeds max size"
            );
            if i + 1 < chunks.len() {
                assert!(
                    chunk.data.len() >= MIN_CHUNK_SIZE,
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

    /// Pins the exact boundary function behind ChunkerId::GENERIC_FASTCDC_V1.
    /// A failure after a fastcdc upgrade means the boundaries moved,
    /// and that change belongs under a new ID.
    /// Editing this list to match new output makes every stored chunk unmatchable,
    /// and nothing reports it.
    #[test]
    fn golden_fastcdc_boundaries() {
        let data = deterministic_bytes(0x0DEDA7A5EED, 1024 * 1024);
        let chunks = chunk_all(&data);
        assert_tiles_input(&chunks, &data);

        let boundaries: Vec<(u64, usize)> =
            chunks.iter().map(|c| (c.offset, c.data.len())).collect();
        let expected: Vec<(u64, usize)> = vec![
            // Recorded offset and length pairs for seed 0x0DEDA7A5EED over 1 MiB.
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

    /// All-zero input never matches either mask.
    /// The maximum size is then the only thing that can end a chunk.
    #[test]
    fn zeros_chunk_at_max_size() {
        let len = 3 * MAX_CHUNK_SIZE + 1000;
        let data = vec![0u8; len];
        let chunks = chunk_all(&data);
        assert_tiles_input(&chunks, &data);
        let lens: Vec<usize> = chunks.iter().map(|c| c.data.len()).collect();
        assert_eq!(
            lens,
            vec![MAX_CHUNK_SIZE, MAX_CHUNK_SIZE, MAX_CHUNK_SIZE, 1000]
        );
    }

    /// A file under the minimum size is one chunk, and an empty file is none.
    /// The same path produces the short final chunk at the end of every file.
    #[test]
    fn small_inputs() {
        assert!(chunk_all(&[]).is_empty());

        let tiny = deterministic_bytes(7, 100);
        let chunks = chunk_all(&tiny);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].data, tiny);
    }

    /// The property the feature rests on.
    /// An insertion changes the chunks around it,
    /// while everything before and far enough after keeps the same hashes.
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
            "expected at least 80% of chunks shared after a middle insertion, got {shared}/{total}"
        );
    }

    /// A failing reader yields an error from the iterator.
    /// Swallowing it would end iteration early,
    /// and the chunk list would look like a complete short file.
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
