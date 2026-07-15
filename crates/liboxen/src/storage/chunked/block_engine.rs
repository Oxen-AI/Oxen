//! The local block engine: packs chunked file versions into blocks on disk, indexes
//! them, and reconstructs file bytes from manifests.
//!
//! This is the shared core the version-store backends drive; it is sync on purpose
//! (callers bridge with one `spawn_blocking` per operation, per
//! `docs/async_policy.md`). Layout on disk:
//!
//! ```text
//! {blocks_dir}/{hash[..2]}/{hash[2..]}/data   # immutable sealed blocks
//! {index_dir}/                                 # LMDB chunk index (derived state)
//! ```
//!
//! Block hashes are rendered as fixed-width 32-digit hex, so the two-level fan-out
//! is uniform. Durability ordering is publish-last everywhere: a block is written
//! atomically (hash-verified) *before* its chunks are indexed, so a crash between
//! the two leaves only a reclaimable orphan block that [`BlockEngine::rebuild_index`]
//! discovers.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use xxhash_rust::xxh3::Xxh3;

use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::util;
use crate::util::fs::AtomicFile;
use crate::util::hasher::hash_buffer_128bit;

use super::block::{BlockWriter, SealedBlock, parse_block_footer, verify_block};
use super::chunk_index::{ChunkIndex, ChunkLocation};
use super::compressor::{EncodedChunk, decode_chunk, encode_chunk};
use super::error::ChunkedError;
use super::manifest::{ChunkEntry, ChunkManifest, MANIFEST_VERSION};
use super::policy::EncodePolicy;
use super::registry::chunker;

/// File name of the block payload within its hash directory (mirrors the version
/// store's `data` convention).
const BLOCK_DATA_FILE: &str = "data";

/// Packs, indexes, and reconstructs chunked file versions against a local blocks
/// directory and chunk index.
#[derive(Debug)]
pub struct BlockEngine {
    blocks_dir: PathBuf,
    index: ChunkIndex,
}

impl BlockEngine {
    /// Open (creating if absent) the engine over `blocks_dir` and the chunk index
    /// at `index_dir`.
    pub fn open(blocks_dir: &Path, index_dir: &Path) -> Result<Self, OxenError> {
        let index = ChunkIndex::open(index_dir)?;
        Ok(Self {
            blocks_dir: blocks_dir.to_path_buf(),
            index,
        })
    }

    pub fn index(&self) -> &ChunkIndex {
        &self.index
    }

    /// The on-disk path of a block.
    pub fn block_path(&self, block_hash: u128) -> PathBuf {
        let hex = format!("{block_hash:032x}");
        self.blocks_dir
            .join(&hex[..2])
            .join(&hex[2..])
            .join(BLOCK_DATA_FILE)
    }

    /// Chunk `reader` in a single streaming pass — hashing the whole file while
    /// chunking, deduplicating against the chunk index, packing new chunks into
    /// blocks — and return the version's manifest.
    ///
    /// Blocks seal and publish as they fill ([`super::MAX_BLOCK_SIZE`]) and at the
    /// end of the pass, so memory stays `O(open block)`, independent of file size.
    /// The returned manifest is validated but **not** persisted; the version store
    /// owns manifest placement and publish ordering.
    pub fn ingest(
        &self,
        reader: &mut (dyn Read + Send),
        policy: &EncodePolicy,
    ) -> Result<ChunkManifest, OxenError> {
        let chunker = chunker(policy.chunker)?;

        let mut file_hasher = Xxh3::new();
        let mut file_size = 0u64;
        let mut entries: Vec<ChunkEntry> = Vec::new();
        let mut writer = BlockWriter::new();
        // Chunks already appended to the open (unpublished) block; without this a
        // repeated chunk within one file would be packed twice.
        let mut pending = std::collections::HashSet::new();

        for raw_chunk in chunker.chunk(Box::new(reader)) {
            let raw_chunk = raw_chunk?;
            file_hasher.update(&raw_chunk.data);
            file_size += raw_chunk.data.len() as u64;
            let chunk_hash = hash_buffer_128bit(&raw_chunk.data);
            entries.push(ChunkEntry {
                hash: chunk_hash,
                offset: raw_chunk.offset,
                len: raw_chunk.data.len() as u32,
            });

            if pending.contains(&chunk_hash) || self.index.contains(chunk_hash)? {
                continue;
            }
            let encoded = encode_chunk(policy.codec, &raw_chunk.data)?;
            if writer.would_exceed_max_size(encoded.data.len()) {
                self.publish_block(std::mem::take(&mut writer).seal()?)?;
                pending.clear();
            }
            writer.append(chunk_hash, raw_chunk.data.len() as u32, &encoded)?;
            pending.insert(chunk_hash);
        }
        if !writer.is_empty() {
            self.publish_block(writer.seal()?)?;
        }

        let manifest = ChunkManifest {
            version: MANIFEST_VERSION,
            file_hash: MerkleHash::new(file_hasher.digest128()),
            file_size,
            chunker_id: policy.chunker,
            transform_id: policy.transform,
            chunks: entries,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Atomically publish a sealed block, then index its chunks (publish-last: the
    /// index is only ever behind the blocks on disk, never ahead).
    fn publish_block(&self, block: SealedBlock) -> Result<(), OxenError> {
        AtomicFile::new(self.block_path(block.hash))
            .with_hash(MerkleHash::new(block.hash))
            .write(&block.data)?;
        self.index.insert_block(block.hash, &block.chunks)
    }

    /// Verify and store a complete block that arrived from elsewhere (a transfer
    /// peer, a repair source): check its content hash against `expected_hash`,
    /// verify every chunk against the footer's claims (design decision 16), then
    /// publish and index it. Idempotent.
    pub fn store_block(&self, expected_hash: u128, data: &[u8]) -> Result<(), OxenError> {
        let actual = hash_buffer_128bit(data);
        if actual != expected_hash {
            return Err(ChunkedError::BlockHashMismatch {
                expected: expected_hash,
                actual,
            }
            .into());
        }
        let chunks = verify_block(data)?;
        AtomicFile::new(self.block_path(expected_hash))
            .with_hash(MerkleHash::new(expected_hash))
            .write(data)?;
        self.index.insert_block(expected_hash, &chunks)
    }

    /// Stream the file a manifest describes into `writer`, in order.
    ///
    /// The caller owns end-to-end verification (e.g. `AtomicFile::with_hash` when
    /// materializing to the working tree).
    pub fn reconstruct_to(
        &self,
        manifest: &ChunkManifest,
        writer: &mut dyn Write,
    ) -> Result<(), OxenError> {
        let mut cursor = ChunkPayloadCursor::new();
        for entry in &manifest.chunks {
            let raw = cursor.read_chunk(self, entry)?;
            writer.write_all(&raw)?;
        }
        Ok(())
    }

    /// Read `len` bytes at `offset` of the reconstructed file — a binary search for
    /// the covering chunks plus partial chunk reads, never a whole-file pass. Reads
    /// past EOF truncate (like `pread`).
    pub fn read_range(
        &self,
        manifest: &ChunkManifest,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, OxenError> {
        let end = offset.saturating_add(len).min(manifest.file_size);
        if offset >= end {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity((end - offset) as usize);
        let mut cursor = ChunkPayloadCursor::new();
        let mut pos = offset;
        while pos < end {
            let entry = manifest.chunk_at(pos).ok_or_else(|| {
                ChunkedError::InvalidManifest(format!(
                    "no chunk covers offset {pos} of {}",
                    manifest.file_size
                ))
            })?;
            let raw = cursor.read_chunk(self, entry)?;
            let start_in_chunk = (pos - entry.offset) as usize;
            let end_in_chunk = (end - entry.offset).min(entry.len as u64) as usize;
            out.extend_from_slice(&raw[start_in_chunk..end_in_chunk]);
            pos = entry.offset + end_in_chunk as u64;
        }
        Ok(out)
    }

    /// Whether every chunk a manifest references is present in the index (and thus,
    /// barring corruption, in a local block).
    pub fn has_all_chunks(&self, manifest: &ChunkManifest) -> Result<bool, OxenError> {
        for entry in &manifest.chunks {
            if !self.index.contains(entry.hash)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Pack exactly the requested chunks into fresh transfer blocks (the same
    /// on-disk block format, formed per transfer).
    ///
    /// Stored payloads are copied as-is — compressed chunks travel compressed, no
    /// decode/re-encode. Duplicate hashes pack once. A hash this store doesn't
    /// have fails with [`ChunkedError::MissingChunk`]. Memory is bounded by the
    /// requested chunks; callers bound their batches (e.g. by summed raw length)
    /// to keep transfers incremental.
    pub fn pack_chunks(&self, hashes: &[u128]) -> Result<Vec<SealedBlock>, OxenError> {
        let mut blocks = Vec::new();
        let mut writer = BlockWriter::new();
        let mut packed = std::collections::HashSet::new();
        let mut cursor = ChunkPayloadCursor::new();

        for &chunk_hash in hashes {
            if !packed.insert(chunk_hash) {
                continue;
            }
            let location = self
                .index
                .get(chunk_hash)?
                .ok_or(ChunkedError::MissingChunk { chunk_hash })?;
            let payload = cursor.read_payload(self, &location)?;
            let encoded = EncodedChunk {
                codec: location.codec,
                data: payload,
            };
            if writer.would_exceed_max_size(encoded.data.len()) {
                blocks.push(std::mem::take(&mut writer).seal()?);
            }
            writer.append(chunk_hash, location.raw_len, &encoded)?;
        }
        if !writer.is_empty() {
            blocks.push(writer.seal()?);
        }
        Ok(blocks)
    }

    /// Rebuild the chunk index from block footers: clear it, scan every block on
    /// disk (verifying each block's content hash), and re-index every chunk.
    ///
    /// Returns the number of blocks scanned. The index is derived state, so this is
    /// always safe; a block whose bytes no longer match its name is reported as
    /// corrupt rather than indexed.
    pub fn rebuild_index(&self) -> Result<u64, OxenError> {
        self.index.clear()?;
        let mut num_blocks = 0u64;
        for (block_hash, path) in self.list_blocks()? {
            let data = util::fs::read_bytes_from_path(&path)?;
            let actual = hash_buffer_128bit(&data);
            if actual != block_hash {
                return Err(ChunkedError::BlockHashMismatch {
                    expected: block_hash,
                    actual,
                }
                .into());
            }
            let chunks = parse_block_footer(&data)?;
            self.index.insert_block(block_hash, &chunks)?;
            num_blocks += 1;
        }
        Ok(num_blocks)
    }

    /// Every block on disk, as `(block_hash, path)` pairs.
    pub fn list_blocks(&self) -> Result<Vec<(u128, PathBuf)>, OxenError> {
        let mut blocks = Vec::new();
        if !self.blocks_dir.exists() {
            return Ok(blocks);
        }
        for prefix_entry in std::fs::read_dir(&self.blocks_dir)? {
            let prefix_entry = prefix_entry?;
            if !prefix_entry.metadata()?.is_dir() {
                continue;
            }
            for block_entry in std::fs::read_dir(prefix_entry.path())? {
                let block_entry = block_entry?;
                if !block_entry.metadata()?.is_dir() {
                    continue;
                }
                let hex = format!(
                    "{}{}",
                    prefix_entry.file_name().to_string_lossy(),
                    block_entry.file_name().to_string_lossy()
                );
                let Ok(block_hash) = u128::from_str_radix(&hex, 16) else {
                    log::warn!("skipping non-block directory in blocks dir: {hex}");
                    continue;
                };
                let path = block_entry.path().join(BLOCK_DATA_FILE);
                if path.exists() {
                    blocks.push((block_hash, path));
                }
            }
        }
        Ok(blocks)
    }
}

/// Reads raw chunk bytes through the index and block files, keeping the most
/// recently used block file open — consecutive chunks of a file land consecutively
/// in blocks, so this makes sequential reconstruction cheap without a handle cache.
#[derive(Default)]
pub(super) struct ChunkPayloadCursor {
    open_block: Option<(u128, File)>,
}

impl ChunkPayloadCursor {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Read and decode the raw bytes of one manifest chunk.
    pub(super) fn read_chunk(
        &mut self,
        engine: &BlockEngine,
        entry: &ChunkEntry,
    ) -> Result<Vec<u8>, OxenError> {
        let location = engine
            .index
            .get(entry.hash)?
            .ok_or(ChunkedError::MissingChunk {
                chunk_hash: entry.hash,
            })?;
        if location.raw_len != entry.len {
            return Err(ChunkedError::CorruptChunkIndex(format!(
                "chunk {:x} indexed with raw length {} but manifest says {}",
                entry.hash, location.raw_len, entry.len
            ))
            .into());
        }
        let payload = self.read_payload(engine, &location)?;
        Ok(decode_chunk(
            location.codec,
            &payload,
            location.raw_len as usize,
        )?)
    }

    /// Read a chunk's stored payload from its block file.
    fn read_payload(
        &mut self,
        engine: &BlockEngine,
        location: &ChunkLocation,
    ) -> Result<Vec<u8>, OxenError> {
        let block_hash = location.block_hash;
        let io_err = |source| ChunkedError::BlockRead { block_hash, source };

        if self.open_block.as_ref().map(|(hash, _)| *hash) != Some(block_hash) {
            let file = File::open(engine.block_path(block_hash)).map_err(io_err)?;
            self.open_block = Some((block_hash, file));
        }
        // The `if` above guarantees an open block; re-match instead of unwrap.
        let Some((_, file)) = self.open_block.as_mut() else {
            return Err(ChunkedError::CorruptChunkIndex(
                "open block handle unexpectedly missing".to_string(),
            )
            .into());
        };
        let mut payload = vec![0u8; location.stored_len as usize];
        file.seek(SeekFrom::Start(location.offset as u64))
            .map_err(io_err)?;
        file.read_exact(&mut payload).map_err(io_err)?;
        Ok(payload)
    }
}

/// A sync [`Read`] over the reconstructed bytes of a chunked version, decoding one
/// chunk at a time. Owns its engine handle, so it can move into a `spawn_blocking`
/// or feed `AtomicFile::stream` from any thread.
pub struct ReconstructReader {
    engine: Arc<BlockEngine>,
    manifest: ChunkManifest,
    cursor: ChunkPayloadCursor,
    next_chunk: usize,
    buf: Vec<u8>,
    buf_pos: usize,
}

impl ReconstructReader {
    pub fn new(engine: Arc<BlockEngine>, manifest: ChunkManifest) -> Self {
        Self {
            engine,
            manifest,
            cursor: ChunkPayloadCursor::new(),
            next_chunk: 0,
            buf: Vec::new(),
            buf_pos: 0,
        }
    }
}

impl Read for ReconstructReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.buf_pos == self.buf.len() {
            let Some(entry) = self.manifest.chunks.get(self.next_chunk) else {
                return Ok(0); // clean EOF
            };
            self.buf = self
                .cursor
                .read_chunk(&self.engine, entry)
                .map_err(std::io::Error::other)?;
            self.buf_pos = 0;
            self.next_chunk += 1;
        }
        let n = out.len().min(self.buf.len() - self.buf_pos);
        out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EntryDataType;
    use crate::storage::chunked::policy::encode_policy;
    use crate::storage::chunked::{MAX_BLOCK_SIZE, MIN_CHUNK_SIZE};

    struct TestEngine {
        _dir: tempfile::TempDir,
        engine: BlockEngine,
    }

    fn test_engine() -> TestEngine {
        let dir = tempfile::tempdir().expect("create temp dir");
        let engine = BlockEngine::open(&dir.path().join("blocks"), &dir.path().join("chunk_index"))
            .expect("open block engine");
        TestEngine { _dir: dir, engine }
    }

    /// Deterministic compressible pseudo-text: CSV-ish lines seeded so edits and
    /// re-runs are reproducible.
    fn csv_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 64);
        let mut state = seed;
        let mut row = 0u64;
        while out.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.extend_from_slice(
                format!(
                    "{row},image_{state:x}.jpg,label_{},0.{:04}\n",
                    state % 10,
                    state % 10000
                )
                .as_bytes(),
            );
            row += 1;
        }
        out.truncate(len);
        out
    }

    fn csv_policy() -> EncodePolicy {
        encode_policy(&EntryDataType::Tabular, "csv")
    }

    fn ingest(engine: &BlockEngine, data: &[u8]) -> ChunkManifest {
        engine
            .ingest(&mut &data[..], &csv_policy())
            .expect("ingest test data")
    }

    fn reconstruct(engine: &BlockEngine, manifest: &ChunkManifest) -> Vec<u8> {
        let mut out = Vec::new();
        engine
            .reconstruct_to(manifest, &mut out)
            .expect("reconstruct test data");
        out
    }

    /// Ingest → reconstruct is byte-exact, the manifest carries the right identity,
    /// and every chunk is indexed.
    #[test]
    fn ingest_reconstruct_round_trip() -> Result<(), OxenError> {
        let t = test_engine();
        let data = csv_bytes(11, 3 * 1024 * 1024);

        let manifest = ingest(&t.engine, &data);
        assert_eq!(manifest.file_size, data.len() as u64);
        assert_eq!(manifest.file_hash.to_u128(), hash_buffer_128bit(&data));
        assert!(manifest.chunks.len() > 1, "3 MiB must chunk");
        assert!(t.engine.has_all_chunks(&manifest)?);

        assert_eq!(reconstruct(&t.engine, &manifest), data);
        Ok(())
    }

    /// The empty file: no chunks, no blocks, still reconstructs to empty.
    #[test]
    fn empty_file() -> Result<(), OxenError> {
        let t = test_engine();
        let manifest = ingest(&t.engine, &[]);
        assert!(manifest.chunks.is_empty());
        assert_eq!(manifest.file_size, 0);
        assert!(t.engine.list_blocks()?.is_empty());
        assert!(reconstruct(&t.engine, &manifest).is_empty());
        Ok(())
    }

    /// Re-ingesting identical bytes stores nothing new, and an edited version
    /// shares its unchanged chunks — the dedup this feature exists for.
    #[test]
    fn dedup_across_versions() -> Result<(), OxenError> {
        let t = test_engine();
        let data = csv_bytes(23, 2 * 1024 * 1024);

        let first = ingest(&t.engine, &data);
        let blocks_after_first = t.engine.list_blocks()?.len();
        let chunks_after_first = t.engine.index().num_chunks()?;

        // Identical bytes: same manifest, zero new blocks or chunks.
        let again = ingest(&t.engine, &data);
        assert_eq!(again, first);
        assert_eq!(t.engine.list_blocks()?.len(), blocks_after_first);
        assert_eq!(t.engine.index().num_chunks()?, chunks_after_first);

        // A one-row edit in the middle: only the perturbed chunks are new.
        let mut edited = data.clone();
        let mid = edited.len() / 2;
        edited.splice(
            mid..mid,
            b"9999999,new_image.jpg,label_9,0.9999\n".iter().copied(),
        );
        let edited_manifest = ingest(&t.engine, &edited);
        assert_eq!(reconstruct(&t.engine, &edited_manifest), edited);

        let new_chunks = t.engine.index().num_chunks()? - chunks_after_first;
        assert!(
            new_chunks <= 4,
            "a one-row edit should add a handful of chunks, added {new_chunks}"
        );
        Ok(())
    }

    /// Random and boundary-aligned range reads match slices of the original.
    #[test]
    fn range_reads_match_source() -> Result<(), OxenError> {
        let t = test_engine();
        let data = csv_bytes(37, 1_500_000);
        let manifest = ingest(&t.engine, &data);

        let len = data.len() as u64;
        let chunk1_end = manifest.chunks[0].offset + manifest.chunks[0].len as u64;
        let ranges = [
            (0u64, 1u64),         // first byte
            (0, 100),             // head
            (chunk1_end - 1, 2),  // straddles the first chunk boundary
            (chunk1_end, 10),     // starts exactly on a boundary
            (len / 2, 65536 * 3), // spans several chunks
            (len - 1, 1),         // final byte
            (len - 100, 200),     // truncates at EOF
            (len, 10),            // starts at EOF: empty
            (len + 1000, 10),     // past EOF: empty
            (0, len),             // the whole file
        ];
        for (offset, range_len) in ranges {
            let actual = t.engine.read_range(&manifest, offset, range_len)?;
            let start = offset.min(len) as usize;
            let end = offset.saturating_add(range_len).min(len) as usize;
            assert_eq!(actual, &data[start..end], "range {offset}+{range_len}");
        }
        Ok(())
    }

    /// Blocks seal at the size cap: ingesting more than one block's worth of
    /// incompressible data produces multiple blocks, all within the cap.
    #[test]
    fn blocks_seal_at_max_size() -> Result<(), OxenError> {
        let t = test_engine();
        // Incompressible bytes so encoded size ≈ raw size, sized past one block.
        let mut state = 0x5EEDu64;
        let data: Vec<u8> = std::iter::repeat_with(|| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()
        })
        .flatten()
        .take((MAX_BLOCK_SIZE + MAX_BLOCK_SIZE / 2) as usize)
        .collect();

        let manifest = ingest(&t.engine, &data);
        let blocks = t.engine.list_blocks()?;
        assert!(blocks.len() >= 2, "expected multiple blocks");
        for (_, path) in &blocks {
            assert!(util::fs::metadata(path)?.len() <= MAX_BLOCK_SIZE);
        }
        assert_eq!(reconstruct(&t.engine, &manifest), data);
        Ok(())
    }

    /// The index is disposable: wipe it, rebuild from block footers, and every
    /// read works again with identical placements.
    #[test]
    fn index_rebuild_preserves_reads() -> Result<(), OxenError> {
        let t = test_engine();
        let data = csv_bytes(53, 2 * 1024 * 1024);
        let manifest = ingest(&t.engine, &data);

        let before: Vec<_> = manifest
            .chunks
            .iter()
            .map(|c| t.engine.index().get(c.hash))
            .collect::<Result<_, _>>()?;

        t.engine.index().clear()?;
        assert!(!t.engine.has_all_chunks(&manifest)?);
        assert!(matches!(
            t.engine.read_range(&manifest, 0, 100),
            Err(OxenError::ChunkedError(ChunkedError::MissingChunk { .. }))
        ));

        let num_blocks = t.engine.rebuild_index()?;
        assert_eq!(num_blocks as usize, t.engine.list_blocks()?.len());
        assert!(t.engine.has_all_chunks(&manifest)?);
        assert_eq!(reconstruct(&t.engine, &manifest), data);

        let after: Vec<_> = manifest
            .chunks
            .iter()
            .map(|c| t.engine.index().get(c.hash))
            .collect::<Result<_, _>>()?;
        assert_eq!(before, after, "rebuild must reproduce placements");
        Ok(())
    }

    /// `store_block` (the transfer ingest path) verifies hash and chunks, is
    /// idempotent, and rejects a lying name or corrupted bytes.
    #[test]
    fn store_block_verifies_and_is_idempotent() -> Result<(), OxenError> {
        let source = test_engine();
        let data = csv_bytes(71, MIN_CHUNK_SIZE as usize * 40);
        let manifest = ingest(&source.engine, &data);

        let dest = test_engine();
        for (block_hash, path) in source.engine.list_blocks()? {
            let bytes = util::fs::read_bytes_from_path(&path)?;

            // Wrong expected hash is rejected before anything is written.
            assert!(matches!(
                dest.engine.store_block(block_hash ^ 1, &bytes),
                Err(OxenError::ChunkedError(
                    ChunkedError::BlockHashMismatch { .. }
                ))
            ));

            dest.engine.store_block(block_hash, &bytes)?;
            // Idempotent: storing the same block again succeeds.
            dest.engine.store_block(block_hash, &bytes)?;
        }
        assert_eq!(reconstruct(&dest.engine, &manifest), data);
        Ok(())
    }
}
