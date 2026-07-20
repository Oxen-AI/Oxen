//! The block engine: packs chunked file versions into blocks, indexes them, and
//! reconstructs file bytes from manifests.
//!
//! This is the shared core every version-store backend drives; backends differ
//! only in the raw [`BlockByteIo`] the engine runs over (local file ranges, S3
//! ranged GETs / PUTs). It is sync on purpose (callers bridge with one
//! `spawn_blocking` per operation, per `docs/async_policy.md`); the chunk index is
//! always local LMDB, even for remote block storage.
//!
//! Durability ordering is publish-last everywhere: a block is durably written
//! (hash-verified) *before* its chunks are indexed, so a crash between the two
//! leaves only a reclaimable orphan block that [`BlockEngine::rebuild_index`]
//! discovers.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use xxhash_rust::xxh3::Xxh3;

use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::util::hasher::hash_buffer_128bit;

use super::block::{BlockWriter, SealedBlock, parse_block_footer, verify_block};
use super::block_io::{BlockByteIo, LocalBlockIo};
use super::chunk_index::ChunkIndex;
use super::compressor::{
    EncodedChunk, PreparedDict, compress_with_base, compress_with_dict, decode_chunk,
    decompress_with_base, decompress_with_dict, encode_chunk,
};
use super::error::ChunkedError;
use super::manifest::{ChunkEntry, ChunkManifest, MANIFEST_VERSION};
use super::policy::EncodePolicy;
use super::registry::{ChunkerId, CodecId, TransformId, chunker};
use super::trace_chunker::{AUTO_SNIFF_BYTES, sniff_long_rows};
use super::unwrap_transform::{try_unwrap, unwrap_inverse};

/// Maximum bytes of raw chunk samples buffered to train the store's shared
/// dictionary (also bounds the deferred-encode queue during the first ingest).
const DICT_SAMPLE_CAP: usize = 4 * 1024 * 1024;
/// Minimum sample bytes worth training a dictionary from; below this the first
/// ingest simply encodes without one and a later, larger ingest trains it.
const DICT_MIN_SAMPLE: usize = 256 * 1024;
/// Maximum trained dictionary size.
const DICT_MAX_SIZE: usize = 64 * 1024;
/// Raw bytes hashed for a chunk's prefix sketch (delta-base candidate lookup).
const SKETCH_PREFIX: usize = 256;
/// Only chunks at least this large attempt delta encoding — building a per-base
/// dictionary costs CPU that tiny chunks can't repay.
const DELTA_MIN_RAW: usize = 8 * 1024;
/// Files larger than this skip the unwrap transform (it needs the whole file in
/// memory for frame scanning and verification) and ingest under identity.
const UNWRAP_INPUT_CAP: usize = 64 * 1024 * 1024;
/// Maximum raw bytes assembled into one block-window delta base. Bounds the
/// memory and one-time digestion cost of a window dictionary.
const WINDOW_RAW_CAP: usize = 8 * 1024 * 1024;
/// Window start positions quantize to this many members, so near-miss ranges
/// share one cached window dictionary instead of digesting per chunk.
const WINDOW_SEGMENT_MEMBERS: usize = 128;
/// Assembled window dictionaries kept warm (each up to [`WINDOW_RAW_CAP`] raw
/// bytes plus digest tables).
const WINDOW_CACHE_ENTRIES: usize = 2;
/// Chunks decoded per parallel reconstruction batch. Bounds reconstruction
/// memory at roughly `RECONSTRUCT_BATCH × MAX_CHUNK_SIZE` per file.
const RECONSTRUCT_BATCH: usize = 64;
/// Decoded delta-base chunks kept warm during reads: many delta chunks share one
/// base (request-log rows, page windows), and re-decoding the base per delta is
/// the dominant repeated cost of reconstruction.
const BASE_CACHE_ENTRIES: usize = 64;

/// Packs, indexes, and reconstructs chunked file versions over a block byte-IO
/// backend and a local chunk index.
#[derive(Debug)]
pub struct BlockEngine {
    io: Arc<dyn BlockByteIo>,
    index: ChunkIndex,
    /// Prepared dictionaries by content hash (a store has one per content class).
    dict_cache: Mutex<HashMap<u128, Arc<PreparedDict>>>,
    /// Parsed block footers, keyed by block hash (footers are immutable).
    footer_cache: Mutex<HashMap<u128, Arc<Vec<super::block::BlockChunk>>>>,
    /// Recently assembled window-delta dictionaries, most recent last.
    window_cache: Mutex<Vec<((u128, u32, u32), Arc<PreparedDict>)>>,
    /// Recently decoded delta-base chunks (chunk hash → raw bytes).
    base_cache: Mutex<HashMap<u128, Arc<Vec<u8>>>>,
    /// Serializes window-dictionary assembly so concurrent decoders of one window
    /// build it once instead of stampeding (each build materializes megabytes).
    window_build: Mutex<()>,
}

impl BlockEngine {
    /// Build an engine over an explicit byte-IO backend and the chunk index at
    /// `index_dir`.
    pub fn new(io: Arc<dyn BlockByteIo>, index_dir: &Path) -> Result<Self, OxenError> {
        let index = ChunkIndex::open(index_dir)?;
        Ok(Self {
            io,
            index,
            dict_cache: Mutex::new(HashMap::new()),
            footer_cache: Mutex::new(HashMap::new()),
            window_cache: Mutex::new(Vec::new()),
            base_cache: Mutex::new(HashMap::new()),
            window_build: Mutex::new(()),
        })
    }

    /// Open (creating if absent) a local-filesystem engine over `blocks_dir` and
    /// the chunk index at `index_dir`.
    pub fn open(blocks_dir: &Path, index_dir: &Path) -> Result<Self, OxenError> {
        Self::new(
            Arc::new(LocalBlockIo::new(blocks_dir.to_path_buf())),
            index_dir,
        )
    }

    pub fn index(&self) -> &ChunkIndex {
        &self.index
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
        // The unwrap transform needs the whole file for frame scanning and
        // verification: buffer it (bounded), and on success chunk the transformed
        // stream while keeping the original bytes' hash as the manifest identity.
        // Files over the cap, and files with no provably reproducible frame,
        // ingest under the identity transform — a pure function of content.
        let mut transform_id = TransformId::IDENTITY;
        let mut original_identity: Option<u128> = None;
        let mut head: Vec<u8> = Vec::new();
        if policy.transform == TransformId::ZSTD_UNWRAP_V1 {
            let mut buf = vec![0u8; 64 * 1024];
            while head.len() <= UNWRAP_INPUT_CAP {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => head.extend_from_slice(&buf[..n]),
                    Err(err) => return Err(ChunkedError::ChunkRead(err).into()),
                }
            }
            if head.len() <= UNWRAP_INPUT_CAP
                && let Some(transformed) = try_unwrap(&head)
            {
                original_identity = Some(hash_buffer_128bit(&head));
                transform_id = TransformId::ZSTD_UNWRAP_V1;
                head = transformed;
            }
        }

        // Resolve the content-adaptive chunker here so the manifest records the
        // actual boundary function used, and so the dictionary class can honor
        // the sniff outcome (long-row and small-row line-delimited JSON are
        // distinct content families).
        let mut dict_class = policy.dict_class;
        debug_assert!(
            policy.transform == TransformId::IDENTITY
                || policy.chunker != ChunkerId::TRACE_AUTO_V1,
            "unwrap and auto-sniff policies are mutually exclusive by extension"
        );
        let chunker_id = if policy.chunker == ChunkerId::TRACE_AUTO_V1 {
            head = vec![0u8; AUTO_SNIFF_BYTES];
            let mut filled = 0usize;
            while filled < head.len() {
                match reader.read(&mut head[filled..]) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(err) => return Err(ChunkedError::ChunkRead(err).into()),
                }
            }
            head.truncate(filled);
            if sniff_long_rows(&head) {
                dict_class |= 0x10;
                ChunkerId::TRACE_JSONL_V1
            } else {
                ChunkerId::GENERIC_FASTCDC_V1
            }
        } else {
            policy.chunker
        };
        let chunker = chunker(chunker_id)?;
        let mut sniffed_reader = std::io::Cursor::new(head).chain(&mut *reader);

        // Dictionary compression applies to zstd-eligible content only. If the
        // store has no dictionary yet (and the backend can persist one), the
        // first zstd-eligible ingest defers encoding of up to DICT_SAMPLE_CAP raw
        // chunk bytes, trains a dictionary from them, then encodes the queue.
        let mut dict = if policy.codec == CodecId::ZSTD {
            self.current_dict(dict_class)?
        } else {
            None
        };
        let mut train_queue: Option<Vec<(u128, Vec<u8>)>> =
            (policy.codec == CodecId::ZSTD && dict.is_none() && self.io.supports_dictionaries())
                .then(Vec::new);
        let mut queued_bytes = 0usize;

        let mut file_hasher = Xxh3::new();
        let mut file_size = 0u64;
        let mut entries: Vec<ChunkEntry> = Vec::new();
        let mut writer = BlockWriter::new();
        // Chunks already appended to the open (unpublished) block; without this a
        // repeated chunk within one file would be packed twice.
        let mut pending = std::collections::HashSet::new();
        // Chunks held in the training queue, not yet packed or indexed.
        let mut queued = std::collections::HashSet::new();

        let mut sketches: Vec<(u64, u128)> = Vec::new();
        // Same-pass delta bases: sketches of chunks already packed in this
        // ingest, so a snapshot's later rows can delta against its earlier rows
        // (the LMDB sketch table only has *prior* ingests).
        let mut flight: HashMap<u64, u128> = HashMap::new();
        let encode_and_pack = |engine: &Self,
                               writer: &mut BlockWriter,
                               pending: &mut std::collections::HashSet<u128>,
                               sketches: &mut Vec<(u64, u128)>,
                               flight: &mut HashMap<u64, u128>,
                               chunk_hash: u128,
                               raw: &[u8],
                               dict: Option<&(u128, Arc<PreparedDict>)>|
         -> Result<(), OxenError> {
            let mut encoded = encode_dict_chunk(policy.codec, raw, dict)?;
            // Near-duplicate delta: a prior chunk with the same prefix sketch is a
            // base candidate; keep the delta only when it beats the plain form.
            if policy.codec == CodecId::ZSTD && raw.len() >= DELTA_MIN_RAW {
                // Prefix, midpoint, and suffix sketches: edits at a chunk's head
                // shift its prefix but usually keep its tail and vice versa, and
                // rows that share a large verbatim middle (the tools+system block
                // of request logs) match at the midpoint when both ends differ.
                let prefix = xxhash_rust::xxh3::xxh3_64(&raw[..SKETCH_PREFIX.min(raw.len())]);
                let mid_at = (raw.len() / 2).saturating_sub(SKETCH_PREFIX / 2);
                let middle = xxhash_rust::xxh3::xxh3_64(
                    &raw[mid_at..(mid_at + SKETCH_PREFIX).min(raw.len())],
                );
                let suffix =
                    xxhash_rust::xxh3::xxh3_64(&raw[raw.len().saturating_sub(SKETCH_PREFIX)..]);
                // Content superfeatures (min-of-permuted rolling gear hash): a
                // probe that survives edits anywhere the positional sketches
                // look, and survives content reordering entirely — two chunks
                // sharing most of their content (a permuted parquet page, a
                // heavily edited row) almost surely share a minimizing window.
                let [feat_a, feat_b, feat_c] = minhash_features(raw);
                let probes = [prefix, middle, suffix, feat_a, feat_b, feat_c];
                for (i, &probe) in probes.iter().enumerate() {
                    if !probes[..i].contains(&probe) {
                        sketches.push((probe, chunk_hash));
                    }
                }
                if let Some(delta) =
                    engine.try_delta_encode(&probes, chunk_hash, raw, flight, writer)?
                    && delta.data.len() < encoded.data.len()
                {
                    encoded = delta;
                }
                if let Some(window) = engine.try_window_delta(&probes, chunk_hash, raw)?
                    && window.data.len() < encoded.data.len()
                {
                    encoded = window;
                }
                for sk in probes {
                    flight.insert(sk, chunk_hash);
                }
            }
            if writer.would_exceed_max_size(encoded.data.len()) {
                engine.publish_block(std::mem::take(writer).seal()?)?;
                pending.clear();
            }
            writer.append(chunk_hash, raw.len() as u32, &encoded)?;
            pending.insert(chunk_hash);
            Ok(())
        };

        for raw_chunk in chunker.chunk(Box::new(&mut sniffed_reader)) {
            let raw_chunk = raw_chunk?;
            file_hasher.update(&raw_chunk.data);
            file_size += raw_chunk.data.len() as u64;
            let chunk_hash = hash_buffer_128bit(&raw_chunk.data);
            entries.push(ChunkEntry {
                hash: chunk_hash,
                offset: raw_chunk.offset,
                len: raw_chunk.data.len() as u32,
            });

            if pending.contains(&chunk_hash)
                || queued.contains(&chunk_hash)
                || self.index.contains(chunk_hash)?
            {
                continue;
            }

            if train_queue.is_some() {
                queued_bytes += raw_chunk.data.len();
                queued.insert(chunk_hash);
                if let Some(queue) = train_queue.as_mut() {
                    queue.push((chunk_hash, raw_chunk.data));
                }
                if queued_bytes >= DICT_SAMPLE_CAP {
                    let drained = train_queue.take().unwrap_or_default();
                    dict = self.train_dictionary(dict_class, &drained)?;
                    for (hash, raw) in drained {
                        encode_and_pack(
                            self,
                            &mut writer,
                            &mut pending,
                            &mut sketches,
                            &mut flight,
                            hash,
                            &raw,
                            dict.as_ref(),
                        )?;
                    }
                    queued.clear();
                }
                continue;
            }

            encode_and_pack(
                self,
                &mut writer,
                &mut pending,
                &mut sketches,
                &mut flight,
                chunk_hash,
                &raw_chunk.data,
                dict.as_ref(),
            )?;
        }

        // EOF with the training queue still open: train if there is enough sample
        // material, then encode the queue either way.
        if let Some(queue) = train_queue.take() {
            if queued_bytes >= DICT_MIN_SAMPLE {
                dict = self.train_dictionary(dict_class, &queue)?;
            }
            for (hash, raw) in queue {
                encode_and_pack(
                    self,
                    &mut writer,
                    &mut pending,
                    &mut sketches,
                    &mut flight,
                    hash,
                    &raw,
                    dict.as_ref(),
                )?;
            }
        }

        if !writer.is_empty() {
            self.publish_block(writer.seal()?)?;
        }
        // Advisory delta-base candidates; recorded after payloads are durable.
        self.index.insert_sketches(&sketches)?;

        let manifest = ChunkManifest {
            version: MANIFEST_VERSION,
            // The manifest identity is always the ORIGINAL file's hash; under a
            // transform the chunk stream (hashed incrementally above) differs.
            file_hash: MerkleHash::new(original_identity.unwrap_or_else(|| file_hasher.digest128())),
            file_size,
            chunker_id,
            transform_id,
            chunks: entries,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Attempt to encode `raw` as a delta against a sketch-matched base chunk.
    /// Bounded: the base must itself be a non-delta chunk (chain depth one), and
    /// the base's raw bytes must be readable from this store.
    fn try_delta_encode(
        &self,
        sketches: &[u64],
        chunk_hash: u128,
        raw: &[u8],
        flight: &HashMap<u64, u128>,
        writer: &BlockWriter,
    ) -> Result<Option<EncodedChunk>, OxenError> {
        let mut best: Option<EncodedChunk> = None;
        let mut tried = [0u128; 6];
        let mut tried_count = 0usize;
        for &sketch in sketches {
            // Same-pass candidates first (most similar recency), then prior
            // ingests via the persistent sketch table.
            let candidates = [
                flight.get(&sketch).copied(),
                self.index.sketch_candidate(sketch)?,
            ];
            for candidate in candidates.into_iter().flatten() {
                if candidate == chunk_hash {
                    continue;
                }
                // Chain depth is at most one: a delta candidate's own base (a
                // full chunk by construction) substitutes as the base.
                let Some((base_hash, codec)) = self.chunk_codec(candidate, writer)? else {
                    continue;
                };
                let base_hash = if codec == CodecId::ZSTD_DELTA {
                    base_hash
                } else {
                    candidate
                };
                if base_hash == chunk_hash || tried[..tried_count].contains(&base_hash) {
                    continue;
                }
                if tried_count >= tried.len() {
                    break;
                }
                tried[tried_count] = base_hash;
                tried_count += 1;
                let Some(base_raw) = self.read_raw_or_in_flight(base_hash, writer)? else {
                    continue;
                };
                let frame = compress_with_base(raw, &base_raw)?;
                if frame.len() + 16 >= raw.len() {
                    continue;
                }
                if best
                    .as_ref()
                    .is_none_or(|b| frame.len() + 16 < b.data.len())
                {
                    let mut data = Vec::with_capacity(16 + frame.len());
                    data.extend_from_slice(&base_hash.to_le_bytes());
                    data.extend_from_slice(&frame);
                    best = Some(EncodedChunk {
                        codec: CodecId::ZSTD_DELTA,
                        data,
                    });
                }
            }
        }
        Ok(best)
    }

    /// The parsed footer of a sealed block, cached (footers are immutable).
    fn block_footer(
        &self,
        block_hash: u128,
    ) -> Result<Arc<Vec<super::block::BlockChunk>>, OxenError> {
        if let Some(footer) = self.footer_cache.lock().get(&block_hash) {
            return Ok(Arc::clone(footer));
        }
        let data = self.io.read_block(block_hash)?;
        let footer = Arc::new(parse_block_footer(&data)?);
        self.footer_cache
            .lock()
            .insert(block_hash, Arc::clone(&footer));
        Ok(footer)
    }

    /// Assemble (or fetch from cache) the window dictionary for members
    /// `[start, start+count)` of a sealed block: the concatenated raw bytes of
    /// the range's non-delta chunks, digested once for reuse. `None` when the
    /// range contains nothing usable.
    fn window_dict(
        &self,
        block_hash: u128,
        start: u32,
        count: u32,
    ) -> Result<Option<Arc<PreparedDict>>, OxenError> {
        let key = (block_hash, start, count);
        if let Some(dict) = self
            .window_cache
            .lock()
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, d)| Arc::clone(d))
        {
            return Ok(Some(dict));
        }
        // Single-flight: build each window once even under parallel decoding.
        let _build_guard = self.window_build.lock();
        if let Some(dict) = self
            .window_cache
            .lock()
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, d)| Arc::clone(d))
        {
            return Ok(Some(dict));
        }
        let footer = self.block_footer(block_hash)?;
        let end = (start as usize).saturating_add(count as usize).min(footer.len());
        let members = &footer[start as usize..end];
        let mut raw = Vec::new();
        for member in members {
            // Delta-coded members are skipped (never recursed into), so window
            // assembly is always depth one; the same rule applies on decode.
            if member.codec == CodecId::ZSTD_DELTA || member.codec == CodecId::ZSTD_WINDOW_DELTA {
                continue;
            }
            let payload = self.io.read_block_range(
                block_hash,
                member.offset as u64,
                member.stored_len as u64,
            )?;
            let bytes = if member.codec == CodecId::ZSTD_DICT {
                let hash_bytes: [u8; 16] = payload
                    .get(..16)
                    .and_then(|b| b.try_into().ok())
                    .ok_or_else(|| {
                        ChunkedError::CorruptBlock("dict chunk shorter than header".to_string())
                    })?;
                let dict = self.load_dict(u128::from_le_bytes(hash_bytes))?;
                decompress_with_dict(&payload[16..], member.raw_len as usize, &dict)?
            } else {
                decode_chunk(member.codec, &payload, member.raw_len as usize)?
            };
            raw.extend_from_slice(&bytes);
            if raw.len() >= WINDOW_RAW_CAP {
                raw.truncate(WINDOW_RAW_CAP);
                break;
            }
        }
        if raw.is_empty() {
            return Ok(None);
        }
        let dict = Arc::new(PreparedDict::new(&raw));
        let mut cache = self.window_cache.lock();
        if cache.len() >= WINDOW_CACHE_ENTRIES {
            cache.remove(0);
        }
        cache.push((key, Arc::clone(&dict)));
        Ok(Some(dict))
    }

    /// Attempt a window delta: when several probes hit distinct chunks of one
    /// sealed block, the chunk's redundancy is dispersed across that block — a
    /// window of the block becomes the dictionary no single base chunk can be.
    fn try_window_delta(
        &self,
        probes: &[u64],
        chunk_hash: u128,
        raw: &[u8],
    ) -> Result<Option<EncodedChunk>, OxenError> {
        // Distinct sealed-block candidates per probe, grouped by block.
        let mut hits: Vec<(u128, u128)> = Vec::new(); // (block, base chunk)
        for &probe in probes {
            let Some(candidate) = self.index.sketch_candidate(probe)? else {
                continue;
            };
            if candidate == chunk_hash {
                continue;
            }
            let Some(location) = self.index.get(candidate)? else {
                continue;
            };
            if !hits.contains(&(location.block_hash, candidate)) {
                hits.push((location.block_hash, candidate));
            }
        }
        let mut best_block = None;
        let mut best_count = 0usize;
        for &(block, _) in &hits {
            let count = hits.iter().filter(|(b, _)| *b == block).count();
            if count > best_count {
                best_count = count;
                best_block = Some(block);
            }
        }
        // One hit is single-base territory (already tried); a window pays off
        // only when the match evidence is dispersed.
        let Some(block_hash) = best_block else {
            return Ok(None);
        };
        if best_count < 2 {
            return Ok(None);
        }

        let footer = self.block_footer(block_hash)?;
        let total_raw: u64 = footer.iter().map(|m| m.raw_len as u64).sum();
        let (start, count) = if total_raw <= WINDOW_RAW_CAP as u64 {
            // The whole block fits one window: a single cache entry serves every
            // chunk that resolves here.
            (0u32, footer.len() as u32)
        } else {
            // Quantize the first hit to a segment boundary and take members up
            // to the raw cap, so nearby chunks share a cached window.
            let first_hit = footer
                .iter()
                .position(|m| hits.iter().any(|(b, c)| *b == block_hash && *c == m.chunk_hash))
                .unwrap_or(0);
            let start = (first_hit / WINDOW_SEGMENT_MEMBERS) * WINDOW_SEGMENT_MEMBERS;
            let mut acc = 0u64;
            let mut count = 0u32;
            for member in &footer[start..] {
                acc += member.raw_len as u64;
                count += 1;
                if acc >= WINDOW_RAW_CAP as u64 {
                    break;
                }
            }
            (start as u32, count)
        };

        let Some(dict) = self.window_dict(block_hash, start, count)? else {
            return Ok(None);
        };
        let frame = compress_with_dict(raw, &dict)?;
        let total = 16 + 4 + 4 + frame.len();
        if total >= raw.len() {
            return Ok(None);
        }
        let mut data = Vec::with_capacity(total);
        data.extend_from_slice(&block_hash.to_le_bytes());
        data.extend_from_slice(&start.to_le_bytes());
        data.extend_from_slice(&count.to_le_bytes());
        data.extend_from_slice(&frame);
        Ok(Some(EncodedChunk {
            codec: CodecId::ZSTD_WINDOW_DELTA,
            data,
        }))
    }

    /// A chunk's codec plus (for deltas) its base hash, whether the chunk lives
    /// in a published block or the open writer. `None` if unknown everywhere.
    fn chunk_codec(
        &self,
        chunk_hash: u128,
        writer: &BlockWriter,
    ) -> Result<Option<(u128, CodecId)>, OxenError> {
        if let Some(loc) = self.index.get(chunk_hash)? {
            if loc.codec != CodecId::ZSTD_DELTA {
                return Ok(Some((chunk_hash, loc.codec)));
            }
            let header = self
                .io
                .read_block_range(loc.block_hash, loc.offset as u64, 16)?;
            let Ok(bytes) = <[u8; 16]>::try_from(header.as_slice()) else {
                return Ok(None);
            };
            return Ok(Some((u128::from_le_bytes(bytes), loc.codec)));
        }
        if let Some((codec, payload, _raw_len)) = writer.encoded_chunk(chunk_hash) {
            if codec != CodecId::ZSTD_DELTA {
                return Ok(Some((chunk_hash, codec)));
            }
            let Ok(bytes) = <[u8; 16]>::try_from(&payload[..16.min(payload.len())]) else {
                return Ok(None);
            };
            return Ok(Some((u128::from_le_bytes(bytes), codec)));
        }
        Ok(None)
    }

    /// Raw bytes of a non-delta chunk from the store or the open writer.
    fn read_raw_or_in_flight(
        &self,
        chunk_hash: u128,
        writer: &BlockWriter,
    ) -> Result<Option<Vec<u8>>, OxenError> {
        if let Some(loc) = self.index.get(chunk_hash)? {
            if loc.codec == CodecId::ZSTD_DELTA || loc.codec == CodecId::ZSTD_WINDOW_DELTA {
                return Ok(None);
            }
            return Ok(Some(self.read_chunk(&ChunkEntry {
                hash: chunk_hash,
                offset: 0,
                len: loc.raw_len,
            })?));
        }
        let Some((codec, payload, raw_len)) = writer.encoded_chunk(chunk_hash) else {
            return Ok(None);
        };
        let raw = match codec {
            CodecId::ZSTD_DELTA | CodecId::ZSTD_WINDOW_DELTA => return Ok(None),
            CodecId::ZSTD_DICT => {
                let Ok(hash_bytes) = <[u8; 16]>::try_from(&payload[..16.min(payload.len())])
                else {
                    return Ok(None);
                };
                let dict = self.load_dict(u128::from_le_bytes(hash_bytes))?;
                decompress_with_dict(&payload[16..], raw_len as usize, &dict)?
            }
            _ => decode_chunk(codec, payload, raw_len as usize)?,
        };
        Ok(Some(raw))
    }

    /// The store's current shared dictionary for a content class, loaded through
    /// the prepared-dictionary cache.
    fn current_dict(&self, class: u8) -> Result<Option<(u128, Arc<PreparedDict>)>, OxenError> {
        let Some(hash) = self.io.current_dictionary(class)? else {
            return Ok(None);
        };
        Ok(Some((hash, self.load_dict(hash)?)))
    }

    fn load_dict(&self, dict_hash: u128) -> Result<Arc<PreparedDict>, OxenError> {
        if let Some(dict) = self.dict_cache.lock().get(&dict_hash) {
            return Ok(Arc::clone(dict));
        }
        let dict = Arc::new(PreparedDict::new(&self.io.read_dictionary(dict_hash)?));
        self.dict_cache.lock().insert(dict_hash, Arc::clone(&dict));
        Ok(dict)
    }

    /// Train a shared dictionary from raw chunk samples and durably publish it
    /// (blob first, current-pointer last). Training failure is non-fatal: the
    /// store simply continues without a dictionary.
    fn train_dictionary(
        &self,
        class: u8,
        samples: &[(u128, Vec<u8>)],
    ) -> Result<Option<(u128, Arc<PreparedDict>)>, OxenError> {
        let total: usize = samples.iter().map(|(_, raw)| raw.len()).sum();
        let mut concat = Vec::with_capacity(total);
        for (_, raw) in samples {
            concat.extend_from_slice(raw);
        }
        // zstd's trainer wants many small samples, not a few large ones: slice the
        // buffer into fixed windows regardless of chunk boundaries.
        const TRAIN_SLICE: usize = 4 * 1024;
        let sizes: Vec<usize> = concat.chunks(TRAIN_SLICE).map(|s| s.len()).collect();
        let dict_bytes = match zstd::dict::from_continuous(&concat, &sizes, DICT_MAX_SIZE) {
            Ok(bytes) if !bytes.is_empty() => bytes,
            Ok(_) => return Ok(None),
            Err(err) => {
                log::warn!("dictionary training failed, continuing without one: {err}");
                return Ok(None);
            }
        };
        let dict_hash = hash_buffer_128bit(&dict_bytes);
        let dict = Arc::new(PreparedDict::new(&dict_bytes));
        self.io
            .put_dictionary(dict_hash, &bytes::Bytes::from(dict_bytes))?;
        self.io.set_current_dictionary(class, dict_hash)?;
        self.dict_cache.lock().insert(dict_hash, Arc::clone(&dict));
        Ok(Some((dict_hash, dict)))
    }

    /// Durably publish a sealed block, then index its chunks (publish-last: the
    /// index is only ever behind the stored blocks, never ahead).
    fn publish_block(&self, block: SealedBlock) -> Result<(), OxenError> {
        self.io.put_block(block.hash, &block.data)?;
        self.index.insert_block(block.hash, &block.chunks)
    }

    /// Verify and store a complete block that arrived from elsewhere (a transfer
    /// peer, a repair source): check its content hash against `expected_hash`,
    /// verify every chunk against the footer's claims (design decision 16), then
    /// publish and index it. Idempotent.
    pub fn store_block(&self, expected_hash: u128, data: bytes::Bytes) -> Result<(), OxenError> {
        let actual = hash_buffer_128bit(&data);
        if actual != expected_hash {
            return Err(ChunkedError::BlockHashMismatch {
                expected: expected_hash,
                actual,
            }
            .into());
        }
        let chunks = verify_block(&data)?;
        self.io.put_block(expected_hash, &data)?;
        self.index.insert_block(expected_hash, &chunks)
    }

    /// Read and decode the raw bytes of one manifest chunk through the index and
    /// block byte IO.
    pub(super) fn read_chunk(&self, entry: &ChunkEntry) -> Result<Vec<u8>, OxenError> {
        let location = self
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
        let payload = self.io.read_block_range(
            location.block_hash,
            location.offset as u64,
            location.stored_len as u64,
        )?;
        if location.codec == CodecId::ZSTD_DELTA {
            // Payload = [16-byte base chunk hash LE][zstd frame over base dict].
            let hash_bytes: [u8; 16] = payload.get(..16).and_then(|b| b.try_into().ok()).ok_or(
                ChunkedError::CorruptChunkIndex(format!(
                    "delta chunk {:x} payload shorter than its base header",
                    entry.hash
                )),
            )?;
            let base_hash = u128::from_le_bytes(hash_bytes);
            let base_loc = self
                .index
                .get(base_hash)?
                .ok_or(ChunkedError::MissingChunk {
                    chunk_hash: base_hash,
                })?;
            if base_loc.codec == CodecId::ZSTD_DELTA {
                return Err(ChunkedError::CorruptChunkIndex(format!(
                    "delta chunk {:x} has a delta base {base_hash:x} (chain depth > 1)",
                    entry.hash
                ))
                .into());
            }
            let base_raw = if let Some(cached) = self.base_cache.lock().get(&base_hash) {
                Arc::clone(cached)
            } else {
                let decoded = Arc::new(self.read_chunk(&ChunkEntry {
                    hash: base_hash,
                    offset: 0,
                    len: base_loc.raw_len,
                })?);
                let mut cache = self.base_cache.lock();
                if cache.len() >= BASE_CACHE_ENTRIES {
                    cache.clear();
                }
                cache.insert(base_hash, Arc::clone(&decoded));
                decoded
            };
            return Ok(decompress_with_base(
                &payload[16..],
                location.raw_len as usize,
                &base_raw,
            )?);
        }
        if location.codec == CodecId::ZSTD_WINDOW_DELTA {
            // Payload = [16-byte block hash LE][u32 start][u32 count][zstd frame].
            let header = payload.get(..24).ok_or_else(|| {
                ChunkedError::CorruptChunkIndex(format!(
                    "window-delta chunk {:x} payload shorter than its header",
                    entry.hash
                ))
            })?;
            let block_hash = u128::from_le_bytes(header[..16].try_into().map_err(|_| {
                ChunkedError::CorruptChunkIndex("window-delta header".to_string())
            })?);
            let start = u32::from_le_bytes(header[16..20].try_into().map_err(|_| {
                ChunkedError::CorruptChunkIndex("window-delta header".to_string())
            })?);
            let count = u32::from_le_bytes(header[20..24].try_into().map_err(|_| {
                ChunkedError::CorruptChunkIndex("window-delta header".to_string())
            })?);
            let dict = self.window_dict(block_hash, start, count)?.ok_or_else(|| {
                ChunkedError::CorruptChunkIndex(format!(
                    "window-delta chunk {:x} references an empty window",
                    entry.hash
                ))
            })?;
            return Ok(decompress_with_dict(
                &payload[24..],
                location.raw_len as usize,
                &dict,
            )?);
        }
        if location.codec == CodecId::ZSTD_DICT {
            // Payload = [16-byte dictionary hash LE][zstd frame].
            let hash_bytes: [u8; 16] = payload.get(..16).and_then(|b| b.try_into().ok()).ok_or(
                ChunkedError::CorruptChunkIndex(format!(
                    "dictionary chunk {:x} payload shorter than its dictionary header",
                    entry.hash
                )),
            )?;
            let dict = self.load_dict(u128::from_le_bytes(hash_bytes))?;
            return Ok(decompress_with_dict(
                &payload[16..],
                location.raw_len as usize,
                &dict,
            )?);
        }
        Ok(decode_chunk(
            location.codec,
            &payload,
            location.raw_len as usize,
        )?)
    }

    /// Read a block's complete bytes (transfer packing, tests, fsck).
    pub fn read_block_bytes(&self, block_hash: u128) -> Result<Vec<u8>, OxenError> {
        self.io.read_block(block_hash)
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
        if manifest.transform_id != TransformId::IDENTITY {
            let original = self.reconstruct_original_bytes(manifest)?;
            writer.write_all(&original)?;
            return Ok(());
        }
        for batch in manifest.chunks.chunks(RECONSTRUCT_BATCH) {
            for raw in self.read_chunk_batch(batch)? {
                writer.write_all(&raw)?;
            }
        }
        Ok(())
    }

    /// Decode a run of manifest entries with rayon, preserving order. Decode is
    /// CPU-bound (zstd) while block reads hit warm page cache, so batches scale
    /// near-linearly with cores.
    pub(super) fn read_chunk_batch(
        &self,
        entries: &[ChunkEntry],
    ) -> Result<Vec<Vec<u8>>, OxenError> {
        use rayon::prelude::*;
        if entries.len() <= 1 {
            return entries.iter().map(|e| self.read_chunk(e)).collect();
        }
        entries.par_iter().map(|e| self.read_chunk(e)).collect()
    }

    /// The stored (post-transform) chunk stream a manifest describes, in memory.
    fn reconstruct_stored_stream(&self, manifest: &ChunkManifest) -> Result<Vec<u8>, OxenError> {
        let mut out = Vec::with_capacity(manifest.file_size as usize);
        for batch in manifest.chunks.chunks(RECONSTRUCT_BATCH) {
            for raw in self.read_chunk_batch(batch)? {
                out.extend_from_slice(&raw);
            }
        }
        Ok(out)
    }

    /// The complete original file bytes for a manifest, applying the inverse
    /// transform when one was recorded. Materializes the whole file; transformed
    /// files are bounded by the ingest-side input cap.
    pub fn reconstruct_original_bytes(
        &self,
        manifest: &ChunkManifest,
    ) -> Result<Vec<u8>, OxenError> {
        let stored = self.reconstruct_stored_stream(manifest)?;
        match manifest.transform_id {
            TransformId::IDENTITY => Ok(stored),
            TransformId::ZSTD_UNWRAP_V1 => Ok(unwrap_inverse(&stored)?),
            other => Err(ChunkedError::UnknownTransformId(other.as_u8()).into()),
        }
    }

    /// Read `len` bytes at `offset` of the reconstructed file — a binary search for
    /// the covering chunks plus partial chunk reads, never a whole-file pass. Reads
    /// past EOF truncate (like `pread`).
    ///
    /// Transformed manifests reconstruct the whole file to serve the range
    /// (offsets address the original file, which chunk offsets no longer tile) —
    /// a known read amplification bounded by the transform's ingest cap.
    pub fn read_range(
        &self,
        manifest: &ChunkManifest,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, OxenError> {
        if manifest.transform_id != TransformId::IDENTITY {
            let original = self.reconstruct_original_bytes(manifest)?;
            let end = offset.saturating_add(len).min(original.len() as u64);
            if offset >= end {
                return Ok(Vec::new());
            }
            return Ok(original[offset as usize..end as usize].to_vec());
        }
        let end = offset.saturating_add(len).min(manifest.file_size);
        if offset >= end {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity((end - offset) as usize);
        let mut pos = offset;
        while pos < end {
            let entry = manifest.chunk_at(pos).ok_or_else(|| {
                ChunkedError::InvalidManifest(format!(
                    "no chunk covers offset {pos} of {}",
                    manifest.file_size
                ))
            })?;
            let raw = self.read_chunk(entry)?;
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

        for &chunk_hash in hashes {
            if !packed.insert(chunk_hash) {
                continue;
            }
            let location = self
                .index
                .get(chunk_hash)?
                .ok_or(ChunkedError::MissingChunk { chunk_hash })?;
            // Dictionary-compressed chunks never cross the wire (the receiver has
            // no dictionary context): re-encode them as plain zstd for transfer.
            let encoded =
                if location.codec == CodecId::ZSTD_DICT
                    || location.codec == CodecId::ZSTD_DELTA
                    || location.codec == CodecId::ZSTD_WINDOW_DELTA
                {
                    let raw = self.read_chunk(&ChunkEntry {
                        hash: chunk_hash,
                        offset: 0,
                        len: location.raw_len,
                    })?;
                    encode_chunk(CodecId::ZSTD, &raw)?
                } else {
                    let payload = self.io.read_block_range(
                        location.block_hash,
                        location.offset as u64,
                        location.stored_len as u64,
                    )?;
                    EncodedChunk {
                        codec: location.codec,
                        data: payload,
                    }
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

    /// Rebuild the chunk index from block footers: validate every stored block,
    /// then atomically replace the live index with the complete scan result.
    ///
    /// Returns the number of blocks scanned. The index is derived state, so this is
    /// always safe; a block whose bytes no longer match its name is reported as
    /// corrupt rather than indexed.
    pub fn rebuild_index(&self) -> Result<u64, OxenError> {
        let block_hashes = self.list_blocks()?;
        let num_blocks = block_hashes.len() as u64;
        let blocks = block_hashes.into_iter().map(|block_hash| {
            let data = self.io.read_block(block_hash)?;
            let actual = hash_buffer_128bit(&data);
            if actual != block_hash {
                return Err(ChunkedError::BlockHashMismatch {
                    expected: block_hash,
                    actual,
                }
                .into());
            }
            let chunks = parse_block_footer(&data)?;
            Ok((block_hash, chunks))
        });
        // `replace_blocks` consumes the scan inside one LMDB write transaction.
        // Its clear is invisible until commit, and any scan/index error aborts
        // the transaction, leaving the prior live index intact.
        self.index.replace_blocks(blocks)?;
        Ok(num_blocks)
    }

    /// Every stored block's hash.
    pub fn list_blocks(&self) -> Result<Vec<u128>, OxenError> {
        self.io.list_blocks()
    }
}

/// Content superfeatures for delta-candidate discovery: one rolling gear-hash
/// pass over the chunk, keeping the minimum of three independently permuted hash
/// values (min-of-permutation ≈ MinHash over ~64-byte content windows). Two
/// chunks sharing a substantial run of content almost surely share each
/// minimizing window, wherever that run sits in either chunk — which is exactly
/// the match the positional prefix/mid/suffix sketches cannot see.
fn minhash_features(raw: &[u8]) -> [u64; 3] {
    // Deterministic pseudo-random gear table (splitmix64): sketch values must be
    // stable across processes because the sketch table persists.
    static GEAR: std::sync::LazyLock<[u64; 256]> = std::sync::LazyLock::new(|| {
        let mut table = [0u64; 256];
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for slot in table.iter_mut() {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            *slot = z ^ (z >> 31);
        }
        table
    });
    const SALTS: [u64; 3] = [
        0x9E37_79B9_7F4A_7C15,
        0xC2B2_AE3D_27D4_EB4F,
        0x1656_67B1_9E37_79F9,
    ];
    let gear = &*GEAR;
    let mut h = 0u64;
    let mut mins = [u64::MAX; 3];
    for &byte in raw {
        h = (h << 1).wrapping_add(gear[byte as usize]);
        for (min, salt) in mins.iter_mut().zip(SALTS) {
            let v = h.wrapping_mul(salt);
            if v < *min {
                *min = v;
            }
        }
    }
    mins
}

/// Encode a chunk, preferring the store's shared dictionary for zstd-eligible
/// content. Falls through to the plain codec (with its universal raw fallback)
/// when there is no dictionary or the dictionary form is not strictly smaller.
fn encode_dict_chunk(
    codec: CodecId,
    raw: &[u8],
    dict: Option<&(u128, Arc<PreparedDict>)>,
) -> Result<EncodedChunk, ChunkedError> {
    if codec == CodecId::ZSTD
        && let Some((dict_hash, dict_bytes)) = dict
    {
        let compressed = compress_with_dict(raw, dict_bytes)?;
        if compressed.len() + 16 < raw.len() {
            let mut data = Vec::with_capacity(16 + compressed.len());
            data.extend_from_slice(&dict_hash.to_le_bytes());
            data.extend_from_slice(&compressed);
            return Ok(EncodedChunk {
                codec: CodecId::ZSTD_DICT,
                data,
            });
        }
    }
    encode_chunk(codec, raw)
}

/// A sync [`Read`] over the reconstructed bytes of a chunked version, decoding one
/// chunk at a time. Owns its engine handle, so it can move into a `spawn_blocking`
/// or feed `AtomicFile::stream` from any thread.
pub struct ReconstructReader {
    engine: Arc<BlockEngine>,
    manifest: ChunkManifest,
    next_chunk: usize,
    /// Chunks decoded ahead by the last parallel batch, in file order.
    decoded: std::collections::VecDeque<Vec<u8>>,
    buf: Vec<u8>,
    buf_pos: usize,
}

impl ReconstructReader {
    pub fn new(engine: Arc<BlockEngine>, manifest: ChunkManifest) -> Self {
        Self {
            engine,
            manifest,
            next_chunk: 0,
            decoded: std::collections::VecDeque::new(),
            buf: Vec::new(),
            buf_pos: 0,
        }
    }
}

impl Read for ReconstructReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        // A transformed manifest yields the original file, materialized once into
        // the buffer (bounded by the transform's ingest cap).
        if self.manifest.transform_id != TransformId::IDENTITY
            && self.next_chunk == 0
            && self.buf.is_empty()
        {
            self.buf = self
                .engine
                .reconstruct_original_bytes(&self.manifest)
                .map_err(std::io::Error::other)?;
            self.buf_pos = 0;
            self.next_chunk = self.manifest.chunks.len();
        }
        while self.buf_pos == self.buf.len() {
            if let Some(decoded) = self.decoded.pop_front() {
                self.buf = decoded;
                self.buf_pos = 0;
                continue;
            }
            if self.next_chunk >= self.manifest.chunks.len() {
                return Ok(0); // clean EOF
            }
            let end = (self.next_chunk + RECONSTRUCT_BATCH).min(self.manifest.chunks.len());
            let batch = &self.manifest.chunks[self.next_chunk..end];
            self.decoded = self
                .engine
                .read_chunk_batch(batch)
                .map_err(std::io::Error::other)?
                .into();
            self.next_chunk = end;
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
        encode_policy(None, &EntryDataType::Tabular, "csv")
    }

    /// The first ingest of a snapshot whose rows share large verbatim regions
    /// deltas later rows against earlier rows in the same pass — the sketch
    /// table alone only covers prior ingests.
    #[test]
    fn same_pass_rows_delta_against_each_other() -> Result<(), OxenError> {
        let t = test_engine();
        // 40 "request log" rows: unique 40-byte head, shared 30KB middle,
        // unique tail — both end sketches differ, the midpoint matches.
        let shared = "tools-and-system-block ".repeat(1400);
        let rows: String = (0..40)
            .map(|i| format!("{{\"session\":\"{i:032}\",\"body\":\"{shared}\",\"turn\":\"reply number {i}\"}}\n"))
            .collect();
        let manifest = ingest(&t.engine, rows.as_bytes());

        let delta_chunks = manifest
            .chunks
            .iter()
            .filter_map(|e| t.engine.index().get(e.hash).transpose())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|loc| loc.codec == CodecId::ZSTD_DELTA)
            .count();
        assert!(
            delta_chunks >= manifest.chunks.len() / 2,
            "expected most same-pass near-duplicate chunks to delta ({delta_chunks}/{})",
            manifest.chunks.len()
        );
        assert_eq!(&reconstruct(&t.engine, &manifest), rows.as_bytes());
        Ok(())
    }

    /// Content dispersed across a prior version's block (a shuffled snapshot of
    /// the same records) window-delta-encodes against a block window, round-trips
    /// exactly, and is re-encoded to plain zstd for transfer.
    #[test]
    fn window_delta_round_trip() -> Result<(), OxenError> {
        let t = test_engine();
        // Version 1: ~2MB of distinct ~300-byte records. Version 2: the same
        // records in a seeded-shuffled order — every v2 chunk's content is
        // dispersed across v1's whole block, which no single ≤128KB base covers.
        let mut records: Vec<String> = (0..7000)
            .map(|i| {
                let mut state = 0x9E37_79B9u64 ^ (i as u64) << 17;
                state ^= state << 13;
                state ^= state >> 7;
                format!(
                    "record {i:05} payload {state:016x} {}\n",
                    "lorem ipsum dolor sit amet consectetur ".repeat(6)
                )
            })
            .collect();
        let v1: String = records.concat();
        // Seeded Fisher-Yates so the shuffle is deterministic.
        let mut state = 0xC0FF_EE00_D15E_A5E5u64;
        for i in (1..records.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            records.swap(i, (state as usize) % (i + 1));
        }
        let v2: String = records.concat();

        let m1 = ingest(&t.engine, v1.as_bytes());
        let m2 = ingest(&t.engine, v2.as_bytes());

        let window_chunks = m2
            .chunks
            .iter()
            .filter_map(|e| t.engine.index().get(e.hash).transpose())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|loc| loc.codec == CodecId::ZSTD_WINDOW_DELTA)
            .count();
        assert!(
            window_chunks > 0,
            "shuffled snapshot must produce window-delta chunks"
        );
        assert_eq!(&reconstruct(&t.engine, &m1), v1.as_bytes());
        assert_eq!(&reconstruct(&t.engine, &m2), v2.as_bytes());

        // A cold engine (fresh caches) decodes window deltas too.
        let cold = BlockEngine::open(
            &t._dir.path().join("blocks"),
            &t._dir.path().join("chunk_index"),
        )?;
        assert_eq!(&reconstruct(&cold, &m2), v2.as_bytes());

        // Transfer packing re-encodes: window deltas never cross the wire.
        let hashes: Vec<u128> = m2.chunks.iter().map(|c| c.hash).collect();
        let receiver = test_engine();
        for block in t.engine.pack_chunks(&hashes)? {
            let parsed = verify_block(&block.data)?;
            assert!(
                parsed.iter().all(|c| c.codec != CodecId::ZSTD_WINDOW_DELTA),
                "window-delta chunks must not cross the wire"
            );
            receiver.engine.store_block(block.hash, block.data)?;
        }
        assert_eq!(&reconstruct(&receiver.engine, &m2), v2.as_bytes());
        Ok(())
    }

    /// A container with embedded zstd frames ingests through the unwrap
    /// transform (manifest records it) and reconstructs byte-for-byte through
    /// every read path.
    #[test]
    fn unwrap_transform_round_trip() -> Result<(), OxenError> {
        use crate::model::EntryDataType;
        use crate::storage::chunked::policy::encode_policy;
        use crate::storage::chunked::registry::TransformId;

        let t = test_engine();
        let page: Vec<u8> = csv_bytes(91, 400 * 1024);
        let mut container = b"PAR1".to_vec();
        for part in page.chunks(64 * 1024) {
            container.extend(zstd::bulk::compress(part, 1).map_err(ChunkedError::Compress)?);
            container.extend_from_slice(b"page-header-bytes");
        }
        container.extend_from_slice(b"footer PAR1");

        let policy = encode_policy(None, &EntryDataType::Tabular, "parquet");
        let manifest = {
            let mut reader: &[u8] = &container;
            t.engine.ingest(&mut reader, &policy)?
        };
        assert_eq!(manifest.transform_id, TransformId::ZSTD_UNWRAP_V1);
        assert_eq!(
            manifest.file_hash.to_u128(),
            hash_buffer_128bit(&container),
            "manifest identity must be the original file's hash"
        );

        // Full reconstruction, range reads, and the streaming reader all yield
        // the original container bytes.
        assert_eq!(reconstruct(&t.engine, &manifest), container);
        let range = t.engine.read_range(&manifest, 2, 100)?;
        assert_eq!(&range, &container[2..102]);
        let past_end = t
            .engine
            .read_range(&manifest, container.len() as u64 + 5, 10)?;
        assert!(past_end.is_empty());
        Ok(())
    }

    /// Near-duplicate chunks across versions delta-encode against a
    /// sketch-matched base, round-trip exactly, respect the depth-one chain
    /// bound, and are re-encoded for transfer.
    #[test]
    fn delta_encoding_round_trip() -> Result<(), OxenError> {
        let t = test_engine();
        // Version 1: a large file. Version 2: same content with a small edit in
        // the middle of each 64KB region — near-duplicate chunks, same prefixes.
        let v1 = csv_bytes(3, 512 * 1024);
        let mut v2 = v1.clone();
        for pos in (32 * 1024..v2.len()).step_by(64 * 1024) {
            v2[pos] = b'#';
        }
        let m1 = ingest(&t.engine, &v1);
        let m2 = ingest(&t.engine, &v2);

        let delta_chunks = m2
            .chunks
            .iter()
            .filter_map(|e| t.engine.index().get(e.hash).transpose())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|loc| loc.codec == CodecId::ZSTD_DELTA)
            .count();
        assert!(delta_chunks > 0, "expected delta-encoded chunks");

        for (m, d) in [(&m1, &v1), (&m2, &v2)] {
            assert_eq!(&reconstruct(&t.engine, m), d);
        }

        // Depth bound: every delta chunk's base is a non-delta chunk.
        for e in &m2.chunks {
            if let Some(loc) = t.engine.index().get(e.hash)? {
                if loc.codec == CodecId::ZSTD_DELTA {
                    let payload =
                        t.engine
                            .io
                            .read_block_range(loc.block_hash, loc.offset as u64, 16)?;
                    let base_hash =
                        u128::from_le_bytes(payload.as_slice().try_into().expect("16-byte header"));
                    let base = t
                        .engine
                        .index()
                        .get(base_hash)?
                        .expect("delta base must be indexed");
                    assert_ne!(base.codec, CodecId::ZSTD_DELTA, "chain depth > 1");
                }
            }
        }

        // Transfer packing inflates deltas: a fresh store can verify and read.
        let hashes: Vec<u128> = m2.chunks.iter().map(|c| c.hash).collect();
        let receiver = test_engine();
        for block in t.engine.pack_chunks(&hashes)? {
            let parsed = verify_block(&block.data)?;
            assert!(parsed.iter().all(|c| c.codec != CodecId::ZSTD_DELTA));
            receiver.engine.store_block(block.hash, block.data)?;
        }
        assert_eq!(&reconstruct(&receiver.engine, &m2), &v2);
        Ok(())
    }

    /// The shared-dictionary lifecycle: a large-enough first ingest trains and
    /// publishes a dictionary, chunks encode against it, reads round-trip, a
    /// second engine instance (cold cache) reads the same store, and transfer
    /// packing re-encodes dictionary chunks so they never cross the wire.
    #[test]
    fn shared_dictionary_round_trip() -> Result<(), OxenError> {
        let t = test_engine();
        let data = csv_bytes(42, DICT_MIN_SAMPLE + 64 * 1024);
        let manifest = {
            let mut reader: &[u8] = &data;
            t.engine.ingest(&mut reader, &csv_policy())?
        };

        // A dictionary was trained and published.
        assert!(
            t.engine
                .io
                .current_dictionary(csv_policy().chunker.as_u8())?
                .is_some()
        );
        // At least some chunks encoded against it.
        let dict_chunks = manifest
            .chunks
            .iter()
            .filter_map(|e| t.engine.index().get(e.hash).transpose())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|loc| loc.codec == CodecId::ZSTD_DICT)
            .count();
        assert!(dict_chunks > 0, "expected dictionary-compressed chunks");

        // Reads reconstruct the exact bytes — including through a fresh engine
        // with a cold dictionary cache.
        let mut out = Vec::new();
        t.engine.reconstruct_to(&manifest, &mut out)?;
        assert_eq!(out, data);
        let cold = BlockEngine::open(
            &t._dir.path().join("blocks"),
            &t._dir.path().join("chunk_index"),
        )?;
        let mut out = Vec::new();
        cold.reconstruct_to(&manifest, &mut out)?;
        assert_eq!(out, data);

        // Transfer packing re-encodes: no packed chunk claims the dict codec, and
        // a store without the dictionary can verify and read the packed blocks.
        let hashes: Vec<u128> = manifest.chunks.iter().map(|c| c.hash).collect();
        let receiver = test_engine();
        for block in t.engine.pack_chunks(&hashes)? {
            let parsed = verify_block(&block.data)?;
            assert!(
                parsed.iter().all(|c| c.codec != CodecId::ZSTD_DICT),
                "dictionary chunks must not cross the wire"
            );
            receiver.engine.store_block(block.hash, block.data)?;
        }
        let mut out = Vec::new();
        receiver.engine.reconstruct_to(&manifest, &mut out)?;
        assert_eq!(out, data);
        Ok(())
    }

    /// An ingest below the training floor stays dictionary-free and readable; a
    /// later large ingest trains the dictionary for subsequent writes.
    #[test]
    fn small_first_ingest_defers_dictionary() -> Result<(), OxenError> {
        let t = test_engine();
        let small = csv_bytes(7, 64 * 1024);
        let m1 = {
            let mut r: &[u8] = &small;
            t.engine.ingest(&mut r, &csv_policy())?
        };
        assert!(
            t.engine
                .io
                .current_dictionary(csv_policy().chunker.as_u8())?
                .is_none()
        );

        let big = csv_bytes(8, DICT_MIN_SAMPLE + 32 * 1024);
        let m2 = {
            let mut r: &[u8] = &big;
            t.engine.ingest(&mut r, &csv_policy())?
        };
        assert!(
            t.engine
                .io
                .current_dictionary(csv_policy().chunker.as_u8())?
                .is_some()
        );

        for (m, d) in [(&m1, &small), (&m2, &big)] {
            let mut out = Vec::new();
            t.engine.reconstruct_to(m, &mut out)?;
            assert_eq!(&out, d);
        }
        Ok(())
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
        for block_hash in &blocks {
            let len = t.engine.read_block_bytes(*block_hash)?.len() as u64;
            assert!(len <= MAX_BLOCK_SIZE);
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

    /// A rebuild validates all durable blocks before replacing the live index.
    /// One corrupt block must not turn an otherwise usable index into an empty
    /// or partially rebuilt one.
    #[test]
    fn failed_index_rebuild_preserves_live_index() -> Result<(), OxenError> {
        let t = test_engine();
        let data = csv_bytes(59, 2 * 1024 * 1024);
        let manifest = ingest(&t.engine, &data);
        let blocks = t.engine.list_blocks()?;
        assert_eq!(blocks.len(), 1, "fixture should produce one block");

        let chunk_hash = manifest.chunks[0].hash;
        let before = t.engine.index().get(chunk_hash)?;
        assert!(before.is_some());

        let block_path = LocalBlockIo::new(t._dir.path().join("blocks")).block_path(blocks[0]);
        std::fs::write(block_path, b"corrupt block")?;

        assert!(t.engine.rebuild_index().is_err());
        assert_eq!(
            t.engine.index().get(chunk_hash)?,
            before,
            "failed rebuild replaced the live index"
        );
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
        let hashes: Vec<u128> = manifest.chunks.iter().map(|c| c.hash).collect();
        for block in source.engine.pack_chunks(&hashes)? {
            // Wrong expected hash is rejected before anything is written.
            assert!(matches!(
                dest.engine.store_block(block.hash ^ 1, block.data.clone()),
                Err(OxenError::ChunkedError(
                    ChunkedError::BlockHashMismatch { .. }
                ))
            ));

            dest.engine.store_block(block.hash, block.data.clone())?;
            // Idempotent: storing the same block again succeeds.
            dest.engine.store_block(block.hash, block.data)?;
        }
        assert_eq!(reconstruct(&dest.engine, &manifest), data);
        Ok(())
    }
}
