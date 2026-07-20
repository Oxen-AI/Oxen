//! The store-local chunk index: `chunk_hash → (block_hash, offset, stored_len,
//! raw_len, codec)` in LMDB.
//!
//! **Disposable, rebuildable derived state — never authoritative.** Blocks are
//! self-describing (their footers carry every fact stored here), so a lost or
//! corrupt index means rebuild-before-read, never data loss. It answers dedup
//! probes at add time, `missing_chunks` during push negotiation, and placement
//! lookups during reconstruction and transfer packing.
//!
//! Placement lives *only* here — manifests are pure content and never name blocks —
//! so a store may repack or GC its blocks without touching any manifest.

use std::path::Path;
use std::sync::Arc;

use bytesize::ByteSize;

use crate::error::OxenError;
use crate::lmdb::store::LmdbStore;
use crate::lmdb::{
    LmdbDb, LmdbEnv, LmdbEnvConfig, compact_lmdb_env_in_place, open_db, open_shared_env,
};

use super::block::BlockChunk;
use super::error::ChunkedError;
use super::registry::CodecId;

/// One database in the env holds every chunk location.
const CHUNKS_DB_NAME: &str = "chunks";
/// A second database maps prefix sketches to a candidate base chunk for delta
/// encoding. Advisory derived state: losing it only costs future delta hits.
const SKETCHES_DB_NAME: &str = "sketches";
/// Manifest lineage bases: the first-chunk hash of a file's manifests maps to
/// the blob hash of the last full-stored manifest with that first chunk, the
/// dictionary base for delta-compressing successor manifests at rest. Advisory:
/// losing it only costs future manifest-delta hits.
const MANIFEST_BASES_DB_NAME: &str = "manifest_bases";
const MAX_DBS: u32 = 3;
/// Sparse upper bound on the mapped size. Sized from the math (~53 B/entry ⇒
/// ~8.5 GB of entries for a 10 TB repo at 64 KiB average chunks), with headroom for
/// LMDB page overhead — not copied from the merkle store's 256 GiB.
const CHUNK_INDEX_MAP_SIZE: ByteSize = ByteSize::gib(32);
/// Reader-lock-table size for this env. The lock file is persistent storage overhead
/// proportional to `max_readers`, so this is sized to plausible concurrency for one
/// store (not the crate-wide 1024 default): the CLI holds a handful of readers, the
/// server a few per in-flight request.
const CHUNK_INDEX_MAX_READERS: u32 = 256;

/// Serialized size of one [`ChunkLocation`] value:
/// block_hash u128 + offset u32 + stored_len u32 + raw_len u32 + codec u8.
const LOCATION_SIZE: usize = 16 + 4 + 4 + 4 + 1;

/// Where one chunk's payload lives: which block, where in it, and how it's encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLocation {
    /// Content hash of the block holding the payload.
    pub block_hash: u128,
    /// Byte offset of the stored payload within the block.
    pub offset: u32,
    /// Stored (possibly compressed) payload length in bytes.
    pub stored_len: u32,
    /// Raw (uncompressed) chunk length in bytes.
    pub raw_len: u32,
    /// The codec the payload was encoded with.
    pub codec: CodecId,
}

impl ChunkLocation {
    fn to_bytes(self) -> [u8; LOCATION_SIZE] {
        let mut buf = [0u8; LOCATION_SIZE];
        buf[..16].copy_from_slice(&self.block_hash.to_le_bytes());
        buf[16..20].copy_from_slice(&self.offset.to_le_bytes());
        buf[20..24].copy_from_slice(&self.stored_len.to_le_bytes());
        buf[24..28].copy_from_slice(&self.raw_len.to_le_bytes());
        buf[28] = self.codec.as_u8();
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, ChunkedError> {
        if bytes.len() != LOCATION_SIZE {
            return Err(ChunkedError::CorruptChunkIndex(format!(
                "location record of {} bytes, expected {LOCATION_SIZE}",
                bytes.len()
            )));
        }
        Ok(Self {
            block_hash: u128::from_le_bytes(bytes[..16].try_into().map_err(|_| {
                ChunkedError::CorruptChunkIndex("location record truncated".to_string())
            })?),
            offset: u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
            stored_len: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
            raw_len: u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]),
            codec: CodecId::from_u8(bytes[28])?,
        })
    }
}

/// LMDB-backed chunk index. Sync on purpose — callers bridge to async with one
/// `spawn_blocking` per operation at the operation boundary.
pub struct ChunkIndex {
    env: Arc<LmdbEnv>,
    db: LmdbDb,
    sketches: LmdbDb,
    manifest_bases: LmdbDb,
}

impl std::fmt::Debug for ChunkIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkIndex").finish_non_exhaustive()
    }
}

impl ChunkIndex {
    fn env_config() -> LmdbEnvConfig {
        LmdbEnvConfig {
            map_size: CHUNK_INDEX_MAP_SIZE,
            max_dbs: MAX_DBS,
            max_readers: CHUNK_INDEX_MAX_READERS,
        }
    }

    /// Open (creating if absent) the chunk index env at `dir`.
    pub fn open(dir: &Path) -> Result<Self, OxenError> {
        let config = Self::env_config();
        let env = open_shared_env(dir, &config)?;
        let db = open_db(&env, CHUNKS_DB_NAME)?;
        let sketches = open_db(&env, SKETCHES_DB_NAME)?;
        let manifest_bases = open_db(&env, MANIFEST_BASES_DB_NAME)?;
        Ok(Self {
            env,
            db,
            sketches,
            manifest_bases,
        })
    }

    /// The lineage-base blob for manifests whose first chunk is `first_chunk`.
    pub fn manifest_base(&self, first_chunk: u128) -> Result<Option<u128>, OxenError> {
        let bases = &self.manifest_bases;
        self.read(|_db, txn| {
            Ok(bases
                .get(txn, &first_chunk.to_le_bytes())?
                .and_then(|bytes| {
                    <[u8; 16]>::try_from(bytes.as_ref())
                        .ok()
                        .map(u128::from_le_bytes)
                }))
        })
    }

    /// Point manifests whose first chunk is `first_chunk` at a new lineage base.
    pub fn set_manifest_base(&self, first_chunk: u128, blob_hash: u128) -> Result<(), OxenError> {
        let bases = &self.manifest_bases;
        self.write(|_db, txn| {
            bases.put(txn, &first_chunk.to_le_bytes(), &blob_hash.to_le_bytes())?;
            Ok(())
        })
    }

    /// A previously indexed chunk whose sketch matches, as a delta-base
    /// candidate. Most recently indexed wins; purely advisory.
    pub fn sketch_candidate(&self, sketch: u64) -> Result<Option<u128>, OxenError> {
        let sketches = &self.sketches;
        self.read(|_db, txn| {
            Ok(sketches.get(txn, &sketch.to_le_bytes())?.and_then(|bytes| {
                <[u8; 16]>::try_from(bytes.as_ref())
                    .ok()
                    .map(u128::from_le_bytes)
            }))
        })
    }

    /// Record sketches for newly stored chunks. Last write wins: recent chunks
    /// make better delta bases than old ones (less version drift).
    pub fn insert_sketches(&self, entries: &[(u64, u128)]) -> Result<(), OxenError> {
        if entries.is_empty() {
            return Ok(());
        }
        let sketches = &self.sketches;
        self.write(|_db, txn| {
            for (sketch, chunk_hash) in entries {
                sketches.put(txn, &sketch.to_le_bytes(), &chunk_hash.to_le_bytes())?;
            }
            Ok(())
        })
    }

    /// The location of `chunk_hash`, if this store has its payload in a block.
    pub fn get(&self, chunk_hash: u128) -> Result<Option<ChunkLocation>, OxenError> {
        self.read(|db, txn| {
            db.get(txn, &chunk_hash.to_le_bytes())?
                .map(|bytes| Ok(ChunkLocation::from_bytes(&bytes)?))
                .transpose()
        })
    }

    /// Whether this store has `chunk_hash`.
    pub fn contains(&self, chunk_hash: u128) -> Result<bool, OxenError> {
        self.read(|db, txn| Ok(db.contains(txn, &chunk_hash.to_le_bytes())?))
    }

    /// The subset of `hashes` this store does not have, in input order (used for
    /// dedup probes and push negotiation).
    pub fn missing(&self, hashes: &[u128]) -> Result<Vec<u128>, OxenError> {
        self.read(|db, txn| {
            let mut missing = Vec::new();
            for hash in hashes {
                if !db.contains(txn, &hash.to_le_bytes())? {
                    missing.push(*hash);
                }
            }
            Ok(missing)
        })
    }

    /// Index every chunk of a durable block, in one atomic commit.
    ///
    /// First location wins: a chunk already indexed (e.g. co-packed into a block by
    /// a concurrent writer) keeps its canonical location, and the duplicate payload
    /// is left for GC. Call only after the block is durably published — a crash
    /// between block publish and this call leaves a safe orphan block that rebuild
    /// discovers.
    pub fn insert_block(&self, block_hash: u128, chunks: &[BlockChunk]) -> Result<(), OxenError> {
        self.write(|db, txn| {
            for chunk in chunks {
                let key = chunk.chunk_hash.to_le_bytes();
                if db.contains(txn, &key)? {
                    continue;
                }
                let location = ChunkLocation {
                    block_hash,
                    offset: chunk.offset,
                    stored_len: chunk.stored_len,
                    raw_len: chunk.raw_len,
                    codec: chunk.codec,
                };
                db.put(txn, &key, &location.to_bytes())?;
            }
            Ok(())
        })
    }

    /// Atomically replace the complete index with locations parsed from a
    /// validated block scan. If clearing or inserting any entry fails, LMDB
    /// aborts the transaction and the previous live index remains intact.
    pub fn replace_blocks(
        &self,
        blocks: impl IntoIterator<Item = Result<(u128, Vec<BlockChunk>), OxenError>>,
    ) -> Result<(), OxenError> {
        self.write(|db, txn| {
            db.clear(txn)?;
            for block in blocks {
                let (block_hash, chunks) = block?;
                for chunk in chunks {
                    let key = chunk.chunk_hash.to_le_bytes();
                    if db.contains(txn, &key)? {
                        continue;
                    }
                    let location = ChunkLocation {
                        block_hash,
                        offset: chunk.offset,
                        stored_len: chunk.stored_len,
                        raw_len: chunk.raw_len,
                        codec: chunk.codec,
                    };
                    db.put(txn, &key, &location.to_bytes())?;
                }
            }
            Ok(())
        })
    }

    /// Reclaim the index's LMDB page slack: rebuild the data file with packed
    /// leaves and atomically swap it in (no-op when there is little to reclaim).
    /// Returns whether a rebuild happened.
    ///
    /// Call only at the end of a write batch: index writes committed through this
    /// process's live env after a rebuild are invisible to later opens of the
    /// path. The index is disposable derived state, so the worst case of a race
    /// is a rebuildable miss, never data loss.
    pub fn compact(&self) -> Result<bool, OxenError> {
        Ok(compact_lmdb_env_in_place(
            &self.env,
            &Self::env_config(),
            &[CHUNKS_DB_NAME, SKETCHES_DB_NAME, MANIFEST_BASES_DB_NAME],
        )?)
    }

    /// Number of indexed chunks.
    pub fn num_chunks(&self) -> Result<u64, OxenError> {
        self.read(|db, txn| Ok(db.len(txn)?))
    }

    /// Drop every entry in one atomic commit.
    pub fn clear(&self) -> Result<(), OxenError> {
        self.write(|db, txn| Ok(db.clear(txn)?))
    }
}

impl LmdbStore for ChunkIndex {
    fn lmdb_env(&self) -> &LmdbEnv {
        &self.env
    }

    fn lmdb_db(&self) -> &LmdbDb {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_index() -> (tempfile::TempDir, ChunkIndex) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let index = ChunkIndex::open(dir.path()).expect("open chunk index");
        (dir, index)
    }

    fn block_chunk(chunk_hash: u128, offset: u32) -> BlockChunk {
        BlockChunk {
            chunk_hash,
            offset,
            stored_len: 100,
            raw_len: 200,
            codec: CodecId::ZSTD,
        }
    }

    /// Locations round-trip; absent chunks read as None; `missing` partitions
    /// correctly; counts and clear behave.
    #[test]
    fn index_contract() -> Result<(), OxenError> {
        let (_dir, index) = test_index();

        assert_eq!(index.get(1)?, None);
        assert!(!index.contains(1)?);
        assert_eq!(index.missing(&[1, 2])?, vec![1, 2]);
        assert_eq!(index.num_chunks()?, 0);

        let block_hash = 0xB10C;
        index.insert_block(block_hash, &[block_chunk(1, 0), block_chunk(2, 100)])?;

        assert!(index.contains(1)?);
        assert_eq!(
            index.get(2)?,
            Some(ChunkLocation {
                block_hash,
                offset: 100,
                stored_len: 100,
                raw_len: 200,
                codec: CodecId::ZSTD,
            })
        );
        assert_eq!(index.missing(&[1, 2, 3])?, vec![3]);
        assert_eq!(index.num_chunks()?, 2);

        index.clear()?;
        assert_eq!(index.num_chunks()?, 0);
        assert_eq!(index.get(1)?, None);
        Ok(())
    }

    /// First location wins: re-indexing a chunk from another block does not move it.
    #[test]
    fn first_location_is_canonical() -> Result<(), OxenError> {
        let (_dir, index) = test_index();

        index.insert_block(0xAAAA, &[block_chunk(7, 0)])?;
        index.insert_block(0xBBBB, &[block_chunk(7, 500), block_chunk(8, 600)])?;

        let location = index.get(7)?.expect("chunk 7 indexed");
        assert_eq!(location.block_hash, 0xAAAA);
        assert_eq!(location.offset, 0);
        assert_eq!(index.get(8)?.expect("chunk 8 indexed").block_hash, 0xBBBB);
        Ok(())
    }

    /// Replacement is one LMDB transaction: an error after new entries have
    /// been inserted aborts both the clear and the partial replacement.
    #[test]
    fn failed_replace_preserves_the_complete_live_index() -> Result<(), OxenError> {
        let (_dir, index) = test_index();
        index.insert_block(0xAAAA, &[block_chunk(1, 0)])?;

        let replacements = vec![
            Ok((0xBBBB, vec![block_chunk(2, 0)])),
            Err(OxenError::basic_str("injected scan failure")),
        ];
        assert!(index.replace_blocks(replacements).is_err());

        assert_eq!(
            index.get(1)?.expect("old entry retained").block_hash,
            0xAAAA
        );
        assert_eq!(index.get(2)?, None, "partial replacement was committed");
        Ok(())
    }
}
