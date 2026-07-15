//! [`BlockByteIo`]: the raw block byte IO a [`BlockEngine`] runs over.
//!
//! The engine's chunking, packing, indexing, and reconstruction logic is shared
//! across storage backends; only how block bytes are read and written differs —
//! local file ranges here, S3 ranged GETs / PUTs in the S3 backend. The trait is
//! sync on purpose (the engine is the sync core; every entry point is wrapped in
//! one `spawn_blocking` at the operation boundary per `docs/async_policy.md`), so
//! a network-backed implementation bridges from its blocking thread to async IO
//! internally.
//!
//! [`BlockEngine`]: super::block_engine::BlockEngine

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::util;
use crate::util::fs::AtomicFile;

use super::error::ChunkedError;

/// File name of a local block payload within its hash directory (mirrors the
/// version store's `data` convention).
const BLOCK_DATA_FILE: &str = "data";

/// Raw byte IO for immutable, content-addressed blocks.
///
/// Implementations must be **called from blocking-capable threads only** (inside
/// `spawn_blocking`): a network-backed implementation may block on internal async
/// IO. Blocks are immutable once published, so no method ever mutates an existing
/// block.
pub trait BlockByteIo: Send + Sync + std::fmt::Debug + 'static {
    /// Durably publish a sealed block's bytes under its content hash. The caller
    /// has already verified `data` hashes to `block_hash`. Idempotent.
    fn put_block(&self, block_hash: u128, data: &Bytes) -> Result<(), OxenError>;

    /// Read `len` bytes at `offset` of a block's payload.
    fn read_block_range(
        &self,
        block_hash: u128,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, OxenError>;

    /// Read a block's complete bytes.
    fn read_block(&self, block_hash: u128) -> Result<Vec<u8>, OxenError>;

    /// Every block present in this store.
    fn list_blocks(&self) -> Result<Vec<u128>, OxenError>;
}

/// Local-filesystem block IO. Layout:
/// `{blocks_dir}/{hash[..2]}/{hash[2..]}/data`, with block hashes rendered as
/// fixed-width 32-digit hex so the two-level fan-out is uniform.
#[derive(Debug)]
pub struct LocalBlockIo {
    blocks_dir: PathBuf,
    /// The most recently used block file handle. Consecutive chunks of a file
    /// land consecutively in blocks, so sequential reconstruction reuses one
    /// handle instead of reopening per chunk.
    open_block: Mutex<Option<(u128, File)>>,
}

impl LocalBlockIo {
    pub fn new(blocks_dir: PathBuf) -> Self {
        Self {
            blocks_dir,
            open_block: Mutex::new(None),
        }
    }

    /// The on-disk path of a block.
    pub fn block_path(&self, block_hash: u128) -> PathBuf {
        let hex = format!("{block_hash:032x}");
        self.blocks_dir
            .join(&hex[..2])
            .join(&hex[2..])
            .join(BLOCK_DATA_FILE)
    }
}

impl BlockByteIo for LocalBlockIo {
    fn put_block(&self, block_hash: u128, data: &Bytes) -> Result<(), OxenError> {
        AtomicFile::new(self.block_path(block_hash))
            .with_hash(MerkleHash::new(block_hash))
            .write(data)
    }

    fn read_block_range(
        &self,
        block_hash: u128,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, OxenError> {
        let io_err = |source| ChunkedError::BlockRead { block_hash, source };

        let mut guard = self.open_block.lock();
        if guard.as_ref().map(|(hash, _)| *hash) != Some(block_hash) {
            let file = File::open(self.block_path(block_hash)).map_err(io_err)?;
            *guard = Some((block_hash, file));
        }
        // The branch above guarantees an open handle; re-match instead of unwrap.
        let Some((_, file)) = guard.as_mut() else {
            return Err(OxenError::basic_str(
                "open block handle unexpectedly missing",
            ));
        };
        let mut payload = vec![0u8; len as usize];
        file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
        file.read_exact(&mut payload).map_err(io_err)?;
        Ok(payload)
    }

    fn read_block(&self, block_hash: u128) -> Result<Vec<u8>, OxenError> {
        util::fs::read_bytes_from_path(self.block_path(block_hash))
    }

    fn list_blocks(&self) -> Result<Vec<u128>, OxenError> {
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
                if block_entry.path().join(BLOCK_DATA_FILE).exists() {
                    blocks.push(block_hash);
                }
            }
        }
        Ok(blocks)
    }
}
