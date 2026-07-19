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

    /// Whether this store persists shared compression dictionaries. Backends
    /// without support keep working — the engine simply never trains or applies a
    /// dictionary, and every chunk stays a self-contained codec.
    fn supports_dictionaries(&self) -> bool {
        false
    }

    /// Durably publish a dictionary blob under its content hash. Idempotent.
    fn put_dictionary(&self, _dict_hash: u128, _data: &Bytes) -> Result<(), OxenError> {
        Err(OxenError::basic_str(
            "this block store does not support compression dictionaries",
        ))
    }

    /// Read a dictionary blob previously published with [`Self::put_dictionary`].
    fn read_dictionary(&self, _dict_hash: u128) -> Result<Vec<u8>, OxenError> {
        Err(OxenError::basic_str(
            "this block store does not support compression dictionaries",
        ))
    }

    /// The store's current dictionary for new writes of the given content class
    /// (dictionaries are per class: content trained on one format is useless for
    /// another). `None` when no dictionary has been published for the class.
    fn current_dictionary(&self, _class: u8) -> Result<Option<u128>, OxenError> {
        Ok(None)
    }

    /// Point new writes of `class` at `dict_hash` (already durably published).
    fn set_current_dictionary(&self, _class: u8, _dict_hash: u128) -> Result<(), OxenError> {
        Err(OxenError::basic_str(
            "this block store does not support compression dictionaries",
        ))
    }
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

    /// Directory holding shared compression dictionaries. Lives inside the blocks
    /// dir (its name is not two hex chars, so block listing skips it).
    fn dicts_dir(&self) -> PathBuf {
        self.blocks_dir.join("dicts")
    }

    fn dict_path(&self, dict_hash: u128) -> PathBuf {
        self.dicts_dir().join(format!("{dict_hash:032x}"))
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
            // Block fan-out dirs are exactly two hex chars; anything else (e.g.
            // the dictionaries dir) is not a block.
            if prefix_entry.file_name().to_string_lossy().len() != 2 {
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

    fn supports_dictionaries(&self) -> bool {
        true
    }

    fn put_dictionary(&self, dict_hash: u128, data: &Bytes) -> Result<(), OxenError> {
        AtomicFile::new(self.dict_path(dict_hash))
            .with_hash(MerkleHash::new(dict_hash))
            .write(data)
    }

    fn read_dictionary(&self, dict_hash: u128) -> Result<Vec<u8>, OxenError> {
        util::fs::read_bytes_from_path(self.dict_path(dict_hash))
    }

    fn current_dictionary(&self, class: u8) -> Result<Option<u128>, OxenError> {
        let path = self.dicts_dir().join(format!("current-{class}"));
        if !path.exists() {
            return Ok(None);
        }
        let hex = String::from_utf8_lossy(&util::fs::read_bytes_from_path(&path)?).to_string();
        let hash = u128::from_str_radix(hex.trim(), 16)
            .map_err(|_| OxenError::basic_str(format!("corrupt current-dictionary pointer: {hex}")))?;
        Ok(Some(hash))
    }

    fn set_current_dictionary(&self, class: u8, dict_hash: u128) -> Result<(), OxenError> {
        AtomicFile::new(self.dicts_dir().join(format!("current-{class}")))
            .write(format!("{dict_hash:032x}").as_bytes())
    }
}
