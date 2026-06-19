//! Leaf error type for the low-level LMDB layer, bridged into `OxenError` via the
//! `#[from]`-derived `OxenError::LmdbLayer` variant (the same module-local-enum pattern as
//! `LmdbError`, `MerkleDbError`, and `RepoConfigError`).
//!
//! A dedicated enum, rather than variants on `OxenError`, for two reasons. It keeps `heed::Error`
//! (carried by `Open`/`Txn`/`Read`/`Write`) out of the top-level `OxenError` surface, so
//! the heed dependency stays contained to this layer. And it keeps this cohesive cluster of
//! low-level infra errors — none of which a public-API caller branches on — together and out of
//! the already-large `OxenError`, while still letting in-layer code branch on a specific heed
//! cause (`is_map_full`, `is_env_already_opened`) against a narrow type before the `?`-conversion.
//!
//! Values are opaque bytes at this layer, so per-record parse errors belong to the caller, not
//! here.

use std::path::PathBuf;

use bytesize::ByteSize;
use heed::MdbError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LmdbLayerError {
    #[error("Cannot create LMDB map of size {0}: larger than this system's addressable memory.")]
    MapSizeUnrepresentable(ByteSize),

    #[error("Could not create LMDB environment directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// `EnvOpenOptions::open` failed. Notably wraps heed `EnvAlreadyOpened`; the handle cache
    /// exists to avoid triggering it, but eviction-while-borrowed can still surface it (see
    /// `handle_cache.rs`), where the registry retries by re-sharing the live handle.
    #[error("Could not open LMDB environment at {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: heed::Error,
    },

    #[error("LMDB transaction error: {0}")]
    Txn(#[source] heed::Error),

    #[error("LMDB read error: {0}")]
    Read(#[source] heed::Error),

    #[error("LMDB write error: {0}")]
    Write(#[source] heed::Error),

    #[error("Could not open LMDB database {name:?}: {source}")]
    OpenDb {
        name: String,
        #[source]
        source: heed::Error,
    },

    #[error(
        "LMDB map is full (capacity {capacity}). The map size is a compile-time constant per \
         store; increase it and rebuild."
    )]
    MapFull { capacity: ByteSize },
}

impl LmdbLayerError {
    /// True if this is heed's `MDB_MAP_FULL` underneath, regardless of whether it surfaced as
    /// `Write` or `Txn`. The write path uses this to translate a raw heed map-full into the
    /// `MapFull { capacity }` variant (which carries the compile-time-constant hint) instead of
    /// an opaque heed error.
    pub fn is_map_full(&self) -> bool {
        matches!(
            self,
            LmdbLayerError::Write(heed::Error::Mdb(MdbError::MapFull))
                | LmdbLayerError::Txn(heed::Error::Mdb(MdbError::MapFull))
        )
    }

    /// True if this is heed's `EnvAlreadyOpened` underneath. The handle cache uses this to
    /// detect an open that raced a concurrent close and re-share the live handle instead of
    /// surfacing the error.
    pub fn is_env_already_opened(&self) -> bool {
        matches!(
            self,
            LmdbLayerError::Open {
                source: heed::Error::EnvAlreadyOpened,
                ..
            }
        )
    }
}
