//! An LMDB "environment", `LmdbEnv` is one file on disk that holds one or more LMDB databases.
//! Opening and sizing LMDB envs, and the `LmdbEnv` type. Map size is page-aligned and
//! `max_readers` is set explicitly.
//!
//! FIXED SIZE SPARSE MAP, NO RUNTIME RESIZE. Each store opens its env with a single generous
//! `map_size` (see `LmdbEnvConfig`) and never resizes at runtime. The map is a sparse
//! virtual-address reservation, not committed RAM — resident memory tracks the working set, not
//! `map_size` — so the size is chosen per store to be comfortably above any realistic on-disk size
//! with headroom to spare. There is therefore no `MapFull → resize → retry` machinery: if a store
//! ever exhausts its fixed map, that is `LmdbLayerError::MapFull`, whose remedy is to raise the
//! store's compile-time `map_size` constant and rebuild — deliberately a code change, not a runtime
//! path. Map sizes are decided PER STORE / case-by-case: the merkle store reserves a generous size,
//! while a store with a small, known bound (e.g. a refs index) sets a much more conservative value.
//!
//! ONE LOGICAL STORE PER ENV. Each env hosts the databases of a SINGLE logical store; `max_dbs` is
//! sized to that store's database count (a one-database store uses `max_dbs = 1`). The env registry's
//! "single live handle per canonical path" invariant therefore means "one env per logical store per
//! repo." This makes the registry's identity model unambiguous.

use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use heed::types::Bytes as HeedBytes;
use heed::{CompactionOption, Database, Env, EnvOpenOptions, PutFlags, WithoutTls};

use super::lmdb_error::LmdbLayerError;

/// The Oxen LMDB environment type: heed's `Env` pinned to the `WithoutTls` marker (`MDB_NOTLS`) so
/// reads can nest per thread and the env is safe to use from a thread pool — see `txn` for the
/// rationale (`Send` read txns are a side effect of that marker, not the reason for it). Aliased so
/// our code reads `LmdbEnv` everywhere rather than a bare `Env`, which is easy to confuse with the
/// process environment / env vars at a glance.
pub type LmdbEnv = Env<WithoutTls>;

/// Default reader-lock-table size for `LmdbEnvConfig::new`. Generous because the server can hold
/// many concurrent readers; stores expecting few readers can set `max_readers` smaller directly.
const DEFAULT_MAX_READERS: u32 = 1024;

/// The name LMDB gives the data file inside an env directory.
// Used only by `copy_lmdb_env_to_dir`; drop this `allow` together with the one on that function
// once a non-test `LmdbStore` implementor exists.
#[allow(dead_code)]
const LMDB_DATA_FILE: &str = "data.mdb";

/// Tunables for opening one LMDB env. Field meanings are backend-shaped so future stores reuse the
/// same surface. This layer deliberately defines **no** default `map_size`: it is a per-store
/// decision (see the module doc), so each store names its own constant.
#[derive(Debug, Clone)]
pub struct LmdbEnvConfig {
    /// The fixed mmap size (sparse virtual reservation); rounded up to the runtime page size at
    /// open. Never resized at runtime — exhausting it is `LmdbLayerError::MapFull`, remedied by
    /// raising the store's constant and rebuilding. Choose generously: it costs address space,
    /// not RAM.
    pub map_size: ByteSize,
    /// Number of named sub-databases (`LmdbDb`s) in THIS env. One logical store per env.
    pub max_dbs: u32,
    /// Reader-lock-table size. Set explicitly; the server can hold many concurrent readers.
    pub max_readers: u32,
}

impl LmdbEnvConfig {
    /// Config for a store with `max_dbs` databases and the given fixed `map_size`, using the crate
    /// default `max_readers` (1024). `map_size` has no default — the caller passes its per-store
    /// constant.
    pub fn new(max_dbs: u32, map_size: ByteSize) -> Self {
        LmdbEnvConfig {
            map_size,
            max_dbs,
            max_readers: DEFAULT_MAX_READERS,
        }
    }
}

/// Build page-aligned `EnvOpenOptions<WithoutTls>` (rounds size up to runtime page size, sets
/// max_dbs + max_readers, selects `WithoutTls`). Opens WITHOUT `MDB_NOSYNC`/`MDB_NOMETASYNC`, so
/// each `RwTxn::commit` fsyncs — commit is the durability boundary; no drop-time `force_sync` is
/// needed (see `copy_lmdb_env_to_dir`). Errors only if the size doesn't fit in `usize`.
fn build_options(config: &LmdbEnvConfig) -> Result<EnvOpenOptions<WithoutTls>, LmdbLayerError> {
    // LMDB requires `map_size` to be a multiple of the OS page size; round up rather than
    // surface a confusing `EINVAL` from `mdb_env_set_mapsize`.
    let map_size = usize::try_from(config.map_size.as_u64())
        .map_err(|_| LmdbLayerError::MapSizeUnrepresentable(config.map_size))?;
    let aligned = map_size
        .checked_next_multiple_of(page_size::get())
        .ok_or(LmdbLayerError::MapSizeUnrepresentable(config.map_size))?;

    let mut options = EnvOpenOptions::new().read_txn_without_tls();
    options.map_size(aligned);
    options.max_dbs(config.max_dbs);
    options.max_readers(config.max_readers);
    Ok(options)
}

/// Open (creating the dir first if needed) an LMDB env at `dir`. Dir creation precedes the open so
/// the caller never has to pre-create it; this also makes the env registry's open-on-miss work for
/// a brand-new repo (see `env_registry.rs`).
///
/// `LmdbEnvRegistry::get_or_open` is the only way to open an env from outside this module: the
/// registry deduplicates so overlapping opens of one path share a single env and do not hit
/// `EnvAlreadyOpened`. This primitive is visible only within the `lmdb` module (it's what the
/// registry is built on), so out-of-module consumers cannot bypass that dedup.
pub(in crate::lmdb) fn open_lmdb_env(
    dir: &Path,
    config: &LmdbEnvConfig,
) -> Result<LmdbEnv, LmdbLayerError> {
    create_dir_all(dir).map_err(|source| LmdbLayerError::CreateDir {
        path: dir.to_path_buf(),
        source,
    })?;
    let options = build_options(config)?;
    // SAFETY: heed's `open` is `unsafe` because LMDB mmaps the env's data file; the soundness
    // obligation is that nothing modifies that file out from under the live mmap. Two things
    // discharge it. (1) The env dir is private to this layer — nothing else in the process or
    // product writes into it. (2) LMDB's lockfile makes a second open of the same canonical dir
    // (this process or another) safe: in-process it returns `EnvAlreadyOpened` (surfaced as
    // `Open`) rather than creating a second live mmap, and cross-process LMDB is MVCC-safe for
    // concurrent opens by design. So soundness rests on the lockfile, not on any caller
    // discipline — the `env_registry` only *deduplicates* (to avoid the
    // `EnvAlreadyOpened` error on the normal path), it is not what makes this sound.
    let lmdb_env = unsafe { options.open(dir) }.map_err(|source| LmdbLayerError::Open {
        path: dir.to_path_buf(),
        source,
    })?;
    Ok(lmdb_env)
}

/// Copy the env's data file to `dst_dir` (fork snapshots), returning the path of the copied data
/// file. The copy runs under its own read txn, so a live env is copied point-in-time
/// consistently; LMDB durability is per-`commit`, so a snapshot taken after a committed write
/// needs no `force_sync`. Compaction is hard-wired off (`CompactionOption::Disabled`) rather than
/// exposed as a parameter: the compacting path runs a page-size-dependent free-page check that
/// fails `MDB_INCOMPATIBLE` on multi-sub-DB envs, and no store wants it.
// `pub(crate)`: an internal layer primitive backing `LmdbStore::snapshot_to`. Until a non-test
// store implements `LmdbStore`, its only callers are that trait's default method and tests, so drop
// this `#[allow(dead_code)]` (and the one on `LMDB_DATA_FILE`) once an implementor lands.
#[allow(dead_code)]
pub(crate) fn copy_lmdb_env_to_dir(
    lmdb_env: &LmdbEnv,
    dst_dir: &Path,
) -> Result<PathBuf, LmdbLayerError> {
    create_dir_all(dst_dir).map_err(|source| LmdbLayerError::CreateDir {
        path: dst_dir.to_path_buf(),
        source,
    })?;
    let dst = dst_dir.join(LMDB_DATA_FILE);
    lmdb_env
        .copy_to_path(&dst, CompactionOption::Disabled)
        .map_err(|source| LmdbLayerError::Copy {
            dst: dst.clone(),
            source,
        })?;
    Ok(dst)
}

/// Minimum reclaimable slack (free pages) before a rebuild-compaction is worth its
/// O(live index) copy. Small envs and freshly compacted envs skip the work.
const COMPACT_MIN_RECLAIM: u64 = 256 * 1024;

/// Rebuild-compact the env's data file: bulk-copy every entry of `db_names` (in LMDB
/// key order, append-mode, so B-tree leaves pack near-full) into a fresh sibling env,
/// then atomically rename the rebuilt data file over the live one. Returns whether a
/// rebuild happened — the work is skipped when the env's free-page slack is under an
/// internal floor, so calling this after every write batch stays cheap.
///
/// The live env keeps its mmap of the *old* inode: reads through existing handles stay
/// correct (the rebuilt file is an equivalent point-in-time snapshot), but writes
/// committed through them after this call are invisible to later opens of the path.
/// Callers therefore run this only at the end of a write batch, and only for stores
/// whose content is disposable, rebuildable derived state.
pub(crate) fn compact_lmdb_env_in_place(
    lmdb_env: &LmdbEnv,
    config: &LmdbEnvConfig,
    db_names: &[&str],
) -> Result<bool, LmdbLayerError> {
    let disk_size = lmdb_env.real_disk_size().map_err(LmdbLayerError::Read)?;
    let live_size = lmdb_env
        .non_free_pages_size()
        .map_err(LmdbLayerError::Read)?;
    if disk_size.saturating_sub(live_size) < COMPACT_MIN_RECLAIM {
        return Ok(false);
    }

    let src_dir = lmdb_env.path().to_path_buf();
    let tmp_dir = src_dir.with_extension(format!("compact-{}", std::process::id()));
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir).map_err(|source| LmdbLayerError::Compact {
            path: tmp_dir.clone(),
            source,
        })?;
    }
    let rebuilt_data = {
        let dst_env = open_lmdb_env(&tmp_dir, config)?;
        let rtxn = lmdb_env.read_txn().map_err(LmdbLayerError::Txn)?;
        let mut wtxn = dst_env.write_txn().map_err(LmdbLayerError::Txn)?;
        for name in db_names {
            let src_db: Option<Database<HeedBytes, HeedBytes>> = lmdb_env
                .open_database(&rtxn, Some(name))
                .map_err(|source| LmdbLayerError::OpenDb {
                    name: name.to_string(),
                    source,
                })?;
            let Some(src_db) = src_db else {
                continue;
            };
            let dst_db: Database<HeedBytes, HeedBytes> = dst_env
                .create_database(&mut wtxn, Some(name))
                .map_err(|source| LmdbLayerError::OpenDb {
                    name: name.to_string(),
                    source,
                })?;
            for item in src_db.iter(&rtxn).map_err(LmdbLayerError::Read)? {
                let (key, value) = item.map_err(LmdbLayerError::Read)?;
                dst_db
                    .put_with_flags(&mut wtxn, PutFlags::APPEND, key, value)
                    .map_err(LmdbLayerError::Write)?;
            }
        }
        // Commit is the durability boundary (the env opens without NOSYNC), so the
        // rebuilt file is fully on disk before the rename publishes it.
        wtxn.commit().map_err(LmdbLayerError::Txn)?;
        dst_env.path().join(LMDB_DATA_FILE)
    };
    std::fs::rename(&rebuilt_data, src_dir.join(LMDB_DATA_FILE)).map_err(|source| {
        LmdbLayerError::Compact {
            path: src_dir.clone(),
            source,
        }
    })?;
    let _ = std::fs::remove_dir_all(&tmp_dir);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lmdb::lmdb_db::LmdbDb;
    use crate::lmdb::txn::with_write_txn;

    /// 100 kB (decimal) is not a multiple of any real page size; the open succeeds because
    /// `build_options` rounds the map size up rather than handing LMDB an unaligned value.
    #[test]
    fn open_lmdb_env_rounds_unaligned_map_size_up_to_page_size() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let config = LmdbEnvConfig::new(1, ByteSize::kb(100));
        open_lmdb_env(dir.path(), &config).expect("open env with unaligned map size");
    }

    /// Exhausting the fixed map surfaces heed's map-full through `is_map_full`, which is what
    /// a store's write path uses to translate the raw error into `MapFull { capacity }`.
    #[test]
    fn exhausting_the_fixed_map_is_detectable_as_map_full() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let config = LmdbEnvConfig::new(1, ByteSize::kib(256));
        let lmdb_env = open_lmdb_env(dir.path(), &config).expect("open env");

        let db =
            with_write_txn(&lmdb_env, |txn| LmdbDb::open(&lmdb_env, txn, "data")).expect("open db");
        // Fill within one txn until the fixed map is exhausted; the failing `put` propagates out
        // of the closure (and aborts the txn).
        let result: Result<(), LmdbLayerError> = with_write_txn(&lmdb_env, |txn| {
            let value = vec![0_u8; 32 * 1024];
            for i in 0..10_000_u32 {
                db.put(txn, &i.to_be_bytes(), &value)?;
            }
            Ok(())
        });
        let err = result.expect_err("a 256 KiB map should fill within a few 32 KiB writes");
        assert!(err.is_map_full(), "expected map-full, got: {err}");

        let translated = LmdbLayerError::MapFull {
            capacity: config.map_size,
        };
        assert!(
            matches!(translated, LmdbLayerError::MapFull { capacity } if capacity == config.map_size)
        );
    }
}
