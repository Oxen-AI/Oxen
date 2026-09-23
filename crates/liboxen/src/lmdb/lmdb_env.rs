//! An LMDB "environment", `LmdbEnv` is one file on disk that holds an LMDB database.
//! Opening and sizing LMDB envs, and the `LmdbEnv` type. Map size is page-aligned and
//! `max_readers` is set explicitly.
//!
//! FIXED SIZE SPARSE MAP, NO RUNTIME RESIZE. Each store opens its env with a single generous
//! `map_size` and never resizes at runtime. The map is a sparse virtual-address reservation, not
//! committed RAM — resident memory tracks the working set, not `map_size` — so the size is chosen
//! per store to be comfortably above any realistic on-disk size with headroom to spare. There is
//! therefore no `MapFull → resize → retry` machinery: if a store ever exhausts its fixed map, that
//! is `LmdbLayerError::MapFull`, whose remedy is to raise the store's compile-time `map_size`
//! constant and rebuild — deliberately a code change, not a runtime path. Map sizes are decided PER
//! STORE / case-by-case: the merkle store reserves a generous size, while a store with a small,
//! known bound (e.g. a refs index) sets a much more conservative value.
//!
//! ONE LOGICAL STORE PER ENV, ONE NAMED DATABASE PER ENV. Each env hosts the single database of a
//! single logical store. The env registry's "single live handle per canonical path" invariant
//! therefore means "one env per logical store per repo." This makes the registry's identity model
//! unambiguous.

use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use heed::{CompactionOption, Env, EnvOpenOptions, WithoutTls};

use super::lmdb_error::LmdbLayerError;

/// The Oxen LMDB environment type: heed's `Env` pinned to the `WithoutTls` marker (`MDB_NOTLS`) so
/// reads can nest per thread and the env is safe to use from a thread pool — see `txn` for the
/// rationale (`Send` read txns are a side effect of that marker, not the reason for it). Aliased so
/// our code reads `LmdbEnv` everywhere rather than a bare `Env`, which is easy to confuse with the
/// process environment / env vars at a glance.
pub type LmdbEnv = Env<WithoutTls>;

/// Reader-lock-table size, generous because the server can hold many concurrent readers.
const MAX_READERS: u32 = 1024;

/// Every env holds exactly one named database.
const MAX_DBS: u32 = 1;

/// The name LMDB gives the data file inside an env directory.
const LMDB_DATA_FILE: &str = "data.mdb";

/// Build page-aligned `EnvOpenOptions<WithoutTls>` (rounds `map_size` up to runtime page size, sets
/// max_dbs + max_readers, selects `WithoutTls`). Opens WITHOUT `MDB_NOSYNC`/`MDB_NOMETASYNC`, so
/// each `RwTxn::commit` fsyncs — commit is the durability boundary; no drop-time `force_sync` is
/// needed (see `copy_lmdb_env_to_dir`). Errors only if the size doesn't fit in `usize`.
fn build_options(map_size: ByteSize) -> Result<EnvOpenOptions<WithoutTls>, LmdbLayerError> {
    // LMDB requires `map_size` to be a multiple of the OS page size; round up rather than
    // surface a confusing `EINVAL` from `mdb_env_set_mapsize`.
    let size = usize::try_from(map_size.as_u64())
        .map_err(|_| LmdbLayerError::MapSizeUnrepresentable(map_size))?;
    let aligned = size
        .checked_next_multiple_of(page_size::get())
        .ok_or(LmdbLayerError::MapSizeUnrepresentable(map_size))?;

    let mut options = EnvOpenOptions::new().read_txn_without_tls();
    options.map_size(aligned);
    options.max_dbs(MAX_DBS);
    options.max_readers(MAX_READERS);
    Ok(options)
}

/// Open (creating the dir first if needed) an LMDB env at `dir`, reserving `map_size`. Dir creation
/// precedes the open so the caller never has to pre-create it; this also makes the env registry's
/// open-on-miss work for a brand-new repo (see `env_registry.rs`).
///
/// `open_shared_env`, which `LmdbStore` opens through, is the only way to open an env from outside
/// this module: its registry deduplicates so overlapping opens of one path share a single env and
/// do not hit `EnvAlreadyOpened`. This primitive is visible only within the `lmdb` module (it's
/// what the registry is built on), so out-of-module consumers cannot bypass that dedup.
pub(in crate::lmdb) fn open_lmdb_env(
    dir: &Path,
    map_size: ByteSize,
) -> Result<LmdbEnv, LmdbLayerError> {
    create_dir_all(dir).map_err(|source| LmdbLayerError::CreateDir {
        path: dir.to_path_buf(),
        source,
    })?;
    let options = build_options(map_size)?;
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

/// Whether an env has been created at `dir`, checked without opening (and so creating) one.
pub(crate) fn lmdb_env_exists(dir: &Path) -> bool {
    dir.join(LMDB_DATA_FILE).exists()
}

/// Copy the env's data file to `dst_dir` (fork snapshots), returning the path of the copied data
/// file. The copy runs under its own read txn, so a live env is copied point-in-time
/// consistently; LMDB durability is per-`commit`, so a snapshot taken after a committed write
/// needs no `force_sync`. Compaction is hard-wired off (`CompactionOption::Disabled`) rather than
/// exposed as a parameter: the compacting path runs a page-size-dependent free-page check that
/// fails `MDB_INCOMPATIBLE` on multi-sub-DB envs, and no store wants it.
pub(in crate::lmdb) fn copy_lmdb_env_to_dir(
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
        open_lmdb_env(dir.path(), ByteSize::kb(100)).expect("open env with unaligned map size");
    }

    /// Exhausting the fixed map surfaces heed's map-full through `is_map_full`.
    #[test]
    fn exhausting_the_fixed_map_is_detectable_as_map_full() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let lmdb_env = open_lmdb_env(dir.path(), ByteSize::kib(256)).expect("open env");

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
    }
}
