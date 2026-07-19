//! Reusable, low-level LMDB layer for oxen.
//!
//! Oxen subsystems using LMDB should not use this layer directly. Instead, they should provide a
//! thin wrapper around this layer that provides their own database names, key ordering, and value
//! framing.
//!
//! The pieces:
//!   * [`lmdb_error`] — [`LmdbLayerError`], the `OxenError`-independent leaf error type.
//!   * [`txn`] — the closure-scoped `with_read_txn`/`with_write_txn` brackets (auto
//!     commit-on-`Ok` / abort-on-`Err`), the txn-lifetime rule, and the `WithoutTls`/`MDB_NOTLS`
//!     choice (nestable reads + thread-pool-safe; `Send` read txns are a side effect).
//!   * [`lmdb_env`] — [`LmdbEnv`] (heed's `Env` pinned to `WithoutTls`) plus `open_lmdb_env` /
//!     `LmdbEnvConfig` with a fixed per-store sparse map size (no runtime resize).
//!   * [`lmdb_db`] — [`LmdbDb`], a named sub-database with raw-bytes keys and opaque-bytes values
//!     copied out as `bytes::Bytes`, plus `open_db` to open one database in its own write txn.
//!   * [`env_registry`] — [`LmdbEnvRegistry`] (the ONE path-keyed, weak-retention env cache that
//!     enforces "at most one live env per canonical path") plus [`open_shared_env`], the
//!     process-global entry point a store calls instead of standing up its own registry.
//!   * [`store`] — the `LmdbStore` default-method lifecycle trait: an implementor supplies the
//!     env + database hook and inherits `read`/`write`/`snapshot_to`; its domain ops stay inherent.

pub mod env_registry;
pub mod lmdb_db;
pub mod lmdb_env;
pub mod lmdb_error;
pub mod store;
pub mod txn;

pub use env_registry::{LmdbEnvRegistry, open_shared_env, shared_env_is_live};
pub use lmdb_db::{LmdbDb, open_db};
pub use lmdb_env::{LmdbEnv, LmdbEnvConfig};
pub(crate) use lmdb_env::compact_lmdb_env_in_place;
pub use lmdb_error::LmdbLayerError;
pub use txn::{with_read_txn, with_write_txn};
