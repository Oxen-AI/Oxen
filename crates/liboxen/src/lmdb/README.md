# Writing an LMDB-backed store

This directory is the reusable, **low-level LMDB layer**. Application code should **not** use it directly. Instead, write a thin per-domain *store wrapper* that owns its env, names its databases, and chooses its own key encoding and value framing. This guide walks through writing one. (For the layer's internal design, read the module doc in `../lmdb.rs` and the per-file docs; this README is the implementor's how-to.)

## The shape, in one breath

A store is **one LMDB env** (one logical store per env) holding **one or more named databases**, opened through **`open_shared_env`** (the layer's process-global registry, so a path is opened at most once), implementing the **`LmdbStore`** trait to get its lifecycle (`read` / `write` / `snapshot_to`) for free, exposing **inherent domain methods** written over `self.read` / `self.write`, and wrapped in **`spawn_blocking`** at the async edge (the layer is synchronous).

## Worked example

A minimal single-database store. Read it top to bottom — every line is explained in the steps below.

```rust
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use bytesize::ByteSize;

use crate::error::OxenError;
use crate::lmdb::store::LmdbStore;
use crate::lmdb::{LmdbDb, LmdbEnv, LmdbEnvConfig, open_db, open_shared_env};

pub struct RefsStore {
    env: Arc<LmdbEnv>,
    db: LmdbDb,
}

impl RefsStore {
    /// Open (or share) the refs env for the repo at `dir`.
    pub fn open(dir: &Path) -> Result<Self, OxenError> {
        // `max_dbs = 1` (single database); the map size is this store's fixed budget (step 1).
        // No per-store registry — the layer's process-global one dedups by path.
        let env = open_shared_env(dir, &LmdbEnvConfig::new(1, ByteSize::mib(256)))?;
        let db = open_db(&env, "refs")?;
        Ok(RefsStore { env, db })
    }

    /// Resolve a ref name to its commit id.
    pub fn get(&self, name: &str) -> Result<Option<Bytes>, OxenError> {
        self.read(|db, txn| Ok(db.get(txn, name.as_bytes())?))
    }

    /// Point a ref name at a commit id — one atomic commit.
    pub fn set(&self, name: &str, commit_id: &[u8]) -> Result<(), OxenError> {
        self.write(|db, txn| Ok(db.put(txn, name.as_bytes(), commit_id)?))
    }
}

impl LmdbStore for RefsStore {
    fn lmdb_env(&self) -> &LmdbEnv {
        &self.env
    }

    fn lmdb_db(&self) -> &LmdbDb {
        &self.db
    }
}
```

## Step 1 — pick a fixed map size

Each store names its own `map_size`; the layer defines no default. It is a **sparse virtual-address reservation, not committed RAM** — resident memory tracks your working set, not the map size — so choose generously *for that store*: a small, bounded index (refs, a name index) picks a conservative size; a large graph store reserves much more. The map is **never resized at runtime**: if a store exhausts it, that surfaces as `LmdbLayerError::MapFull`, whose only remedy is to raise the constant and rebuild — deliberately a code change, not a runtime path. Fleet-wide virtual address is additive across stores × concurrently-open repos, so don't reserve terabytes "to be safe."

## Step 2 — one logical store per env

Build an `LmdbEnvConfig::new(max_dbs, map_size)` where `max_dbs` is the number of databases *this* store uses. Keep one logical store per env. Do **not** lump several unrelated stores into a single env: LMDB allows **one writer per env**, so a shared env serializes every write through one lock and throws away cross-store write concurrency. (Consolidating *many instances of the same store* — e.g. per-resource directories — into one env with composite keys is fine and encouraged; that's different from mixing distinct stores.)

## Step 3 — open through `open_shared_env`, never directly

Open envs only through `open_shared_env(dir, &config)` (the raw open primitive is module-private precisely so this can't be bypassed). It returns a shared `Arc<LmdbEnv>` from the layer's **process-global** registry, enforcing **at most one live env per canonical path** and deduping overlapping opens — so a store does **not** stand up its own registry; it just names its `config`. The registry is **weak-retention**: it does not keep the env alive — *your store does*, by holding the returned `Arc<LmdbEnv>`. When the last `Arc` drops, the env closes; a later `open_shared_env` reopens it cheaply (an idle env's hot pages stay warm in the OS page cache). `config` is consulted only on an actual open; on a shared hit it is ignored. (A higher layer — a per-repo cache — may eventually own env handles instead.)

## Step 4 — open your database(s)

Call `open_db(&env, "name")` for each database; it runs the write txn that heed requires for opening a database, so a single-database store opens in one line (as in the example). To open several databases together, run them inside one `with_write_txn` closure with the lower primitive `LmdbDb::open(&env, txn, "name")` rather than paying a separate write txn per database. A `LmdbDb` is a raw-bytes key / opaque-bytes value accessor; it is cheap to clone and you hold it for the store's lifetime. Keys are plain `&[u8]` and values are opaque bytes — **you** decide how to encode keys and frame values (including any magic/version bytes). LMDB orders keys by lexicographic byte comparison; that is correct for point lookups and for byte/string-ordered scans, but a store needing *numeric*-ordered range scans over a multi-byte integer would first need a big-endian key codec added to the layer (not present yet — add it when a verified consumer needs it).

## Step 5 — implement `LmdbStore` for the lifecycle

Implement the two-method hook (`lmdb_env`, `lmdb_db`) and you inherit:

- `read(|db, txn| …)` / `write(|db, txn| …)` — the transaction brackets with your primary database pre-bound. `read` opens a read snapshot; `write` commits iff your closure returns `Ok` and rolls back on `Err` (or panic). Both are generic over the closure's error type, so you can return `OxenError` directly (the `LmdbLayerError → OxenError` bridge is `#[from]`, so `?` just works, as in the example).
- `snapshot_to(dst_dir)` — a point-in-time env copy (for forks), which always uses `CompactionOption::Disabled` so you can't accidentally trip `MDB_INCOMPATIBLE` on a multi-database env.

If your store has multiple databases, implement the hook with your *primary* database and reach the others inside a `with_read_txn` / `with_write_txn` closure directly (or hold each `LmdbDb` and use them within the txn the bracket gives you).

## Step 6 — write domain methods

Your store's actual API (`get` / `set` above; a node store's `read_tree` / `put_nodes`) are **inherent methods** over `self.read` / `self.write`. One logical unit of work = one `write` closure = one atomic commit (e.g. a whole batch of node writes goes in a single `write`). Reads copy their results out into owned `bytes::Bytes`, so a returned value safely outlives the short transaction it came from — you never hand a caller a borrow into the mmap.

## Step 7 — go async at the edge

The layer is synchronous. Per the repo's sync-core / async-edge policy ([docs/async_policy.md](../../../../docs/async_policy.md)), expose async methods that wrap **one operation** in a single `spawn_blocking`, with the whole transaction living inside that closure:

```rust
impl RefsStore {
    pub async fn get_async(self: &Arc<Self>, name: String) -> Result<Option<Bytes>, OxenError> {
        let store = Arc::clone(self);
        // One operation, one spawn_blocking; the txn never spans an `.await`.
        // Map the JoinError to OxenError per docs/async_policy.md.
        tokio::task::spawn_blocking(move || store.get(&name)).await?
    }
}
```

The store (an `Arc<LmdbEnv>` plus `LmdbDb`s) is `Send + Sync`, so wrap it in `Arc` and clone it into the closure.

## Rules you must follow

- **Never hold a transaction across `.await`.** Read txns are `Send` here (a side effect of `WithoutTls`), so the compiler will *not* catch this for you — it is a written discipline. Beyond unsoundness, a long-lived reader pins old B-tree pages and starves the writer of free pages, pushing a fixed-map store toward `MapFull`. Copy what you need out and drop the txn.
- **One `write` closure = one atomic commit.** Group a logical unit of work into a single `write`; don't split it across multiple commits unless you mean to.
- **Always open through `open_shared_env`.** Never construct a second env for the same path yourself — that breaks the one-live-env-per-path invariant the shared registry exists to hold.
- **The map size is fixed.** Plan step 1; treat `MapFull` as "raise the constant and rebuild," not a runtime condition to recover from.
- **Errors stay in `OxenError`.** Domain methods return `Result<_, OxenError>`; `LmdbLayerError` bridges in via `#[from]`. Don't surface `LmdbLayerError` to callers of the store.
- **Before deleting / renaming an env directory** (or choosing a fork snapshot destination), check `lmdb::shared_env_is_live(path)` — under weak retention a `false` confirms no open mmap is blocking the filesystem operation; a `true` means the precondition ("this repo is not in use") is violated and you should refuse.

## Module map

- `lmdb_env.rs` — `LmdbEnv` (heed `Env<WithoutTls>`), `LmdbEnvConfig`, the open primitive, and `copy_lmdb_env_to_dir` (backing `snapshot_to`).
- `lmdb_db.rs` — `LmdbDb` (the raw-bytes key / opaque-value accessor and its full-scan `iter`) plus `open_db`, the one-line single-database open.
- `txn.rs` — `with_read_txn` / `with_write_txn`, the only sanctioned read/write entry points, and the txn-lifetime rule.
- `env_registry.rs` — `LmdbEnvRegistry` plus `open_shared_env` / `shared_env_is_live`: the path-keyed, weak-retention env cache and its process-global accessors.
- `store.rs` — `LmdbStore`: the default-method lifecycle trait you implement.
- `lmdb_error.rs` — `LmdbLayerError`, the leaf error bridged into `OxenError`.
