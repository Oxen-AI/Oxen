# LMDB layer — notes for AI agents

This is the reusable, low-level LMDB layer. **Before writing or modifying a store that builds on it, read [`README.md`](./README.md) in this directory** — it is the authoritative implementor's guide (opening an env through `open_shared_env`, the `LmdbStore` trait, key/value framing, transactions, and the async edge). The bullets below are a quick reference, not a replacement for it.

Load-bearing invariants:

- **Don't use this layer directly from application code.** Write a thin per-domain store wrapper (see the README's worked example).
- **One logical store per env**, and open envs **only** through `lmdb::open_shared_env` (the layer's process-global registry; the raw open primitive is module-private on purpose), so a path is opened at most once and the env is shared — a store does not stand up its own registry.
- **Never hold a transaction across `.await`.** Read txns are `Send` here, so the compiler will *not* catch this — it is a written discipline. Copy what you need out of a read txn into owned `bytes::Bytes` and drop the txn promptly.
- **One `write` closure = one atomic commit** (commit on `Ok`, roll back on `Err`/panic). The layer is synchronous; wrap each operation in a single `spawn_blocking` at the async edge per `docs/async_policy.md`.
- **Fixed per-store `map_size`, no runtime resize.** Exhausting it is `LmdbLayerError::MapFull`, whose only remedy is to raise the store's constant and rebuild — never a runtime recovery path.
- **Errors stay in `OxenError`.** `LmdbLayerError` bridges in via `#[from]`; store methods return `Result<_, OxenError>` and don't leak `LmdbLayerError` to callers.
- **Least visibility for new items.** Keep additions as private as possible and add public surface only when a consumer actually needs it.
