# `crates/liboxen/src/util/` — leaf utilities, sync-first

Utilities in this directory are **leaf utilities**, not server- or client-operations. Follow the sync/async policy in [`../../../../docs/async_policy.md`](../../../../docs/async_policy.md):

- **Stay sync.** Use `std::fs` (not `tokio::fs::*`), sync DB bindings, and sync CPU work. The `spawn_blocking` that bridges to the async world lives at the **operation boundary** — `repositories::*`, HTTP handlers, CLI handlers — not inside utilities here. One `spawn_blocking` per coherent operation amortizes the dispatch tax across real work; spawning per-syscall inside a utility pays it many times over.
- **Public API stays non-`async`** by default. Make exceptions only when a function genuinely bridges a sync local side with an async network source (e.g. an `AsyncRead` feeding a sync writer) — in that case use the **Channel hand-off** pattern from the policy doc, not `tokio::fs::*` per call.
- **Streamed IO through `AsyncRead` / `AsyncWrite`** still gets the 10 MB `BufWriter` treatment from the project CLAUDE.md, and an explicit `flush().await?` before any downstream `sync_all` / rename / checksum.

Read the policy doc end-to-end before adding new IO. The patterns (Bracket, Sandwich, Channel hand-off) and the anti-patterns ("`tokio::fs::*` in a hot loop", "DB transactions spanning `.await`", "`FuturesUnordered<spawn_blocking>` over CPU work") are the load-bearing parts.

## `AtomicFile` — the only way production code writes to disk

Every production filesystem write in this crate goes through `AtomicFile` in `fs.rs`:

```rust
AtomicFile::new(target).<builders>.<terminal>()
```

The builders are `.with_hash(expected)` and `.with_mtime(t)`; both default to `None` when omitted but cost nothing to add when the data justifies them — so reach for them every time:

- **`.with_hash(expected)`** when the bytes are content-addressed (the canonical filename embeds the hash) or arrive from an untrusted / network source. On mismatch the publish aborts without touching the canonical path. `write` hashes the in-memory bytes before opening any temp file (verify-before-publish); the streaming and copy variants hash in-passing and drop the temp on mismatch.
- **`.with_mtime(t)`** when publishing into the working tree so the `oxen status` size+mtime fast path stays effective on the next pass. The mtime is stamped on the temp file atomically with the rename, so a kill can never leave a complete-but-wrong-mtime file behind.

Terminal methods are `.write(bytes)` (in-memory) / `.stream(reader)` (sync `Read`) / `.stream_async(reader).await` (async `AsyncRead`, via the Channel hand-off pattern) / `.copy_from(src)` (file → file) / `.stream_from_paths(&[path])` (many files → file). Pick by the source-kind you have in hand.

`AtomicFile` is `#[must_use]` — forgetting a terminal method is a compile-time warning, not a silent no-op. The full contract (including atime behavior, cancel-safety, and the verify-before-publish guarantee) lives on the struct rustdoc. Read it before adding a new caller.

The non-atomic `util::fs::write_to_path` is `#[cfg(any(test, feature = "test-utils"))]`-gated for fixture setup only; production code cannot reach it.
