# `crates/lib/src/util/` — leaf utilities, sync-first

Utilities in this directory are **leaf utilities**, not server- or client-operations. Follow the sync/async policy in [`../../../../docs/async_policy.md`](../../../../docs/async_policy.md):

- **Stay sync.** Use `std::fs` (not `tokio::fs::*`), sync DB bindings, and sync CPU work. The `spawn_blocking` that bridges to the async world lives at the **operation boundary** — `repositories::*`, HTTP handlers, CLI handlers — not inside utilities here. One `spawn_blocking` per coherent operation amortizes the dispatch tax across real work; spawning per-syscall inside a utility pays it many times over.
- **Public API stays non-`async`** by default. Make exceptions only when a function genuinely bridges a sync local side with an async network source (e.g. an `AsyncRead` feeding a sync writer) — in that case use the **Channel hand-off** pattern from the policy doc, not `tokio::fs::*` per call.
- **Streamed IO through `AsyncRead` / `AsyncWrite`** still gets the 10 MB `BufWriter` treatment from the project CLAUDE.md, and an explicit `flush().await?` before any downstream `sync_all` / rename / checksum.

Read the policy doc end-to-end before adding new IO. The patterns (Bracket, Sandwich, Channel hand-off) and the anti-patterns ("`tokio::fs::*` in a hot loop", "DB transactions spanning `.await`", "`FuturesUnordered<spawn_blocking>` over CPU work") are the load-bearing parts.
