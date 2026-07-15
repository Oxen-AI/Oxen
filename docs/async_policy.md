# Async/Sync IO Policy

This document defines how Oxen Rust code should approach the boundary between async (Tokio) and sync (std + sync C/C++ bindings) IO.

To jump ahead into the core guidelines, see [The Core Philosophy](#the-core-philosophy).

As Oxen is a rapidly developing pre-1.0 codebase, not all of the code adheres to these principles. New code, however, should adhere to them, and old code should be brought in line on a best-effort basis as it is touched.

## Why we have a policy

Oxen sits across an awkward IO boundary. Several of the libraries we use are async-only — most prominently the AWS SDK (used by the S3 version store) and HTTP server. Others are sync-only and will never have an async API — RocksDB, LMDB, and DuckDB are C/C++ libraries with their own internal threading and synchronous bindings. The local filesystem occupies a special middle ground: Tokio exposes `tokio::fs::*` with an async API, but on Linux and macOS this is internally implemented by dispatching each syscall to a thread pool. There is no true async filesystem IO on macOS, and even on Linux the only real path (io_uring) requires a runtime swap that would fragment the codebase.

The naive "make everything async" approach has measurable costs:

- **`tokio::fs::*` in hot loops pays a `spawn_blocking` tax per syscall.** Sending the closure to the blocking pool, waking a worker, running the sync syscall there, and waking the original task back up dominates the cost of a small read or stat.
- **`!Send` types** (RocksDB read-txn guards, sync DB cursors, `std::sync::MutexGuard`, file handles in some shapes) become hazardous if held across `.await` — even harmless today under actix-web's current-thread-per-worker model, they become hard errors under any multi-threaded runtime (including the oxen client or a possible future migration of the server to axum).
- **DB write transactions** (LMDB, RocksDB write batches) serialize globally; holding one across an `.await` ties up other writers waiting on network IO that has nothing to do with the database.

The naive "make everything sync" approach fails too: the AWS SDK is async, the HTTP server is async, and many of the most valuable concurrency wins (overlapping S3 round-trips, streaming uploads) require async.

The policy below threads this needle.

## Goals

1. **Public APIs callable from HTTP handlers are `async fn`.** This keeps the trait shape uniform across backends (e.g., `VersionStore` for both `LocalVersionStore` and `S3VersionStore`) and avoids forcing `spawn_blocking` boilerplate at every callsite in the server or CLI.

2. **Network IO uses the native async API.** AWS SDK, reqwest, anything that talks over a socket. This is where async genuinely buys overlap — high-latency network round trips that can profitably proceed in parallel on a single thread.

3. **Filesystem and sync-DB IO uses `std::fs` and sync bindings inside `tokio::task::spawn_blocking`, at operation granularity.** One `spawn_blocking` per *coherent unit of work* (ideally ≥1 ms of work inside the closure), not one per syscall. This amortizes the dispatch overhead over real work and avoids the `tokio::fs` per-call tax.

4. **File-based DB transactions live entirely inside one `spawn_blocking` closure.** Open, write, commit — all in the same closure. Never span an `.await`. This is especially important for LMDB write transactions and RocksDB write batches, which serialize global writers.

5. **CPU-parallel batch work uses `rayon`, not `FuturesUnordered<spawn_blocking>`.** Tokio's blocking pool is sized and tuned for "lots of stuff occasionally blocked on syscalls" (default cap 512 threads, no work-stealing). It is not the right tool for "use all cores to hash 50 000 files." Rayon is.

6. **Streaming IO that interleaves sync local work with async network work uses one long-lived `spawn_blocking` task on the sync side, paired with an async task on the network side, bridged by a bounded `tokio::sync::mpsc` channel.** The bound throttles a fast producer to the consumer's pace, keeping memory in check; the long-lived task means one blocking-pool worker for the whole stream rather than a fresh dispatch per chunk.

## The core philosophy

Oxen uses a **sync core, async edge** philosophy: a popular Rust philosophy used by `cargo`, `sccache`, and other heavy Rust tools that mix network and disk IO. Async is reserved for what genuinely benefits from it (concurrent network round trips, HTTP serving). Sync is used for logic and for operations that are implemented synchronously.

> **Public APIs are async code. The sync core runs inside `spawn_blocking` closures sized to a coherent unit of work. Network IO stays in the async sections between and around those closures.**

Avoid situations where sync code calls async code. Public sync functions should not call async functions (e.g. via `handle.block_on`) if it can be avoided.

All **network IO** code must be async. This encompasses all communication over a socket (e.g. HTTP, gRPC, S3, etc.).

Not all Oxen code should be async. All filesystem operations should be sync. Most logic should be sync. As much as possible, logic should be factored out into sync functions and used in `async fn` when the logic needs to be used in some specific async context.

## Implementation patterns

The patterns below cover the cases that come up in practice. Names are useful for code review shorthand.

### Offload: converting a sync liboxen API to async

The most common conversion here: a `liboxen` read or mutation API does purely synchronous sync-DB / filesystem work (no network) and is called from `async` HTTP handlers. Make the API an `async fn` and move its sync core into a single `spawn_blocking`. Three invariants make it correct:

1. **One `spawn_blocking` at operation granularity** — wrap the whole operation in one closure, not one closure per DB call or syscall.
2. **Return an owned result.** The closure's output is the API's result and must be `Send + 'static` — an owned `Vec<Branch>`, `String`, or domain struct. No borrow of the repo or of a DB guard may escape it.
3. **Keep every `!Send` guard inside the closure.** RocksDB read guards, LMDB transactions, iterators/cursors, and `parking_lot` guards are created and dropped within the closure and never cross an `.await`.

`repositories::branches::list` is the worked example. Before:

```rust
pub fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    with_ref_manager(repo, |manager| manager.list_branches())
}
```

After:

```rust
pub async fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let repo = repo.clone();
    tokio::task::spawn_blocking(move || with_ref_manager(&repo, |manager| manager.list_branches()))
        .await?
}
```

`repo.clone()` gives the closure an owned `LocalRepository` to capture, since `spawn_blocking` requires `'static`. The RocksDB read guard that `RefManager::list_branches` acquires lives and dies inside the closure, so it never touches the `.await`, and the returned `Vec<Branch>` is fully owned — nothing `!Send` escapes. `.await?` unwraps twice: `From<JoinError> for OxenError` maps a panicked or cancelled blocking task to an `OxenError`, then the inner `Result` is returned as-is.

The inner sync functions (`with_ref_manager`, `RefManager::list_branches`) stay synchronous; only the public API grows the `async` / `spawn_blocking` edge. Callers ripple outward to `.await` it — a thin sync wrapper that only forwards (e.g. `repositories::is_empty`) becomes `async` in turn, and its own callers, already `async` handlers, add `.await`. The conversion stops at the first `async` boundary above each caller.

The granularity is **one `spawn_blocking` per converted API, not per handler.** A handler or CLI command that needs several reads resolves the repo once (`get_repo`) and `.await`s each converted API in sequence; it does **not** wrap them in one bespoke closure. The per-request hop overhead is negligible — µs-scale hops against ms-to-second-scale reads — and keeping one offload per API is what lets leaf endpoints convert independently.

Two granularity rules that `branches::list` doesn't exercise but the leaf conversions will:

- **A read loop is one operation, not N.** When an API iterates — a value per key, a walk over merkle nodes — the *entire loop* stays inside its single `spawn_blocking` (or moves to a bulk API); never dispatch one hop per iteration, which pays the dispatch tax on every element. (`branches::list`'s inner `list_branches` iterates the whole refs DB inside its one closure.)
- **Independent reads may overlap with `tokio::try_join!`** (optional perf). When a handler's reads don't depend on each other, joining their separate offloads runs them on separate blocking-pool threads in parallel, and the DB handle caches dedup temporally-overlapping opens of the same database. Join the separate `async fn`s — do **not** merge their bodies into one closure, which can neither overlap nor dedup.

### Bracket: async → sync → async

The handler is async. It does any required network calls up front, then a single `spawn_blocking` for the sync core, then any network calls at the end.

```rust
pub async fn handler(req: Request) -> Result<Response, OxenError> {
    let existing = s3.head_object(...).await?;           // async network
    let result = tokio::task::spawn_blocking(move || {   // sync core
        do_local_work(existing)
    }).await??;
    s3.put_object(result.bytes).await?;                  // async network
    Ok(Response::from(result.summary))
}
```

Right shape for most endpoints (status, add, commit, simple uploads). Touch the network at most once or twice, do the heavy local work in one closure.

### Sandwich: alternate async and sync sections

When network results feed back into sync work (or vice versa), each side is its own block:

```rust
let setup    = s3.list_versions(...).await?;
let plan     = spawn_blocking(move || build_plan(setup)).await??;
let fetched  = futures::future::try_join_all(
                   plan.missing.iter().map(|h| s3.get_version(h))
               ).await?;
let result   = spawn_blocking(move || install_locally(fetched, plan)).await??;
s3.put_object(result.manifest).await?;
```

Right shape for `fetch`, `pull`, `push`, and other operations that need to parallel-fan-out on the network and then batch-process the results locally. Each `spawn_blocking` is one coherent chunk of work, so the dispatch tax is amortized.

### Channel hand-off: streaming through the boundary

When sync and async sides need to interleave continuously on the same data stream — uploading a 10 GB file while reading from local disk and hashing — pair one long-lived `spawn_blocking` task with an async task, bridged by a **bounded** `tokio::sync::mpsc` channel. The bound is load-bearing: it is what throttles a fast producer to the consumer's pace. An unbounded channel would let the producer buffer the entire stream in memory before the consumer caught up, defeating the point of streaming.

```rust
let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(8);

let reader = tokio::task::spawn_blocking(move || {
    let mut f = std::fs::File::open(&path)?;
    let mut hasher = Xxh3::new();
    loop {
        let mut buf = vec![0u8; PART_SIZE];
        let n = f.read(&mut buf)?;
        if n == 0 { break; }
        buf.truncate(n);
        hasher.update(&buf);
        tx.blocking_send(Bytes::from(buf))?;       // backpressure via bounded channel
    }
    Ok(hasher.digest128())
});

let uploader = async {
    upload_from_channel(rx).await
};

let (hash, _) = tokio::try_join!(reader, uploader)?;
```

`tokio::sync::mpsc::Sender::blocking_send` and `Receiver::blocking_recv` exist for exactly this case — calling them from inside `spawn_blocking` is correct and does not deadlock the runtime. The bound is what produces backpressure on the producer: when the channel is full, `blocking_send` blocks until the consumer drains. (The consumer waiting when the channel is empty happens with any channel, bounded or not — that part isn't a consequence of the bound.)

`S3VersionStore::store_version_from_reader` in `crates/liboxen/src/storage/s3.rs` is the canonical example of the multipart-upload variant of this pattern (bounded concurrency via `FuturesUnordered`, one upload task per part).

### Rayon for CPU-parallel batch work

When the work is CPU-bound batch processing — hash N files, build N merkle nodes, compress N chunks — use one `spawn_blocking` containing a `rayon::par_iter`:

```rust
let paths = ...; // many file paths
let hashes = tokio::task::spawn_blocking(move || {
    paths.par_iter()
         .map(|p| hash_file(p))   // std::fs + xxh3, runs on rayon's pool
         .collect::<Result<Vec<_>, _>>()
}).await??;
```

One blocking-pool worker leaves the Tokio runtime; rayon's work-stealing pool actually uses the cores; one `.await` resumes. This is dramatically better than `FuturesUnordered<spawn_blocking>` over the same batch, which would spin up many blocking-pool threads with no work-stealing and high scheduler overhead.

### Escape hatch: `Handle::block_on` inside `spawn_blocking`

Sometimes the cleanest code for an otherwise-sync operation needs to make a network call in the middle:

```rust
let handle = tokio::runtime::Handle::current();
tokio::task::spawn_blocking(move || {
    let s3_result = handle.block_on(s3.put_object(...).send())?;
    // ... more sync work using s3_result
})
```

This works, but it ties up a blocking-pool worker on a network call — exactly what async exists to avoid. Allowed sparingly when the alternative significantly complicates the code; not allowed as a habit. If you find yourself reaching for it more than once or twice in a module, refactor toward the **Sandwich** or **Channel hand-off** pattern instead.

The cost is much higher on the server (one of 512 shared blocking workers held while waiting on S3, potentially across many concurrent requests) than in the CLI (one request, plenty of slack). Treat it as a code smell in `oxen-server`; treat it as a pragmatic choice in `oxen`.

## Specific guidance for sync libraries

### RocksDB, LMDB, DuckDB

All three are sync C/C++ libraries with sync Rust bindings. All three do their own internal threading where they do any threading at all. The treatment is identical:

- Call them inside `spawn_blocking`, sized to a coherent unit of work.
- **Never** split a transaction across `.await`. Open, read/write, commit — all inside one closure.
- Read transactions in LMDB are essentially free (memory-mapped, no syscalls per read). For a tight read loop, keep the entire loop inside one `spawn_blocking` rather than yielding between reads — there is nothing to yield to, and each yield costs more than the read it surrounds.
- Write transactions in LMDB serialize all writers, so spanning a write txn across an `.await` would block every other writer in the process on whatever the awaited future is doing. RocksDB write batches behave similarly. This is a correctness/throughput hazard, not just a performance one.

### Local filesystem

Use `std::fs` inside `spawn_blocking`, not `tokio::fs`.

The exception is when the source or sink is genuinely async (an HTTP request body, an S3 stream). In that case, use the **Channel hand-off** pattern: the network side stays async, the disk side runs inside a long-lived `spawn_blocking`, and a channel bridges them.

For unbounded-length streamed IO that goes through `AsyncRead` / `AsyncWrite`, wrap the write side in `tokio::io::BufWriter::with_capacity(10 * 1024 * 1024, ...)` rather than relying on `tokio::io::copy`'s 8 KB default. Always `flush().await?` the `BufWriter` before any downstream `sync_all` / rename / checksum step — `BufWriter`'s `Drop` does not auto-flush, so unflushed bytes are silently dropped. The canonical example is `copy_version_to_path` in `crates/liboxen/src/storage/s3.rs`.

## CLI vs server

The patterns above apply to both binaries. Two nuances differ:

### CLI (`oxen`)

The CLI is structurally a request handler with exactly one request. The "don't tie up shared blocking workers" concern is much weaker — there are no concurrent siblings. Specifically:

- The **escape hatch** (`Handle::block_on` inside `spawn_blocking`) is essentially harmless. Use it when it makes the code clearer.
- **CPU-parallel batch work belongs in rayon, not the Tokio blocking pool.** This is doubly true in the CLI, where a single `oxen add` may want to hash tens of thousands of files in parallel. Tokio's blocking pool is the wrong tool for that; rayon is the right one.
- `OXEN_STACK_SIZE` (in `crates/oxen-cli/src/main.rs`) sets a custom thread stack size for the Tokio runtime threads. This does **not** automatically propagate to the blocking pool or to rayon — if a deep-recursion workload moves into one of those, convert it to an iterative loop.

### Server (`oxen-server`)

The server uses actix-web 4 with `#[actix_web::main]`, which expands to a current-thread Tokio runtime per worker (one worker per CPU). Consequences:

- The blocking pool is still shared across all workers — `spawn_blocking` works the same way as it does anywhere else.
- Handler futures do not have to be `Send` today (current-thread workers), but **write new code as if they did**. Any future move to a multi-threaded runtime (notably a migration to axum) will require it, and the sync-core / async-edge policy already eliminates the biggest hazard (`!Send` DB handles never crossing `.await` because they live inside `spawn_blocking`).
- The escape hatch is a real code smell here: holding a blocking-pool worker on a network call can starve other requests' sync work. Use the **Sandwich** or **Channel hand-off** pattern instead.

## Anti-patterns

These are common shapes to avoid, with the reason in each case:

- **`tokio::fs::*` in a hot loop.** Each call dispatches the syscall through the blocking pool, paying the spawn-and-wake cost per syscall instead of per operation. Wrap the whole loop in one `spawn_blocking` and use `std::fs` inside.
- **Splitting a DB transaction across `.await`.** Holds a global writer lock (LMDB) or a write batch in flight (RocksDB) while waiting on something unrelated. Compile-time hazard under multi-threaded Tokio (`!Send` guards); throughput hazard under any runtime.
- **`FuturesUnordered` of `spawn_blocking` over a large batch of CPU work.** Spawns far more blocking-pool threads than cores, no work-stealing, high scheduler overhead. Use one `spawn_blocking` over a `rayon::par_iter` instead.
- **`Handle::block_on` inside `spawn_blocking` as a habit.** Ties up a blocking-pool worker on network latency. Allowed as a one-off; refactor toward the Sandwich or Channel hand-off pattern when it shows up repeatedly.
- **Relying on `BufWriter`'s `Drop` to flush.** It does not. Always `flush().await?` explicitly before any downstream operation that depends on the bytes being written.

## What this policy is not

This policy is **not** "go all the way async." `tokio-uring`, `monoio`, `glommio`, and `compio` exist on Linux, but:

- macOS has no real async filesystem IO and will not for the foreseeable future; any cross-platform async-fs library on macOS is doing exactly what `tokio::fs` does (thread pool dispatch), so there is no win there.
- Adopting a non-default Tokio runtime fragments the codebase from the wider Tokio ecosystem (actix, AWS SDK, reqwest, tower, all the `tokio::sync::*` primitives) and locks us into a runtime that is not the default for the most common Rust HTTP frameworks.
- RocksDB, LMDB, and DuckDB will never be async-native regardless of runtime choice, so the "fully async stack" goal is unreachable.

If a specific hot path measurably benefits from io_uring on Linux production, that is a separate, narrow conversation — not a reason to rework the codebase's runtime story.

This policy is also **not** "minimize async usage." Where async genuinely buys overlap (concurrent S3 calls, multipart uploads, HTTP request handling), it is the right tool and the existing code is structured well for it. `S3VersionStore` is an example of async being used as intended.
