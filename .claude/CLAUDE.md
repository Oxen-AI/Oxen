**CLAUDE.md**

This file provides guidance to AI agents when working with code in this repository.

# Project Overview

Oxen is a fast, unstructured data version control system written in Rust. It's designed to version large machine learning datasets efficiently and provides both a CLI tool and server implementation.

# Project Organization

The Cargo workspace lives at the repository root, with crates under `crates/`:
- `crates/liboxen/` - Core shared library (`liboxen`)
- `crates/oxen-cli/` - Command-line interface binary (`oxen`)
- `crates/oxen-server/` - HTTP server binary (`oxen-server`)
- `crates/oxen-py/` - Python bindings (Rust source for `oxen-python`)
- `oxen-python/` - Python package source, tests, and `pyproject.toml`

## Architecture

The project follows a workspace structure with crates in the `crates/` directory:

- **`liboxen/`** - Core shared library (`liboxen`) containing all the business logic
- **`oxen-cli/`** - Command-line interface binary (`oxen`)
- **`oxen-server/`** - HTTP server binary (`oxen-server`)

The CLI and server both depend on the shared library to avoid code duplication. All core functionality should be implemented in the lib first, then exposed through appropriate interfaces in the CLI and server.

## Key Components

### Core Architecture (`crates/liboxen/src/`)
- **`core/`** - Core data structures and database operations (RocksDB for metadata, DuckDB for tabular data)
- **`model/`** - Data structures representing commits, branches, entries, diffs, etc.
- **`repositories/`** - Repository operations (init, clone, add, commit, push, pull, etc.) - Most high-level operations start here
- **`view/`** - Response/view models for API endpoints
- **`storage/`** - Storage backends (local filesystem, S3)

### Data Storage
- Uses RocksDB for metadata and version control information
- Uses DuckDB for tabular data processing and querying
- Implements Merkle trees for efficient change detection

## Common Development Commands

*IMPORTANT*: Our codebase assumes cargo commands are run on the whole workspace, from the workspace root, _NOT_ on specific packages.
GOOD: `cargo check --workspace`
BAD: `cargo check --package liboxen`

### Building
```bash
cargo build --workspace                           # Debug build
```

### Testing
Use the `bin/test-rust` script to run the tests — do not invoke `cargo test` directly. The script builds the workspace, raises the file-handle limit, sets up a ramdisk for test data, starts `oxen-server` on a free port, runs the suite with `cargo test --workspace --no-fail-fast`, and tears everything down on exit. Its full usage is documented in a comment at the top of the script.
```bash
bin/test-rust                         # Build and run all Rust tests
bin/test-rust test_function_name      # Run only Rust tests matching test_function_name
bin/test-rust -p                      # Run the Python test suite (via pytest + maturin)
bin/test-rust -p -k test_init         # Run Python tests matching test_init
```
- The script starts `oxen-server` itself, so you do not need to start it separately.
- It does not install prerequisites by default. If a dependency is missing, run `bin/install-prereqs` (or re-run with `bin/test-rust --install-deps`).
- If the ramdisk cannot be mounted, pass `--no-ramdisk` to run against the regular filesystem.
- Arguments after the script's own flags are forwarded after `--` to the libtest binaries (or `pytest` under `-p`).
- Output the terminal doesn't show goes to two log files, both overwritten per run: `./data/test/oxen-server.log` (server subprocess stdout+stderr) and `./data/test/cargo-test.log` (cargo test's stderr — indicatif progress bars, and any `tracing` output emitted from tokio worker or `spawn_blocking` threads that don't inherit libtest's per-thread capture). Check these first when the terminal report doesn't explain a failure.

### Testing with Debug Output
```bash
env RUST_LOG=warn,liboxen=debug,integration_test=debug bin/test-rust --no-capture test_name
```

### Code Quality
```bash
cargo fmt --all                                    # Format code
cargo clippy --workspace --no-deps -- -D warnings  # Lint code
pre-commit run --all-files                         # Run pre-commit hooks (runs format and lint)
```

### Benchmarks
The `liboxen::test` helper module is gated behind the `test-utils` feature so it isn't compiled
into release builds. Benches that consume it (`download`, `fetch`, `push`, `workspace_add`)
declare `required-features = ["test-utils"]` and are skipped unless the feature is enabled:
```bash
cargo bench --features liboxen/test-utils           # Run all benches
cargo bench --features liboxen/test-utils --bench push
```
Downstream crates (`oxen-cli`, `oxen-server`) enable `test-utils` via dev-dependencies, so their
own tests can call `liboxen::test::*` as before.

### Server Development
```bash
ulimit -n 10240                     # Increase file handles before running the server
bacon server                        # Start server with live reload
```

### CLI Usage
```bash
export PATH="$PATH:/path/to/Oxen/target/debug"
oxen init                           # Initialize repository
oxen status                         # Check status
oxen add images/                    # Add files
oxen commit -m "message"            # Commit changes
oxen push origin main               # Push to remote
```

## Code Organization
- We define module exports in a `<module_name>.rs` file at the same level as the corresponding `module_name/` directory and *NOT* the older `mod.rs` pattern.
- Prefer importing bare items (structs, enums, traits, functions, constants, macros) and referring to them unqualified, rather than importing a parent module and qualifying at each use site — e.g. `use std::time::Duration;` then `Duration::from_secs(5)`, not `std::time::Duration::from_secs(5)`. Exception: keep enough of the path to disambiguate when a bare import would be ambiguous or misleading, such as two same-named items from different modules, or where a module qualifier is the established idiom (the classic case is `use std::fmt;` then `fmt::Result` to avoid clashing with the prelude `Result`); reach for an `as` alias when that reads better than a module qualifier.
- Prefer a concrete `&str` parameter over `impl AsRef<str>`. Deref coercion already lets callers pass `&String` to a `&str` parameter, so the generic form buys little while adding monomorphization cost and `.as_ref()` noise in the body. Follow this in new and changed function signatures even though much of this codebase uses `impl AsRef<str>` — prefer `&str` rather than matching that surrounding idiom, but do not mass-refactor existing signatures you aren't otherwise touching. `impl AsRef<Path>` is a separate, weaker case: keep deciding it per function, since it unifies `&str`/`String`/`PathBuf` callers in a way `&Path` cannot — do not blanket-replace it with `&Path`.

## Error Handling
- Use the result type (`Result<T, Error>`) when an operation could fail.
- Never use `.unwrap()` or `.expect()` on a `Result` or on an `Option`.
  + Exception: In test-only code, it is ok to use use `.expect(<descriptive explanation of invariant that was violated>)` since we want to fail fast and have good stack traces for failing test cases.
  + This rule still applies when the panic feels "guaranteed unreachable" because of an upstream invariant. If the enclosing function returns a `Result`, propagate it with `?` — a panic in production code is never preferable to a clean error path, no matter how confident you are the case can't happen.
- If an error type is acted upon in code, then use as specific of an error type as possible. Don't use a wider type unless it's necessary. When making modules and related pieces of code, try to use a locally-defined error enum for them if they all have similar errors.
- liboxen uses `OxenError` as the top-level type for errors. Unify different error types under `OxenError`. Fallible functions should return `Result<T, OxenError>`. Do not create or use additional error types outside of `OxenError` if it can be avoided. If an error is never inspected internally and cannot be returned to a caller of the liboxen library through a public API, then use `OxenError::InternalError` with a formatted string. If an error is inspected or can be returned to a caller of the liboxen library through a public API, make a structured error variant on the `OxenError` `enum`.
- oxen-server uses `OxenHttpError` as the top-level type for errors. All `OxenError` variants that we want to differentiate to the caller of the oxen-server API should be mapped to specific `OxenHttpError` variants. Otherwise they should be mapped to `OxenHttpError::InternalServerError`. Do not create or use additional error types outside of `OxenHttpError` if it can be avoided.
- Implement proper error propagation through the `?` operator.

# Making Changes

- This repository is **public**. Do not mention Oxen's private/internal repositories — by name or description — in code comments, doc-comments, error messages, commit messages, PR titles or descriptions, or any other code or documentation committed here. Keep references to private repos out of public artifacts entirely; if internal context is genuinely needed, point to the relevant Linear issue rather than inlining private-repo details. This also bars describing a private/internal system's behavior as the justification for a change — even when the system is not named (e.g. "a downstream client loops forever without this"); describe what the code in this repo does and guarantees instead.
- When changing something that is documented in nearby code, or appears in any markdown files in the repository, update the affected documentation.
- The `oxen` client (the CLI and other user-facing client behavior) is documented on the public docs site at docs.oxen.ai, maintained in the separate `docs` repo (Mintlify/MDX — the CLI guides live under `getting-started/command-line/`). After any change to the client — a new or changed command, flag, default, or user-visible output — check whether a docs page needs updating, and update it (likely as a separate `docs`-repo change) or call out the gap.
- When prompted to always do something a certain way in general, add an entry to this section of the CLAUDE.md file.
- When calling `get_staged_db_manager`, follow the doc comment on that function: drop the returned `StagedDBManager` as soon as possible (via a block scope or explicit `drop()`) to avoid holding the shared database handle longer than necessary.
- When altering the `OxenError` enum, consider whether a hint needs to be added or updated in the `hint` method.
- Instead of using `cargo test` to test Rust code, use the `bin/test-rust` script. The script usage is documented in a comment at the top of its file.
- If the ram disk is not able to be mounted in `bin/test-rust`, then use the `--no-ramdisk` option.
- The `bin/test-rust` script does not install prerequisites by default. If any dependencies turn out to be missing, prompt the user to run `bin/install-prereqs` (or re-run `bin/test-rust --install-deps`).
- Prefer using inline code over creating a new function when the function would only be called once and the function body would be less than 15 lines.
- Do not use "out parameters" (functions that take an `&mut Vec` / `&mut HashMap` / etc. for the callee to fill). Return the value directly instead. Exceptions: the user explicitly asks for an out parameter, or the caller genuinely needs to reuse a pre-allocated buffer across many calls to avoid allocation churn in a measured hot path.
- Don't add type annotations the compiler doesn't need. Let inference do its job and annotate only where the code won't compile (or won't resolve to the type you intend) without it — e.g. a `collect()`/`parse()`/`sum()` whose target type is otherwise ambiguous, or an integer literal that needs pinning. Redundant annotations on bindings whose type is already obvious from the right-hand side just add noise and rot when the expression changes.
- Default function parameters to the concrete type the callers actually pass (`String`, `&str`, `PathBuf`, `&Path`) instead of generic conversion bounds like `impl Into<String>` / `impl Into<PathBuf>`. Add a conversion bound only when it demonstrably removes boilerplate across several call sites that pass a genuine mix of owned and borrowed types — not by habit or to match surrounding style. A lone `""`/`String::new()` caller is not a reason; let the caller write `.to_string()` / `String::new()`, or take `&str` if no ownership is needed. (`impl AsRef<Path>` stays acceptable where it is the established idiom for path-taking helpers.)
- Version-gated deprecation checks must inline the triggering version as a literal right at the comparison site, with a brief comment naming what it gates and pointing to `docs/deprecations.md`. Do not hoist it into a named constant defined in another module/crate (e.g. `crate::constants::SOME_VERSION`): the point of the check is to see *which version* trips it while reading the check, and the indirection defeats that. (This is distinct from broad, widely-reused version floors like `MIN_OXEN_VERSION`, which legitimately stay named constants.)
- When adding server endpoints that supersede existing ones, introduce them additively alongside the old endpoints rather than replacing them. Switch the client to the new path, register the old endpoints in `docs/deprecations.md` with a removal target ~5 minor versions out, and remove them in a later tech-debt PR. Avoids flag-day breakage for older clients still in the field.
- Preserve code comments whenever possible. Comments that were written by someone other than Claude should always be preserved or updated if possible.
- Function doc-comments describe **what** the function does, not **how** it does it — unless the "how" is caller-relevant (ordering constraints like "call before `commit()`", cross-filesystem rules like "src and target must live on the same filesystem", thread-safety invariants like "never hold a guard across `.await`"). The body shows the implementation; the doc-comment's job is to convey the contract callers rely on, which rots if it duplicates internal mechanism. Trim "stamps the temp file with mtime before the rename" to "the published file carries mtime"; trim "uses the Channel hand-off helper" to whatever visible behavior the caller observes. Applies to public and private doc-comments alike, and to inline comments that risk narrating implementation details rather than describing current behavior.
- Before manually serializing a value (hand-built `serde_json::json!({...})`, a one-off request/response struct, format-string assembly), check whether an existing type or helper already serializes it. Most domain types already derive `Serialize`/`Deserialize` (and often `utoipa::ToSchema`), and there are usually conversion helpers between them (e.g. `UserConfig::to_user()` yields a `User` that serializes to the exact `{name, email}` the merge endpoint expects). Reuse the existing type with `.json(&value)` / `serde_json::to_*` and deserialize back into that same type rather than re-describing its shape by hand — the manual version silently drifts from the canonical type when fields change. Introduce a new serialization struct only when no existing type fits.
- The Python project calls into the Rust project. Whenever changing the Rust code, check to see if the Python code needs to be updated.
- After changing any Rust or Python code, verify that Rust tests pass with `bin/test-rust` and Python tests pass with `bin/test-rust -p`
- When updating a dependency, prefer updating to the latest stable version.
- When adding or updating GitHub Actions in `.github/workflows/`, pin them per the org-wide policy in [docs/github_actions_pinning.md](../docs/github_actions_pinning.md) — version tag for first-party/high-trust orgs, full commit SHA (with a `# vX.Y.Z` comment) for third-party.
- Code that touches IO follows the **sync-core / async-edge** policy in [docs/async_policy.md](../docs/async_policy.md). Summary: public APIs are `async fn`; network IO (AWS SDK, reqwest, etc.) uses native async APIs; filesystem and sync-DB IO (`std::fs`, RocksDB, LMDB, DuckDB) runs inside `tokio::task::spawn_blocking` at *operation* granularity, not per syscall; DB transactions live entirely inside one `spawn_blocking` closure (never spanning `.await`); CPU-parallel batch work uses `rayon` inside one `spawn_blocking`, not `FuturesUnordered<spawn_blocking>`; streaming sync↔network IO uses a long-lived `spawn_blocking` task paired with the async side via `tokio::sync::mpsc`. Do **not** reach for `tokio::fs` in hot loops — it dispatches each syscall through the blocking pool and pays per-call overhead. See the doc for the full patterns (Bracket, Sandwich, Channel hand-off) and anti-patterns.
- Streamed IO (anything reading or writing through an `AsyncRead`/`AsyncWrite` whose total length isn't bounded ahead of time) must use a large buffer rather than rely on `tokio::io::copy`'s 8 KB default. Wrap the write side in `tokio::io::BufWriter::with_capacity(10 * 1024 * 1024, ...)` (and the read side in `BufReader` if the source isn't already buffered), and remember to explicitly `flush().await?` the `BufWriter` before any downstream `sync_all`/rename/checksum step — `BufWriter`'s `Drop` does **not** auto-flush, so unflushed bytes are silently dropped. The canonical example is the S3 store's local-cache path in `crates/liboxen/src/storage/s3.rs` (`copy_version_to_path`). See [docs/async_policy.md](../docs/async_policy.md) for the broader async/sync context this fits into.
- All production filesystem writes go through `AtomicFile` (`crates/liboxen/src/util/fs.rs`) — never `std::fs::write` / `tokio::fs::write` / `File::create` to a canonical path directly. `AtomicFile` writes to a sibling temp file and atomically renames over the target, so a crash mid-write can never leave a torn or partially-written canonical file. Chain `.with_hash(h)` for content-addressed writes and `.with_mtime(t)` when publishing into the working tree. The non-atomic `util::fs::write_to_path` is gated to test / `test-utils` builds for fixture setup only. Detailed contract on the `AtomicFile` struct rustdoc.
- Prefer `bytes::Bytes` over `&[u8]` and `Vec<u8>` for byte payloads that cross a module, trait, `spawn_blocking`, or external-SDK boundary. `Bytes` is `'static + Clone + Send` and refcounted: `Bytes::from(Vec<u8>)` reuses the allocation (zero copy), `Bytes::from_static(b"...")` is compile-time (zero cost), and cloning a `Bytes` is a refcount bump rather than a memcpy. Most async IO ecosystems we touch (`axum` request bodies, `reqwest::Response::bytes_stream`, `aws_sdk_s3::primitives::ByteStream`) speak `Bytes` natively — handing them a `Vec<u8>` costs an allocation and handing them a `&[u8]` costs an allocation **and** a memcpy. The canonical example is `VersionStore::store_version` in `crates/liboxen/src/storage/version_store.rs`: the trait signature went from `data: &[u8]` to `data: Bytes` so the in-memory writers can move bytes across the `spawn_blocking` boundary as a refcount transfer and the S3 impl can pass through to `ByteStream::from` without a `.to_vec()` copy. `&[u8]` is still the right type for transient borrows that don't cross a boundary (hashing once and discarding, inline inspection); use `BytesMut` for growable buffers and `freeze()` into `Bytes` when handing them off (e.g. the `read_buf` + `split().freeze()` pattern in `AtomicFile::stream_async`).
- Stream a file's *content* rather than buffering the whole thing into memory: a function that reads or writes file data should pass it incrementally, using whatever streaming primitive fits the layer — an `AsyncRead`/`AsyncWrite`, a byte-chunk `Stream` (e.g. the `BoxedByteStream` alias the version store and client return), a chunked reader, an HTTP body — instead of materializing a whole-file `Vec<u8>` / `Bytes`. Every path that handles file data needs to eventually handle files larger than memory, so whole-file reads don't scale. Where a caller genuinely needs the entire payload in memory, collect it inline at the call site (a visible `while let Some(chunk) = stream.next().await { buf.extend_from_slice(&chunk?); }` drain, or `read_to_end` for an `AsyncRead`) rather than hiding it behind a shared collect helper — keeping each buffering site apparent and greppable for future migration to full streaming. (This rule is about whether to hold the whole payload at all; the `Bytes`-over-`&[u8]` rule just above is about which type to use for payloads you *do* hold.)
- When computing the number of fixed-size chunks needed to cover a total byte count, use `total_size.div_ceil(chunk_size)`, not `(total_size / chunk_size) + 1`. The `+1` form overshoots by one when `total_size` is an exact multiple of `chunk_size`, producing a spurious zero-byte chunk request that the server rejects with HTTP 500 "beyond end of file" — see the bug fixed in `download_large_entry` for what this looks like in production.
- oxen-server operations should never touch a local checkout on disk when doing operations initiated by its API.
- Always use `metadata.is_dir()` instead of `path.is_dir()`. `path.is_dir()` follows symlinks, which Oxen does not track — using it risks descending into directories outside the working tree (or into cycles via cyclic links).
- Oxen does not track symlinks. New code that traverses the working tree should check `metadata.is_symlink()` and skip rather than resolve, follow, or record symlinks.
- Never use `unsafe` code when a safe alternative would meet our needs. Reach for `unsafe` only when there is a concrete reason no safe construct will do (e.g. FFI, a measured performance requirement that safe code cannot satisfy, no clear alternative); in that case, justify the choice in a comment at the `unsafe` site.
- Any `unsafe` block or `unsafe fn` must be preceded by a `// SAFETY:` comment that explains why the operation is sound — which invariants the caller relies on, why they hold here, and what would break if they didn't. Follow the Rust style guide: <https://std-dev-guide.rust-lang.org/policy/safety-comments.html>. For an `unsafe fn`, document the caller's obligations with a `# Safety` section in the doc comment, and pair each call site with its own `// SAFETY:` comment justifying that those obligations are met.

# Testing Rules
- Use the test helpers in `crates/liboxen/src/test.rs` (e.g., `run_empty_local_repo_test`) for unit tests in the lib code.
- When picking a helper from `crates/liboxen/src/test.rs`, choose the lightest one that meets your test's actual needs. Rough cost order, cheapest first: `run_empty_dir_test_async` → `run_empty_local_repo_test_async` → `run_one_commit_local_repo_test_async` → `run_readme_remote_repo_test` (remote with a single README pushed) → `run_one_commit_sync_repo_test` (local + remote, one inline commit) → `run_training_data_repo_test_no_commits_async` (training files written, not committed) → `run_training_data_repo_test_fully_committed_async` (training files + 6 commits, no remote) → `run_training_data_fully_sync_remote` (training files + 6 commits + remote + push; ~1–2s per test). Reach for the training-data helpers **only** when your assertions depend on the specific tree structure (paths under `nlp/classification/`, `annotations/test/`, etc.). A test that just modifies, renames, or asserts about `README.md` belongs on `run_readme_remote_repo_test`, not `run_training_data_fully_sync_remote`.
- Files under `data/test/` exist to serve specific tests. When you delete or rewrite a test, audit whether any fixture file under `data/test/` is now unreferenced and delete the orphan in the same PR. Never add a fixture file speculatively or "for a future test" — add it only when the test that uses it already exists.
- When a test needs a small directory of placeholder text files, call `test::populate_dir_with_txt_files(dir, prefix, count)` instead of open-coding `util::fs::create_dir_all` + a loop over `write_txt_file_to_path`.
- Test runs override `OXEN_STREAM_SEGMENT_SIZE` to 128 KiB via `bin/test-rust`, so any file larger than ~128 KiB exercises the streamed-transfer / chunked-download code paths. When writing a test that needs to exercise that path, size the file via `stream_segment_size() + N` (where N is small — anywhere from 1 byte to ~1 MiB), **not** a hardcoded multi-MB value. A 1.1 MiB test file exercises the same chunked-transfer code as a 100 MB production file at a fraction of the cost.
- When possible, put tests in the higher-level `repositories` module rather than the lower-level, version-specific implementation.
    - e.g., Tests should go in `repositories/commits.rs` rather than `core/v_latest/commits.rs`.
- Tests create unique temporary directories and clean up automatically
- Tests share one process per crate under `cargo test`, so any per-process state (env vars, `OnceLock` / `LazyLock` / `OnceCell` singletons, cached HTTP clients / connection pools, static caches keyed by name) can leak from one test into the next. New tests must not depend on per-process state that isn't explicitly re-initialized: don't `std::env::set_var` in a test body without a scoped restore (or a `Once`-guarded write of a constant), don't cache anything at process scope that's keyed on values a sibling test might reuse (e.g. bucket names, workspace names, repo names — use UUID-derived values), and don't build `LazyLock`s whose init reads state a sibling test might have already read at a different value. When a test genuinely needs exclusive access to a process-wide singleton (a DB-cache flush, a global counter), gate it with `#[serial_test::serial(named_key)]` — pair every test that touches the same singleton on the same key. Canonical examples: the s3 cloud_reads tests mint UUID-tagged buckets in `setup()` so `polars-io`'s cloud-store cache can't collide; the `df_db` flush tests share `#[serial_test::serial(df_db_cache)]`.
