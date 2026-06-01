**CLAUDE.md**

This file provides guidance to AI agents when working with code in this repository.

# Project Overview

Oxen is a fast, unstructured data version control system written in Rust. It's designed to version large machine learning datasets efficiently and provides both a CLI tool and server implementation.

# Project Organization

The Cargo workspace lives at the repository root, with crates under `crates/`:
- `crates/lib/` - Core shared library (`liboxen`)
- `crates/cli/` - Command-line interface binary (`oxen`)
- `crates/server/` - HTTP server binary (`oxen-server`)
- `crates/oxen-py/` - Python bindings (Rust source for `oxen-python`)
- `oxen-python/` - Python package source, tests, and `pyproject.toml`

## Architecture

The project follows a workspace structure with crates in the `crates/` directory:

- **`lib/`** - Core shared library (`liboxen`) containing all the business logic
- **`cli/`** - Command-line interface binary (`oxen`)
- **`server/`** - HTTP server binary (`oxen-server`)

The CLI and server both depend on the shared library to avoid code duplication. All core functionality should be implemented in the lib first, then exposed through appropriate interfaces in the CLI and server.

## Key Components

### Core Architecture (`crates/lib/src/`)
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
Many tests require the oxen server to be running. If it is not running on port 3000 and
a test fails because it cannot connect to oxen-server, then start it:
```bash
cargo run -p oxen-server start
```

Run specific tests:
```bash
cargo test test_function_name         # Run specific matching tests
cargo test --lib test_function_name   # Run specific library test
```

Run all tests
```bash
ulimit -n 10240                       # Increase file handles before running tests
cargo nextest run                     # Run all tests
```

### Testing with Debug Output
```bash
env RUST_LOG=warn,liboxen=debug,integration_test=debug cargo test -- --nocapture test_name
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

## Error Handling
- Use the result type (`Result<T, Error>`) when an operation could fail.
- Never use `.unwrap()` or `.expect()` on a `Result` or on an `Option`.
  + Exception: In test-only code, it is ok to use use `.expect(<descriptive explanation of invariant that was violated>)` since we want to fail fast and have good stack traces for failing test cases.
  + This rule still applies when the panic feels "guaranteed unreachable" because of an upstream invariant. If the enclosing function returns a `Result`, surface the case as a structured `OxenError` variant and propagate with `?` — a panic in production code is never preferable to a clean error path, no matter how confident you are the case can't happen.
- Use as specific of an error type as possible for a function. Don't use a wider type unless it's necessary. When making modules and related pieces of code, try to use a locally-defined error enum for them if they all have similar errors.
- Make sure there's an `OxenError` variant for every error type. Be liberal in wrapping other modules error types, or other specific error types, in a new variant. Use a `Box<>` wrapper for it and have a `#[from]` to derive.
- `OxenError` is the top-level type for everything. If you need to unify different error types into one, use `OxenError`. These kinds of functions should return `Result<T, OxenError>`
- Implement proper error propagation through the `?` operator.
- Never write code that uses `OxenError::Basic` or `OxenError::InternalError`. Do not use their constructor methods either (`OxenError::basic_str` and `OxenError::internal_error`, respectively). These variants are deprecated and will be removed. As a general principle, never encode an error as a string: it throws away valuable structured information about the error, which makes it impossible for a caller to handle the error programmatically. Instead, make a structured error variant of an appropriate error `enum` that describes the error. Include a `#[error("...")]` on the variant to provide a helpful user-facing error message describing the problem and, if applicable, a possible command or procedure the user can perform to fix the error.
- If making logically related code that all share the same kind of errors, then strongly consider making a unique error `enum` _for that code only_. If that code is called by other code that has a different error enum, then define an explicit `impl From<SourceError> for TargetError` conversion to convert. If that makes the code messy, then make a conversion into `OxenError` and upgrade the calling function to return `OxenError` instead.

# Making Changes

- When changing something that is documented in nearby code, or appears in any markdown files in the repository, update the affected documentation.
- When prompted to always do something a certain way in general, add an entry to this section of the CLAUDE.md file.
- When calling `get_staged_db_manager`, follow the doc comment on that function: drop the returned `StagedDBManager` as soon as possible (via a block scope or explicit `drop()`) to avoid holding the shared database handle longer than necessary.
- When altering the `OxenError` enum, consider whether a hint needs to be added or updated in the `hint` method.
- Instead of using `cargo test` to test Rust code, use the `bin/test-rust` script. The script usage is documented in a comment at the top of its file.
- If the ram disk is not able to be mounted in `bin/test-rust`, then use the `--no-ramdisk` option.
- The `bin/test-rust` script does not install prerequisites by default. If any dependencies turn out to be missing, prompt the user to run `bin/install-prereqs` (or re-run `bin/test-rust --install-deps`).
- Prefer using inline code over creating a new function when the function would only be called once and the function body would be less than 15 lines.
- Do not use "out parameters" (functions that take an `&mut Vec` / `&mut HashMap` / etc. for the callee to fill). Return the value directly instead. Exceptions: the user explicitly asks for an out parameter, or the caller genuinely needs to reuse a pre-allocated buffer across many calls to avoid allocation churn in a measured hot path.
- Preserve comments whenever possible. Comments that were written by someone other than Claude should always be preserved or updated if possible.
- The Python project calls into the Rust project. Whenever changing the Rust code, check to see if the Python code needs to be updated.
- After changing any Rust or Python code, verify that Rust tests pass with `bin/test-rust` and Python tests pass with `bin/test-rust -p`
- When updating a dependency, prefer updating to the latest stable version.
- Code that touches IO follows the **sync-core / async-edge** policy in [docs/async_policy.md](../docs/async_policy.md). Summary: public APIs are `async fn`; network IO (AWS SDK, reqwest, etc.) uses native async APIs; filesystem and sync-DB IO (`std::fs`, RocksDB, LMDB, DuckDB) runs inside `tokio::task::spawn_blocking` at *operation* granularity, not per syscall; DB transactions live entirely inside one `spawn_blocking` closure (never spanning `.await`); CPU-parallel batch work uses `rayon` inside one `spawn_blocking`, not `FuturesUnordered<spawn_blocking>`; streaming sync↔network IO uses a long-lived `spawn_blocking` task paired with the async side via `tokio::sync::mpsc`. Do **not** reach for `tokio::fs` in hot loops — it dispatches each syscall through the blocking pool and pays per-call overhead. See the doc for the full patterns (Bracket, Sandwich, Channel hand-off) and anti-patterns.
- Streamed IO (anything reading or writing through an `AsyncRead`/`AsyncWrite` whose total length isn't bounded ahead of time) must use a large buffer rather than rely on `tokio::io::copy`'s 8 KB default. Wrap the write side in `tokio::io::BufWriter::with_capacity(10 * 1024 * 1024, ...)` (and the read side in `BufReader` if the source isn't already buffered), and remember to explicitly `flush().await?` the `BufWriter` before any downstream `sync_all`/rename/checksum step — `BufWriter`'s `Drop` does **not** auto-flush, so unflushed bytes are silently dropped. The canonical example is the S3 store's local-cache path in `crates/lib/src/storage/s3.rs` (`store_version_to_path`). See [docs/async_policy.md](../docs/async_policy.md) for the broader async/sync context this fits into.
- Prefer `bytes::Bytes` over `&[u8]` and `Vec<u8>` for byte payloads that cross a module, trait, `spawn_blocking`, or external-SDK boundary. `Bytes` is `'static + Clone + Send` and refcounted: `Bytes::from(Vec<u8>)` reuses the allocation (zero copy), `Bytes::from_static(b"...")` is compile-time (zero cost), and cloning a `Bytes` is a refcount bump rather than a memcpy. Most async IO ecosystems we touch (`axum` request bodies, `reqwest::Response::bytes_stream`, `aws_sdk_s3::primitives::ByteStream`) speak `Bytes` natively — handing them a `Vec<u8>` costs an allocation and handing them a `&[u8]` costs an allocation **and** a memcpy. The canonical example is `VersionStore::store_version` in `crates/lib/src/storage/version_store.rs`: the trait signature went from `data: &[u8]` to `data: Bytes` so the in-memory writers can move bytes across the `spawn_blocking` boundary as a refcount transfer and the S3 impl can pass through to `ByteStream::from` without a `.to_vec()` copy. `&[u8]` is still the right type for transient borrows that don't cross a boundary (hashing once and discarding, inline inspection); use `BytesMut` for growable buffers and `freeze()` into `Bytes` when handing them off (e.g. the `read_buf` + `split().freeze()` pattern in `atomic_write_from_async_reader`).
- oxen-server operations should never touch a local checkout on disk when doing operations initiated by its API.
- Always use `metadata.is_dir()` instead of `path.is_dir()`. `path.is_dir()` follows symlinks, which Oxen does not track — using it risks descending into directories outside the working tree (or into cycles via cyclic links).
- Oxen does not track symlinks. New code that traverses the working tree should check `metadata.is_symlink()` and skip rather than resolve, follow, or record symlinks.
- Never use `unsafe` code when a safe alternative would meet our needs. Reach for `unsafe` only when there is a concrete reason no safe construct will do (e.g. FFI, a measured performance requirement that safe code cannot satisfy, no clear alternative); in that case, justify the choice in a comment at the `unsafe` site.
- Any `unsafe` block or `unsafe fn` must be preceded by a `// SAFETY:` comment that explains why the operation is sound — which invariants the caller relies on, why they hold here, and what would break if they didn't. Follow the Rust style guide: <https://std-dev-guide.rust-lang.org/policy/safety-comments.html>. For an `unsafe fn`, document the caller's obligations with a `# Safety` section in the doc comment, and pair each call site with its own `// SAFETY:` comment justifying that those obligations are met.

# Testing Rules
- Use the test helpers in `crates/lib/src/test.rs` (e.g., `run_empty_local_repo_test`) for unit tests in the lib code.
- Try to use the minimal helper for the scenario you are testing. E.g., don't use `run_training_data_fully_sync_remote` when `run_one_commit_local_repo_test` is enough.
- When possible, put tests in the higher-level `repositories` module rather than the lower-level, version-specific implementation.
    - e.g., Tests should go in `repositories/commits.rs` rather than `core/v_latest/commits.rs`.
- Tests create unique temporary directories and clean up automatically
