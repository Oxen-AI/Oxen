**AGENTS.md**

This file provides guidance to AI agents when working with code in this repository.

Before working on files in a directory below the session's starting working directory, check each directory along the path for `AGENTS.md`. Read each of them from shallowest to deepest, and follow its instructions for work in that subtree. Repeat when work moves into a different subtree.

# Project Overview

Oxen is a fast, unstructured data version control system written in Rust. It's designed to version large machine learning datasets efficiently and provides both a CLI tool and server implementation.

# Project Organization

The Cargo workspace lives at the repository root, with crates under `crates/`:
- `crates/liboxen/`: the core shared library (`liboxen`), holding all the business logic
- `crates/oxen-cli/`: the command-line interface binary (`oxen`)
- `crates/oxen-server/`: the HTTP server binary (`oxen-server`)
- `crates/oxen-py/`: the Python bindings (Rust source for `oxen-python`)
- `oxen-python/`: the Python package source, tests, and `pyproject.toml`

The CLI and server both depend on the shared library. Implement core functionality in the lib first, then expose it through the CLI and server.

## Key Components (`crates/liboxen/src/`)

- **`core/`**: core data structures and database operations
- **`model/`**: commits, branches, entries, diffs, and the like
- **`repositories/`**: repository operations (init, clone, add, commit, push, pull, and so on). Most high-level operations start here.
- **`view/`**: response and view models for API endpoints
- **`storage/`**: storage backends (local filesystem, S3)

Metadata and version control information live in RocksDB and LMDB (through `heed`), tabular data is processed and queried with DuckDB, and Merkle trees detect changes.

## Common Development Commands

*IMPORTANT*: run cargo commands on the whole workspace, from the workspace root, _NOT_ on specific packages.
GOOD: `cargo check --workspace`
BAD: `cargo check --package liboxen`

### Building

```bash
cargo build --workspace                           # Debug build
cargo build --workspace --bins --tests            # Debug build that a later `cargo test` reuses
```

A `cargo build` that a `cargo test` follows, in a script or a CI job, builds `--bins --tests` with the same packages and features the test command selects, as `bin/test` and the Rust Tests jobs do. Dev-dependencies add features to crates such as `hyper` and `getrandom`, so a plain `cargo build` resolves a different feature set, and the `cargo test` after it compiles a second copy of every dependency above them, DuckDB, RocksDB, polars, and the AWS SDK included.

### Testing

Run tests with `bin/test`, never `cargo test` directly. The script builds the workspace, raises the file-handle limit, sets up a ramdisk for test data, starts `oxen-server` on a free port, exports the environment the tests expect, runs `cargo test --workspace --no-fail-fast`, and tears everything down on exit. Its full usage is in a comment at the top of the script. `cargo test` alone works only after reproducing that setup by hand (the "Manual Test Setup" section of `crates/liboxen/README.md`).

```bash
bin/test                         # Build and run all Rust tests
bin/test test_function_name      # Run only Rust tests matching test_function_name
bin/test --timings test_name     # Report each matching test's duration
bin/test -p                      # Run the Python test suite (via pytest + maturin)
bin/test -p -k test_init         # Run Python tests matching test_init
env RUST_LOG=warn,liboxen=debug,integration_test=debug bin/test --no-capture test_name  # Debug output
```

- It does not install prerequisites by default. If a dependency is missing, ask the user to run `bin/install-prereqs`, or re-run with `bin/test --install-deps`.
- If the ramdisk cannot be mounted, pass `--no-ramdisk` to run against the regular filesystem.
- Arguments after the script's own flags are forwarded after `--` to the libtest binaries (or `pytest` under `-p`).
- Output the terminal doesn't show goes to two log files: `./data/test/oxen-server.log` (the server's stdout and stderr), overwritten by every run, and `./data/test/cargo-test.log` (cargo test's stderr: indicatif progress bars, and `tracing` output from tokio worker or `spawn_blocking` threads, which don't inherit libtest's per-thread capture), overwritten only by a Rust run, so after `bin/test -p` it holds the last Rust run's output. Check these first when the terminal report doesn't explain a failure.
- After changing any Rust or Python code, check that the Rust tests pass with `bin/test` and the Python tests with `bin/test -p`.
- **Read [docs/testing.md](docs/testing.md) before writing, editing, merging, deleting, or timing a test.** It covers choosing a fixture helper, folding tests that share a fixture into one, keeping coverage when tests merge, timing a change, and process-global state.

### Code Quality

```bash
cargo fmt --all                                    # Format code
cargo clippy --workspace --no-deps -- -D warnings  # Lint code
pre-commit run --all-files                         # Run pre-commit hooks (runs format and lint)
```

### Benchmarks

The `liboxen::test` helper module is gated behind the `test-utils` feature, so it isn't compiled into release builds. Benches that use it (`download`, `fetch`, `push`, `workspace_add`) declare `required-features = ["test-utils"]` and are skipped unless the feature is enabled:

```bash
cargo bench --features liboxen/test-utils           # Run all benches
cargo bench --features liboxen/test-utils --bench push
```

`oxen-cli` and `oxen-server` enable `test-utils` through their dev-dependencies, so their own tests can call `liboxen::test::*`.

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

- Define module exports in a `<module_name>.rs` file beside the `module_name/` directory, *NOT* the older `mod.rs` pattern.
- Import bare items (structs, enums, traits, functions, constants, macros) and refer to them unqualified, rather than importing a parent module and qualifying at each use site: `use std::time::Duration;` then `Duration::from_secs(5)`, not `std::time::Duration::from_secs(5)`. Keep enough of the path to disambiguate where a bare import would be ambiguous or misleading (two same-named items from different modules), or where a module qualifier is the established idiom (`use std::fmt;` then `fmt::Result`, which avoids clashing with the prelude `Result`). Reach for an `as` alias when that reads better than a module qualifier.
- **Take the concrete type, and make a generic earn its place.** When you write or change a function, give each parameter the type its callers pass: `&str`, `&Path`, `&OsStr`, `&[T]`, `String`, `PathBuf`. Deref coercion already turns `&String`, `&PathBuf`, `&OsString`, and `&Vec<T>` into the borrowed form, so a conversion bound (`impl AsRef<_>`, `impl Into<_>`, `impl Borrow<_>`) or a type parameter buys those callers nothing and adds monomorphization cost and `.as_ref()`/`.into()` noise in the body.
  + Add a generic, or keep an existing one, only when the function must accept whatever type its caller has, such as any iterator (`impl Iterator<Item = T>`) or any reader (`impl Read`).
  + Callers passing different types is not a reason, even when coercion can't unify them: if some pass a `&str` and others a `&PathBuf`, take `&Path` and let the `&str` callers write `Path::new(s)`. Nor is a lone `""` or `String::new()` caller: let it write `.to_string()`, or take `&str` if no ownership is needed. Nor is matching the surrounding functions, even though much of this codebase uses `impl AsRef<str>` and `impl AsRef<Path>`.
  + Reconsider a function's generics only when you're already changing that function for the task at hand.
- **Carry a hash as `MerkleHash`, by value.** It is a 16-byte `Copy` type, so a parameter takes `MerkleHash`, not `&MerkleHash` or a hex `&str`, and code holding one passes it rather than formatting it for a callee to parse back. Format it to a string only where a format needs one: a JSON field (through `MerkleHashAsString`), a path or S3 key, or a message. ENG-1014 tracks moving the rest of the codebase over.
- **Give every new item the narrowest visibility that compiles, and widen it only when a real caller at that distance appears.** Prefer private, then `pub(super)` or `pub(crate)`, then `pub`. In `liboxen`, `pub` means the item is intended for `oxen-server`, `oxen-cli`, or the Python bindings to call. This applies to every new `fn`, `struct`, `enum`, `trait`, `const`, type alias, field, and module, and to each name added to a `pub use` re-export. Widening later is backward-compatible and cheap, while narrowing later is a breaking change. A child module can already read its ancestors' private items through `super`, so a helper shared between a parent and its children needs no `pub`, and a `pub fn` returning a third-party type leaks that dependency into the wider API.
- Prefer inline code over a new function when the function would be called once and its body would be under 15 lines.
- Do not use out parameters (a function taking an `&mut Vec` or `&mut HashMap` for the callee to fill). Return the value instead, unless the user asks for one or a measured hot path needs to reuse a pre-allocated buffer across many calls.
- Don't add type annotations the compiler doesn't need. Annotate only where the code won't compile, or won't resolve to the intended type, without one: a `collect()`, `parse()`, or `sum()` whose target type is otherwise ambiguous, or an integer literal that needs pinning.
- Before serializing a value by hand (a `serde_json::json!({...})`, a one-off request or response struct, format-string assembly), check whether an existing type or helper already serializes it. Most domain types derive `Serialize` and `Deserialize` (and often `utoipa::ToSchema`), and there are usually conversion helpers between them (`UserConfig::to_user()` yields a `User` that serializes to the exact `{name, email}` the merge endpoint expects). Reuse the existing type with `.json(&value)` or `serde_json::to_*`, and deserialize back into that same type, since a hand-written shape drifts from the canonical type when fields change. Introduce a new serialization struct only when no existing type fits.
- **Name every `tracing` field by the OpenTelemetry conventions.** Each field on a span or event is exported as an OpenTelemetry attribute, and a concept carries the same name on every span and log record, so a query on one name finds all of them. Search for an existing name (`rg -oI 'oxen\.[a-z_.]+ =' crates | sort -u` lists the ones in use) before adding a new one.
  + Use the name the [semantic conventions](https://opentelemetry.io/docs/specs/semconv/) give a concept when they have one: `exception.message` for an error (`exception.message = ?err`), and `file.path` for a path on the local filesystem that is not repository data.
  + Name an Oxen concept `oxen.<snake_case_noun>`: `oxen.repository_path` (a repository's directory), `oxen.repository_id`, `oxen.commit_id`, `oxen.branch_name`, `oxen.workspace_id`, `oxen.namespace`, `oxen.file_path` (a path inside a repository), `oxen.file_hash`. A count ends in `_count` (`oxen.entry_count`, not `num_entries`).
  + Write the full name at each call site (`tracing::warn!(oxen.commit_id = %id, ...)`), not through a constant or a helper that adds the prefix, so a name in a query can be found with a plain search.
  + Give `#[tracing::instrument]` `skip_all` and list the fields in `fields(...)`. Without `skip_all`, every argument not named in `skip(...)` is recorded as a field named after the argument, which renaming a parameter silently changes.
  + Leave alone the names libraries emit, such as `log.target` on records bridged from the `log` crate and `http.*` on the request span.
- Never use `unsafe` code when a safe alternative would meet our needs. Use it only for a concrete reason no safe construct can meet (FFI, a measured performance requirement), and justify the choice in a comment at the `unsafe` site.
- Precede every `unsafe` block or `unsafe fn` with a `// SAFETY:` comment saying why the operation is sound: which invariants it relies on, why they hold here, and what would break if they didn't, per the [Rust style guide](https://std-dev-guide.rust-lang.org/policy/safety-comments.html). Document an `unsafe fn`'s obligations in a `# Safety` section of its doc comment, and pair each call site with its own `// SAFETY:` comment saying why they are met.

## Comments

- Preserve code comments where possible. Comments written by someone other than the agent are always preserved or updated.
- Function doc-comments describe **what** the function does, not **how**, unless the how matters to callers (an ordering constraint such as "call before `commit()`", a filesystem rule such as "src and target must live on the same filesystem", a thread-safety invariant such as "never hold a guard across `.await`"). The doc-comment conveys the contract callers rely on, and it rots when it duplicates the body's mechanism. Trim "stamps the temp file with mtime before the rename" to "the published file carries mtime". This applies to public and private doc-comments alike, and to inline comments that narrate implementation rather than describe behavior.
- The set of merkle nodes and version blobs reachable from a commit is the **reachable objects** or **reachable set**, never the "closure" or "object closure", in identifiers, comments, error messages, commit messages, and issue text. "Closure" is fine for Rust closures.
- A file in the version store is a **version file** (or versioned file) in comments, doc-comments, error messages, commit messages, and issue text, never a bare "version" or "the version's data", since "version" also means a release or a commit. Identifiers such as `version_path` are fine.

## Error Handling

- **Read [docs/error_handling.md](docs/error_handling.md) before writing or changing any error handling, and follow it**: whenever a change adds an `OxenError` or `OxenHttpError` variant, changes how one is raised or classified, alters an HTTP status or a log level, or touches `hint`, `is_not_found`, `is_fatal_for_retry`, or `error_response`. Classification lives in `match` arms that fall through to a default, so a variant missing from a list still compiles and passes the tests while being classified wrong at runtime. Walk the doc's checklist after adding a variant, including whether `hint` needs an entry.
- Never use `.unwrap()` or `.expect()` on a `Result` or an `Option` in production code, even where a panic looks unreachable because of an upstream invariant: propagate the error with `?`. Test-only code may use `.expect(<the invariant that was violated>)` to fail fast with a good stack trace.
- `liboxen` returns `Result<T, OxenError>`. An error that is never inspected internally and cannot reach a caller of the library's public API is `OxenError::InternalError` with a formatted string. One that is inspected or can reach such a caller gets a structured variant on `OxenError`. Where code acts on an error, use as specific a type as possible, and a module whose errors are alike may define a local error enum unified under `OxenError`.
- `oxen-server` uses `OxenHttpError`. Map each `OxenError` variant the API's caller needs to tell apart to a specific `OxenHttpError` variant, and the rest to `OxenHttpError::InternalServerError`.

# Making Changes

- This repository is **public**. Do not mention Oxen's private or internal repositories, by name or description, in code comments, doc-comments, error messages, commit messages, PR titles or descriptions, or any other code or documentation committed here. Where internal context is needed, point to the Linear issue. This also bars justifying a change by a private system's behavior even without naming the system ("a downstream client loops forever without this"): describe what the code in this repo does and guarantees instead.
- When changing something documented in nearby code or in any Markdown file in the repository, update that documentation.
- The `oxen` client (the CLI and other user-facing client behavior) is documented on docs.oxen.ai, maintained in the separate `docs` repo (Mintlify MDX, with the CLI guides under `getting-started/command-line/`). After a change to the client (a new or changed command, flag, default, or user-visible output), check whether a docs page needs updating, and update it in a separate `docs`-repo change or call out the gap.
- The Python project calls into the Rust project, so a Rust change may need a Python one. **When a Rust model or view gains a field, add it to the Python interface too.** `PyWorkspace`, `PyCommit`, `PyRemoteRepo`, and the other `crates/oxen-py/src/py_*.rs` wrappers each mirror a Rust type, and so do their `oxen-python/python/oxen/*.py` counterparts. A new field is not done until both layers expose it, even though the crate compiles and no test fails without it. Follow the precedent for the field's type (`PyCommit::timestamp` returns a `String`, so a timestamp crosses as an RFC 3339 `String`), add the getter to the `#[pymethods]` block, and add the matching `@property` with a docstring to the Python class. `oxen-python/AGENTS.md` covers the docstrings.
- When prompted to always do something a certain way in general, add an entry to this section of the AGENTS.md file.
- **Server-global state lives directly under `$SYNC_DIR`, never under a `$SYNC_DIR/.oxen/` directory.** A file goes at `$SYNC_DIR/<name>` and a set of files in `$SYNC_DIR/<dir>/`, beside `last_migration.txt` and the `name_table/` index. A `.oxen` directory means "this is a repository" everywhere else in the product, so one at the top of the sync dir makes the whole data directory answer to `util::fs::get_repo_root`, which the CLI walks up from the working directory to find. A new top-level entry:
  + adds its name to `SERVER_OWNED_DIRS` in `crates/liboxen/src/sync_dir.rs`, since otherwise `namespaces::list` and `namespaces::get` report the directory as a namespace on `GET /api/namespaces` and `GET /api/namespaces/<name>`, and `sync_dir::namespace_dirs`, which every walk over the server's repositories starts from, treats it as one
  + stays out of `SelfHosting.md`, which covers what an operator configures and sees rather than the server's internals (`last_migration.txt` is likewise absent). Document there only the operator-visible behavior the state creates, if any.
- **Until 1.0, `SelfHosting.md` covers the current state of hosting, not how it got there.** Leave out past behavior, legacy on-disk forms, upgrade notes, and migrations (a step a CLI user or self-hosted operator runs on upgrading goes in `docs/migrations.md`). When a current behavior carries a caveat that applies only to repositories an older release created, drop the caveat rather than explaining the older release.
- **A RocksDB database a repository opens by path goes through a `WeakDbCache` registry** (`crates/liboxen/src/core/db/weak_cache.rs`), as refs, staged, the workspace name index, and the commit count caches do: a `LazyLock<WeakDbCache<_>>` static with its own warm capacity, opening through `get_or_open`. Never hold a process-wide lock across `DB::open`, which makes every repository's open wait on every other's, and never reopen the database on every call. Kept-warm handles hold the database's files open after the last caller drops them, so code that moves or removes a directory holding one evicts through the store's `remove_*_from_cache_with_children` both before and after the filesystem call, capturing the call's result and propagating it after the second eviction, since a handle opened while the move or removal runs keeps the old files open. A new registry also gets an eviction call in the test cleanup helpers that remove repository directories (`maybe_cleanup_repo` in liboxen, `cleanup_sync_dir` in oxen-server), which evict every registry first.
- When calling `get_staged_db_manager`, follow its doc comment: drop the returned `StagedDBManager` as soon as possible, with a block scope or an explicit `drop()`.
- Version-gated deprecation checks inline the triggering version as a literal at the comparison site, with a brief comment saying what it gates and pointing to `docs/deprecations.md`. Do not hoist it into a named constant in another module or crate (`crate::constants::SOME_VERSION`), since the point is to see which version trips the check while reading it. Broad, widely reused version floors such as `MIN_OXEN_VERSION` stay named constants.
- When adding server endpoints that supersede existing ones, introduce them alongside the old endpoints rather than replacing them. Switch the client to the new path, register the old endpoints in `docs/deprecations.md` with the release numbers that deprecate and remove them (removal about 5 minor versions out, and ask for the deprecating release's number rather than writing a placeholder when you don't know it), and remove them in a later tech-debt PR. This avoids flag-day breakage for older clients still in the field.
- When updating a dependency, prefer the latest stable version.
- Code that touches IO follows the **sync-core, async-edge** policy in [docs/async_policy.md](docs/async_policy.md). Read it before adding IO, a `spawn_blocking`, a DB transaction, `rayon` work, or a channel between sync and async code. In short: public APIs callable from HTTP handlers are `async fn`, network IO uses native async APIs, and filesystem and sync-DB IO (`std::fs`, RocksDB, LMDB, DuckDB) runs inside `tokio::task::spawn_blocking` at operation granularity, never `tokio::fs` in a hot loop. `crates/oxen-server/AGENTS.md` has the server's own spawning rule.
- Streamed IO (anything reading or writing through an `AsyncRead` or `AsyncWrite` whose total length isn't bounded ahead of time) uses a large buffer rather than `tokio::io::copy`'s 8 KB default. Wrap the write side in `tokio::io::BufWriter::with_capacity(10 * 1024 * 1024, ...)` (and the read side in `BufReader` if the source isn't already buffered), and explicitly `flush().await?` the `BufWriter` before any downstream `sync_all`, rename, or checksum step, since its `Drop` does **not** flush and unflushed bytes are silently lost. The canonical example is the S3 store's local-cache path in `crates/liboxen/src/storage/s3.rs` (`copy_version_to_path`).
- **All production filesystem writes go through `AtomicFile`** (`crates/liboxen/src/util/fs.rs`), never `std::fs::write`, `tokio::fs::write`, or `File::create` to a canonical path. `crates/liboxen/src/util/AGENTS.md` and the struct's rustdoc have the contract.
- **A temp file in production code goes on the data volume**, in a directory under the repository or the sync dir (`NamedTempFile::new_in(dir)`), never the OS temp dir (`NamedTempFile::new()`, `std::env::temp_dir()`), which can be small or memory-backed while a version file can be many GiB. `VersionStore::materialize`'s doc-comment says when a caller should reach for a local copy at all.
- Prefer `bytes::Bytes` over `&[u8]` and `Vec<u8>` for byte payloads that cross a module, trait, `spawn_blocking`, or external-SDK boundary. `Bytes` is `'static + Clone + Send` and refcounted: `Bytes::from(Vec<u8>)` reuses the allocation, `Bytes::from_static(b"...")` costs nothing, and cloning one is a refcount bump rather than a copy. The async IO libraries we use (`axum` request bodies, `reqwest::Response::bytes_stream`, `aws_sdk_s3::primitives::ByteStream`) take `Bytes` natively, so handing them a `Vec<u8>` costs an allocation and a `&[u8]` costs an allocation and a copy. The canonical example is `VersionStore::store_version` in `crates/liboxen/src/storage/version_store.rs`, which takes `data: Bytes` so the in-memory writers move bytes across `spawn_blocking` as a refcount transfer and the S3 impl passes them to `ByteStream::from` without a copy. `&[u8]` stays right for transient borrows that don't cross a boundary (hashing once, inline inspection). Use `BytesMut` for growable buffers and `freeze()` it into `Bytes` when handing it off (the `read_buf` and `split().freeze()` pattern in `AtomicFile::stream_async`).
- Stream a file's *content* rather than buffering the whole file in memory: a function that reads or writes file data passes it incrementally through whatever streaming primitive fits the layer (an `AsyncRead` or `AsyncWrite`, a byte-chunk `Stream` such as the `BoxedByteStream` alias the version store and client return, a chunked reader, an HTTP body), since every path that handles file data eventually has to handle files larger than memory. Where a caller needs the entire payload in memory, collect it inline at the call site (a visible `while let Some(chunk) = stream.next().await { buf.extend_from_slice(&chunk?); }` drain, or `read_to_end` for an `AsyncRead`) rather than behind a shared collect helper, so each buffering site stays easy to find. The `Bytes` rule above is about which type to use for a payload you do hold.
- Count the fixed-size chunks covering a total byte count with `total_size.div_ceil(chunk_size)`, not `(total_size / chunk_size) + 1`. The `+1` form overshoots by one when `total_size` is an exact multiple of `chunk_size`, producing a zero-byte chunk request that the server rejects with HTTP 500 "beyond end of file" (the bug fixed in `download_large_entry`).
- When traversing a repository's working tree, use `metadata.is_dir()`, not `path.is_dir()`, which follows symlinks into directories outside the working tree or around cycles. Oxen does not track symlinks, so new code that walks the working tree checks `metadata.is_symlink()` and skips them rather than resolving, following, or recording them. This applies to the working tree only: walks over oxen-server's sync dir and over a repository's `.oxen` internals follow symlinks, as the server does when it serves a repository.
