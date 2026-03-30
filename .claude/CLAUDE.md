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
- Use `OxenError` for all error types
- Functions should return `Result<T, OxenError>`
- Implement proper error propagation through the `?` operator

# Making Changes

- When changing something that is documented in nearby code, or appears in any markdown files in the repository, update the affected documentation.
- When prompted to always do something a certain way in general, add an entry to this section of the CLAUDE.md file.
- When calling `get_staged_db_manager`, follow the doc comment on that function: drop the returned `StagedDBManager` as soon as possible (via a block scope or explicit `drop()`) to avoid holding the shared database handle longer than necessary.
- When altering the `OxenError` enum, consider whether a hint needs to be added or updated in the `hint` method.
- After changing any Rust code, verify that tests pass with the `bin/test-rust` script (not `cargo`). The script is documented in a comment at the top of its file.
- The Python project calls into the Rust project. Whenever changing the Rust code, check to see if the Python code needs to be updated.
- After changing any Rust or Python code, verify that Rust tests pass with `bin/test-rust` and Python tests pass with `/bin/test-rust -p`

# Testing Rules
- Use the test helpers in `crates/lib/src/test.rs` (e.g., `run_empty_local_repo_test`) for unit tests in the lib code.
- Try to use the minimal helper for the scenario you are testing. E.g., don't use `run_training_data_fully_sync_remote` when `run_one_commit_local_repo_test` is enough.
- When possible, put tests in the higher-level `repositories` module rather than the lower-level, version-specific implementation.
    - e.g., Tests should go in `repositories/commits.rs` rather than `core/v_latest/commits.rs`.
- Tests create unique temporary directories and clean up automatically
