**CLAUDE.md**

This file provides guidance to AI agents when working with code in this repository.

# Project Overview

Oxen is a fast, unstructured data version control system written in Rust. It's designed to version large machine learning datasets efficiently and provides both a CLI tool and server implementation.

# Project Organization

This repository is broken into two main components:
- `oxen-rust`: contains the core Rust implementation of the library, CLI, and server.
- `oxen-python`: provides Python bindings for the Oxen library.

# oxen-rust

## Architecture

The project follows a workspace structure with three main components in the `src/` directory:

- **`lib/`** - Core shared library (`liboxen`) containing all the business logic
- **`cli/`** - Command-line interface binary (`oxen`)
- **`server/`** - HTTP server binary (`oxen-server`)

The CLI and server both depend on the shared library to avoid code duplication. All core functionality should be implemented in the lib first, then exposed through appropriate interfaces in the CLI and server.

## Key Components

### Core Architecture (`src/lib/src/`)
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
GOOD: `cargo check`
BAD: `cargo check --package liboxen`

### Building
```bash
cargo build                           # Debug build
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
cargo fmt                              # Format code
cargo clippy --no-deps -- -D warnings  # Lint code
pre-commit run --all-files             # Run pre-commit hooks (runs format and lint)
```

### Server Development
```bash
ulimit -n 10240                     # Increase file handles before running the server
bacon server                        # Start server with live reload
```

### CLI Usage
```bash
export PATH="$PATH:/path/to/Oxen/oxen-rust/target/debug"
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

# Testing Rules
- Use the test helpers in `src/lib/src/test.rs` (e.g., `run_empty_local_repo_test`) for unit tests in the lib code.
- Try to use the minimal helper for the scenario you are testing. E.g., don't use `run_training_data_fully_sync_remote` when `run_one_commit_local_repo_test` is enough.
- When possible, put tests in the higher-level `repositories` module rather than the lower-level, version-specific implementation.
    - e.g., Tests should go in `repositories/commits.rs` rather than `core/v_latest/commits.rs`.
- Tests create unique temporary directories and clean up automatically

