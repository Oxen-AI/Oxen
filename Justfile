# Oxen: Fast, unstructured data version control

mod rust 'oxen-rust/Justfile'
mod python 'oxen-python/Justfile'

# Build all sub-projects
build:
    just rust build
    just python build

# Run all linters and format checkers
lint:
    just rust lint
    just python lint

# Type-check all sub-projects (cargo check + mypy)
check:
    just rust check
    just python check

# Run all tests (Rust, CLI integration, and Python)
test:
    just rust test
    just rust test-cli
    just python test

# Generate all documentation
doc:
    just rust doc
    just python doc
