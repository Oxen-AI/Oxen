# Oxen CLI Tests

This directory contains the Python test suite for the Oxen CLI.

## Setup

1. Install dependencies using uv:
```bash
uv sync
```

2. Ensure oxen CLI is in your PATH:
```bash
export PATH="$PATH:/path/to/oxen-rust/target/debug"
```

3. Start a local Oxen server (required for remote operations):
```bash
oxen-server start
```

## Running Tests

Run all tests:
```bash
uv run pytest tests   # or from the oxen-rust root: `just test-cli`
```

Run specific test file:
```bash
uv run pytest tests/test_add.py
```

Run specific test:
```bash
uv run pytest tests/test_add.py::TestAddCommand::test_add_with_relative_paths_from_subdirectories
```

## Test Patterns

### CLI Runner Helper

All tests use the `CLIRunner` helper class (provided via `oxen` fixture) to run commands:

```python
def test_example(oxen):
    # Run a command
    result = oxen.run("init")

    # Check success
    assert result.returncode == 0

    # Check output
    assert "Initialized" in result.stdout
```

### Test Directory Management

Tests use the `test_dir` fixture which:
- Creates a temporary directory
- Changes to that directory
- Automatically cleans up after the test

```python
def test_with_temp_dir(test_dir, oxen):
    # Already in a temporary directory
    oxen.run("init")
    # Files created here will be cleaned up automatically
```

### Unique IDs

Use the `unique_id` fixture to generate unique identifiers for test resources:

```python
def test_with_unique_resource(unique_id, oxen):
    repo_name = f"test-repo-{unique_id}"
    # Creates a unique repo name to avoid conflicts
```

## Environment Variables

The tests respect `.env` files via `python-dotenv`. Create a `.env` file in the cli-test directory if needed.

## Performance Tests

The `performance_tests` directory is currently not used. The intent is for some basic performance tests to run in CI.
