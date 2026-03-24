# Scripts



## Data Generation & Organization

### `generate-dataset`

Generates synthetic test datasets with configurable size, structure, and file types (text, image, binary). Useful for creating reproducible test data to exercise Oxen with datasets of known characteristics -- from a handful of files to millions.

- Specify datasets by any two of: total size, number of files, average file size.
- Supports statistical file-size distributions (any `scipy.stats` distribution) and multi-tier datasets where different groups of files follow different size profiles.
- Parallel generation with progress reporting; `--dry-run` to preview the plan without writing anything.

### `reorganize-dataset`

Reshuffles a flat (or shallow) directory of files into a deeply nested, organically named directory tree. Designed to be used after `generate-dataset` to give synthetic data the kind of directory structure you'd find in a real project.

- Tree shape is controlled by a configurable `scipy.stats` distribution, max depth, and random seed.
- Plans can be saved to JSON and replayed on different datasets so multiple datasets share the same directory layout.
- Supports `--dry-run` to preview the tree without moving any files.



## Uploading to Oxen

### `batch-workspace-upload`

Uploads a local directory to a remote Oxen repository by splitting the work into batched workspace commits. Useful for pushing large datasets that are too big to commit in a single operation.

- Each batch creates a workspace, adds files, and commits before moving to the next batch.
- Auto-creates the remote repository if it doesn't exist.
- Configurable batch size, commit message template, host, and scheme; `--dry-run` to preview the plan.

### `reupload-single`

Uploads an explicit list of files to a remote Oxen repository in a single workspace commit. Useful for re-uploading specific files that failed or need to be replaced, without re-processing an entire directory.

- Takes file paths as positional arguments with a `--parent` flag to set the relative root.
- Validates that every file exists and is under the parent directory before uploading.



## Release & Versioning

### `bump-version`

Bumps the project version across the Rust workspace and Python package in one step. Useful when cutting a new release so all version strings stay in sync.

- Accepts an explicit semver string (`0.39.0`) or a component keyword (`major`, `minor`, `patch`).
- Updates `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`, and the Rust README, then creates a git commit and tag.

### `patch_dev_version`

Patches `oxen-python`'s `pyproject.toml` and `Cargo.toml` with a PEP 440 dev version (e.g. `0.44.1.dev3`). Used by `publish_test_pypi` and CI workflows to stamp pre-release builds without permanently changing the checked-in version.

- Converts the PEP 440 version to a semver pre-release for `Cargo.toml` automatically.

### `publish_test_pypi`

Builds a `.devN` Python wheel for `oxen-python` and publishes it to Test PyPI. Useful for validating packaging and installation before a real release.

- Temporarily patches version files, builds with `maturin`, uploads, then restores originals on exit.
- Requires `TEST_PYPI_API_TOKEN` to be set.



## Developer Setup

### `install-pre-reqs`

Installs every development prerequisite for building Oxen from source (system packages, Rust toolchain, cargo tools, `uv`, and pre-commit hooks). Useful for onboarding new contributors or setting up a fresh machine.

- Works on macOS (Homebrew) and Linux (apt).
- Idempotent: safe to run repeatedly; skips anything already installed at the correct version.
- Reads pinned tool versions from `tool-versions.env` at the repo root.



## Recomended Workflows

### Testing Large Scale Upload to `oxen-server`

Use `generate-dataset` to make the raw, randomized data files for your upload. Pay careful attention to the `--tier` parameter, which lets you make a segmented distribution for sampling file sizes.
Have this script output everything to a flat directory (which is the default behavior).

Then, use `reorganize-dataset` to generate a directory tree and move the generated files into it.

Finally, point `batch-workspace-upload` to that generated root directory to have it upload files in large batches using the workspace `add_files` function.

