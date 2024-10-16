# Oxen Core Commands

## Initialization
- `oxen init`: Initialize a new Oxen repository
- `oxen init -v 0.10.0`: Initialize with a specific version

## Configuration
- `.oxen/config.toml`: Contains the `min_version` attribute, indicating the logic version of the repo

## Repository Management
- `oxen tree`: Display the repository tree structure
- `oxen node hash`: Display the hash of a specific node
- `oxen rm -r .oxen`: Remove Oxen metadata and restore the repo to its original state

## Commit History
- `/history/commit_hash/dir_hashes`: Helps locate the node hash for a specific path in a commit