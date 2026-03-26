# `oxen lfs` vs `git lfs` — Parity Roadmap

## Current State

The `oxen lfs` integration is feature-complete for core local and remote workflows: clean/smudge filters, long-running filter process, git hooks, CLI commands, local clone support, and remote push/pull via the Oxen workspace API.

---

## Remaining TODOs

### From the `TODO` File

1. **Auto-init on `git clone`** -- Detect `.gitattributes` with `filter=oxen` and auto-run `oxen lfs init`
2. **Fix gaps (oxen lfs push)** -- Vague; likely refers to edge cases

### Missing Commands

| Priority | Command | What It Does | Effort |
|----------|---------|-------------|--------|
| High | `lfs fetch` | Download objects without restoring (separate from `pull`) | Small |
| High | `lfs checkout` | Restore files from local cache only | Small (essentially `pull --local` as named command) |
| High | `lfs ls-files` | List all LFS-tracked files with their OIDs | Small (reuse `status::get_status`) |
| Medium | `lfs prune` | Delete unreferenced objects from `.oxen/versions/` | Medium (needs reachability analysis) |
| Medium | `lfs migrate import` | Rewrite history to convert large files to pointers | Large (needs `git filter-repo` integration) |
| Medium | `lfs migrate export` | Rewrite history to remove LFS, restore files inline | Large |
| Low | `lfs lock`/`unlock`/`locks` | File locking for binary assets | Large (needs server API) |
| Low | `lfs fsck` | Verify integrity of local objects | Small (hash each file, compare) |

### Missing Features

| Priority | Feature | Notes |
|----------|---------|-------|
| **High** | Skip re-uploading already-pushed files | Push doesn't check if remote already has a hash before uploading |
| **High** | Progress indicators | No progress bars during push/pull of large files |
| Medium | Per-branch/per-ref fetch | `fetch-all` downloads everything; no way to fetch for a specific ref |
| Medium | SSH transfer adapter | Only HTTP supported |
| Low | Custom transfer adapters | Extensibility for non-HTTP transports |
| Low | Custom merge driver | `merge=oxen` is declared in `.gitattributes` but no driver is implemented |
| Low | Deduplication / storage optimization | No chunking or dedup beyond content-addressing |

---

## Intentional Divergences (Not Gaps)

These are architectural decisions, not missing features:

- **Hash**: xxHash3-128 vs SHA-256 -- speed over cryptographic guarantees
- **Server protocol**: Oxen workspace API vs git-lfs Batch API -- leverages existing Oxen infrastructure
- **Config**: `.oxen/lfs.toml` vs git config -- clean separation from git config namespace
- **Pointer namespace**: `oxen.ai/spec/v1` vs `git-lfs.github.com/spec/v1`

---

## Full `git lfs` Command Coverage

| `git lfs` Command | `oxen lfs` Equivalent | Status |
|-------------------|-----------------------|--------|
| `install` | `oxen lfs install` | Done |
| `uninstall` | `oxen lfs install --uninstall` | Done (flag, not separate command) |
| `track` | `oxen lfs track` | Done |
| `untrack` | `oxen lfs untrack` | Done |
| `push` | `oxen lfs push` | Done |
| `pull` | `oxen lfs pull` | Done |
| `fetch` | -- | Not implemented (separate from pull) |
| `checkout` | `oxen lfs pull --local` | Done (as flag, not separate command) |
| `status` | `oxen lfs status` | Done |
| `ls-files` | -- | Not implemented |
| `env` | `oxen lfs env` | Done |
| `clean` | `oxen lfs clean` | Done |
| `smudge` | `oxen lfs smudge` | Done |
| `filter-process` | `oxen lfs filter-process` | Done |
| `lock` / `unlock` | -- | Not implemented |
| `locks` | -- | Not implemented |
| `prune` | -- | Not implemented |
| `migrate import` | -- | Not implemented |
| `migrate export` | -- | Not implemented |
| `fsck` | -- | Not implemented |
| `clone` | -- | Not applicable (use `git clone` + `oxen lfs init`) |
| `dedup` | -- | Not implemented |
| `merge-driver` | -- | Not implemented |
| `logs` | -- | Not implemented |
| `pointer` | -- | Not implemented as CLI (library only) |

### Additional `oxen lfs` Commands (No `git lfs` Equivalent)

| Command | Purpose |
|---------|---------|
| `oxen lfs init [--remote URL]` | One-step repo setup (creates .oxen/, hooks, .gitignore, optional remote) |
| `oxen lfs fetch-all` | Strict sync: errors if any pointer can't be resolved (combines fetch + checkout + strict validation) |
