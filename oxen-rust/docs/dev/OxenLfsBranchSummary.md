# `oxen lfs` — Git Integration: Branch Summary

## What This Is

A **drop-in replacement for `git lfs`** that stores large file content in Oxen's version store and syncs it to an Oxen server. Users keep using Git for version control while offloading large binary files to Oxen's infrastructure instead of GitHub's LFS.

---

## How It Works

### Architecture

```
Git Repository
├── .git/hooks/
│   ├── pre-push          → oxen lfs push
│   ├── post-checkout     → oxen lfs pull --local
│   └── post-merge        → oxen lfs pull --local
├── .gitattributes        *.bin filter=oxen diff=oxen merge=oxen -text
├── .gitignore            .oxen/
├── .oxen/
│   ├── lfs.toml          remote_url = "https://hub.oxen.ai/ns/repo"
│   └── versions/         content-addressable store (xxh3 hashes)
│       └── <ab>/<cdef…>/data
└── working tree
    └── model.bin          (pointer file in Git, real content on disk)
```

### Pointer Format

```
version https://oxen.ai/spec/v1
oid xxh3:a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8
size 5242880
```

Uses xxHash3-128 (fast, non-cryptographic) instead of git-lfs's SHA-256.

### Key Data Flows

**Clean (file -> pointer):** Git add/commit triggers the clean filter. Hashes content (xxHash3-128), stores blob in `.oxen/versions/`, returns a 3-line pointer (~100 bytes) that Git commits.

**Smudge (pointer -> file):** Git checkout triggers the smudge filter. Tries 4 tiers:
1. Local `.oxen/versions/` store
2. Origin's `.oxen/versions/` (for local `git clone`)
3. Configured Oxen remote (HTTP, 30s timeout)
4. Fallback: return pointer bytes + warn

**Push:** `pre-push` hook (or `oxen lfs push`) creates a temporary workspace on the Oxen server, uploads versioned blobs via `add_files` (handles batching + multipart), commits the workspace, cleans up.

**Pull:** `post-checkout`/`post-merge` hooks (or `oxen lfs pull`) scan for pointer files, restore content from local -> origin -> remote, then `git add` to refresh the index stat cache.

---

## All Files on This Branch

### Library (`oxen-rust/src/lib/src/lfs/`)

| File | Purpose |
|------|---------|
| `lfs.rs` | Module declaration (9 submodules) |
| `pointer.rs` | Pointer file encode/decode/validation (xxh3, 200-byte max) |
| `config.rs` | `.oxen/lfs.toml` load/save + `resolve_remote()` -> `RemoteRepository` |
| `gitattributes.rs` | `.gitattributes` track/untrack/list patterns |
| `install.rs` | Global `~/.gitconfig` filter driver install/uninstall |
| `hooks.rs` | `.git/hooks/` pre-push, post-checkout, post-merge (idempotent, preserves existing) |
| `filter.rs` | Clean filter (hash+store) and smudge filter (4-tier lookup with 30s remote timeout) |
| `filter_process.rs` | Git long-running filter protocol v2 (pkt-line, capability negotiation) |
| `status.rs` | Walk working tree, find pointers matching tracked patterns, check local availability |
| `sync.rs` | `push_to_remote` (workspace API), `pull_from_remote` (batch download), `fetch_all`, `git_add` |

### CLI (`oxen-rust/src/cli/src/cmd/lfs/`)

| Command | Purpose |
|---------|---------|
| `oxen lfs init [--remote URL]` | Initialize LFS in a git repo (creates .oxen/, hooks, .gitignore) |
| `oxen lfs install [--uninstall]` | Global filter driver in `~/.gitconfig` |
| `oxen lfs track <pattern>` | Add pattern to `.gitattributes` |
| `oxen lfs untrack <pattern>` | Remove pattern from `.gitattributes` |
| `oxen lfs push` | Upload versioned blobs to Oxen remote via workspace API |
| `oxen lfs pull [--local]` | Download + restore pointer files |
| `oxen lfs fetch-all` | Strict sync: errors if anything can't be resolved |
| `oxen lfs status` | Show tracked files + local/missing status |
| `oxen lfs clean` | Stdin->stdout clean filter for Git |
| `oxen lfs smudge` | Stdin->stdout smudge filter for Git |
| `oxen lfs filter-process` | Long-running filter process (pkt-line v2) |
| `oxen lfs env` | Print version, remote URL, versions dir, tracked patterns |

### Modified Shared Code

| File | Change |
|------|--------|
| `api/client/versions.rs` | Added `download_versions_to_store()` -- generic batch download to any `VersionStore` (refactored existing download to delegate, zero behavior change) |
| `constants.rs` | Added `OXEN_HIDDEN_DIR` constant |
| `lib.rs` / `cmd.rs` / `main.rs` | Registered lfs module and subcommands |

---

## Tests

44 LFS tests pass. Clippy clean. Coverage includes:
- Pointer serialization/deserialization/validation
- Config save/load/defaults
- `.gitattributes` manipulation (track, untrack, list, idempotency)
- Hook installation (creation, idempotency, preservation, permissions, path quoting)
- Global filter install/uninstall
- Clean filter (stores content, returns pointer, idempotent)
- Smudge filter (restores content, passthrough non-pointer, fallback on missing, remote fallback on unreachable server)
- pkt-line protocol (text/binary roundtrips, key=value pairs)
- Status detection (finds pointers matching patterns)
- Push with no remote (silent success)
- Pull local-only (no network, restores local content)
- `git_add` returns Result (empty list, non-git dir)
