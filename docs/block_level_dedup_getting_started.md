# Block-Level Deduplication — Getting Started

This is the practical companion to [`block_level_dedup_plan.md`](./block_level_dedup_plan.md)
(the design reference). It covers what is implemented today on the
`block-level-dedup` branch, how to try it, and how migration of existing
repositories works — both what you can do now and what the finished migration
tooling will look like.

## What it is, in three sentences

When a repository's content format is `block-v1`, `oxen add` splits every file
≥ 1 MiB into content-defined chunks (FastCDC, ~64 KiB target), compresses each
chunk (zstd-3 with raw fallback), and packs the chunks into immutable
content-addressed **blocks** (≤ 64 MiB). Each file version is then described by
a **manifest** — the ordered list of `(chunk_hash, offset, len)` — stored where
the whole-file blob used to live. Because chunk boundaries follow content, a
one-row edit to a 5 GB CSV stores only the few chunks around the edit instead
of a second 5 GB copy, and every read API reconstructs the exact original bytes
transparently.

## Implementation status

| Piece | Status |
| --- | --- |
| Chunker, codecs, ID registry, policy (`storage/chunked/`) | ✅ implemented, golden-fixture pinned |
| Block + manifest formats, hardened untrusted-input parser | ✅ implemented |
| LMDB chunk index (rebuildable derived state) | ✅ implemented |
| Local block engine: single-pass ingest, reconstruction, range reads | ✅ implemented |
| `VersionStore::chunked()` seam + transparent reads on `LocalVersionStore` | ✅ implemented |
| `oxen add` routing on block-v1 repos (CLI add path) | ✅ implemented |
| Other write paths (workspaces, server uploads, remote-mode checkout) | ⬜ still whole-file; planned (plan §6.8) |
| Push/pull wire protocol for chunked versions (plan §7) | ⬜ not yet — push of a block-v1 repo fails early with a structured error |
| S3 backend parity, `SeekableVersionReader`/`read_version_df` (plan §6.9, Phase 3–4) | ⬜ not yet |
| Migration command, GC, fsck (plan §9–11) | ⬜ not yet |

**In short: block-v1 is currently a local, experimental format.** Everything you
commit locally round-trips exactly (add → commit → checkout/restore), but you
cannot push a block-v1 repository yet, and `oxen df` on a chunked version is
not yet wired to the seekable reader.

## Try it on a new repository

1. Initialize a repository as usual:

   ```bash
   oxen init my-repo && cd my-repo
   ```

2. Enable the block format by adding one line to `.oxen/config.toml` under the
   `[storage]` table (create the table if it isn't there):

   ```toml
   [storage]
   content_format = "block-v1"
   ```

   (There is no CLI command for this yet; the planned surface is
   `oxen storage migrate --to block-v1`, plan §12.)

3. Add and commit a large file:

   ```bash
   oxen add train.csv        # ≥ 1 MiB → stored chunked
   oxen commit -m "initial dataset"
   ```

4. See what happened on disk:

   - `.oxen/versions/files/{hash[..2]}/{hash[2..]}/manifest` — the version's
     chunk recipe (a chunked version has a `manifest` instead of a `data` blob).
   - `.oxen/versions/blocks/{hash[..2]}/{hash[2..]}/data` — immutable blocks
     holding the compressed chunk payloads.
   - `.oxen/versions/chunk_index/` — the LMDB chunk index. Derived state: if it
     is ever lost or corrupted, it is rebuilt from block footers, never a source
     of data loss.

5. Edit a row, add, commit again — and compare block growth to the file size.
   Only the chunks around the edit are new; the rest dedup against the chunk
   index. Files under 1 MiB keep the classic whole-file path (chunking them is
   pure overhead).

Everything downstream is unchanged: `oxen status`, `oxen checkout`,
`oxen restore` all work identically, because every read API (streams, range
reads, working-tree publishes) reconstructs chunked versions transparently and
verifies the whole-file hash as bytes land in your working tree.

From Rust, the same switch is `LocalRepository::set_content_format(ContentFormat::BlockV1)`
followed by `repo.save()`; the ingest/read seam is `repo.version_store().chunked()`.

## What never changes

- **Merkle tree and commit IDs.** Chunking is a storage concern. No FileNode
  field signals chunking; manifest existence does. Enabling block-v1 (or later
  migrating) never rewrites a commit, a tree node, or a hash.
- **File identity.** A version is still identified by the xxh3-128 of its whole
  original bytes, and every reconstruction is verified against it.
- **Small files.** Files under the 1 MiB floor stay ordinary whole-file blobs,
  in the same place they've always been. Both representations coexist per repo.

## Migrating an existing repository

### Today (interim state)

Setting `content_format = "block-v1"` on an existing repository is safe and
takes effect **for new writes only**:

- Every version committed *after* the switch that meets the 1 MiB floor is
  stored chunked.
- Every version committed *before* the switch keeps its whole-file blob and
  keeps working — the two representations coexist indefinitely, and reads pick
  whichever exists for each version.
- Nothing in history is rewritten; flipping the config back to `"legacy"`
  stops new chunked writes (already-chunked versions remain readable).

There is **no tooling yet** to convert historical versions, so an old repo's
existing 5 GB CSV history stays at its current size until the migration
command ships. Also remember the current hard limitation: a block-v1
repository cannot be pushed yet — the push command refuses up front. Don't
enable it on a repository you need to sync today.

### The finished migration (plan §9, not yet implemented)

The planned `oxen storage migrate --to block-v1 [--dry-run] [--resume]` is an
explicit, resumable maintenance operation that converts all reachable history:

1. **Inventory** every reachable file version (all refs, retained commits,
   workspaces, staged content) while writes are blocked — reads stay allowed.
2. **Per version (checkpointed batches):** verify the legacy blob against its
   hash, chunk it through the standard pipeline (deduplicating against
   everything already chunked), publish blocks as they fill, then build and
   validate the manifest by full reconstruction.
3. **Only after** a version's verified manifest is durable is its legacy blob
   deleted, and the checkpoint advances — a crash at any point resumes with
   `--resume`, and at worst leaves reclaimable orphans, never a torn state.
4. **Cutover:** re-verify the inventory, rebuild-and-compare the chunk index,
   then atomically set `content_format = "block-v1"`.

Because manifests are pure content and block placement lives only in the
store-local index, migration never touches commits, FileNodes, or merkle
hashes — every commit ID is preserved. Reverse migration (block-v1 → legacy)
is the same machinery in the other direction: reconstruct each blob, verify,
publish, then GC the manifests and blocks. Each clone migrates independently;
block layouts never need to match across machines.

## Under the hood — one-page mental model

```
oxen add train.csv (block-v1 repo, file ≥ 1 MiB)
  │
  ├─ FastCDC splits bytes into content-defined chunks (8/64/128 KiB min/avg/max)
  ├─ each chunk: xxh3-128 identity → already in chunk index? skip : compress (zstd-3,
  │    raw fallback) and append to the open block
  ├─ blocks seal at 64 MiB → content-addressed, self-describing footer, atomic publish,
  │    then indexed (publish-last: the index is never ahead of the blocks)
  └─ manifest (ordered chunk list) validated and published under the file's hash

reads — get_version / streams / range reads / checkout: manifest + index + block
        ranges, decoded per chunk, verified whole-file hash on working-tree publish
```

Key invariants to keep in mind when extending this code:

- **Manifests never record block placement** — placement lives only in the
  LMDB chunk index, which is disposable and rebuildable from block footers.
- **Blocks are store-private and immutable** — never appended to after
  sealing; block IDs never become durable cross-store references.
- **Publish-last ordering everywhere** — blocks durable → index → manifest →
  tree metadata, so a crash leaves either the old complete representation or
  the new one.
- **Unknown IDs fail loudly** — chunker/codec/transform IDs are append-only
  `u8`s in `storage/chunked/registry.rs`; an unknown ID is a structured
  upgrade-required error, never a silent fallback.
