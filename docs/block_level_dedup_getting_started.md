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
| `SeekableVersionReader`, chunked `read_version_df` / `oxen df --revision` (plan §6.9) | ✅ implemented |
| Push wire protocol: chunk negotiation, block transfer, server-side manifest validation (plan §7) | ✅ implemented — a server without block support is rejected with a structured error |
| Pull of chunked-on-server versions | ✅ correct via the server's transparent reads (whole-file on the wire); chunk-level pull dedup is planned |
| S3 backend parity (plan Phase 4) | ⬜ not yet |
| `oxen storage status` / `oxen storage migrate --to block-v1\|legacy` | ✅ implemented (local, resumable; converts every stored version) |
| Maintenance lease, reachable-set inventory, GC, fsck (plan §9–11) | ⬜ not yet — reverse migration leaves blocks on disk until GC ships |

**In short: block-v1 is an experimental format with a working local + push
path.** Everything you commit round-trips exactly (add → commit →
checkout/restore), `oxen push` negotiates at chunk granularity and uploads
only the blocks the server is missing, and the server validates every pushed
manifest by full reconstruction before publishing it. Clones and pulls of
chunked-on-server versions are served through the server's transparent read
path (correct, not yet dedup-optimized on the wire).

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

### Today: `oxen storage migrate`

From inside any repository:

```bash
oxen storage status                    # format + how many versions are chunked
oxen storage migrate --to block-v1     # convert history to chunked storage
oxen storage migrate --to legacy       # convert back to whole-file blobs
```

Forward migration walks every version in the store; each version at/above the
1 MiB floor is chunked through the standard single-pass ingest (verified
against the version's content hash), its manifest is published, and **only
then** is its whole-file blob deleted — at every point each version has at
least one complete representation, so interrupting and re-running is always
safe (`already migrated` versions are skipped, and a version whose blob
deletion was interrupted is finished). Small versions keep the whole-file path
by policy. The `content_format` flips to `block-v1` only after everything is
converted.

Reverse migration reconstructs every chunked version into its verified
whole-file blob before removing the manifest. Blocks stay on disk until GC
ships (other versions may share their chunks), so a reverse migration does not
immediately reclaim space.

Commits, merkle nodes, and hashes are never touched in either direction.

Alternatively, setting `content_format = "block-v1"` in the config *without*
migrating is also valid: it takes effect for new writes only, and the two
representations coexist indefinitely.

Current scope notes (vs the full plan §9): the command converts **every**
stored version (no reachable-set inventory yet — unreachable versions' chunks
are harmless and reclaimable by future GC), takes no maintenance lease (don't
run it concurrently with writes), and stored versions don't carry their
original data type, so migration uses the universal zstd-with-raw-fallback
codec policy for all content.

### The finished migration (plan §9)

The full plan adds to this command an explicit, resumable maintenance
operation over all reachable history:

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
