# Block-Level Deduplication — Testing Guide

How to test the block-v1 storage format on the `block-level-dedup` branch: what
the automated suite covers and how to run it, plus manual end-to-end
walkthroughs for the paths that deserve eyes-on verification. Companion to
[`getting_started.md`](./getting_started.md)
(usage) and [`plan.md`](./plan.md) (design).

## Automated tests

Run everything through `bin/test-rust` (never `cargo test` directly — the
script builds the workspace, sets up a ramdisk, and starts `oxen-server` for
the remote tests):

```bash
bin/test-rust                    # full suite
bin/test-rust chunked            # storage-layer chunked tests (local + S3)
bin/test-rust block_v1           # end-to-end block-v1 tests (add/push/migrate)
bin/test-rust storage            # everything under the storage modules
```

To watch a single test with debug logging:

```bash
env RUST_LOG=warn,liboxen=debug bin/test-rust --no-capture test_add_block_v1_dedups_edited_file
```

If the ramdisk can't be mounted, add `--no-ramdisk`.

### Coverage map

| Layer | Where | What it proves |
| --- | --- | --- |
| Chunker, codecs, formats | `storage/chunked/{chunker,trace_chunker,compressor,block,manifest,registry,policy}.rs` unit tests | FastCDC and trace-chunker boundaries and the block/manifest encodings are pinned by golden fixtures (a chunking or format drift fails loudly); the manifest/block parsers reject hostile input; the trace chunker's reorder/insert/append-tail dedup properties and malformed-input degradation are asserted directly |
| Storage-profile routing | `storage/chunked/policy.rs`, `storage/version_store.rs`, `repositories/add.rs` (`test_add_block_v1_storage_profile_routing`) | `[[storage.profiles]]` marks resolve per path (first match wins, unknown names error loudly), override extension sniffing in both directions, and the chosen chunker lands in the published manifest |
| Chunk index + engine | `storage/chunked/{chunk_index,block_engine,seekable}.rs` unit tests | Ingest/reconstruction round-trips, index rebuild from block footers, seekable range reads |
| Local store conformance | `storage/local.rs`, `mod chunked` | Transparent reads (`get_version`, streams, range reads) return the exact original bytes; ingest is idempotent and hash-verified; manifests are validated before publish; hostile ranges fail cleanly; `clean_corrupted_versions` never eats a chunked version |
| S3 store conformance | `storage/s3.rs`, `mod chunked` | The same conformance suite against an **in-process `s3s` mock server** — hermetic, no bucket or credentials needed. Blocks/manifests in S3, chunk index on local disk, `list_versions` excludes block objects |
| CLI add path | `repositories/add.rs` (`test_add_commit_restore_block_v1_repo`, `test_add_block_v1_dedups_edited_file`) | On a block-v1 repo: big files land as manifest + blocks, small files stay whole-file blobs, commit → restore round-trips exact bytes, and an edited file's second commit stores only the changed chunks |
| Push wire protocol | `repositories/push.rs` (`test_push_block_v1_repo_chunked_round_trip`, `test_push_file_with_exact_avg_chunk_size`) | Chunk negotiation + block transfer against a real `oxen-server`, server-side manifest validation, and the exact-multiple chunk-count edge case |
| Migration + recovery | `repositories/storage.rs` | `migrate --to block-v1` → `--to legacy` round-trip, `rebuild-index` recovers reads after chunk-index loss, migration skips orphans and fences `min_version` |
| Server endpoints | `oxen-server` `controllers/chunks.rs` | Oversized block/manifest upload bodies are rejected; persisting a manifest fences the repo's min version |

Python bindings don't expose block-v1 controls yet, but run `bin/test-rust -p`
after Rust changes anyway — block-v1 must not disturb the legacy paths Python
exercises.

### Test-writing knobs

- `OXEN_DEDUP_MIN_FILE_SIZE` overrides the 1 MiB chunking floor. It is read
  **once per process** and cached (`storage/chunked/policy.rs`), so set it
  before the process starts, not mid-test.
- In tests, size "big" files relative to `storage::chunked::dedup_min_file_size()`
  rather than hardcoding sizes (see `test_add_commit_restore_block_v1_repo` for
  the pattern), and remember `bin/test-rust` sets `OXEN_STREAM_SEGMENT_SIZE` to
  128 KiB, so anything over ~128 KiB also exercises the streamed-transfer path.

## Manual end-to-end testing

Build first and put the debug binaries on your `PATH`:

```bash
cargo build --workspace
export PATH="$PATH:$(pwd)/target/debug"
```

Tip: to make chunking cheap to trigger, lower the floor for your shell —
`export OXEN_DEDUP_MIN_FILE_SIZE=65536` — and use ~1 MiB test files instead of
multi-GB ones. The scenarios below assume the default 1 MiB floor.

### 1. Fresh block-v1 repo: layout and round-trip

```bash
oxen init dedup-test && cd dedup-test
```

Enable the block format in `.oxen/config.toml` — both keys are required (the
repo refuses to load a block-v1 config without the matching version fence):

```toml
min_version = "0.53.0"

[storage]
content_format = "block-v1"
```

Generate a compressible file over the floor plus a small one, then commit:

```bash
python3 -c "print('file,label'); [print(f'images/img_{i}.jpg,label_{i%7}') for i in range(60000)]" > train.csv
echo "# readme" > README.md
oxen add . && oxen commit -m "initial"
```

Verify the on-disk layout:

```bash
H=$(oxen info train.csv | cut -f1)              # the version's xxh3-128 content hash
ls .oxen/versions/files/${H:0:2}/${H:2}/        # → manifest (not data)
ls .oxen/versions/files/blocks/                 # → content-addressed blocks
ls .oxen/versions/files/chunk_index/            # → LMDB chunk index
oxen storage status                             # → block-v1, 1 chunked / 1 whole-file version
```

The small README's version dir should still contain a whole-file `data` blob.

Round-trip check — the reconstructed bytes must be identical:

```bash
shasum train.csv                     # note the digest
rm train.csv
oxen restore train.csv
shasum train.csv                     # must match
```

### 2. Dedup effectiveness: edit one row, measure block growth

```bash
du -sk .oxen/versions/files/blocks   # baseline
sed -i.bak 's/img_31337.jpg/img_edited.jpg/' train.csv && rm train.csv.bak
oxen add train.csv && oxen commit -m "edit one row"
du -sk .oxen/versions/files/blocks   # growth
```

The growth should be a few hundred KiB at most (the chunks around the edit,
~64 KiB average each) — nowhere near the full file size. Committing the same
bytes again should grow blocks by ~0. This is the core dedup claim; if block
growth tracks file size, dedup is broken.

### 3. Push / clone round-trip against a local server

Start a server (auth is off by default) and push:

```bash
SYNC_DIR=/tmp/oxen-sync-dir oxen-server start   # serves on 0.0.0.0:3000
```

```bash
cd dedup-test
oxen create-remote --name test/dedup-test --host localhost:3000 --scheme http
oxen config --set-remote origin http://localhost:3000/test/dedup-test
oxen push origin main
```

With `RUST_LOG=liboxen=debug` on the push you can watch the chunk negotiation:
the first push uploads blocks; pushing a third commit that edits one row again
should negotiate away almost everything and upload only the new blocks.

Then verify a fresh clone reconstructs exact bytes (pull is served through the
server's transparent read path — whole-file on the wire, correct today,
chunk-level pull dedup still planned):

```bash
cd /tmp && oxen clone http://localhost:3000/test/dedup-test dedup-clone
shasum dedup-clone/train.csv         # must match the original
```

### 4. Migration, both directions

Start from a **legacy** repo with a couple of large committed versions, then:

```bash
oxen storage status                  # legacy, N whole-file versions
oxen storage migrate --to block-v1
oxen storage status                  # block-v1, large versions now chunked
shasum train.csv && oxen checkout <old-commit> && shasum train.csv   # reads still exact
oxen storage migrate --to legacy     # reverse: blobs reconstructed + verified
```

Things to confirm along the way:

- Commit IDs before and after migration are identical (`oxen log`) — chunking
  is storage-only and must never rewrite history.
- Interrupting a forward migration (Ctrl-C mid-run) and re-running completes
  cleanly — every version keeps at least one full representation at all times.
- After reverse migration, blocks remain on disk (shared chunks; reclaimed
  only when GC ships) — that's expected, not a leak.
- Migration refuses to run concurrently with writes only by convention today
  (no maintenance lease yet) — don't race it against an `oxen add`.

### 5. Chunk-index loss and recovery

The chunk index is derived state and its loss must never lose data:

```bash
rm -rf .oxen/versions/files/chunk_index
oxen storage rebuild-index           # rescans block footers
shasum train.csv                     # reads work again, bytes exact
```

### 6. S3 backend

The hermetic `mod chunked` suite in `storage/s3.rs` covers S3 parity on every
test run — no setup needed. For eyes-on testing against a real endpoint, run
`oxen-server` with S3 storage configured (MinIO works) and repeat scenarios
1–3 against it; blocks and manifests land under the bucket prefix while the
chunk index stays on the server's local disk.

## What to watch for in any scenario

- **Bytes are sacred.** Every read path (checkout, restore, `oxen df`, clone)
  must return the exact original bytes — compare digests, don't eyeball sizes.
- **History is sacred.** No scenario may change a commit ID, tree hash, or
  FileNode — chunking lives entirely below the merkle tree.
- **Small files stay whole-file.** Files under the floor must keep a `data`
  blob; chunking them is a regression.
- **Failures are loud.** Corrupted manifests, unknown chunker/codec IDs, and
  hostile ranges must produce structured errors, never silent fallbacks or
  panics.
