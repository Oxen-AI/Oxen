# Trace-Aware Compression: Storage Profiles and the `trace-jsonl` Pipeline

How block-level dedup becomes *extensible per file and data type*, and the first
format-aware pipeline built on that seam: structure-anchored chunking for agent
traces and chat-message tables that are committed repeatedly as they evolve —
the "incrementally growing fine-tuning dataset" workload.

Companion docs: [`plan.md`](./plan.md)
(core design), [`chat_jsonl_report.md`](./chat_jsonl_report.md)
(the experiment that motivated this work),
[`testing.md`](./testing.md) (how to test).

## 1. The extensibility seam, verified

The claim "dedup is customizable per file type / data type" holds, with one
qualification. The architecture has three pluggable axes, all resolved by one
policy function at ingest:

```
encode_policy(profile, data_type, extension) -> { chunker_id, codec_id, transform_id }
```

- **Chunkers** (`Chunker` trait, `ChunkerId` in `registry.rs`): the boundary
  function. Recorded per manifest. Critically, `ChunkManifest::validate`
  deliberately does **not** reject unknown chunker IDs — reconstruction never
  re-chunks — so a new chunker is a *write-side-only* change: old binaries and
  old servers read, validate (by full reconstruction), and serve files chunked
  by newer policies with no upgrade. Different files in one repo freely use
  different chunkers.
- **Codecs** (`Compressor` trait, `CodecId`): per-chunk encoding, recorded in
  block footers, store-local. New codecs never become durable cross-store
  references; a store can adopt one for new blocks without any manifest or
  wire change. (A peer *receiving* blocks that use a codec it doesn't know
  fails with a structured upgrade-required error — loud, never silent.)
- **Transforms** (`TransformId`): reversible whole-file transforms applied
  before chunking. The first non-identity transform is live —
  `ZSTD_UNWRAP_V1` (embedded-zstd unwrap with verified recompression; see
  `unwrap_transform.rs` and `autoresearch_results_jul19.md`). `validate`
  rejects transform IDs a build does not know with a structured
  upgrade-required error. Chunkers and codecs are not gated.

All IDs are append-only `u8`s; changing any frozen behavior ships as a new ID.
Golden fixtures pin each boundary function and encoding.

## 2. File-level marks: storage profiles

Files can now be **marked** with file-level metadata that determines their
compression and dedup pipeline, analogous to how tabular files carry schema
metadata. A mark is a named **storage profile** attached per path in the repo
config:

```toml
[[storage.profiles]]
pattern = "traces/**/*.jsonl"
profile = "trace-jsonl"

[[storage.profiles]]
pattern = "raw/*.jsonl"
profile = "generic"        # explicit opt-out back to FastCDC
```

Resolution (implemented end to end; see `StorageConfig::profile_for_path` and
the routing test `test_add_block_v1_storage_profile_routing`):

1. First matching `[[storage.profiles]]` rule wins (glob over the
   repo-relative path; a single exact path is a valid pattern, so any one file
   can be marked). An unknown profile name is a **loud error** at add time,
   never a silent fallback.
2. No mark → extension-based default: `.jsonl`/`.ndjson` → `trace-jsonl`,
   everything else → `generic`.
3. The chosen chunker is recorded in the version's manifest, so the decision
   is auditable after the fact.

Profile names are an append-only contract like the IDs beneath them.

Roadmap for richer marking (design intent, not yet built): a tracked
`.oxenattributes`-style file so marks travel with clones and branches, and a
user-metadata slot on `FileNode` when the tree format next bumps — the config
rules become the local override layer over those. The policy signature
(`profile` as an explicit input) is already shaped for that; only the mark
*source* grows.

## 3. `trace-jsonl`: structure-anchored chunking (`TRACE_JSONL_V1`)

### Why byte-CDC loses on this workload

The [chat-JSONL experiment](./chat_jsonl_report.md) showed
the failure precisely: FastCDC's ~64 KiB chunks span 10–30 rows, and a chunk
is reusable only if *every* row in it is unchanged. Scattered row growth (the
natural life of a session/trace table) makes almost every chunk dirty — the
storage amplification is the rows-per-chunk factor, ~5.4 bytes stored per byte
of new content and rising with file size.

### The cut rule

`TRACE_JSONL_V1` (`storage/chunked/trace_chunker.rs`) cuts only at structural
anchors, with all decisions made byte-by-byte on the prefix — no lookahead, no
rolling hash:

1. **Forced cut** when a chunk reaches the format cap (128 KiB) — safety on
   structureless input.
2. **Row anchor**: cut after `\n` when the just-ended row reached 1 KiB
   (`ROW_ISOLATE_MIN`), or when that much has accumulated across smaller rows.
3. **Element anchor**: cut after an element separator of a *depth-2 JSON
   array* — i.e. between the elements of `messages: [...]` in a row — once
   4 KiB (`INTRA_ROW_TARGET`) has accumulated.

A ~50-line scanner tracks string/escape state and a container stack to
recognize depth-2 array separators; it resets at every newline, so a malformed
line can't poison later rows. Correctness (chunks tile the file) never depends
on the input being valid JSONL — only dedup quality does. Binary or
single-line input degrades to forced 128 KiB cuts.

### The properties those three rules buy

- **Rows ≥1 KiB both start and end at chunk boundaries**, so an unchanged
  row's chunks are a pure function of the row's bytes: dedup is *invariant to
  row insertion, deletion, reordering, and sorting*. Commit strategy stops
  mattering for unchanged rows.
- **Intra-row cuts are prefix-deterministic**, so a session that grows by
  appending messages keeps every chunk before its tail byte-identical: an
  appended row costs one partial tail chunk (≤ ~4 KiB) plus its genuinely new
  bytes — delta-like economics with no delta machinery, no read
  amplification, and no GC changes.
- **Mid-row edits** (a message edited in place) re-store only that row from
  the edit point onward — damage bounded by the row.
- Rows under 1 KiB coalesce with neighbors (bounded metadata for tiny-row
  files), trading position-independence for overhead only where rows are tiny.

The two thresholds were chosen by measured sweep (2 KiB/4 KiB, 1 KiB/4 KiB,
1 KiB/2 KiB across the ordered, shuffled, and append scenarios below) and are
frozen by the chunker ID: 1 KiB isolation dominated on shuffled workloads at
negligible cost elsewhere; halving the intra-row target moved cost from blocks
to manifests for no net gain.

### Measured results

Same benchmark as the original report (5,000 base sessions growing over 12
commits, ~23% of rows gaining messages per commit in `high`/`shuffle`, ~2.6%
in `low`; 13 versions ≈ 372 MB logical in the large scenarios; every scenario
byte-verified by checkout + digest comparison). Totals are everything under
`.oxen/versions` — blocks, manifests, and chunk index:

| Scenario | Logical | FastCDC | `trace-jsonl` | Improvement |
| --- | --- | --- | --- | --- |
| `high` — ordered, ~23% rows/commit | 372.4 MB | 134.1 MB | **50.5 MB** | 2.7× |
| `low` — ordered, ~2.6% rows/commit | 196.5 MB | 45.7 MB | **15.6 MB** | 2.9× |
| `shuffle` — `high` + rows re-shuffled every commit | 372.2 MB | 137.6 MB | **63.3 MB** | 2.2× |
| `append` — append-only rows | 202.4 MB | **7.6 MB** | 14.3 MB | 0.5× (regression) |

The per-commit storage amplification in `high` is the headline: FastCDC stored
~5.4 bytes per byte of new content *and growing with file size*; `trace-jsonl`
stores a flat **~1.25–1.3×** across all twelve commits — storage growth became
O(new content). Thirteen versions of a 46 MB table now cost 1.1× the final
file. Against the whole-snapshot-zstd baseline (123.5 MB) it is 2.4× better;
the measured `zstd --patch-from` ideal (17.3 MB) marks the remaining ~3×
headroom that only real delta encoding can close (§5).

Two honest costs, both inherent to finer granularity: the `append` scenario
regresses ~2× against FastCDC (smaller chunks compress worse and mint more
metadata; 64 KiB chunks are simply optimal when rows never change — mark such
files `generic` if the difference matters), and the chunk index grows with
chunk count (6.5 MB for the `high` run — disposable, rebuildable derived
state, but worth watching). Manifest size scales with chunk count too
(~3 MB across the 13 `high` versions); manifests compress well and are a
clean future win.

## 4. Parquet

Parquet cannot be rescued by a smarter chunker alone: pages are compressed, so
any logical change re-encodes whole column chunks, and dictionary encoding
scrambles bytes globally. Measured on the same evolving table (pyarrow,
committed as `.parquet` to a block-v1 repo — generic chunker, raw codec):

| Scenario / writer | Final file | 13-version total | Verdict |
| --- | --- | --- | --- |
| `append`, 1000-row groups, zstd | 7.2 MB | **9.8 MB** | dedup works: stable-prefix row groups are byte-identical across rewrites |
| `append`, single row group, zstd | 7.2 MB | 12.4 MB | partial — append-only dictionaries keep prefix pages stable |
| `low` (~2.6% rows edited), 1000-row groups, zstd | 7.0 MB | **72.8 MB** | full re-store every commit: a 1000-row group survives only if all 1000 rows are untouched |

The same logical data as JSONL under `trace-jsonl` costs 15.6 MB versus
72.8 MB as Parquet — **4.7× better**. Hence the guidance, which is also the
marketing story for evolving fine-tuning datasets:

- **Keep the evolving table in JSONL** (Oxen's `df` APIs read it fine, and
  export to Parquet is a cheap derived artifact). Append-only writers should
  bound `row_group_size` if they must commit Parquet.
- **Parquet-aware dedup is a transform, not a chunker** — exactly what the
  reserved `TransformId` seam is for. The designed (not yet built)
  `parquet-shred-v1` transform: decompress each page (verifying at ingest
  that recompression reproduces the original bytes, falling back per page to
  the raw bytes where it can't), emit the decompressed page payloads
  column-aligned followed by a structural shell, chunk that stream with
  page/column anchors, and invert on read. It rides the same profile
  mechanism (`profile = "trace-parquet"`) once the manifest version bumps.

## 5. Roadmap

In leverage order:

1. **Chunk-level delta encoding** (closes the remaining ~3× on scattered
   edits, and the 17× on the naive mutable-snapshot workflow measured in the
   report): store a dirty chunk as a delta against its predecessor chunk,
   packfile-style — bounded chain depth, periodic full chunks, GC that keeps
   bases reachable. This is a new codec plus engine-level resolution (a delta
   chunk's payload names its base chunk hash) and a transfer rule that ships
   bases with deltas or inflates on pack.
2. **`parquet-shred-v1` transform** (§4), behind a manifest version bump.
3. **Manifest compression** — manifests are msgpack chunk lists that zstd
   ~2×; at trace-chunker granularity they're ~0.7% of file size per version.
4. **Richer mark sources** — tracked `.oxenattributes`, FileNode metadata
   (§2), and migration passing per-file profiles (today `storage migrate`
   re-chunks everything under the generic profile because stored versions
   don't carry their path or data type).

## 6. Extending it yourself (the open-source pitch, verified)

Adding a format-aware pipeline is a contained, write-side change — the
`trace-jsonl` pipeline in this branch is the worked example:

1. Implement `Chunker` (or `Compressor`) — `trace_chunker.rs` is ~200 lines
   plus tests.
2. Register an append-only ID in `registry.rs` and pin the behavior with
   golden fixtures.
3. Add a `StorageProfile` name and its mapping in `policy.rs`.
4. Nothing else moves: manifests, blocks, the wire protocol, servers, and old
   clients are untouched, because chunker choice is write-local and reads are
   manifest-driven.
