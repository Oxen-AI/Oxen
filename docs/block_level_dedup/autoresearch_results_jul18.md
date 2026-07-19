# Autoresearch Results — jul18: Compression for Versioned Agent-Trace Datasets

Findings of the autonomous research run defined in
[`autoresearch_block_level_dedup.md`](./autoresearch_block_level_dedup.md),
conducted on branch `autoresearch-block-dedup/jul18` (base:
`block-level-dedup`). Seven experiments were benchmarked on a fixed,
deterministic 100.6 MB multi-version corpus (agent-trace JSONL, fine-tuning
JSONL, labels CSV, Parquet mirrors; append / scattered-growth / reorder /
shared-prompt-edit / insert+delete / annotation-column / cross-format
lifecycle over 7 commits — `benchmark/dedup/`). Byte-exact reconstruction of
every file at every commit was a hard gate; every retained experiment passes
it, and the full product test suite passes on the final state.

## Headline result

| | Session baseline | Final retained | Δ |
| --- | --- | --- | --- |
| stored_bytes (100.6 MB logical) | 24.19 MB | **14.37 MB** | **−40.6%** |
| storage_ratio | 0.2405 | **0.1428** | |
| incremental_stored_bytes | 16.95 MB | **11.45 MB** | −32.4% |
| incremental_ratio | 0.1982 | **0.1340** | |
| restore_seconds / restore_mb_s | 2.6 / 39.4 | 2.4 / 42.7 | ≈ tied (better) |
| random_read_ms (single-file, historical commit) | 262 | 269 | tied |
| compression_seconds | 10.4 | 56.6 | 5.4× (within the ≥10%-storage guardrail exemption) |
| peak_memory_mb | 121 | 139 | bounded |
| correctness | pass | pass | |

Against the *pure FastCDC* configuration (the guide's reference baseline,
measured via a config-only profile mark): 20.40 MB → 14.37 MB = **−29.6%**.

## The final architecture (what actually won)

Four retained mechanisms, each independently benchmarked, composing into one
pipeline:

1. **Content-adaptive chunking (`TRACE_AUTO_V1`)** — line-delimited JSON
   sniffs the average row size of its first 64 KB and delegates the whole
   stream: rows ≥ 4 KB (long agent traces, where edit locality dominates) →
   the structure-anchored `TRACE_JSONL_V1` chunker (row-isolated,
   prefix-stable intra-row cuts at message boundaries); smaller rows → generic
   FastCDC (64 KB windows, where compression and metadata economy dominate).
   The sniff rule is part of the frozen chunker ID: boundaries stay a pure
   function of content. Everything non-JSONL stays on FastCDC.
2. **Per-class shared zstd dictionaries (`ZSTD_DICT`)** — the single biggest
   storage lever. A 112 KB dictionary per content class (keyed by chunker),
   trained once from the first ≥256 KB ingest of that class (4 KB training
   slices, deferred first-file encode) and stored content-addressed under
   `blocks/dicts/`. Captures the cross-row redundancy no chunker can see —
   repeated system prompts, tool schemas, JSON keys, log/code boilerplate —
   and rescues small-chunk compression. Dictionaries are digested once
   (prepared-dict cache); the naive per-chunk digest was a 40× compression
   blowup.
3. **Bounded delta encoding (`ZSTD_DELTA`)** — near-duplicate chunks (edited
   versions of prior chunks) are found by an 8-byte prefix sketch (xxh3-64 of
   the first 256 raw bytes, an advisory LMDB table) and stored as a zstd
   frame over the base chunk's raw bytes. Strictly bounded: chain depth ≤ 1
   (a base is never itself a delta), attempted only for chunks ≥ 8 KB, kept
   only when smaller than the dict/plain form. This is what finally captured
   scattered-edit redundancy: −14.1% stored on its own.
4. **Storage-first compression settings** — chunk zstd at level 19 (storage
   dominates ingest speed per the research policy; the level is
   decode-compatible and can become adaptive later), and manifests
   zstd-wrapped at rest (sniffed magic, wire format unchanged).

Wire compatibility is preserved throughout: `ZSTD_DICT` and `ZSTD_DELTA` are
store-local codecs — transfer packing re-encodes them as plain zstd, receivers
verify exactly as before, and a store that can't persist dictionaries simply
never uses them. Reads are transparent; reconstruction never re-chunks.

## Experiment log (priority order: storage, then reads, then compression)

| # | Experiment | stored_bytes | Δ vs prev best | Verdict |
| --- | --- | --- | --- | --- |
| — | baseline: branch HEAD (FastCDC + trace-jsonl for `.jsonl`, zstd-3) | 24,194,771 | — | baseline |
| — | reference: pure FastCDC everywhere (config-only) | 20,401,059 | — | reference |
| 001a | zstd level 9 | 23,224,133 | −4.0% | superseded |
| 001b | zstd level 19 | 22,638,108 | −6.4% | **keep** |
| 002a | store-wide dict, per-chunk digest | 22,858,922 | +1.0%, 40× compress | discard |
| 002b | per-class prepared dictionaries | 20,270,857 | −10.5% | **keep** |
| 003 | CSV through row-anchored chunker | 26,753,583 | +32% | discard |
| 004 | manifests zstd-wrapped at rest | 20,074,570 | −1.0% | **keep** |
| 005 | 8 KB intra-row target (16 KB tied) | 19,906,421 | −0.8% | **keep** |
| — | reference: all-generic + dict + level 19 (config-only) | 17,745,123 | — | reference |
| 006 | `TRACE_AUTO_V1` adaptive chunker | 16,727,892 | −16.0% | **keep** |
| 007 | bounded delta encoding | **14,367,173** | −14.1% | **keep** |

Read metrics stayed within the 5% tie band throughout (restore 2.3–3.1 s;
random read 262–270 ms, dominated by CLI process startup in the debug-build
harness). Compression grew from 10.4 s to 56.6 s — permitted at each step by
the guardrail (≥10% storage reduction lifts the 5× cap), but it is the
architecture's main operational debt; see roadmap.

## Per-workload behavior of the final pipeline

Physical bytes added by each benchmark commit (final architecture):

| Commit | Workload | Logical MB | Stored MB | Ratio |
| --- | --- | --- | --- | --- |
| c1 | base snapshots (3 files) | 15.1 | 2.91 | 0.193 |
| c2 | traces append-only | 10.2 | 0.32 | **0.031** |
| c3 | scattered session growth + finetune append | 15.2 | 1.38 | 0.091 |
| c4 | full row reorder + growth + labels append | 15.8 | 2.16 | 0.137 |
| c5 | shared-prompt edit + deletes + inserts + label edits | 15.9 | 1.44 | 0.090 |
| c6 | big append + filtered copy + annotation column | 24.3 | 2.06 | 0.085 |
| c7 | Parquet mirrors of trace content | 4.1 | 4.10 | **1.00** |

Appends approach the theoretical floor; scattered edits and even the
adversarial shared-prompt edit (which touches the first message of a quarter
of all rows) land near 0.09 thanks to delta encoding; the full reorder costs
most among text workloads (FastCDC boundary phase shifts; the trace chunker
path avoids this for long-row data). Parquet is the outlier: stored ≈ 1.0×.

## What failed, and what it taught

- **One store-wide dictionary + per-chunk digestion (002a)**: dictionaries
  must match content class (a CSV-trained dict is useless for traces) and be
  digested once. The fix turned a +1.0% regression into −10.5%.
- **Row-anchored chunking for CSV (003)**: 40-byte rows coalesce into ~1–2 KB
  chunks whose metadata and compression costs dwarf any edit-locality gain,
  even with dictionaries. Small-row tabular data wants big windows.
- **Structural chunking as a universal JSONL default**: the original trace
  chunker beat FastCDC 2.7× on a long-row (7 KB+), high-churn synthetic
  corpus (see `chat_jsonl_report.md`), then *lost* by 11% on this corpus's
  short-row traces once dictionaries existed. Neither is universally right —
  hence the measured, frozen sniff rule of `TRACE_AUTO_V1`. The broader
  lesson: **granularity trades compression for edit locality, and the
  break-even moves with row size and redundancy — measure, don't assume.**
- **zstd 19 vs 9**: −2.5% storage for 2.9× compression time was kept under
  the storage-first policy, but a production default likely wants
  level-tiering (fast on ingest, repack cold blocks at 19) — recorded as
  roadmap, since the benchmark's policy ranks storage strictly first.

## Answers to the guide's architecture checklist

- **Identities**: chunks are xxh3-128 of raw bytes (position-independent);
  blocks and dictionaries are content-addressed; manifests are pure content.
  Delta bases are referenced by chunk hash inside the payload, never by
  location.
- **Boundaries**: `TRACE_AUTO_V1` sniff → structural row/message anchors for
  long-row JSONL, FastCDC 8/64/128 KB otherwise. All boundary functions are
  frozen behind append-only chunker IDs with golden fixtures.
- **Indexes**: one LMDB env — `chunks` (hash → block/offset/len/codec, the
  only authoritative index, rebuildable from block footers) and `sketches`
  (prefix-sketch → candidate base, purely advisory, safely lossy).
- **JSONL**: adaptive as above. **CSV**: FastCDC + class dictionary (columnar
  decomposition unexplored — roadmap). **Parquet**: raw FastCDC dedup only —
  works for byte-stable row groups, defeated by re-encoded exports; the fix
  is the reserved whole-file transform seam (`parquet-shred-v1`, designed in
  [`trace_compression.md`](./trace_compression.md) §4), not a chunker.
- **`messages[]`**: represented physically (structure-anchored chunks +
  dictionary patterns + deltas). Logical message interning / prefix tries
  were not reached; the dictionary captured much of that redundancy at far
  lower complexity.
- **Near-duplicates**: prefix sketches; bounded candidate search (one lookup
  per new chunk ≥ 8 KB); cost model = "keep only if strictly smaller".
- **Reconstruction**: manifest-driven, chunk-at-a-time, dict/delta resolution
  in the engine with depth-1 recursion; hash-verified on working-tree
  publish.
- **Compaction/GC**: not yet built. GC must treat delta bases (parseable from
  delta payload headers) and referenced dictionaries as reachable.
- **Strategy selection**: the frozen sniff rule plus per-path
  `[[storage.profiles]]` marks for explicit user control.

## Roadmap (in expected-value order)

1. **Parquet transform** (`parquet-shred-v1`): the single remaining 1.0×
   workload; requires the manifest-version bump for the reserved transform
   slot.
2. **Compression-time tiering**: level-3/9 on the ingest hot path plus a
   cold-block repack (`oxen storage repack`) to level 19 + dict + delta —
   removes the 5.4× ingest cost without giving back storage.
3. **Dictionary lifecycle**: retraining generations as content drifts;
   S3-backend dictionary persistence (trait hooks exist, unimplemented).
4. **GC/fsck awareness** of dictionaries, delta bases, and sketch rebuild.
5. **CSV columnar decomposition** and message-level logical interning —
   plausible wins the dictionary only partially captures.

## Reproduction

```bash
git checkout autoresearch-block-dedup/jul18
oxen-python/.venv/bin/python benchmark/dedup/prepare.py   # deterministic corpus
cargo build --workspace
OXEN_BIN=$PWD/target/debug/oxen python3 benchmark/dedup/benchmark.py
```

Per-experiment rows (including discards) are in the run's untracked
`results.tsv`; every experiment is one commit on this branch's history.
