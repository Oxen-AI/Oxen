# Autoresearch Results — jul18: Compression for Versioned Agent-Trace Datasets

Findings of the autonomous research run defined in
[`autoresearch_block_level_dedup.md`](./autoresearch_block_level_dedup.md),
conducted on branch `autoresearch-block-dedup/jul18` (base:
`block-level-dedup`). Eighteen experiments across three sessions were
benchmarked on two fixed, deterministic corpora: the original 100.6 MB
multi-version corpus (agent-trace JSONL, fine-tuning JSONL, labels CSV,
Parquet mirrors; append / scattered-growth / reorder / shared-prompt-edit /
insert+delete / annotation-column / cross-format lifecycle over 7 commits),
and a 177.8 MB **prompt-cache-structured corpus** (see below) modeling the
tools → system → messages prefix hierarchy that provider prompt caching
exploits, including 10 real Claude Code sessions from the public
`trace-commons/agent-traces` dataset (CC-BY-4.0) versioned as live-logged
growing files — `benchmark/dedup/`. Byte-exact reconstruction of every file
at every commit was a hard gate; every retained experiment passes it, and the
full product test suite passes on the final state. A standalone visual
version of these findings lives at
[`technical_report.html`](./technical_report.html).

## Headline result

| | Session baseline | Final retained | Δ |
| --- | --- | --- | --- |
| stored_bytes (100.6 MB logical) | 24.19 MB | **12.90 MB** | **−46.7%** |
| storage_ratio | 0.2405 | **0.1282** | |
| incremental_stored_bytes | 16.95 MB | **9.63 MB** | −43.2% |
| incremental_ratio | 0.1982 | **0.1127** | |
| restore_seconds / restore_mb_s | 2.6 / 39.4 | 2.4 / 41.3 | ≈ tied (better) |
| random_read_ms (single-file, historical commit) | 262 | 252 | tied |
| compression_seconds | 10.4 | 39.9 | 3.8× (within the ≥10%-storage guardrail exemption) |
| peak_memory_mb | 121 | 142 | bounded |
| correctness | pass | pass | |

Against the *pure FastCDC* configuration (the guide's reference baseline,
measured via a config-only profile mark): 20.40 MB → 12.73 MB = **−37.6%**.
Storage results are deterministic (repeat runs are byte-identical).

On the prompt-cache corpus (177.8 MB logical), the session-3 experiments took
the same architecture from 16.02 MB to **9.56 MB (ratio 0.0538, −40.4%)** —
storing 6 commits' full history of 178 MB of agent-trace data in under 10 MB.

## The final architecture (what actually won)

Four retained mechanisms, each independently benchmarked, composing into one
pipeline:

1. **Content-adaptive chunking** — line-delimited JSON sniffs the average
   row size of its first 64 KB (now resolved in the engine, so the manifest
   records the actual delegate chunker) and routes the stream: rows ≥ 2 KB
   (agent traces, where edit locality and reorder robustness dominate) → the
   structure-anchored `TRACE_JSONL_V1` chunker (row-isolated at 1 KB,
   prefix-stable intra-row cuts at message boundaries, 8 KB target); smaller
   rows → generic FastCDC (64 KB windows, where compression and metadata
   economy dominate). The threshold moved from 4 KB to 2 KB once delta
   encoding existed — structural chunking wins sooner when near-duplicate
   tails delta away. Everything non-JSONL stays on FastCDC (64 KB average
   re-confirmed optimal in both directions under the full stack).
2. **Per-class shared zstd dictionaries (`ZSTD_DICT`)** — the single biggest
   storage lever. A 64 KB dictionary per **content family** (extension family
   refined by the row-size sniff: long-row JSONL, small-row JSONL, and CSV
   are distinct classes), trained once from the first ≥256 KB ingest of that
   class (4 KB training slices, deferred first-file encode) and stored
   content-addressed under `blocks/dicts/`. Captures the cross-row redundancy
   no chunker can see — repeated system prompts, tool schemas, JSON keys,
   log/code boilerplate — and rescues small-chunk compression. Class purity
   matters as much as existence: one mixed-content dictionary measurably
   poisons both classes (+33% on this corpus). 64 KB is the sweep knee
   (32 < **64** > 112 > 256); dictionaries are digested once (prepared-dict
   cache) — the naive per-chunk digest was a 40× compression blowup.
3. **Bounded delta encoding (`ZSTD_DELTA`)** — near-duplicate chunks (edited
   versions of prior chunks) are found by prefix *and suffix* sketches
   (xxh3-64 of the first/last 256 raw bytes, an advisory LMDB table,
   last-writer-wins so bases track recent versions) and stored as a zstd
   frame over the base chunk's raw bytes. Strictly bounded: chain depth ≤ 1
   (a delta-coded candidate contributes its own full base transitively),
   attempted only for chunks ≥ 8 KB, kept only when smaller than the
   dict/plain form. This is what finally captured scattered-edit redundancy:
   −14.1% stored on its own, plus −0.9% from the v2 refinements.
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
| 007 | bounded delta encoding | 14,367,173 | −14.1% | **keep** |
| 008a | 2 KB auto threshold alone | 17,441,237 | +21% | diagnosed |
| 008b | engine-resolved sniff + content-family dict classes | 13,097,917 | −8.8% | **keep** |
| 009 | delta v2: suffix sketches, last-wins, transitive bases | 12,978,816 | −0.9% | **keep** |
| 010a | 256 KB dictionaries | 13,192,291 | +1.6% | discard |
| 010b | 64 KB dictionaries (knee of 32/64/112/256) | **12,900,104** | −0.6% | **keep** |
| 011 | FastCDC avg 32 KB / 96 KB | 12,984,608 / 13,000,189 | +0.7% / +0.8% | discard |
| 012 | zstd 9 recheck under full stack | 13,590,530 | +5.4%, 2.6× faster | discard |
| 013 | 2 KB row isolation | 13,695,144 | +6.2% | discard |
| 014 | block-id chunk-index indirection | 12,867,336 | −0.25% (tie) | discard (simpler wins) |
| — | baseline-v2: final session-2 stack on the prompt-cache corpus | 16,024,098 (v2) | — | baseline |
| 015 | omit offsets from stored manifests | 15,957,584 (v2) | −0.4% (tie both corpora) | discard |
| 016 | manifest lineage-delta at rest (first-chunk-keyed bases) | 16,081,229 (v2) / 12,733,068 (v1) | v2 tie, v1 −1.3% | **keep** |
| 017 | 16 KB chunking floor (was 1 MiB; 4 KB tied) | **9,999,967** (v2) | **−37.8%** | **keep** |
| 018 | large-element isolation ≥ 4 KB (prompt-prefix chunks) | **9,555,852** (v2) | −4.4%; v1 byte-identical | **keep** |

Read metrics stayed within the 5% tie band throughout (restore 2.3–3.1 s;
random read ~250–270 ms, dominated by CLI process startup in the debug-build
harness). Compression sits at 39.9 s vs 10.4 s baseline (3.8×) — permitted at
each step by the guardrail (≥10% storage reduction lifts the 5× cap), and
experiment 012 quantified the tiering trade exactly: a fast-ingest mode
(zstd 9) costs +5.4% storage for 2.6× faster ingest, which is the repack
opportunity in the roadmap.

## Session 3: the prompt-caching insight

Provider prompt caching works because agent requests share a byte-identical
prefix — tool definitions, then system prompt, then the growing message
history — with exact-prefix matching and per-model minimum cacheable sizes of
512–4,096 tokens (≈2–16 KB of text). The same redundancy structure dominates
*stored* agent-trace datasets. A second fixed corpus was built to measure it:
session-per-row traces carrying 12–40 KB verbatim config prefixes (with a
config-rollout edit as the cache-invalidation analog), request-per-row logs
where consecutive rows re-serialize the conversation prefix, and 10 real
Claude Code sessions versioned as live-logged append-only files.

Findings, in the order they were forced by measurement:

1. **The current stack already captures most cross-row prefix redundancy** —
   baseline-v2 landed at 0.090 stored ratio before any new work, because
   suffix sketches delta near-identical prefix chunks and prefix-stable
   intra-row cuts make request-log rows share their predecessors' chunks.
2. **The 1 MiB chunking floor was the real bottleneck** (−37.8%): live-logged
   session files under 1 MiB bypassed the entire dedup+compression machine
   and were re-stored as raw whole-file blobs at every growth step. The
   floor's rationale (chunking small files is pure overhead) predates
   dictionaries and deltas; at 16 KB every version of a growing log chunk,
   compresses, and dedups. This is likely the most broadly applicable single
   finding of the whole run.
3. **Large-element isolation** (−4.4% v2, byte-identical on v1): a cut at the
   start and end of any depth-2 array element ≥ 4 KB is the storage twin of a
   cache breakpoint — the row's unique header (uuid) separates from the
   config's verbatim system+tools element, so every row of a config shares
   *one* stored prefix chunk exactly, and the config-rollout edit re-stores
   one chunk per config rather than one per row. Decisions stay
   lookahead-free (the element's start is recorded when it begins, the split
   is emitted at its end), preserving all prefix-stability properties.
4. **Manifest lineage-deltas** (kept for v1's −1.3%): manifests of successive
   versions share long entry runs; at rest they now delta against a
   content-lineage base keyed by first-chunk hash, with bases as separate
   compressed blobs so manifest deletion can never break a chain. Modest
   today; grows with version count.
5. **Offset omission from manifests: discarded** — zstd was already
   compressing the monotonic offset sequences to almost nothing; the
   incompressible hashes are the real manifest weight (hence #4).

Per-commit behavior on the prompt-cache corpus (final architecture): base
snapshot 0.167→0.150 after isolation, appends 0.011–0.033, request-log growth
0.024–0.113, config-rollout commit 0.033 — and the real Claude Code sessions
version at append-only cost despite full-file rewrites each commit.

## Per-workload behavior of the final pipeline

Physical bytes added by each benchmark commit (final architecture):

| Commit | Workload | Logical MB | Stored MB | Ratio |
| --- | --- | --- | --- | --- |
| c1 | base snapshots (3 files) | 15.1 | 3.27 | 0.216 |
| c2 | traces append-only | 10.2 | 0.67 | 0.065 |
| c3 | scattered session growth + finetune append | 15.2 | 0.95 | 0.062 |
| c4 | full row reorder + growth + labels append | 15.8 | 0.52 | **0.033** |
| c5 | shared-prompt edit + deletes + inserts + label edits | 15.9 | 1.11 | 0.070 |
| c6 | big append + filtered copy + annotation column | 24.3 | 2.01 | 0.083 |
| c7 | Parquet mirrors of trace content | 4.1 | 4.38 | **1.07** |

Every text workload sits between 0.03 and 0.09 — the full row reorder, once
the worst text case, is now the *cheapest* (structural row isolation makes
chunks position-independent), and the adversarial shared-prompt edit (which
touches the first message of a quarter of all rows) delta-encodes to 0.07.
The base snapshot pays a small structural-chunking premium over pure FastCDC
(0.216 vs 0.19) that the version chain repays many times over. Parquet is
the outlier at ≈ 1.0×, now with a measured explanation (below).

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
- **zstd 19 vs 9**: kept 19 under the storage-first policy; the session-2
  recheck under the full stack pinned the trade at +5.4% storage for 2.6×
  faster ingest — the exact economics for a future level-tiered repack.
- **Dictionary-class identity by chunker ID (008a)**: keying dictionaries on
  the chunker mixed long-row and small-row JSONL into one dictionary and
  regressed 21%; content family (extension × row-size class) is the correct
  identity. A one-line-sounding decision was worth 4.3 MB.
- **Bigger dictionaries and bigger/smaller chunks (010a/011/013)**: every
  capacity knob is now at a measured optimum — 64 KB dictionaries, 64 KB
  FastCDC average, 1 KB row isolation, 8 KB intra-row target. All four were
  swept in both directions under the final stack.
- **Chunk-index micro-layout (014)**: block-id indirection shrank index
  values 41% but total storage only 0.25% (LMDB page overhead dominates) —
  under the tie threshold, so the simpler layout stays.
- **Parquet, closed with evidence (three probes)**: (1) pyarrow's zstd pages
  are *not* byte-exactly reproducible by single-shot libzstd at any level
  (frame divergence, not level mismatch) — a decompress/recompress transform
  cannot reconstruct original bytes for zstd parquet; (2) between the two
  benchmark mirrors (reordered row groups), only 8 of 39 pages (~4 KB of
  2.35 MB) are byte-identical — page-aligned chunking has nothing to grab;
  (3) append-stable parquet already dedups through plain FastCDC. Conclusion:
  re-encoded/reordered zstd parquet is a storage floor for byte-exact
  systems. Product guidance: version evolving traces as JSONL (4.7× better,
  see `chat_jsonl_report.md`); where parquet must be versioned, write it
  uncompressed (or snappy) with bounded row groups so the store's own
  dictionary+zstd-19 stack sees byte-stable pages.

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

1. **Compression-time tiering**: zstd-9 ingest (+5.4% storage, 2.6× faster —
   measured) plus a cold-block repack (`oxen storage repack`) back to
   level 19 + dict + delta: hot-path speed without giving back storage.
2. **Uncompressed-parquet ingestion path**: let the policy try zstd on
   parquet whose pages are uncompressed (raw fallback already makes this
   safe), pairing with the guidance above — the only parquet lever that
   survives the byte-exactness evidence. A decompress-transform remains
   viable for *snappy* parquet (deterministic codec) if demand exists.
3. **Dictionary lifecycle**: retraining generations as content drifts;
   S3-backend dictionary persistence (trait hooks exist, unimplemented).
4. **GC/fsck awareness** of dictionaries, delta bases (parseable from delta
   payload headers), and sketch-table rebuild.
5. **Compaction**: retro-delta of superseded full chunks against newer
   versions (reverse-chain packs), bounded by the same depth-1 rule.
6. **CSV columnar decomposition** and message-level logical interning —
   plausible wins the dictionary only partially captures.

## Reproduction

```bash
git checkout autoresearch-block-dedup/jul18
oxen-python/.venv/bin/python benchmark/dedup/prepare.py               # corpus v1
oxen-python/.venv/bin/python benchmark/dedup/prepare_promptcache.py   # corpus v2
cargo build --workspace
OXEN_BIN=$PWD/target/debug/oxen python3 benchmark/dedup/benchmark.py
BENCH_CORPUS=corpus-promptcache OXEN_BIN=$PWD/target/debug/oxen \
    python3 benchmark/dedup/benchmark.py
```

Corpus v2 additionally requires the real-session source files in
`benchmark/dedup/corpus-v2-src/` (10 Claude Code sessions from
`trace-commons/agent-traces`, CC-BY-4.0; content pinned by the corpus
manifest).

Per-experiment rows (including discards) are in the run's untracked
`results.tsv`; every experiment is one commit on this branch's history.
