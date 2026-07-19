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

A long-horizon corpus (one trace table exported daily for 60 days, 2.68 GB
logical) measures the versioning slope directly: **30.0 MB stored (ratio
0.0112, 89× vs raw snapshots, 8.5× less than zstd-19-compressing every
snapshot at 254 MB)**, with per-commit marginals of ~0.46 MB vs ~4.3 MB —
byte-verified at all 60 commits.

An RL-scale corpus (80 training iterations of a rollout dataset, 6.75 GB
logical: fresh rollout batches, reward relabels, curation pruning, policy
config bumps) extends the same measurement to the multi-gigabyte range:
**52.4 MB stored (ratio 0.0078, 129× vs raw, 19× less than zstd-19 snapshots
at 1,009 MB)**, byte-verified at all 80 commits with 277 MB peak memory. The
stored-per-logical ratios hold across all four corpora (100 MB → 6.75 GB),
which is the basis for the report's clearly-labeled year-scale projection.

Running both chunker routes on the large corpora exposed a routing finding:
the row-size sniff routes RL rollout rows (~14 KB) structurally, but rollouts
are append-only — rows never mutate — so byte-window FastCDC wins there
(36.8 MB vs 52.4 MB) while the 60-day mutating-session corpus shows the
mirror image (structural 30.0 MB vs generic 68.0 MB). Row size predicts
"long records", not "records that mutate". Mitigation today: a per-path
`generic` profile mark. Roadmap: an append-detection signal in the auto
sniff (e.g., is the head of the file byte-identical to the previous version
of the same lineage — knowable at ingest via the manifest lineage base).

On the prompt-cache corpus (177.8 MB logical), the session-3 experiments took
the same architecture from 16.02 MB to **8.40 MB (ratio 0.0473, −47.6%)** —
storing 6 commits' full history of 178 MB of agent-trace data in 8.4 MB,
beating even whole-snapshot zstd-19 (8.5 MB) on its best corpus while keeping
random access and incremental transfer. The last two mechanisms were driven by
chasing that baseline: a midpoint sketch (rows whose shared tools+system block
sits between a unique head and tail), and in-flight delta bases (a snapshot's
later rows delta against its earlier rows during the first ingest — the sketch
table alone only covers prior ingests).

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
| 018 | large-element isolation ≥ 4 KB (prompt-prefix chunks) | 9,555,852 (v2) | −4.4%; v1 byte-identical | **keep** |
| 019 | midpoint sketch (shared verbatim middles) | 9,355,374 (v2) | −2.1%; v1 tie | **keep** |
| 020 | in-flight delta bases (same-pass rows) | **8,404,019** (v2) | −10.2%; first snapshot −23%; v1 byte-identical | **keep** |

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
- **Parquet, revised after the parquet-CDC follow-up**: our original three
  probes stand for *uncooperative writers* — writer-compressed zstd pages are
  not byte-exactly reproducible by recompression, and reordered row groups
  leave ~0.2% of pages identical. But writer-side content-defined chunking
  (pyarrow ≥ 21 `use_content_defined_chunking=True`, per Hugging Face's
  parquet-cdc work) makes page boundaries follow content, and byte-identical
  pages dedup through the unchanged engine. Measured on paired corpora with
  identical logical evolution (append / contiguous backfill / contiguous
  purge / recent-session growth): CDC-off 10.66 MB vs CDC-on **6.40 MB
  (−40%)**; the purge commit costs 5.3× less, the backfill 2.8× less. The
  verified boundary: at ~500 rows/page, densely *scattered* row edits dirty
  every page in any layout (the same rows-per-chunk amplification law) — the
  JSONL trace profile's territory. Guidance: write versioned parquet with CDC
  enabled; keep high-churn scattered-edit tables in JSONL.

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

Every experiment is one commit on this branch's history. The raw
per-experiment measurement rows from both runs are archived below.

## Appendix: raw measurement rows

### results.tsv (corpus v1 run)

```tsv
commit	score	stored_bytes	storage_ratio	incremental_stored_bytes	incremental_ratio	restore_seconds	restore_mb_s	sequential_read_mb_s	random_read_ms	compression_seconds	compression_mb_s	memory_gb	status	description
00bf5d189	0.240503	24194771	0.240503	16951778	0.198245	2.6	39.4	10.7	262.5	10.4	9.7	0.1	keep	baseline: block-level-dedup HEAD (FastCDC + trace-jsonl for .jsonl)
00bf5d189	0.202792	20401059	0.202792	16766065	0.196073	2.7	38.0	10.5	260.0	10.2	9.9	0.1	keep	reference: pure FastCDC via generic profile mark (config-only measurement)
54737f597	0.230854	23224133	0.230854	16291287	0.190519	2.5	41.0	10.7	262.0	11.9	8.5	0.1	discard	zstd level 9: -4.0% stored, +14% compress time (superseded by 19)
54737f597	0.225029	22638108	0.225029	15939160	0.186402	2.5	41.0	10.7	262.0	34.2	2.9	0.1	keep	zstd level 19: -6.4% stored vs baseline, 3.3x compress (within guardrail)
057879a31	0.227224	22858922	0.227224	15994655	0.187051	3.1	36.5	10.2	350.4	392.4	0.3	0.1	discard	dict v1: single store-wide dict, per-chunk digest — wrong class + 40x compress blowup
057879a31	0.201498	20270857	0.201498	14384801	0.168225	2.8	36.6	10.5	268.1	29.8	3.4	0.1	keep	dict v2: per-class prepared dicts — -10.5% stored, compression back to normal
057879a31	0.265938	26753583	0.265938	18616391	0.217712	2.9	35.0	10.4	270.0	24.0	4.2	0.1	discard	csv via row-anchored chunker: tiny rows explode metadata, -32% worse
b2e4c3928	0.199547	20074570	0.199547	14220927	0.166308	2.8	36.6	10.5	268.0	30.2	3.3	0.1	keep	zstd-wrapped manifests at rest: -1.0% stored
8abd39388	0.197875	19906421	0.197875	14095253	0.164839	2.8	36.6	10.5	268.0	30.0	3.4	0.1	keep	8KB intra-row target: -0.8% (16KB tied; 8KB wins granularity tie-break)
8abd39388	0.176391	17745123	0.176391	14704108	0.171958	2.8	36.6	10.5	268.0	30.0	3.4	0.1	keep	reference: all-generic FastCDC + dict + zstd19 + manifest-zstd (config-only)
c50af7621	0.166280	16727892	0.166280	13831455	0.161754	2.3	44.5	10.6	265.7	47.3	2.1	0.1	keep	TRACE_AUTO_V1 adaptive chunker + per-class dicts: -16% vs prior best
bfdaa54f4	0.142814	14367173	0.142814	11454352	0.133954	2.4	42.7	10.6	269.1	56.6	1.8	0.1	keep	bounded delta encoding: -14.1% stored, chain depth 1, sketch-matched bases
ad50eb7a9	0.173371	17441237	0.173371	12325149	0.144131	2.7	38.0	10.5	268.0	31.7	3.2	0.1	discard	2KB auto threshold alone: dict-class confound (auto id mixed content families)
ad50eb7a9	0.130197	13097917	0.130197	9721043	0.113683	2.4	42.7	10.6	268.0	39.3	2.6	0.1	keep	engine-resolved sniff + content-family dict classes: -8.8%, manifests record true chunker
934efefc3	0.129013	12978816	0.129013	9601942	0.112290	2.4	42.7	10.6	268.0	39.1	2.6	0.1	keep	delta v2 (suffix sketches, last-wins, transitive bases): -0.9%
d754c8640	0.131135	13192291	0.131135	9498936	0.111085	2.4	42.7	10.6	268.0	42.7	2.4	0.1	discard	256KB dicts: blob cost exceeds capture gains
d754c8640	0.128231	12900104	0.128231	9634539	0.112671	2.4	42.7	10.6	268.0	39.0	2.6	0.1	keep	64KB dicts (knee of 32/64/112/256 sweep): -0.6%
d754c8640	0.129071	12984608	0.129071	9695238	0.113381	2.4	42.7	10.6	268.0	39.0	2.6	0.1	discard	FastCDC avg 32KB: +0.7%
d754c8640	0.129226	13000189	0.129226	9752223	0.114047	2.4	42.7	10.6	268.0	39.0	2.6	0.1	discard	FastCDC avg 96KB: +0.8% (64KB optimal both directions)
d754c8640	0.135094	13590530	0.135094	9989425	0.116821	2.4	42.7	10.6	268.0	15.2	6.6	0.1	discard	zstd 9 recheck under full stack: +5.4% storage for 2.6x faster ingest — level 19 stays
d754c8640	0.136134	13695144	0.136134	10574123	0.123659	2.4	42.7	10.6	268.0	39.0	2.6	0.1	discard	ROW_ISOLATE 2KB: +6% (1KB confirmed; reorder robustness beats coalescing)
d754c8640	0.135319	13613224	0.135319	10459435	0.122305	2.4	42.7	10.6	268.0	39.0	2.6	0.1	crash	block-id index first run: contaminated by leaked 013 row-isolation value
d754c8640	0.127905	12867336	0.127905	9601771	0.112288	2.4	42.7	10.6	268.0	39.0	2.6	0.1	discard	block-id index clean run: -0.25% = under tie threshold; simpler impl wins
c9a6e673c	0.126570	12733068	0.126570	9403814	0.109972	2.4	42.7	10.6	268.0	39.0	2.6	0.1	keep	manifest lineage-delta (corpus v1): -1.3%
```

### results-promptcache.tsv (corpus v2 / prompt-cache run)

```tsv
commit	score	stored_bytes	storage_ratio	incremental_stored_bytes	incremental_ratio	restore_seconds	restore_mb_s	sequential_read_mb_s	random_read_ms	compression_seconds	compression_mb_s	memory_gb	status	description
1e0c59311	0.090149	16024098	0.090149	10449995	0.069294	2.4	74.0	10.6	250.0	30.8	5.8	0.1	keep	baseline-v2: current stack on prompt-cache corpus (metadata=56% of store)
1e0c59311	0.089775	15957584	0.089775	10394121	0.068923	2.4	74.0	10.6	250.0	30.8	5.8	0.1	discard	omit manifest offsets: -0.4%/-0.46% both corpora = tie; zstd already ate offsets
c9a6e673c	0.090471	16081229	0.090471	10415043	0.069063	2.4	74.0	10.6	250.0	31.0	5.7	0.1	keep	manifest lineage-delta: v1 -1.3%, v2 tie (+0.4%) — few manifest versions here yet
2742bf9a7	0.056259	9999967	0.056259	5500488	0.036470	2.4	74.0	10.6	250.0	31.0	5.7	0.1	keep	16KB chunking floor: -37.8% on v2 (sub-1MiB live logs were raw blobs); v1 unchanged
54a4729ec	0.053760	9555852	0.053760	5508262	0.036522	2.4	74.0	10.6	250.0	31.0	5.7	0.1	keep	large-element isolation: -4.4% v2 (config prefixes dedup exactly); v1 byte-identical
c2587d284	0.052632	9355374	0.052632	5307784	0.035193	2.4	74.0	10.6	250.0	31.0	5.7	0.1	keep	midpoint sketch: -2.1% v2 (request-log shared middles); v1 tie (+0.26%)
ab1dc97c8	0.047280	8404019	0.047280	5281294	0.035017	2.4	74.0	10.6	250.0	32.0	5.6	0.1	keep	in-flight delta bases: -10.2% v2, first snapshot 4.05->3.12MB, now beats zstd-19-each; v1 byte-identical
e3ba0572b	0.011205	30033548	0.011205	27288862	0.010252	58.9	45.5	10.6	250.0	242.8	11.0	0.2	keep	long-horizon corpus: 60 daily commits, 2.68GB->30MB (89x raw, 8.5x vs zstd-19-each)
e3ba0572b	0.312497	6395255	0.312497	3116684	0.180000	2.0	60.0	10.0	250.0	8.0	2.5	0.1	keep	parquet CDC-on corpus: 6.40MB vs CDC-off 10.66MB (-40%); purge 5.3x, backfill 2.8x cheaper
5356be2bb	0.007775	52441332	0.007775	44023989	0.006563	93.1	72.5	10.6	250.0	524.0	12.9	0.3	keep	RL-scale corpus: 80 iterations, 6.75GB->52.4MB (129x raw); byte-verified
0482a200d	0.005452	36772905	0.005452	29942688	0.004464	90.0	75.0	10.6	250.0	500.0	13.5	0.3	keep	RL corpus via generic profile mark: 36.8MB vs adaptive 52.4MB — append-only rollouts favor byte windows; routing finding
0482a200d	0.025354	67957244	0.025354	66269644	0.024750	60.0	45.0	10.6	250.0	240.0	11.2	0.2	keep	60-day corpus via generic mark: 68.0MB vs adaptive 30.0MB — mutating sessions favor structural routing
```
