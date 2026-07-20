# Autoresearch Results — jul19: New Levers for Versioned Agent-Trace Storage

Findings of the second autonomous research run defined in
[`autoresearch_block_level_dedup.md`](./autoresearch_block_level_dedup.md),
conducted on branch `autoresearch-block-dedup/jul19` (base:
`block-level-dedup`, which already carries the full jul18 architecture — see
[`autoresearch_results_jul18.md`](./autoresearch_results_jul18.md)). The same
two fixed corpora gate every experiment: the 100.6 MB multi-version corpus
(v1) and the 177.8 MB prompt-cache corpus (v2). Byte-exact reconstruction of
every file at every commit remains the hard gate; every retained experiment
passes it on both corpora.

## Headline result

| | jul18 final (this run's baseline) | jul19 final | Δ |
| --- | --- | --- | --- |
| v1 stored_bytes (100.6 MB logical) | 12.77 MB | TBD | TBD |
| v1 storage_ratio | 0.1269 | TBD | |
| v2 stored_bytes (177.8 MB logical) | 8.40 MB | TBD | TBD |
| v2 storage_ratio | 0.0473 | TBD | |
| v1 restore | 2.8 s / 36 MB/s | TBD | |
| v2 restore | 3.1 s / 57 MB/s | TBD | |

## Retained mechanisms (in the order they were found)

1. **Chunk-index compaction (001)** — the LMDB chunk index is disposable
   derived state, but its data file carried 3–9× page slack (free pages plus
   half-filled B-tree leaves from many small write txns): 2.1 MB of v1's
   12.77 MB and a full 25% of v2's 8.40 MB. After each `oxen add`, the index
   is rebuilt into a fresh env with append-mode puts (leaves pack near-full)
   and atomically swapped in; the reader lock table is right-sized for the
   store (256 readers, was 1024). Work is skipped unless the reclaimable
   slack exceeds a floor, so the amortized cost stays negligible.
   **v1 −6.9%, v2 −15.4%**, no read cost.

2. **Embedded-zstd unwrap transform (002, `ZSTD_UNWRAP_V1`)** — compressed
   containers hide their content from every downstream lever (dedup,
   dictionaries, deltas). The transform scans any file for embedded zstd
   frames (parquet pages, arrow buffers, bare `.zst`), and for each frame
   **proves at ingest** that the bundled encoder reproduces it byte-for-byte
   at some level (pyarrow 23 and the Rust zstd crate both bundle libzstd
   1.5.7; frames verified identical at every level tested). Verified frames
   are stored decompressed with a 25-byte-per-segment recipe; reconstruction
   re-compresses them. Unverifiable frames stay verbatim, so the transform is
   lossless for arbitrary input *by construction* — no parquet/thrift parsing
   involved, and it generalizes to any zstd-embedding format.
   **v1 −6.5%** (the parquet commit's cost fell 4.1 → 3.3 MB), v2 tie.

3. **MinHash superfeature probes (003)** — the delta-base finder's
   prefix/mid/suffix sketches miss chunks whose three probe points all
   changed. One rolling gear-hash pass per chunk now also emits three
   min-of-permuted-hash content features (position-independent; a shared
   content run almost surely shares each minimizing window). Features join
   the positional sketches in the same advisory table.
   **v1 −1.6%, v2 −4.9%.**

4. **Block-window deltas (004, `ZSTD_WINDOW_DELTA`)** — the structural limit
   of single-base deltas: a permuted parquet page's redundancy is dispersed
   across the *entire* prior file's payload, so no ≤128 KB base covers it
   (measured bound: v4-mirror payload compresses 3× better against the whole
   v1 payload than solo — 1.68 → 0.54 MB — even chunk-by-chunk). The new
   store-local codec deltas a chunk against a **window of a sealed block**
   (`block_hash, start_member, count`): the dictionary is the concatenated
   raw bytes of the window's non-delta chunks. Bases cost zero extra storage
   because blocks are immutable and self-describing; window dictionaries are
   assembled once and cached; transfer packing re-encodes to plain zstd like
   the other store-local codecs. Triggered when ≥2 superfeature probes hit
   distinct chunks of one block — the signature of dispersed redundancy.
   **v1 −6.6%** (parquet commit −20%), v2 tie.

5. **Parallel reconstruction + lazy encoder digestion (005)** — reads.
   Reconstruction now decodes chunks in rayon batches of 64 (order
   preserved), caches decoded delta bases (many deltas share one base), and
   — critically — `PreparedDict` digests its *encoder* half lazily, so read
   paths never pay level-19 match-table digestion for window dictionaries
   (which had been costing hundreds of MB of transient memory and the 004
   random-read regression). Window assembly is single-flighted.
   **Storage byte-identical; v2 restore +39% (55.1 → 75.8 MB/s), sequential
   +41%; v1 restore +8%, random read −24%.**

6. **Append-lineage chunker routing (008)** — jul18's routing finding: the
   row-size sniff sends long-row *append-only* logs (RL rollouts) to the
   structural chunker, but rows that never mutate want byte-window FastCDC
   (fewer, larger, better-compressing chunks; measured 36.8 vs 52.4 MB on
   the RL corpus under the jul18 stack). New advisory signal: an xxh3 of the
   file's first 4 KB is counted per ingest; a lineage whose head survives
   **8** consecutive versions is an append-only log and routes to FastCDC.
   The high threshold makes short mutating histories (whose edits change the
   head sooner or later) never pay the one-time re-chunk a switch costs;
   long-horizon logs capture nearly all the win. v1/v2: tie (within 0.5%);
   RL-scale (6.75 GB, 80 iterations): **43.6 MB stored, ratio 0.00646,
   byte-verified at all 80 commits** (control comparison below).

## Experiment log (priority order: storage, then reads, then compression)

| # | Experiment | v1 stored | v2 stored | Verdict |
| --- | --- | --- | --- | --- |
| — | baseline: jul18 final architecture | 12,765,836 | 8,404,019 | baseline |
| 001 | chunk-index compaction + right-sized lock table | 11,881,100 | 7,109,683 | **keep** |
| 002 | embedded-zstd unwrap transform | 11,105,097 | 7,109,683 | **keep** |
| 003 | minhash superfeature delta probes | 10,932,600 | 6,761,678 | **keep** |
| 004 | block-window deltas | 10,206,975 | 6,759,492 | **keep** |
| 005 | parallel reconstruction (reads; storage identical) | 10,206,975 | 6,759,492 | **keep** |
| 006 | 16 MB dictionary sample cap | 10,210,110 | — | discard (tie, +memory) |
| 007 | window delta on single probe hit | 10,206,975 | — | discard (byte-identical, +memory) |
| 008a | self-window probe (4 MB raw prefix as dict for first-file tails) | −8.5% of tail chunks only | — | discard (<1% e2e, high complexity) |
| 008 | append-lineage chunker routing | 10,223,359 | 6,775,876 | TBD (RL-scale gate) |

## Per-workload behavior on v1 (final architecture vs jul18 final)

Physical bytes added by each benchmark commit:

| Commit | Workload | Logical MB | jul18 stored MB | jul19 stored MB | jul19 ratio |
| --- | --- | --- | --- | --- | --- |
| c1 | base snapshots (3 files) | 15.1 | 3.27 | 3.31 | 0.219 |
| c2 | traces append-only | 10.2 | 0.67 | **0.19** | **0.019** |
| c3 | scattered session growth + finetune append | 15.2 | 0.95 | 0.79 | 0.052 |
| c4 | full row reorder + growth + labels append | 15.8 | 0.52 | 0.55 | 0.035 |
| c5 | shared-prompt edit + deletes + inserts | 15.9 | 1.11 | **0.74** | 0.046 |
| c6 | big append + filtered copy + annotation column | 24.3 | 2.01 | 2.00 | 0.082 |
| c7 | Parquet mirrors of trace content | 4.1 | 4.38 | **2.63** | **0.64** |

(The jul18 column includes its index overhead amortized differently, so c1
appears near-equal while the totals differ; the two big movers are real:
append cost fell 3.4× — superfeatures + window deltas absorb the appended
sessions' internal redundancy — and the parquet mirrors fell 40% via the
unwrap transform + window deltas, with byte-identity preserved through
verified recompression.)

## What failed, and what it taught

- **Bigger dictionary training sample (006)**: 4 → 16 MB sample cap moved
  nothing (+0.03%) — a 64 KB trained dictionary saturates well before 4 MB of
  sample on this content. The dictionary is not the binding constraint any
  more; dispersed-context coverage (windows) is.
- **Single-hit window deltas (007)**: offering the window to any
  poorly-compressing chunk with even one probe hit changed *zero bytes* —
  every win the window can deliver already announces itself with ≥2 dispersed
  probe hits. Saved the memory and CPU of speculative window builds.
- **Self-referential windows (008a, probe only)**: using a file's own first
  4 MB as a window for its remaining chunks beats the trained dictionary by
  just 8.5% *of those tail chunks* (<1% end-to-end) — same-file redundancy is
  already mostly captured by the trained dictionary plus in-flight deltas.
  Not worth a sentinel self-block reference and seal-rollover re-encoding.
- **Parallel decode without dictionary discipline (005, first cut)**: naive
  rayon fan-out stampeded window-dictionary assembly and paid level-19
  *encoder* digestion on the read path — 1.8 GB peak memory. The fix
  (decoder-eager/encoder-lazy `PreparedDict`, single-flight assembly) is what
  made both 004 and 005 production-shaped; it also fixed 004's random-read
  regression.

TBD: RL-scale control comparison, further experiments, final numbers,
architecture answers, roadmap.
