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
| v1 stored_bytes (100.6 MB logical) | 12.77 MB | **10.21 MB** | **−20.0%** |
| v1 storage_ratio | 0.1269 | **0.1015** | |
| v2 stored_bytes (177.8 MB logical) | 8.40 MB | **6.76 MB** | **−19.6%** |
| v2 storage_ratio | 0.0473 | **0.0380** | |
| v1 restore | 2.8 s / 36.1 MB/s | 2.3 s / 44.0 MB/s | **+22% throughput** |
| v2 restore | 3.1 s / 56.9 MB/s | 2.4 s / 73.6 MB/s | **+29% throughput** |
| v1 / v2 random read | 262 / 233 ms | 241 / 220 ms | better |
| v1 / v2 compression | 46.7 / 42.5 s | 69.0 / 38.1 s | v1 1.5× slower (within guardrail) |
| v1 / v2 peak memory | 141 / 151 MB | 332 / 182 MB | bounded by window caps |
| correctness (both corpora, every commit) | pass | pass | |

Storage results are deterministic; every retained experiment was gated on both
corpora and byte-exact reconstruction. Cumulative over the two runs, the
original session baseline (24.19 MB v1) is down **2.4×**, and pure FastCDC
(20.40 MB) is down **2.0×**.

At scale, the final architecture stores the 6.75 GB RL corpus (80 training
iterations) at **45.9 MB — ratio 0.0068, 147× vs raw snapshots** — and the
2.68 GB long-horizon corpus (60 daily exports) at **25.43 MB — ratio 0.0095,
105×, 15% below jul18's 30.0 MB** — byte-verified at every commit on both.

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

6. **Append-lineage chunker routing — attempted, measured, and reverted.**
   Five gate iterations (full arc in "What failed") showed that switching an
   existing lineage's chunker cannot reliably recoup its one-time re-chunk
   cost under honest adversarial gates: the only variant that won on the RL
   corpus was the one blind enough to break the long-horizon corpus by 2.7×.
   The final architecture ships **no** automatic chunker switching; per-path
   `[[storage.profiles]]` marks (`generic`) remain the way to route a known
   append-only log, worth ~5% on the RL corpus. The negative result is the
   deliverable: it cost four corpora and ~4 hours of benchmarks to avoid
   shipping a regression that three of four corpora would never have shown.

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
| 008 | append-lineage routing, head-streak signal | 10,223,359 (tie) | 6,775,876 (tie) | reverted (broke long-horizon 2.7×) |
| 009 | zstd level 22 | 10,203,723 (tie) | 6,758,134 (tie) | discard (2× slower compress for a tie) |
| 010a–c | append routing, measured-verdict variants | tie | tie | reverted (flip-flop / late switch at scale) |

### The routing gates at scale (why it was reverted)

Same final stack, only the routing policy differs. Control = no routing
(structural chunking throughout, the shipped behavior):

| RL-scale (6.75 GB, 80 iters) | stored | restore | verdict |
| --- | --- | --- | --- |
| control (no routing — **shipped**) | 45,891,332 | 82.5 s | final |
| head-streak switch at v8 | 43,590,720 (−5.0%) | 200 s | reverted: same signal stores the long-horizon corpus at 80.4 MB vs 25.4 MB |
| measured verdict, 0.5% tolerance, symmetric reset | 71,933,125 (+57%) | 94 s | chunker flip-flop |
| measured verdict, asymmetric hysteresis (7 in / 3 out) | 47,756,643 (+4.1%) | 207 s | switches too late to recoup the re-chunk |

| long-horizon (2.68 GB, 60 days) | stored | verdict |
| --- | --- | --- |
| no routing / verdict-routing that never fires (**shipped**) | 25,446,028 | final — **15% better than jul18's 30.0 MB** |
| head-streak switch at day 8 | 80,421,415 (2.7×) | the falsifying measurement |
| 2% verdict tolerance (switches ~day 47) | 35,534,595 | late-switch damage |

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
- **Append routing took four falsifications to get right.** The arc, every
  step benchmark-driven:
  1. *Head-streak signal* ("same first-4KB across 8 versions ⇒ append-only")
     passed v1, v2, *and* the RL corpus (−5.0%), then failed loudly on the
     long-horizon corpus: a mutating table whose oldest sessions sit at the
     head keeps a byte-stable head forever, so routing flipped it to generic
     at day 8 and stored **80.4 MB instead of ~30 MB (2.7× worse)**. Head
     stability is not evidence of append-only behavior.
  2. *Measured verdict, 2% interior tolerance* (new chunks confined to the
     tail, majority reused): the long-horizon corpus's slow old-session
     churn measures ~1–1.5% interior per day — under the tolerance — so it
     still switched late and stored 35.5 MB.
  3. *Tightened to 0.5% with symmetric reset*: long-horizon fixed
     (**25.45 MB — 15% better than jul18**), but the RL corpus now
     **flip-flopped**: streaks built during pure-append stretches, switched
     to generic, then a periodic reward-relabel iteration exceeded the
     tolerance, reset the streak, and re-chunked structurally — repeatedly.
     71.9 MB, worse than never routing at all. A symmetric reset treats one
     mutating version as disqualifying when the actual cost model is "every
     mode flip re-chunks the file".
  4. *Asymmetric mode hysteresis* (kept): 7 consecutive append verdicts to
     enter generic routing, 3 consecutive mutating verdicts to leave.
     Isolated relabels can't thrash it; genuinely mutating lineages exit in
     three versions; long-horizon never enters.
  One corpus away from shipping a 2.7× regression, twice: every routing
  heuristic needs adversarial corpora in the gate set, and any switch whose
  cost is a full re-chunk needs hysteresis matched to that cost.
- **Parallel decode without dictionary discipline (005, first cut)**: naive
  rayon fan-out stampeded window-dictionary assembly and paid level-19
  *encoder* digestion on the read path — 1.8 GB peak memory. The fix
  (decoder-eager/encoder-lazy `PreparedDict`, single-flight assembly) is what
  made both 004 and 005 production-shaped; it also fixed 004's random-read
  regression.

## Answers to the guide's architecture checklist (delta from jul18)

- **Identities**: unchanged (xxh3-128 raw-chunk hashes; content-addressed
  blocks, dictionaries, manifests) — with one addition: a *transformed*
  manifest's `file_hash` is still the **original** file's hash (the version
  identity), while its chunks tile the transformed stream; the original size
  is recoverable from the stream's own recipe, so the manifest wire format is
  unchanged.
- **Boundaries**: unchanged chunkers; `TRACE_AUTO_V1`'s resolution gains the
  append-lineage signal (advisory store state — the manifest still records
  the resolved concrete chunker, so reconstruction and manifest purity are
  unaffected).
- **Codecs**: two new store-local codecs. `ZSTD_WINDOW_DELTA` (base = a
  member range of a sealed block; zero base storage; depth-one by
  construction — delta-coded members are skipped in window assembly, and its
  own base is always plain/dict chunks). Both new codecs are re-encoded to
  plain zstd by transfer packing, so the wire contract is untouched.
- **Transforms**: the reserved seam is now active. `ZSTD_UNWRAP_V1` is
  format-agnostic (frame scan + verified recompression), so "parquet support"
  needs no parquet parser at all; the same transform covers arrow and `.zst`.
  Its durability contract: the transform ID freezes the bundled encoder's
  output for stored streams — a libzstd upgrade that changes compressed
  output must ship as a new transform ID with the old encoder pinned for
  reads (same discipline as chunker IDs).
- **Indexes**: the LMDB env gains a fourth advisory table (`lineage_heads`)
  and, after every `oxen add`, an append-mode rebuild-compaction with an
  atomic file swap (skipped when there is little slack). The index remains
  disposable derived state; compaction never touches blocks or manifests.
- **Reads**: reconstruction decodes in rayon batches (order preserved);
  decoded delta bases are cached; window dictionaries digest their decoder
  half eagerly and their encoder half lazily, and assembly is
  single-flighted. Random access on transformed manifests reconstructs the
  file (bounded by the transform's 64 MB ingest cap) — the documented read
  amplification of the unwrap path.

## Scale measurements (final architecture)

| Corpus | Logical | Stored | Ratio | vs raw snapshots |
| --- | --- | --- | --- | --- |
| v1 multi-version (7 commits) | 100.6 MB | 10.21 MB | 0.1015 | 9.9× |
| v2 prompt-cache (6 commits) | 177.8 MB | 6.76 MB | 0.0380 | 26× |
| long-horizon (60 daily exports) | 2.68 GB | **25.43 MB** | **0.00949** | 105× |
| RL-scale (80 training iterations) | 6.75 GB | **45.89 MB** | **0.00680** | 147× |

All byte-verified at every commit.

## Reproduction

```bash
git checkout autoresearch-block-dedup/jul19
oxen-python/.venv/bin/python benchmark/dedup/prepare.py                # corpus v1
oxen-python/.venv/bin/python benchmark/dedup/prepare_promptcache.py    # corpus v2
python3 benchmark/dedup/prepare_longhorizon.py                         # 2.68 GB
python3 benchmark/dedup/prepare_rlscale.py                             # 6.75 GB
cargo build --workspace
OXEN_BIN=$PWD/target/debug/oxen python3 benchmark/dedup/benchmark.py
BENCH_CORPUS=corpus-promptcache OXEN_BIN=$PWD/target/debug/oxen \
    python3 benchmark/dedup/benchmark.py
BENCH_CORPUS=corpus-longhorizon OXEN_BIN=$PWD/target/debug/oxen \
    python3 benchmark/dedup/benchmark.py
BENCH_CORPUS=corpus-rlscale OXEN_BIN=$PWD/target/debug/oxen \
    python3 benchmark/dedup/benchmark.py
```

Every experiment is one commit on this branch's history; discarded
experiments are recorded in the log above and in the raw rows appendix.

## Appendix: raw measurement rows

The untracked `results.tsv` rows for this run (columns per the guide):

```tsv
commit	score	stored_bytes	storage_ratio	incremental_stored_bytes	incremental_ratio	restore_seconds	restore_mb_s	sequential_read_mb_s	random_read_ms	compression_seconds	compression_mb_s	memory_gb	status	description
9ad1af6a	0.126896	12765836	0.126896	9436582	0.110357	2.8	36.1	10.4	260.0	46.7	2.2	0.1	keep	baseline v1: jul18 final architecture (branch HEAD)
9ad1af6a	0.047280	8404019	0.047280	5281294	0.035020	3.1	56.9	51.9	233.0	42.5	4.2	0.1	keep	baseline v2 (corpus-promptcache): jul18 final architecture
8e4600f	0.118101	11881100	0.118101	8600998	0.100585	2.5	40.0	10.2	263.6	41.9	2.4	0.1	keep	001 v1: LMDB index append-rebuild compaction post-add + 256 max_readers (-6.9%)
8e4600f	0.039998	7109683	0.039998	4478478	0.029697	3.1	56.5	40.4	237.9	31.5	5.6	0.1	keep	001 v2: same (-15.4%)
38fa38c	0.110388	11105097	0.110388	7824995	0.091510	2.5	40.7	10.3	265.2	52.4	1.9	0.1	keep	002 v1: embedded-zstd unwrap transform, parquet pages verified+decompressed (-6.5%)
38fa38c	0.039998	7109683	0.039998	4478478	0.029697	3.1	56.5	40.4	237.9	31.5	5.6	0.1	keep	002 v2: tie (no parquet in corpus)
9efeb3a	0.108673	10932600	0.108673	7619730	0.089110	2.5	40.0	6.7	259.9	65.7	1.5	0.1	keep	003 v1: minhash superfeature delta probes (-1.6%)
9efeb3a	0.038040	6761678	0.038040	4331992	0.028726	3.1	57.6	51.3	219.5	36.9	4.8	0.1	keep	003 v2: same (-4.9%)
a68cc2e	0.101459	10206975	0.101459	6894105	0.080624	2.5	39.8	7.0	322.4	68.9	1.4	0.3	keep	004 v1: block-window deltas, 8MB sealed-block bases (-6.6%; parquet commit -20%)
a68cc2e	0.038028	6759492	0.038028	4329806	0.028712	3.2	55.1	50.9	245.8	38.1	4.7	0.2	keep	004 v2: tie (-0.03%)
5d78631ce	0.101459	10206975	0.101459	6894105	0.080624	2.3	44.5	7.9	245.7	68.4	1.4	0.4	keep	005 v1: parallel batched reconstruction + lazy encoder digests (storage tied, reads +8-24%)
5d78631ce	0.038028	6759492	0.038028	4329806	0.028712	2.3	75.8	71.6	215.3	38.1	4.7	0.2	keep	005 v2: storage tied, restore +39%, sequential +41%
9c0a1f0	0.101490	10210110	0.101490	6897240	0.080660	2.3	44.0	7.8	250.0	69.9	1.4	0.5	discard	006 v1: 16MB dict sample cap (tie +0.03%, +memory)
e40f9c1	0.101459	10206975	0.101459	6894105	0.080624	2.3	44.1	7.8	250.1	68.7	1.4	0.6	discard	007 v1: single-hit window widening (storage byte-identical, +memory)
-	-	-	-	-	-	-	-	-	-	-	-	-	discard	008-probe: self-window 4MB vs trained dict on first-file tails: only -8.5% of tail chunk bytes (<1% e2e) - not worth sentinel complexity
05f66ab	0.101622	10223359	0.101622	6910489	0.080816	2.4	42.0	7.7	255.0	69.5	1.4	0.3	keep	008 v1: append-lineage routing (tie +0.16%)
05f66ab	0.038120	6775876	0.038120	4346190	0.028821	3.1	57.0	50.2	230.0	38.6	4.6	0.2	keep	008 v2: tie +0.24%
05f66ab	0.006462	43590720	0.006462	34993153	0.005400	200.0	33.7	0	0	751.9	9.0	0.8	keep	008 rlscale: -5.0% vs control 45891332 (restore 2.4x slower, under 3x guardrail)
-	-	10203723	-	-	-	-	-	-	-	137.2	-	0.5	discard	009 v1: zstd 22 (tie -0.03%, 2x slower compress); v2 6758134 tie
2778863	0.101622	10223359	0.101622	6894105	0.080624	2.3	44.0	7.7	241.2	69.0	1.5	0.3	keep	FINAL v1: complete jul19 architecture (clean timing run)
2778863	0.038120	6775876	0.038120	4329806	0.028712	2.4	73.6	70.5	220.1	38.1	4.7	0.2	keep	FINAL v2: complete jul19 architecture (clean timing run)
2778863	0.030004	80421415	0.030004	0	0	32.0	83.8	0	0	750.8	3.6	0.5	crash	008-as-shipped longhorizon: head-streak misroutes mutating corpus to generic (80.4MB vs jul18 30.0MB, 2.7x worse) - signal falsified
8a41c33	0.013258	35534595	0.013258	0	0	0	0	0	0	0	0	0.5	discard	010a longhorizon: 2% verdict tolerance still switches late (~day 47), 35.5MB vs jul18 30.0 - tightened to 0.5%
8f10f2c	0.009494	25446028	0.009494	0	0	53.0	50.6	0	0	236.1	11.4	0.5	keep	010b longhorizon: verdict routing 0.5% tolerance stays structural - 25.4MB, beats jul18 30.0MB by 15%
8f10f2c	0.010664	71933125	0.010664	0	0	94.4	71.5	0	0	854.1	7.9	0.9	discard	010b rlscale: 0.5% symmetric reset flip-flops chunker (71.9MB vs control 45.9) - replaced with asymmetric mode hysteresis
fc00cd6	0.009494	25446028	0.009494	0	0	0	0	0	0	0	0	0.5	keep	010c longhorizon: asymmetric hysteresis, byte-identical to 010b (never enters generic)
fc00cd6	0.007080	47756643	0.007080	0	0	206.6	32.6	0	0	772.8	8.7	0.7	discard	010c rlscale: asymmetric hysteresis switches too late to recoup re-chunk (+4.1% vs control) - routing feature reverted entirely
aabd115	0.009488	25429644	0.009488	0	0	51.6	51.9	0	0	240.5	11.1	0.5	keep	FINAL longhorizon: shipped state (no routing) 25.43MB, 15% below jul18 30.0MB
aabd115	0.006804	45891332	0.006804	0	0	82.5	81.8	0	0	636.3	10.6	0.3	keep	FINAL rlscale: shipped state (no routing) 45.89MB ratio 0.0068
```

## Roadmap (in expected-value order)

1. **Generational dictionaries** (carried over, now sharper): the class
   dictionary trains once, on the first ingest; long-horizon corpora drift
   (curation, reward relabels, new tools). Track the per-ingest
   dict-encoded compression ratio; when it degrades against its trailing
   baseline, retrain from recent samples and publish a new content-addressed
   generation (old chunks keep their old dictionary — the reference is in
   the payload).
2. **Chained-group ("solid") encoding for cold repack**: the self-window
   probe caps same-file gains at ~8.5% of tail chunks; a repack pass that
   compresses groups of 8–16 consecutive chunks with a shared context (with
   group-restart bounds on read amplification) could bank that during
   `oxen storage repack` without touching the hot ingest path.
3. **Window deltas across blocks**: windows are currently single-block; a
   lineage whose payload spans blocks (files > 64 MB) can't be covered by
   one window. Multi-range windows (or per-lineage "anchor blocks") extend
   the mechanism to large files.
4. **Unwrap transform for snappy parquet**: snappy is deterministic and
   self-framing; the same verified-recompression contract applies. Needs a
   snappy encoder dependency decision.
5. **Streaming unwrap**: the transform currently buffers files ≤64 MB in
   memory; a temp-file spill extends it to arbitrary sizes, and the seekable
   reader can learn to map original↔transformed offsets through the recipe
   instead of materializing.
6. **Index self-healing**: `MissingChunk` on read should trigger one
   automatic `rebuild_index` retry — it makes the compaction swap (and any
   crash window) fully self-repairing.

