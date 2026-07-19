# Block-Level Dedup on Evolving Chat-Message JSONL — Experiment Report

How well does the block-v1 content format compress a chat-message history
table that is committed repeatedly as its rows grow? Short answer: **it
depends almost entirely on what fraction of rows change between commits** —
from 26× better than legacy storage (append-only) down to *no better than
zstd-compressing every snapshot* (when ~20% of rows change per commit). The
mechanism is quantified below; the practical takeaway is that byte-level CDC
amplifies scattered row-level edits by roughly the number of rows that share a
chunk, and data layout matters more than the chunker.

Run on the `block-level-dedup` branch (block-v1: FastCDC 8/64/128 KiB
min/avg/max chunks, zstd-3 per chunk, 64 MiB blocks, 1 MiB chunking floor),
debug build, macOS/APFS. Every scenario's bytes round-tripped exactly
(checkout of the base commit and back to head, SHA-256 verified).

## Workload

One JSONL file, one chat session per line, three columns:

```json
{"uuid": "...", "messages": [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}], "source": "web"}
```

`messages` follows the OpenAI chat format (`system`/`user`/`assistant`/`tool`
roles; assistant turns are long, occasionally with code blocks; ~15% of
exchanges include a tool call). Content is seeded pseudo-English that
zstd-compresses ~3.1× — in the same regime as real chat text.

The file starts at 5,000 sessions (11.5 MB, avg row ~2.3 KB) and evolves over
12 commits. Each commit appends new turns to some sessions **in place**
(scattered through the file, rows keep their position) and appends new
sessions at the end. A "hot" subset grows most commits, so traces get long
(hot sessions end at 50–200+ messages). Three edit-density scenarios:

| Scenario | Rows gaining messages per commit | Models |
| --- | --- | --- |
| `high` | ~23% | busy product, snapshots taken rarely |
| `low` | ~2.6% | the same traffic snapshotted ~10× more often |
| `append` | 0% (only new rows at EOF) | append-only message-log layout |

## Results

Per-commit numbers. **new** = bytes of genuinely new content (appended
messages + new sessions); **stored Δ** = growth of `.oxen/versions` (blocks +
manifests + chunk index) for that commit; **amp** = stored Δ / new — the
storage amplification of the format on that commit (lower is better; values
below 1.0 mean compression beat the amplification).

### `high` — ~23% of rows touched per commit: dedup collapses

| ver | file MB | zstd MB | new MB | stored Δ MB | amp |
| --- | --- | --- | --- | --- | --- |
| v0 | 11.5 | 3.7 | — | 4.3 | — |
| v1 | 14.3 | 4.7 | 2.80 | 5.3 | 1.9× |
| v4 | 22.8 | 7.5 | 2.77 | 8.4 | 3.0× |
| v8 | 34.3 | 11.4 | 2.89 | 12.4 | 4.3× |
| v12 | 46.1 | 15.3 | 2.94 | 16.0 | 5.4× |

Every commit stores ≈ the *entire compressed snapshot* (stored Δ tracks the
zstd column, not the new column). Dedup found essentially nothing to reuse,
and the per-commit cost grows with file size, not with edit size. Final:
**134.1 MB stored for 372.4 MB logical (13 snapshots of ≤46 MB).**

### `low` — ~2.6% of rows touched per commit: dedup recovers about half

| ver | file MB | zstd MB | new MB | stored Δ MB | amp |
| --- | --- | --- | --- | --- | --- |
| v0 | 11.5 | 3.7 | — | 4.3 | — |
| v1 | 12.1 | 3.9 | 0.61 | 2.9 | 4.7× |
| v4 | 13.9 | 4.6 | 0.60 | 3.4 | 5.6× |
| v8 | 16.3 | 5.3 | 0.60 | 3.7 | 6.3× |
| v12 | 18.7 | 6.1 | 0.61 | 4.0 | 6.5× |

Each commit stores ~60% of a full compressed snapshot — better than `high`,
but still ~6 bytes stored per byte of new content. Final: **45.7 MB for
196.5 MB logical.**

### `append` — rows never change: dedup is near-perfect

| ver | file MB | zstd MB | new MB | stored Δ MB | amp |
| --- | --- | --- | --- | --- | --- |
| v0 | 11.5 | 3.7 | — | 4.3 | — |
| v1 | 12.2 | 4.0 | 0.69 | 0.32 | 0.5× |
| v4 | 14.2 | 4.6 | 0.68 | 0.27 | 0.4× |
| v8 | 16.9 | 5.5 | 0.66 | 0.26 | 0.4× |
| v12 | 19.6 | 6.4 | 0.68 | 0.26 | 0.4× |

Only the new tail rows (plus the one resplit chunk at the old EOF boundary)
are stored, compressed. Final: **7.6 MB for 202.4 MB logical** — barely more
than one compressed copy of the final file.

### Cumulative comparison

"Legacy" is what the pre-dedup format stores (every snapshot as an
uncompressed whole-file blob); "zstd snapshots" is the naive alternative of
just compressing each snapshot whole.

| Scenario | Logical | Legacy | zstd snapshots | block-v1 | vs legacy | vs zstd snapshots |
| --- | --- | --- | --- | --- | --- | --- |
| `high` | 372.4 MB | 372.4 MB | 123.4 MB | **134.1 MB** | 2.8× | **0.92× (worse)** |
| `low` | 196.5 MB | 196.5 MB | 64.3 MB | **45.7 MB** | 4.3× | 1.4× |
| `append` | 202.4 MB | 202.4 MB | 65.9 MB | **7.6 MB** | 26.5× | 8.6× |

Metadata overhead is negligible throughout: manifests + LMDB chunk index
totaled 0.3–1.4 MB (~1% of stored bytes).

## Why: the arithmetic of scattered row edits

A chunk is reusable only if **every** byte in it is unchanged. With average
chunk size ~64 KiB and rows of ~2–7 KB, each chunk spans `r ≈ 10–28` rows. If
a fraction `p` of rows gains messages between commits, the probability a
chunk survives untouched is about

```
P(chunk clean) ≈ (1 − p)^r
```

which is brutal at these parameters:

| Scenario | p | r (rows/chunk) | predicted clean | observed clean* |
| --- | --- | --- | --- | --- |
| `high` | 0.23 | 9–28 | 0.1%–10% | ~0% |
| `low` | 0.026 | 21–28 | 48%–58% | ~40% |
| `append` | 0 | — | ~100% | ~100% |

\* observed = 1 − (stored Δ ÷ compressed snapshot size). Observed runs
slightly dirtier than predicted because an edit also resplits its
neighborhood — FastCDC resynchronizes within a chunk or two, but the
boundary-adjacent chunk is often lost too.

Two properties of this workload conspire against content-defined chunking:

1. **Edits are scattered, not clustered.** Chat sessions grow all over the
   file. CDC handles the byte *shifts* fine (boundaries resync quickly — this
   is not the failure mode), but every chunk that *contains* an edited row is
   new by definition.
2. **Rows are much smaller than chunks.** The amplification factor is
   precisely `r`: one edited 3 KB row invalidates a 64 KB chunk. In the
   `low` scenario the measured amplification (~6×) is `p·r·S / (p·S_row-ish)`
   — the format stores ~20 clean rows to record 1 dirty one.

This also explains the `high` scenario's small *negative* result vs plain
zstd snapshots: with a ~0% dedup hit rate, block-v1 degenerates into
"zstd-compress every snapshot in 64 KiB windows", and 64 KiB windows compress
~8% worse than whole-file zstd. That is the format's floor when dedup finds
nothing.

A counterintuitive corollary on commit frequency: in the dense regime
(`p·r ≫ 1`, e.g. `high`), each snapshot costs one full compressed copy no
matter how little changed — so slicing the *same* edit traffic into 10× more
commits stores up to ~10× more. In the sparse regime (`p·r ≪ 1`) total
storage is frequency-independent (each commit stores its own dirty chunks
either way). Committing *less* often never hurts and caps the worst case.

## Delta-compression headroom

Chunk-level dedup is all-or-nothing per chunk, but a "dirty" chunk here is
still ~95% identical to its predecessor — headroom that *delta* compression
can capture and chunk dedup cannot. As an upper bound, compressing each
`high`-scenario snapshot as a delta against the previous version
(`zstd -3 --long=27 --patch-from=<prev>`):

| Store as | `high` cumulative (13 versions) |
| --- | --- |
| block-v1 (measured) | 134.1 MB |
| zstd whole snapshots | 123.5 MB |
| zstd + patch-from chain | **17.3 MB** |

Per commit the delta is ~1.1 MB against ~2.9 MB of new content — compression
beats amplification even in the scenario where chunk dedup found nothing.
Delta encoding is what recovers the scattered-small-edit workload; see
implication 2 below.

## Implications

1. **Data layout dominates everything else.** The same logical chat history
   stored as an append-only message log (one row per *message*, keyed by
   session uuid, sessions materialized on read) versus mutable session rows
   is the difference between 7.6 MB and 134 MB — a 17× spread on identical
   content. Where users control the schema, "append new rows, don't grow old
   ones" is the single most effective guidance for versioning evolving
   tables in a CDC-based store.
2. **Chunk-level delta encoding is the highest-leverage format improvement.**
   Storing a dirty chunk as a delta against the corresponding chunk of the
   file's previous version (rather than as a brand-new chunk) would capture
   the measured 134 MB → ~17 MB headroom above — turning the worst case into
   append-layout economics *without* asking users to change their schema.
   The costs are real: delta chains add read amplification and complicate
   GC (a base chunk stays reachable while any delta references it), so it
   needs packfile-style discipline — bounded chain depth and periodic full
   chunks.
3. **Chunk size is a real tuning lever for tabular data, but not a rescue.**
   Amplification is linear in rows-per-chunk, so dropping the average chunk
   from 64 KiB to 16 KiB cuts `r` ~4× at the cost of ~4× manifest/index
   entries — worthwhile for line-delimited tables (the chunker-ID registry
   supports adding a named policy without a format break), and roughly a 3×
   improvement in the `low` scenario. It cannot rescue `high`: even at
   8 KiB average, ~60% of chunks are dirty per commit.
4. **Row-aligned chunk boundaries help only at the margin.** Anchoring CDC
   boundaries to newlines would stop the "neighbor chunk also dirtied"
   spillover (~1.2–1.5× observed vs predicted), but does nothing about the
   `r`-row amplification, which is the dominant term.
5. **Dedup's floor is acceptable.** Even in the adversarial scenario,
   block-v1 stored 2.8× less than legacy whole-file storage and only 8%
   more than ideal whole-snapshot compression — the format never loses badly,
   it just stops winning.
6. **Ingest cost is O(file), not O(delta)** — `oxen add` re-chunks the whole
   file each commit (it must hash the full content for identity anyway).
   Wall-clock scaled linearly with file size (1.1 s at 11.5 MB → 3.0 s at
   46 MB, debug build). Fine at these sizes; worth remembering for
   multi-GB tables committed frequently.

## Caveats

- Synthetic seeded text (~3.1× zstd ratio). Real chat text with heavier
  repetition may compress somewhat better, but the *dedup* behavior depends
  on edit geometry, not text realism.
- Single file, single machine, debug binaries — timings are indicative only.
- Cumulative sizes never shrink: reverse migration/GC doesn't exist yet, and
  none was needed here (no version was deleted).
- JSONL is the *friendly* case for CDC among tabular formats: a Parquet
  version of this table would re-encode and re-compress whole row groups on
  any change, scrambling bytes globally and landing at or below the `high`
  scenario's result.

## Follow-up: the trace-jsonl chunker

The findings above motivated a structure-anchored chunker for this workload —
`TRACE_JSONL_V1`, selectable per file via storage-profile marks — which cuts
only at row boundaries and message-array element boundaries so dedup
granularity follows the data's own edit granularity. Re-running these
scenarios with it: `high` 134.1 → **50.5 MB** (amplification ~5.4× → flat
~1.3×), `low` 45.7 → **15.6 MB**, rows-reshuffled-every-commit 137.6 →
**63.3 MB**; the append-only scenario regresses to 14.3 MB (finer chunks cost
metadata + compression ratio where byte-CDC was already ideal). Design,
trade-offs, and the Parquet analysis:
[`trace_compression.md`](./trace_compression.md).

## Reproduction

Deterministic generator + runner (seed 42): base corpus of 5,000 sessions;
per commit, hot/super-hot/warm session subsets gain 1–12 user/assistant/tool
turn pairs in place and 100–300 new sessions append at EOF; each version is
`oxen add`-ed and committed to a fresh block-v1 repo; storage is measured as
the byte-sum of `.oxen/versions/files/` split into blocks, chunk index, and
manifests. Baselines: raw snapshot sizes (legacy) and `zstd -3` of each
snapshot. Scenario parameters are the `p` values in the table above.
