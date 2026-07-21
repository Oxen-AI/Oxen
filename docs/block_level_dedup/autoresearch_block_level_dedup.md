# autoresearch-dedup

This is an experiment in having an LLM autonomously research and optimize block-level deduplication and lossless compression strategies for versioned tabular datasets. You are working in a repository for the oxen version control tool, which looks a lot like git on the surface, but is optimized for large data (unlike git). The commands are `oxen add` and `oxen commit` similar to git. You will use git to track the experiments, but `oxen` is the version control tool we are optimizing.

We have already run some experiments and reported the findings and more context in this notion file: https://app.notion.com/p/oxenai/Block-level-Deduplication-1f2faad69c0880a4a124fc78fa581a33?source=copy_link

Our goal is to explore new novel avenues, new algorithms, and try things no one has ever thought of before to improve on these baselines. The old experiments should still be on branches for your reference, but let's explore new territory rather than exploit old ideas. You may bring in wisdom from other fields as you are exploring new avenues.

The repository already contains:

- A base git branch named `block-level-dedup`
- The core storage and benchmark scaffolding
- A working baseline implementation based on FastCDC
- Documentation of all our planning so far in docs/block_level_dedup directory

Treat the existing FastCDC implementation as the reference baseline, not as the final architecture. The purpose of this research is to discover substantially better approaches tailored to structured AI training data.

The target data consists of:

- JSONL files
- CSV files
- Parquet files
- Rows containing agent traces and fine-tuning examples
- Nested `messages[]` arrays containing system, user, assistant, and tool messages
- Multiple versions, branches, and snapshots of related datasets

The goal is to develop the best practical storage engine for versioning this type of data.

This is not merely a general-purpose file compression experiment. The system should exploit knowledge of:

- File format
- Schema
- Column types
- Nested message structure
- Repeated prompts and tool definitions
- Shared conversation prefixes
- Similar agent traces
- Dataset version history
- Append-only changes
- Row-level edits
- Reordered rows
- Cross-file and cross-version redundancy

Different formats, columns, and logical data types may use entirely different deduplication, chunking, encoding, and compression strategies. That specialization is the point of the research and a core product advantage.

## Research objective

Develop a lossless, format-aware, block-level storage strategy that minimizes the physical storage required for a repository containing many versions of related datasets.

The optimization priorities are, in strict order:

1. **Stored size after deduplication and compression**
2. **Speed of reconstructing and reading files**
3. **Time required to deduplicate and compress the data**

The primary objective is:

> Minimize the total and incremental bytes required to store dataset versions, then maximize reconstruction and read performance, then minimize compression time.

Read performance is very important as well. Make sure you explore some paths that exploit the ability to reconstruct the file extremely fast.

Correctness is a hard gate. Any failure to reconstruct the original file byte-for-byte is an automatic failure regardless of storage savings or speed.

The benchmark may produce one canonical `score`, but every experiment must also report the raw metrics behind that score. Do not allow a blended score to hide a worse storage result.

The benchmark should heavily reward:

1. Low total stored bytes
2. Low incremental bytes for each new version
3. Cross-file and cross-version deduplication
4. Exact reconstruction
5. Fast full-file reconstruction
6. Fast sequential and random reads
7. Low compression and ingestion time
8. Robustness to insertions, deletions, reordering, and small edits
9. Reasonable memory usage
10. Implementation simplicity

A solution that achieves excellent compression but makes reconstruction or compression operationally unusable is not considered a good storage engine. However, storage size remains the dominant optimization target.

## Metric priority and tradeoff policy

Rank valid experiments lexicographically rather than treating all metrics as equally important.

### Priority 1: stored size

Track at minimum:

- `source_bytes`: total logical bytes presented to the storage engine
- `stored_bytes`: total physical bytes consumed by blocks, indexes, manifests, dictionaries, deltas, and metadata
- `storage_ratio`: `stored_bytes / source_bytes`
- `incremental_stored_bytes`: physical bytes added by each new version
- `incremental_ratio`: incremental physical bytes divided by logical bytes introduced by new versions
- `metadata_bytes`: bytes consumed by indexes, manifests, recipes, and other metadata
- `unique_payload_bytes`: bytes occupied by unique stored payloads

`stored_bytes` must include all persistent storage overhead. Do not report only compressed payload bytes while excluding indexes or reconstruction metadata.

A candidate with materially larger output must not be selected merely because it is faster.

### Priority 2: reconstruction and read speed

Among candidates whose storage size is effectively tied, prefer the one with better read performance.

Track at minimum:

- `restore_seconds`: wall-clock time to reconstruct the benchmark corpus
- `restore_mb_s`: reconstructed source bytes per second
- `sequential_read_mb_s`: throughput for sequential reads
- `random_read_ms`: latency for benchmark random reads
- `row_read_ms`: latency for reading selected rows when supported
- `column_read_ms`: latency for reading selected columns when supported
- `read_amplification`: physical bytes read divided by logical bytes returned

Full-file reconstruction is the most important read metric. Random and selective reads are secondary but must remain practical.

### Priority 3: compression time

Among candidates whose storage and read performance are effectively tied, prefer the one that compresses faster.

Track at minimum:

- `compression_seconds`: total wall-clock time to parse, deduplicate, compress, index, and persist the benchmark corpus
- `compression_mb_s`: logical source bytes processed per second
- `version_add_seconds`: time required to add each subsequent dataset version
- `hashing_seconds`, `parsing_seconds`, `matching_seconds`, and `codec_seconds` when profiling is available

Compression timing must include parsing, hashing, similarity search, delta generation, metadata construction, and persistent writes. Do not time only the final codec call.

### Default comparison rule

Unless the repository already defines stricter thresholds, use this decision process:

1. Reject any candidate that fails correctness.
2. Compare `stored_bytes` and `incremental_stored_bytes`.
3. Treat storage results within **0.5%** as effectively tied.
4. If storage is tied, compare full reconstruction throughput, sequential reads, random-read latency, and read amplification.
5. Treat the aggregate read result within **5%** as effectively tied.
6. If storage and reads are tied, compare `compression_seconds` and `compression_mb_s`.
7. Use memory usage and implementation simplicity only as final tie-breakers or operational guardrails.

Do not change these tolerances during a run merely to retain a favored experiment.

### Operational guardrails

Storage remains dominant, but avoid pathological implementations.

By default:

- Reject implementations that are more than **3× slower** at full reconstruction unless they reduce stored size by at least **10%**.
- Reject implementations that are more than **5× slower** to compress unless they reduce stored size by at least **10%**.
- Reject implementations with unbounded memory growth, index growth, delta-chain depth, or read amplification.
- Record promising storage-only ideas as `discard` with a clear description if their runtime costs are currently impractical. They may inspire a bounded version later.

These are guardrails, not equal-weighted objectives.

## Important distinction

Traditional compression reduces the size of one object in isolation.

This research is primarily about reducing the storage cost of an evolving collection of related objects:

- Version 1 contains 100,000 traces.
- Version 2 adds 5,000 traces and modifies 500.
- Version 3 reorders rows and changes a common system prompt.
- Version 4 adds another 100,000 traces.
- A branch filters the dataset and adds annotations.
- Another file contains many of the same examples in Parquet instead of JSONL.
- Thousands of traces reuse the same system prompt, tool schema, conversation prefix, code block, or tool output.

The best strategy may combine:

- Physical block deduplication
- Content-defined chunking
- Logical record deduplication
- Column-level encoding
- Message-level structural interning
- Delta encoding
- Dictionary encoding
- Shared prefix and suffix extraction
- Format-specific storage
- Cross-format canonical representations
- Reconstruction recipes
- Adaptive compression selection

The benchmark, not theoretical elegance, determines whether an idea is successful.

## Resources

Some nice research is done by the xet team at hugging face. Our objective is to BEAT their benchmarks, but you may get some inspiration from their work.

- [From Files to Chunks: Improving HF Storage Efficiency](https://huggingface.co/blog/from-files-to-chunks)
- [From Chunks to Blocks: Accelerating Uploads and Downloads on the Hub](https://huggingface.co/blog/from-chunks-to-blocks)
- [Parquet Content-Defined Chunking](https://huggingface.co/blog/parquet-cdc)
- [Xet Chunk-Level Deduplication Specification](https://huggingface.co/docs/xet/en/deduplication)
- [Deduplication](https://huggingface.co/docs/hub/en/xet/deduplication)

# Branch model

The existing branch `block-level-dedup` is the authoritative starting point.

Do not commit experiments directly to it.

Because Git cannot have both a branch named `block-level-dedup` and branches named `block-level-dedup/<tag>`, use separate namespaces for research branches.

For each autonomous research run:

- Base branch: `block-level-dedup`
- Persistent best-result branch: `autoresearch-block-dedup/<tag>`
- Temporary experiment branches: `experiment-block-dedup/<tag>/<number>-<slug>`

Example:

```text
block-level-dedup
└── autoresearch-block-dedup/jul18
    ├── experiment-block-dedup/jul18/001-fastcdc-parameters
    ├── experiment-block-dedup/jul18/002-message-aware-chunks
    └── experiment-block-dedup/jul18/003-parquet-page-reuse
```

The persistent run branch always points to the best retained implementation found during that run.

Each experimental idea is developed and benchmarked on its own temporary branch. Successful experiments are fast-forwarded into the persistent run branch. Failed experiments are logged and their temporary branches are deleted.

# Setup

To begin a new research run:

1. **Agree on a run tag**

   Propose a tag based on today's date, such as `jul18`.

   Confirm these branches do not already exist:

   ```bash
   git branch --list "autoresearch-block-dedup/<tag>"
   git branch --list "experiment-block-dedup/<tag>/*"
   ```

2. **Start from the existing base branch**

   ```bash
   git checkout block-level-dedup
   git status --short
   ```

   The working tree should be clean except for intentionally untracked benchmark logs or results files.

3. **Update the base branch only when explicitly appropriate**

   Do not automatically merge another branch into `block-level-dedup`.

   If the local repository has a configured remote and the human expects the latest remote state, inspect the branch first. Do not rewrite or pull over uncommitted work.

4. **Create the persistent research branch**

   ```bash
   git checkout -b autoresearch-block-dedup/<tag>
   ```

5. **Read all in-scope files**

   Read the repository for complete context. At minimum, inspect:

   - `README.md`
   - The fixed data preparation code
   - The fixed benchmark and correctness harness
   - The storage implementation you are allowed to modify
   - The existing FastCDC implementation
   - Storage format definitions
   - Index and metadata structures
   - Existing tests
   - Dependency declarations

   The repository may use filenames other than those shown in this prompt. Identify the actual implementation and benchmark files before editing anything.

6. **Identify the modification boundary**

   Determine exactly which implementation files the benchmark permits you to edit.

   Do not modify:

   - Benchmark scoring
   - Correctness checks
   - Prepared datasets
   - Expected outputs
   - Test fixtures
   - Dependency files
   - Package lockfiles
   - Data generation logic

   If the repository already defines a narrower scope, follow it.

7. **Verify benchmark data exists**

   Confirm the prepared benchmark corpus exists in the documented cache or data directory.

   If it is missing, tell the human to run the repository's documented preparation command.

   A likely command is:

   ```bash
   uv run prepare.py
   ```

   Do not invent data paths when the repository documents a different location.

8. **Initialize `results.tsv`**

   Create an untracked `results.tsv` containing only:

   ```text
   commit	score	stored_bytes	storage_ratio	incremental_stored_bytes	incremental_ratio	restore_seconds	restore_mb_s	sequential_read_mb_s	random_read_ms	compression_seconds	compression_mb_s	memory_gb	status	description
   ```

9. **Confirm repository safety**

   Confirm that:

   - `block-level-dedup` has not been modified.
   - `autoresearch-block-dedup/<tag>` was created from it.
   - The benchmark corpus exists.
   - The editable implementation boundary is understood.
   - The benchmark and correctness harness remain unchanged.
   - `results.tsv` is untracked.
   - The current FastCDC implementation is available as the baseline.

Once setup is valid, begin experimentation.

# Baseline

The first benchmark run must use the existing implementation exactly as found on `block-level-dedup`.

This existing FastCDC approach is the reference baseline.

Before changing implementation code, run the full benchmark:

```bash
uv run benchmark.py > run.log 2>&1
```

If the repository uses a different documented command, use that command instead.

Record the baseline in `results.tsv` with:

```text
baseline: existing FastCDC implementation
```

Do not claim an improvement until a candidate beats this baseline under the fixed benchmark and passes every correctness check.

# Scope of experimentation

The benchmark should test the complete lifecycle of a versioned dataset repository:

- Ingesting the first dataset snapshot
- Adding related snapshots
- Adding append-only versions
- Adding versions with scattered row edits
- Adding versions with inserted and deleted records
- Adding reordered versions
- Adding filtered branches
- Adding annotation columns
- Adding equivalent or overlapping data in different file formats
- Restoring complete files
- Reading selected rows
- Reading selected columns
- Reading nested messages
- Validating byte-for-byte reconstruction

## What you can modify

Modify only the implementation files explicitly permitted by the repository.

Everything within that boundary is fair game, including:

- Chunking
- Hashing
- Fingerprints
- Block indexing
- Serialization
- Canonicalization
- Delta encoding
- Compression selection
- Format detection
- Schema analysis
- Column encodings
- Message-aware encodings
- Metadata structures
- Reconstruction recipes
- Caching
- Read paths
- Write paths
- Compaction
- Garbage collection
- Parallelism
- Buffer sizes
- Block sizes
- Heuristics
- Strategy selection

## What you cannot modify

You may not:

- Modify benchmark scoring
- Modify correctness checks
- Modify benchmark datasets
- Modify expected reconstruction outputs
- Install packages
- Add dependencies
- Change workload weights
- Skip workloads
- Detect benchmark filenames and return hard-coded results
- Store fixture-specific hashes or precomputed outputs
- Special-case individual benchmark files
- Use lossy transformations
- Read the original source file during reconstruction except through blocks and metadata written by the implementation

The implementation must generalize to unseen datasets.

# Required properties

## Lossless reconstruction

Every file must be reconstructed byte-for-byte.

This includes preserving, when applicable:

- Whitespace
- Newlines
- JSON key order
- CSV quoting
- CSV delimiters
- CSV line endings
- Null representations
- Floating-point text representations
- Parquet metadata
- Parquet row-group layout
- Parquet encodings
- Original compression metadata
- File-level metadata

Logical canonicalization is allowed internally, but all information needed to reconstruct the original physical file must be retained.

## Stable reusable identities

Identical reusable content should receive stable identities across:

- Dataset versions
- Files
- Directories
- Branches
- Container formats when practical

Identities should not depend unnecessarily on:

- Absolute file offsets
- Row position
- Dataset version number
- Repository path
- Surrounding unrelated content

## Edit robustness

Small insertions, deletions, or edits should invalidate as little unrelated stored data as possible.

The benchmark should intentionally include boundary-shifting edits.

## Practical resource usage

Avoid algorithms with:

- Unbounded indexes
- Quadratic comparisons
- Full-corpus scans for every write
- Excessive temporary storage
- Extremely high read amplification
- Excessive reconstruction latency
- Memory usage proportional to the entire repository when avoidable

# FastCDC as a reference, not a constraint

The current FastCDC implementation provides a useful baseline for:

- Content-defined chunk boundaries
- Resistance to byte-offset shifts
- Block hashing
- Cross-version block reuse
- Generic binary-file handling

Study its strengths and weaknesses.

Do not limit the search to minor FastCDC parameter tuning.

Explore whether FastCDC should be:

- Replaced
- Wrapped in a hierarchical strategy
- Applied only to selected regions
- Applied after structural decomposition
- Used as a fallback
- Combined with semantic or logical chunking
- Tuned independently by format or column type
- Used for large payload fields but not metadata
- Used inside rows, messages, Parquet pages, or column chunks
- Augmented with similarity matching and delta encoding

A generic CDC algorithm may miss redundancy that becomes obvious after parsing the data.

# Research directions

Invent, combine, and experimentally validate techniques. Do not restrict the work to known general-purpose compressors.

## Generic block-level techniques

Explore:

- FastCDC parameter optimization
- Alternative rolling hashes
- Gear hashing variants
- Rabin fingerprinting
- Buzhash
- Zstandard-style rolling match concepts
- Fixed-size chunking
- Content-defined chunking
- Record-aligned chunking
- Hierarchical chunking
- Large chunks with small-chunk fallback
- Anchor-based chunking
- Multi-resolution fingerprints
- Superfeatures
- Similarity-based block grouping
- Delta compression between related blocks
- Base-block selection
- Locality-sensitive hashing
- MinHash-style signatures
- Chunk boundary normalization
- Chunk packing
- Small-object aggregation
- Adaptive minimum, average, and maximum chunk sizes
- Entropy-aware chunking
- Compression-aware chunk boundaries
- Repository-level dictionaries
- Version-local dictionaries
- Generational dictionaries
- Hot and cold block layouts
- Store-versus-reference thresholds
- Inline storage for tiny values
- Metadata compression
- Adaptive codec selection

## Novel chunking strategies

Develop and test chunkers tailored to structured datasets, such as:

- Delimiter-aware CDC
- Newline-anchored CDC
- Record-boundary CDC
- Column-boundary chunking
- JSON-token-boundary chunking
- Message-boundary chunking
- Role-aware chunking
- Tool-call-boundary chunking
- Code-block-aware chunking
- Parquet page and row-group alignment
- Stable semantic anchors
- Schema-derived anchors
- Chunking based on repeated n-grams
- Chunking based on structural hashes
- Two-dimensional row-and-column chunking
- Chunking that isolates high-churn fields from stable fields
- Adaptive chunking based on observed edit history

Measure whether these outperform generic FastCDC on incremental storage, not just on a single snapshot.

## JSONL-specific techniques

Explore:

- Separating row boundaries from row contents
- Structural JSON tokenization
- Canonical logical representation plus reconstruction metadata
- Deduplicating repeated keys
- Deduplicating repeated field paths
- Shared string dictionaries
- Type-aware scalar encoding
- Nested-object structural interning
- Repeated array-shape encoding
- Row templates
- Schema-clustered blocks
- Field-level deltas
- Shared prefix and suffix extraction
- Independent treatment of large text fields
- Preserving original whitespace and key order through compact edit recipes
- Separating stable fields from high-churn fields
- Hashing values independently from serialization
- Reusing logical rows across physically different JSON encodings
- Dictionary coding for roles, tool names, content types, and common metadata
- Structural tries for recurring nested objects
- Columnizing selected JSON paths
- Hybrid parsed and opaque storage

## CSV-specific techniques

Explore:

- Dialect detection
- Delimiter-aware chunking
- Header deduplication
- Column-oriented internal storage
- Per-column dictionaries
- Run-length encoding
- Frame-of-reference and delta encoding
- Numeric bit packing
- Timestamp-specific deltas
- Null bitmap sharing
- Repeated-row templates
- Quoting-pattern encoding
- Row-boundary indexes
- Column type inference
- Separating logical values from exact textual representation
- Compact reconstruction metadata for quoting, escaping, whitespace, and line endings
- Stable row identities
- Primary-key-aware deduplication when inferable
- Column-family grouping based on change frequency
- Transposed storage for wide tables
- Row groups optimized for version deltas

## Parquet-specific techniques

Explore:

- Whole-file block deduplication
- Row-group reuse
- Column-chunk reuse
- Page-level deduplication
- Dictionary-page reuse
- Footer and metadata deltas
- Schema metadata deduplication
- Reusing already compressed pages
- Selective decompression and logical rechunking
- Physical-page deduplication versus logical-value deduplication
- Detecting when recompression destroys useful shared blocks
- Shared logical columns with file-specific reconstruction metadata
- Adaptive strategies based on codec, row-group size, page size, and encoding
- Stable fingerprints over decoded values
- Page similarity matching
- Delta encoding of related pages
- Footer reconstruction recipes
- Repacking versus preserving original layout
- Column-selective storage strategies
- Cross-file dictionary reuse
- Parquet-aware chunk indexes

Do not assume decompressing and re-encoding Parquet is automatically beneficial. Measure CPU cost, lost byte identity, metadata overhead, and reconstruction complexity.

## Agent-trace-specific techniques

Agent traces contain unusually strong semantic and structural redundancy. Exploit it directly.

Common repeated structures include:

- System prompts
- Tool definitions
- JSON schemas
- Tool names
- Tool-call argument keys
- Assistant prefixes
- Conversation templates
- Shared conversation prefixes
- Retry traces
- Similar failed and successful trajectories
- Repeated observations
- Boilerplate tool outputs
- Repository context
- Environment metadata
- Long code blocks
- Log fragments
- Stack traces
- Generated answers that differ only slightly

Potential strategies include:

- Content-addressed individual messages
- Message-array chunking
- Role-aware encoding
- Conversation-prefix tries
- Persistent trace trees
- Parent-trace references
- Shared tool-schema dictionaries
- Shared system-prompt dictionaries
- Tool-call structural encoding
- Separating tool-call structure from argument values
- Deduplicating repeated code blocks inside messages
- Line-level chunking for code and logs
- Syntax-aware chunking for JSON, Python, shell, and common languages
- Prefix and suffix deltas for retries
- Longest-common-prefix references
- Message-array templates
- Structural fingerprints for near-duplicate traces
- Delta encoding between related assistant responses
- Repository-level interning of repeated strings and JSON fragments
- Separating message metadata from message content
- Stable subtree hashes for nested message objects
- Merkle-DAG representations for traces
- Token-aware chunking where it improves reuse
- Normalized tool schemas with exact reconstruction recipes
- Shared observation blocks
- Trace clustering followed by local delta coding
- Churn-aware decomposition of messages

## Cross-format techniques

Equivalent logical data may appear in JSONL, CSV, and Parquet.

Investigate whether it is beneficial to:

- Store a canonical logical representation
- Share logical columns across formats
- Share strings and nested values across formats
- Preserve each file using a compact physical reconstruction recipe
- Deduplicate agent messages independently of container format
- Maintain both physical and logical block layers
- Select physical or logical deduplication adaptively
- Use a hierarchical content-addressed representation
- Fingerprint logical rows independent of serialization
- Store format-specific envelopes around shared logical values
- Reuse dictionaries across JSONL, CSV, and Parquet
- Detect semantically equivalent columns and nested paths

Cross-format deduplication is valuable only when metadata, parsing, reconstruction, and CPU costs are justified by measured gains.

## Delta compression and near-duplicate matching

Exact block hashes miss near-duplicates.

Explore practical ways to find and encode related blocks:

- Prefix and suffix deltas
- Copy-and-insert instruction streams
- XOR deltas
- Binary diff variants
- Dictionary-based deltas
- Base block plus patch
- Similarity signatures
- Superfeatures
- MinHash
- Winnowing
- Locality-sensitive bucketing
- Recent-version candidate windows
- Same-column candidate selection
- Same-schema candidate selection
- Same-message-role candidate selection
- Parent-version hints
- Bounded candidate searches
- Cost models deciding exact storage versus delta storage
- Delta-chain depth limits
- Periodic rebasing
- Multi-base deltas
- Delta compaction

Avoid unbounded global similarity search. Candidate selection must remain practical.

## Adaptive strategy selection

The best algorithm may depend on:

- File format
- File size
- Row count
- Column count
- Schema stability
- Column type
- Average value length
- Cardinality
- Entropy
- Repetition rate
- Edit pattern
- Version distance
- Append-only versus rewrite-heavy behavior
- Presence of nested `messages[]`
- Parquet codec and layout
- Random-access requirements

Develop a strategy selector that measures these properties and chooses among:

- Opaque FastCDC
- Record-aware chunking
- Columnar decomposition
- Message-aware storage
- Exact deduplication
- Delta compression
- Dictionary encoding
- Physical Parquet reuse
- Logical Parquet reuse
- Cross-format canonicalization

The selection policy itself must be benchmarked and kept simple enough to understand.

# Benchmark output

A completed run should print a summary similar to:

```text
---
score:                     0.284192
source_bytes:              10737418240
stored_bytes:              3354365952
storage_ratio:             0.312400
incremental_stored_bytes:  903090176
incremental_ratio:         0.084100
metadata_bytes:            73400320
unique_block_ratio:        0.217300
restore_seconds:           26.1
restore_mb_s:              391.8
sequential_read_mb_s:      622.4
random_read_ms:            4.7
read_amplification:        1.42
compression_seconds:       55.6
compression_mb_s:          184.2
peak_memory_mb:            2860.4
correctness:               pass
benchmark_seconds:         301.8
```

The raw metrics are authoritative.

Use this priority order:

1. Lowest `stored_bytes`, `storage_ratio`, and `incremental_stored_bytes`
2. Fastest `restore_seconds`, highest `restore_mb_s` and `sequential_read_mb_s`, and lowest `random_read_ms`
3. Lowest `compression_seconds` and highest `compression_mb_s`

A canonical `score` may summarize the benchmark, but it must respect this ordering. A speed improvement must not conceal materially worse storage.

Use the repository's actual metric names if they differ, but ensure equivalent raw values are recorded.

A likely extraction command is:

```bash
grep "^score:\|^source_bytes:\|^stored_bytes:\|^storage_ratio:\|^incremental_stored_bytes:\|^incremental_ratio:\|^restore_seconds:\|^restore_mb_s:\|^sequential_read_mb_s:\|^random_read_ms:\|^read_amplification:\|^compression_seconds:\|^compression_mb_s:\|^peak_memory_mb:\|^correctness:" run.log
```

# Logging results

Every completed experiment must be recorded in `results.tsv`.

The columns are:

```text
commit	score	stored_bytes	storage_ratio	incremental_stored_bytes	incremental_ratio	restore_seconds	restore_mb_s	sequential_read_mb_s	random_read_ms	compression_seconds	compression_mb_s	memory_gb	status	description
```

Where:

1. `commit` is the seven-character short Git commit hash.
2. `score` is the benchmark score. Use `0.000000` for crashes.
3. `stored_bytes` is the total physical repository size, including payloads, indexes, manifests, dictionaries, deltas, and reconstruction metadata.
4. `storage_ratio` is total stored bytes divided by total original logical bytes.
5. `incremental_stored_bytes` is the physical storage added by subsequent dataset versions.
6. `incremental_ratio` is incremental physical bytes divided by logical bytes introduced across versions.
7. `restore_seconds` is wall-clock time to reconstruct the benchmark files.
8. `restore_mb_s` is full-file reconstruction throughput.
9. `sequential_read_mb_s` is sequential logical read throughput.
10. `random_read_ms` is benchmark random-access latency.
11. `compression_seconds` is total wall-clock time to parse, deduplicate, compress, index, and persist the corpus.
12. `compression_mb_s` is logical source throughput during compression.
13. `memory_gb` is peak memory in GiB, rounded to one decimal place.
14. `status` is `keep`, `discard`, or `crash`.
15. `description` is a short description of the hypothesis tested.

Example:

```text
commit	score	stored_bytes	storage_ratio	incremental_stored_bytes	incremental_ratio	restore_seconds	restore_mb_s	sequential_read_mb_s	random_read_ms	compression_seconds	compression_mb_s	memory_gb	status	description
a1b2c3d	0.421800	5156109517	0.480200	2055148954	0.191400	23.8	430.2	681.4	3.8	46.3	221.4	2.1	keep	baseline existing FastCDC implementation
b2c3d4e	0.371100	4520452096	0.421000	1317478400	0.122700	25.5	401.7	648.2	4.1	50.4	203.2	2.3	keep	message-aligned CDC with FastCDC fallback
c3d4e5f	0.379500	4285300736	0.399100	1184337920	0.110300	86.4	118.6	177.3	18.9	141.4	72.4	5.8	discard	better size but unacceptable global similarity-search latency
d4e5f6g	0.000000	0	0.000000	0	0.000000	0.0	0.0	0.0	0.0	0.0	0.0	0.0	crash	conversation trie corrupted reconstruction offsets
```

Do not commit `results.tsv`. Leave it untracked.

When reviewing results, sort mentally and report them in the declared priority order:

1. Stored size
2. Reconstruction and read speed
3. Compression time

Do not lead with the blended score when the raw metrics tell a different story.

# Experiment branch workflow

Each hypothesis must be tested on a temporary branch derived from the current best persistent run branch.

Assume:

```text
RUN_BRANCH=autoresearch-block-dedup/<tag>
```

For experiment number `NNN` with a short slug:

```text
EXP_BRANCH=experiment-block-dedup/<tag>/<NNN>-<slug>
```

## Start an experiment

```bash
git checkout "$RUN_BRANCH"
git status --short
git checkout -b "$EXP_BRANCH"
```

## Implement and commit

State the hypothesis before editing:

- What redundancy it targets
- Why the current best implementation misses it
- Which storage metric should improve
- Expected impact on reconstruction and reads
- Expected impact on compression time
- What cost it may introduce
- What outcome would falsify it

Then edit only permitted implementation files and commit:

```bash
git add <permitted implementation files>
git commit -m "experiment: <short description>"
```

## Run the benchmark

```bash
uv run benchmark.py > run.log 2>&1
```

Use the repository's documented command if different.

Redirect all output. Do not use `tee`.

## Read the result

```bash
grep "^score:\|^source_bytes:\|^stored_bytes:\|^storage_ratio:\|^incremental_stored_bytes:\|^incremental_ratio:\|^restore_seconds:\|^restore_mb_s:\|^sequential_read_mb_s:\|^random_read_ms:\|^read_amplification:\|^compression_seconds:\|^compression_mb_s:\|^peak_memory_mb:\|^correctness:" run.log
```

If output is missing or correctness fails:

```bash
tail -n 80 run.log
```

Fix trivial implementation mistakes and rerun.

Do not spend unlimited attempts rescuing a fundamentally broken idea.

## Keep a successful experiment

If the experiment passes correctness and wins under the ordered comparison of stored size, reconstruction/read speed, and compression time:

```bash
git checkout "$RUN_BRANCH"
git merge --ff-only "$EXP_BRANCH"
git branch -d "$EXP_BRANCH"
```

The persistent run branch now becomes the new best state.

## Discard a failed experiment

If the experiment is worse, invalid, excessively complex, or impractical:

```bash
git checkout "$RUN_BRANCH"
git branch -D "$EXP_BRANCH"
```

Do not merge it.

Its result must still remain in the untracked `results.tsv`.

## Crashes

For a crash:

- Record `status=crash`.
- Use zeroes for unavailable metrics.
- Return to the persistent run branch.
- Delete the temporary experiment branch.
- Continue with a different hypothesis.

# Experiment loop

Repeat indefinitely:

1. Inspect the current persistent run branch and best recorded result.
2. Review previous experiments.
3. Select one specific hypothesis.
4. Create a temporary experiment branch from the current best run branch.
5. Implement the idea.
6. Commit the implementation.
7. Run focused correctness tests if available.
8. Run the complete benchmark.
9. Extract metrics.
10. Inspect failures when needed.
11. Append the result to `results.tsv`.
12. Compare against the best retained implementation in this order: stored size, reconstruction/read speed, compression time.
13. Fast-forward the run branch only if it wins under that ordered comparison.
14. Delete the temporary branch whether kept or discarded.
15. Continue with the next hypothesis.

Never modify `block-level-dedup` during the autonomous run.

# Benchmark noise

Storage metrics should be deterministic, while reconstruction and compression timings may vary.

Use separate noise handling for each priority tier:

- Compare `stored_bytes` directly. Investigate any unexpected nondeterminism.
- Treat storage results within 0.5% as tied.
- For tied storage results, run the benchmark at least two additional times and compare median reconstruction and read metrics.
- Treat aggregate read results within 5% as tied.
- For tied storage and read results, compare median compression time across at least three runs.
- Prefer the simpler implementation when all three priority tiers are effectively tied.

A timing fluctuation must never override a clear storage-size difference.

# Decision criteria

## Keep

Keep an experiment when:

- Correctness passes.
- It reduces stored size beyond the storage tie threshold without violating operational guardrails; or
- Storage is tied and it materially improves reconstruction or read performance; or
- Storage and read performance are tied and it materially reduces compression time.
- The improvement generalizes across workloads.
- Resource usage remains practical.
- The result is not benchmark-specific hard-coding.
- Complexity is justified by the gain.

## Discard

Discard an experiment when:

- Correctness fails.
- It materially increases stored size solely to improve speed.
- Storage is tied and reconstruction or read performance becomes worse.
- Storage and reads are tied and compression becomes slower.
- A storage improvement violates the reconstruction or compression guardrails.
- Memory usage grows disproportionately.
- Metadata overhead erases the payload savings.
- It adds substantial complexity for negligible benefit.
- It only works for one narrow fixture.
- It cannot be reproduced.
- It produces long or fragile delta chains.
- It harms important file formats to improve one minor workload.

## Crash

Mark an experiment as a crash when:

- It cannot reconstruct files exactly.
- It corrupts repository state.
- It exceeds memory limits.
- It exceeds the timeout.
- It fails after a few reasonable fixes.
- Its core hypothesis is incompatible with the storage model.

# Simplicity criterion

All else being equal, simpler is better.

Examples:

- A 0.1% score improvement requiring hundreds of fragile lines is probably not worth keeping.
- A 0.1% improvement from a clean chunk-size or strategy-selection change may be worthwhile.
- Equal performance with substantially less code is a successful experiment.
- Removing an index while improving read speed is a strong result.
- A specialized JSONL path is justified if it produces a meaningful gain without destabilizing other formats.
- Format-specific complexity is acceptable when it creates durable, measurable product advantages.

Do not force every format through one generic algorithm merely for conceptual neatness.

# Experimental discipline

Each experiment should test one primary idea whenever possible.

Before implementing it, explicitly state:

- The targeted redundancy
- The reason the current best implementation misses it
- The expected benchmark impact
- The expected costs
- The falsification condition

Prefer evidence over intuition.

Do not keep layering mechanisms onto a failing approach. Delete the experiment branch and return to the current best implementation.

Periodically categorize results:

- FastCDC tuning
- Alternative CDC algorithms
- Generic hierarchical chunking
- JSONL specialization
- CSV specialization
- Parquet specialization
- Agent-message specialization
- Cross-format deduplication
- Exact deduplication
- Near-duplicate matching
- Delta encoding
- Dictionary encoding
- Index design
- Read-path optimization
- Metadata reduction
- Compaction
- Strategy selection

Combine successful ideas only after measuring them independently.

# Suggested research sequence

Begin with high-leverage fundamentals:

1. Establish the existing FastCDC baseline.
2. Profile where stored bytes and metadata are going.
3. Measure redundancy by file type.
4. Measure redundancy across versions.
5. Measure repetition inside `messages[]`.
6. Tune FastCDC parameters to understand the baseline envelope.
7. Compare FastCDC with alternative CDC algorithms.
8. Test record-aligned and hierarchical chunking.
9. Isolate stable fields from high-churn fields.
10. Add message-aware block identities.
11. Add shared dictionaries for system prompts, tool schemas, roles, and keys.
12. Test conversation-prefix DAGs or tries.
13. Test bounded near-duplicate matching and delta encoding.
14. Test CSV columnar decomposition.
15. Compare physical and logical Parquet reuse.
16. Investigate cross-format logical sharing.
17. Build adaptive strategy selection.
18. Optimize indexes and reconstruction.
19. Combine the strongest independent techniques.
20. Simplify the final architecture.

Do not assume sophisticated algorithms will outperform simple structural decomposition.

# Profiling and observability

Add temporary instrumentation when useful, but remove or disable it before retaining a production implementation unless it has ongoing value.

Useful measurements include:

- Block-size distributions
- Exact duplicate rates
- Near-duplicate candidate rates
- Chunk-boundary stability across versions
- Metadata bytes per stored object
- Index bytes per unique block
- Compression ratio by block type
- Reuse rate by file format
- Reuse rate by column
- Reuse rate by message role
- Repeated system prompt bytes
- Repeated tool schema bytes
- Delta-chain depth
- Reconstruction read amplification
- Time spent parsing
- Time spent hashing
- Time spent compressing
- Time spent searching for bases
- Cache hit rates

Use these measurements to generate hypotheses, not to modify benchmark scoring.

# Timeouts and failures

Use the benchmark's documented timeout.

If no timeout is documented and a normal run is expected to finish in roughly five minutes, terminate runs exceeding ten minutes.

Treat a timeout as a crash:

- Log it.
- Return to the persistent run branch.
- Delete the temporary experiment branch.
- Continue with another idea.

For bugs:

- Fix obvious syntax errors, offset mistakes, missing imports, and similar implementation issues.
- Do not spend unlimited iterations rescuing a fundamentally flawed design.
- After a few failed repair attempts, record the crash and move on.

# Never stop

Once the experiment loop begins, do not pause to ask the human whether to continue.

Do not ask:

- “Should I keep going?”
- “Would you like more experiments?”
- “Is this a good stopping point?”
- “Which approach should I try next?”

Continue autonomously until manually interrupted.

When ideas become scarce:

- Re-read the implementation and benchmark.
- Profile the current best implementation.
- Analyze which workloads dominate the score.
- Inspect stored block composition.
- Inspect metadata overhead.
- Inspect repeated structures by format and field.
- Revisit discarded near-misses.
- Combine independently successful techniques.
- Remove complexity.
- Try more radical storage representations.
- Reconsider whether deduplication should occur at file, row, column, message, field, token, line, page, or byte-block level.
- Compare physical and logical representations.
- Search for transformations that keep small edits local.
- Explore adaptive policies based on measured data characteristics.
- Reconsider assumptions inherited from FastCDC.

The loop continues until the human explicitly stops it.

# Final research standard

The objective is not to produce a clever benchmark demo.

The objective is to discover a storage architecture that can make the product the best data-versioning system for structured AI datasets.

A successful final design should explain:

- How blocks and logical objects are identified
- How chunk boundaries are selected
- When FastCDC is used
- Which alternative chunkers are used
- How indexes are organized
- How versions reference shared data
- How JSONL is represented
- How CSV is represented
- How Parquet is represented
- How agent `messages[]` arrays are represented
- How near-duplicates are detected
- How deltas are selected and bounded
- How cross-format redundancy is exploited
- How original bytes are reconstructed
- How random rows and columns are retrieved
- How compaction works
- How garbage collection works
- Which heuristics select among strategies
- Which techniques produced measured improvements
- Which plausible techniques failed
- What tradeoffs remain

Every architectural claim must be supported by benchmark results on real versioned datasets.

The final report must present every leading strategy in this order:

1. Final stored bytes and storage ratio
2. Incremental stored bytes across versions
3. Full reconstruction time and throughput
4. Sequential and random-read performance
5. Compression time and throughput
6. Memory usage and implementation complexity

Do not describe an approach as “best” without showing these tradeoffs against the original FastCDC baseline and the strongest competing experiments.
