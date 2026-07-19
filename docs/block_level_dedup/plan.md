# Block-Level Deduplication — Design

Status: **design finalized, pre-implementation** (2026-07-14; revised the same
day after design review — corrections and hardening folded in throughout).

This document is the reference for the implementation phase.

## 1. Motivation

Oxen is a version control system that specializes in handling large files. We
want compression — specifically of large files — to be a core benefit of using
Oxen. Certain file types, like tabular data (CSV, JSONL, parquet), have nice
properties for compression: when a user labels a dataset, adding rows or
columns, we should smartly compress the file and deduplicate chunks of it over
time instead of storing and transferring a brand-new copy on every commit.

Today, dedup is whole-file only: `oxen add` hashes the file (xxh3-128) and
copies it into the content-addressed version store. A one-row change to a 5 GB
CSV stores and pushes a brand-new 5 GB blob. This is not efficient or practical.

Oxen prides itself on being fast and robust; the implementation quality must
match — no shortcuts on durability, verification, or performance.

The solution must be robust, pragmatic, extensible, and customizable per file type —
future contributors should be able to add different chunking and compression
implementations for files, chunks, and blocks. It must work across storage
backends (local filesystem and S3).

**Per-file-type compression is a core differentiator** versus other version
control tools: as format-aware chunkers, codecs, and transforms are added,
every repository gets better compression for its data types. The architecture
must make adding one a contained, well-tested contribution — not a format
redesign. §6.10 defines the extension model.

### Prior art (Hugging Face / Xet)

- [From files to chunks](https://huggingface.co/blog/from-files-to-chunks):
  content-defined chunking (CDC) places chunk boundaries by *content*, not
  offset, so insertions/deletions only perturb neighboring chunks. ~50%
  storage/transfer savings measured on incrementally updated datasets.
- [From chunks to blocks](https://huggingface.co/blog/from-chunks-to-blocks):
  pure chunk-level CAS does not scale — metadata and request counts that grow
  1:1 with chunk count are ruinous. Their fix: dedup at chunk granularity but
  **store and transfer at block granularity** (xorbs, ≤64 MB), with manifests
  ("shards") holding the file→chunk mapping.
- [Xet chunking docs](https://huggingface.co/docs/xet/chunking): the
  production profile is 8 KiB minimum / 64 KiB target / 128 KiB maximum.
- [Parquet CDC](https://huggingface.co/blog/parquet-cdc): generic CDC over
  already-compressed bytes cannot recover semantic similarity; format-aware
  work must happen at a logical layer and remain byte-reversible.

The core lesson adopted: *deduplication is a performance optimization — never
let chunk count drive your storage or network schema.*

## 2. Glossary

The word "chunk" is overloaded in this codebase. This design fixes the
vocabulary:

| Term            | Meaning                                                                                                                       |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Chunk**       | A content-defined slice of a file (~64 KiB target), identified by xxh3-128 of its raw bytes. The unit of *deduplication*.     |
| **Block**       | An immutable pack of chunk payloads (≤64 MiB), content-addressed by xxh3-128 of its bytes. The unit of *storage and transfer*. |
| **Manifest**    | The file→chunk mapping for one file version: an ordered list of `(chunk_hash, offset, len)`. Deterministic from file content. |
| **Chunk index** | A store-local LMDB cache mapping `chunk_hash → (block_hash, offset, stored_len, raw_len, flags)`. Rebuildable from block footers. |
| **Segment**     | The existing 10 MiB fixed wire slice used by the legacy large-file transfer path (today misleadingly named "chunk" in `store_version_chunk` / the `/chunks` routes). |

## 3. Current state of the codebase (verified 2026-07-14)

- **Merkle tree**: node types `Commit=0, Dir=1, VNode=2, File=3, FileChunk=4`
  (`model/merkle_tree/node_type.rs`; the u8 values are a stable on-disk
  contract). The newer merkle node store is **LMDB-backed** via `heed`
  (`core/db/merkle_node/lmdb_merkle_node_store.rs`). The whole tree is eagerly
  synced on a cold fetch (clone); warm pulls take the incremental
  `sync_from_head` path in `fetch.rs`, which prunes already-shared subtrees.
- `FileNode` already carries dormant chunking scaffolding:
  `chunk_hashes: Vec<u128>`, `chunk_type: FileChunkType`
  (`SingleFile | Chunked`), `storage_backend: FileStorageType`. Note:
  `commit_writer.rs` currently fills `chunk_hashes` with the **whole-file
  hash** (`vec![file_node.hash().to_u128()]`) — it is not empty and not a real
  chunk recipe. These fields feed the serialized node *blob* but **not** tree
  or commit hashes: node identity in `merkle_node_db.rs` is the pre-computed
  content hash, and parent hashes combine child hash fields, not serialized
  bytes (see decision 12). `FileNode` also carries `data_type: EntryDataType`,
  `mime_type`, `extension`, and metadata — file-type detection is already in
  the tree.
- `FileChunkNode` exists (bytes inline!) but nothing constructs it. A dormant
  fixed-16 KiB chunker exists at `core/v_latest/index/file_chunker.rs` (its
  writer is commented out, so shards are never written), plus
  `io/chunk_reader.rs` which reads the dormant fields. `ChunkReader` is
  **not** dead code: it is reachable from `oxen df <path> --revision` via
  `core/df/tabular.rs::show_node`, though functionally broken since no shard
  data exists. See §15.1.
- **Version store** (`storage/version_store.rs`): async `VersionStore` trait,
  keyed by content-hash string, layout
  `.oxen/versions/files/{hash[..2]}/{hash[2..]}/data`. Impls: local FS
  (`storage/local.rs`) and S3 (`storage/s3.rs`). Writes go through
  `AtomicFile`. Reads used broadly: `get_version_stream`,
  `copy_version_to_path`, `get_version_chunk(hash, offset, size)` (range
  reads, e.g. CSV sniffing), `version_exists`, `find_missing_versions`.
- **Tabular reads**: `core/df/tabular.rs::read_version_df` uses a local path
  for local storage and a direct `s3://.../data` Polars cloud scan for S3. A
  block-backed version has no contiguous S3 object, so this path must change.
- **Push**: small files (≤10 MiB) go gzipped-multipart to `POST /versions`;
  large files as `POST /versions/{hash}/create` (currently a no-op), raw
  segments of `stream_segment_size()` (default 10 MiB) to
  `PUT /versions/{hash}/chunks?offset=`, then `POST /versions/{hash}/complete`.
- **Pull**: tree syncs first; client diffs trees, batch-downloads small files
  as a gzip tarball (via `QUERY /versions` — the custom `QUERY` method is
  already in production on client and server) and large files as range
  segments.
- **Upload verification today**: the local backend verifies every uploaded
  blob against its claimed hash before publishing (`AtomicFile::with_hash`,
  both the multipart and combine-segments paths). The S3 backend's
  `store_version` (small-multipart path) does a bare `put_object` with **no
  hash check** — an existing gap the new unified ingest closes (§6.8).
- **Client version gating today** is parsed from the `User-Agent` header in
  `params::app_data()`; requests with a missing or non-Oxen `User-Agent` are
  allowed through. Advisory only — not usable as the block-format gate (§8).
- **Hashing**: xxh3-128 everywhere (`util/hasher.rs`).
- **Prune** (`core/v_latest/prune.rs`) treats the dormant `chunk_hashes` as a
  version catalog; it must become manifest-aware.
- Tests can force streamed-transfer paths by shrinking
  `OXEN_STREAM_SEGMENT_SIZE`; new dedup constants follow the same pattern.

## 4. Design decisions

The load-bearing decisions, each with its rationale and the alternatives it
rejects. Later sections reference these by number.

1. **Content-defined chunking with FastCDC** (v2020, normalized, streaming).
   Rejected: the dormant fixed-size chunker — fixed boundaries shift on any
   mid-file insertion, destroying dedup for exactly the labeling/append
   workload this feature targets.
2. **Immutable content-addressed blocks ≤64 MiB are the unit of storage and
   bulk transfer.** Rejected: one CAS object per ~64 KiB chunk — object
   counts, index sizes, and request volume that grow 1:1 with chunk count do
   not scale.
3. **Per-chunk compression inside blocks, with raw fallback.** Rejected:
   whole-block compression — a small range read would require decompressing
   an entire block.
4. **Manifests live outside the merkle tree**, keyed by the existing
   xxh3-128 file hash. Rejected: chunk hashes in `FileNode` (bloats every
   tree sync and walk) and per-chunk merkle nodes (millions of tree nodes;
   couples physical layout to logical history).
5. **xxh3-128 identifies chunks and blocks**, consistent with all existing
   Oxen hashing; the logical file identity stays xxh3-128 and commit/merkle
   hashes are never changed by physical representation. Dedup scope is
   per-repository, where writers already trust each other — identical to
   today's whole-file xxh3 trust model — and xxh3 is the add-path hot loop
   with 16-byte refs. **Revisit trigger (recorded on purpose)**: if dedup
   ever crosses repo/tenant trust boundaries, chunk hashing must move to a
   keyed cryptographic hash (per-server key, à la Xet). The manifest format
   version byte makes this a format bump, not a redesign.
6. **Manifests are pure content — they never record block placement.**
   Placement lives only in the store-local chunk index. Same file bytes ⇒
   same manifest, so manifests dedup themselves and are identical across
   stores; repack/GC never rewrite manifests (no generation counter needed);
   blocks stay fully store-private. Cost accepted: the chunk index is
   read-critical (still rebuildable from block footers — a lost index means
   rebuild-before-read, never data loss). Rejected: placement-bearing
   manifests, which become per-store physical metadata and must be rewritten
   on every repack.
7. **Push negotiates at chunk granularity, transfers at block granularity,
   and the client packs the blocks** after the server reports missing
   chunks, via stateless idempotent endpoints. Rejected: block-hash-only
   negotiation (equal chunks packed differently produce different blocks),
   server-side packing for client pushes (many small requests), and
   resumable upload sessions in v1 (idempotent block PUTs already give
   coarse resume; sessions are recorded future work).
8. **The chunk index is disposable, rebuildable derived state in LMDB
   (`heed`)** — never authoritative, consistent with the merkle node store
   (`core/db/merkle_node/lmdb_merkle_node_store.rs`). This is a standing
   directive: all key-value storage that needs fast lookup uses LMDB, not
   RocksDB. Applies to the chunk index and any future dedup-related KV
   store.
9. **v1 is the full program, not a write-path-only MVP**: every first-party
   write path chunks eligible files, all reachable history migrates under
   maintenance mode, and GC and fsck ship in v1. Rejected: new-writes-only
   adoption — existing repositories would never see the compression benefit,
   and deleted versions would strand chunks with no reclamation story.
10. **Hard version gate.** A block-enabled repository requires block-capable
    clients and servers and rejects older software with a structured,
    actionable minimum-version error before any mutation — no silent
    whole-file fallback. This is not a fleet-wide flag day: activation is an
    explicit per-repository format migration, unmigrated repos keep working
    with old clients indefinitely, and the additive-endpoint policy still
    applies to the HTTP surface (legacy routes remain registered and serve
    legacy repositories). A block-enabled local repository pushing to a
    server that lacks block support fails early with an actionable error.
11. **The server validates a manifest by full streamed reconstruction +
    xxh3 verification before publishing it.** Without reconstruction, a
    buggy client could publish a manifest whose chunk list does not
    reproduce the claimed file hash, discovered only at checkout. Rejected:
    existence-probes-only (that failure mode), and re-running CDC to police
    boundary choices (protects only dedup *quality*, not correctness, and
    roughly doubles server CPU). **Cost model, recorded honestly**: this is
    O(logical file size) per pushed manifest, not O(uploaded bytes) — a
    1-row edit to a 5 GB CSV uploads kilobytes but costs the server a 5 GB
    read+hash (on S3, ranged GETs) per push, inverting today's economics
    where verification cost tracks transferred bytes. Decision 16 narrows
    what this protects against to client *assembly* bugs; Phase 0
    benchmarks the cost on the labeling workload. **Revisit trigger**: if
    measured cost is unacceptable at scale, add a size-thresholded or
    per-repo async validation mode (manifest quarantined until validated) —
    recorded future work, not v1.
12. **No FileNode field signals chunking; manifest existence decides, per
    store.** Note `chunk_type`/`chunk_hashes` feed the serialized node
    *blob* but **not** tree or commit hashes (verified in
    `merkle_node_db.rs`: node identity is the pre-computed content hash;
    parent hashes combine child hash fields). The real reasons: tree sync
    distributes node blobs by hash, so mutating serialized fields under an
    unchanged hash breaks the hash→bytes immutability sync relies on;
    representation is inherently per-store (a server may chunk what a
    client stored whole); and migration must not rewrite anything in the
    tree at all.
13. **Parameters**: FastCDC min 8 KiB / target 64 KiB / max 128 KiB (the
    documented Xet production profile); eligibility floor 1 MiB (includes
    the small-but-hot labeled-CSV workload); zstd level 3 for text-like
    types with a skip policy for already-compressed media and containers
    (e.g. parquet — see §6.4) and raw fallback everywhere.
14. **`SeekableVersionReader` ships in v1.** Block-backed versions have no
    contiguous S3 object for Polars to scan, and materializing whole files
    to serve a parquet footer read would regress exactly the large-file
    behavior this feature improves.
15. **Extensibility via compile-time per-file-type chunker/codec policy
    registries with stable IDs** — not dynamic plugins, and not a trait for
    every struct: blocks and manifests have one versioned format; traits
    exist only where real variants exist. Three extension axes, each with
    its own recorded ID (§6.10): chunker (manifest), codec (block footer,
    store-local), and a **reserved whole-file transform slot** in the
    manifest — the known path to semantic compression of already-compressed
    formats — so each future technique is a versioned addition, never a
    redesign.
16. **Every chunk is hash-verified at block ingest.** `store_block` (server
    and client alike) decompresses each chunk and verifies it against its
    footer `chunk_hash` before indexing — O(uploaded bytes), cheap relative
    to the transfer. Without this, a buggy or hostile writer could upload a
    block whose footer maps chunk hash H to bytes that don't hash to H; the
    lie gets indexed (co-packed chunks are indexed with no manifest
    referencing them), negotiation then tells an honest client the store
    already has H, and the honest push fails decision-11 validation — and
    keeps failing until fsck. Ingest verification turns that permanent
    wedge into an immediate rejection of the bad block.

## 5. Design overview

```
oxen add (file ≥ 1 MiB, block-v1 repo)
  │
  ├─ Chunker (per EntryDataType; default FastCDC 8/64/128 KiB) ──> chunks
  ├─ Compressor policy (per EntryDataType + extension; default zstd-3, raw fallback)
  ├─ new chunks appended to current block; block sealed ≤64 MiB,
  │    content-addressed, footer written, indexed in LMDB
  ├─ Manifest (ordered chunk list) stored keyed by the file hash
  └─ FileNode unchanged; manifest existence marks the version as chunked

reads (checkout, streams, range reads, Read+Seek) — transparent reconstruction
push/pull — chunk-hash negotiation, transfer blocks on the wire
```

Design invariants:

1. **The merkle tree does not change shape or content.** Chunking is a
   storage concern. The tree answers "what changed"; the version store
   answers "how do I get the bytes". No FileNode field signals chunking.
2. **Manifests are pure content.** Same file bytes ⇒ same chunk boundaries ⇒
   same manifest. Manifests never contain block placement.
3. **Blocks are store-private.** Each store (client, server, local, S3) packs
   chunks into blocks independently. Block IDs never cross the wire as
   durable references, so any store may repack/GC its blocks without
   coordination.
4. **Reads are transparent.** Every existing read API works identically on
   chunked versions. No caller outside the store and the transfer paths knows
   chunking exists.
5. **A published manifest reconstructs exactly its file**: `logical_size`
   bytes hashing to its xxh3-128 file hash, verified before publication on
   both client and server. Client-side at add time this is
   **hash-while-chunking** during the single ingest pass — never a separate
   reconstruction read. Server-side it is streamed reconstruction
   (decision 11). On pull, the server already validated the manifest and the
   checkout-time `AtomicFile` hash check is the client's verification.
6. **Publish-last ordering everywhere**: blocks durable → chunk index updated
   → manifest validated and published → merkle/commit metadata → ref update.
   Crash at any point leaves either the old complete representation or the
   new one, plus at worst reclaimable orphans.

## 6. Storage design

All new code lives in one module: `crates/liboxen/src/storage/chunked/`
(chunker, compressor, ID registry, policy, manifest, block format, chunk
index, reconstruction, seekable reader, migration, GC). This is the single
place future contributors extend — the extension contract is §6.10.

### 6.1 Chunker

```rust
pub trait Chunker: Send + Sync {
    fn id(&self) -> ChunkerId;                 // recorded in the manifest
    // yields (offset, len) boundaries + payloads over a streaming reader
}
```

- Default (and only v1) implementation: **FastCDC** via the `fastcdc` crate
  (v2020 variant, streaming), **min 8 KiB / target 64 KiB / max 128 KiB** —
  the documented Xet production profile. The implementation must be
  streaming with bounded memory (at most a window plus the current maximum
  chunk).
- Selected per `(EntryDataType, extension)` by the single policy function
  (§6.10), so future format-aware chunkers (parquet row groups, JSONL line
  boundaries) are additive. A format-specific policy must still reproduce
  the original bytes exactly — no parse-and-reserialize of committed bytes.
- **`ChunkerId` pins the exact boundary function, not the crate.** FastCDC
  boundaries depend on the gear table and normalization constants; a
  `fastcdc` crate upgrade that shifts boundaries would silently degrade
  dedup fleet-wide while all our own tests still pass. `generic-fastcdc-v1`
  freezes the boundary function (golden boundary fixtures pin it); any
  boundary-affecting change is a **new** chunker ID, never an in-place
  upgrade.

### 6.2 Chunking policy (which files)

- **Size-only in v1: chunk every file ≥ 1 MiB, any data type.** Smaller files
  keep today's whole-file path (chunking them is pure overhead). The 1 MiB
  floor deliberately includes the small-but-hot labeled-CSV workload this
  feature targets.
- The repo config value `[storage.block] min_file_size` (§8) is
  authoritative, default 1 MiB. `OXEN_DEDUP_MIN_FILE_SIZE` is a test-infra
  env override (same pattern as `OXEN_STREAM_SEGMENT_SIZE`), not a
  user-facing knob.
- Policy isolated in one `should_chunk(data_type, size)` function.

### 6.3 Chunk identity

**xxh3-128** over the raw (uncompressed) chunk bytes, consistent with all
existing Oxen hashing. See decision 5 and its recorded revisit trigger.

### 6.4 Compression

```rust
pub trait Compressor: Send + Sync {
    fn id(&self) -> CodecId;                   // recorded in per-chunk flags
    fn compress(&self, raw: &[u8]) -> Vec<u8>;
    fn decompress(&self, stored: &[u8], raw_len: usize) -> Result<Vec<u8>, OxenError>;
}
```

- **Granularity: per-chunk, inside the block** — every chunk stays
  independently fetchable and decodable, so range reads survive.
- Codec policy consults `EntryDataType` **and** the extension/MIME already
  carried on `FileNode`:
  - Text-like content (`Text`; `Tabular` in text encodings — CSV, TSV,
    JSONL): try **zstd level 3**.
  - Already-compressed containers (parquet, compressed arrow) and `Binary`,
    `Image`, `Video`, `Audio`: skip the attempt. Parquet is `Tabular` by
    data type, so a data-type-only policy would pay a wasted zstd pass on
    every parquet chunk that raw fallback then discards. Phase 0's corpus
    confirms the skip list with data.
- **Raw fallback always applies**: if the compressed chunk is not smaller,
  store raw. The per-chunk `flags` byte records the codec actually used
  (`0 = raw`, `1 = zstd`); future codecs are additive. Once a codec id has
  been written, its decoder must remain available until a migration rewrites
  every block that uses it.
- Codec choice lives in the block footer, **not** the manifest — it is
  store-local, so a store can adopt a better codec for new blocks (or repack
  old ones) without any manifest or wire change (§6.10).
- The chunked write path takes the file's `EntryDataType` and extension as
  parameters.

### 6.5 Manifest

Stored in the version store keyed by the **file's content hash**, as a
sibling of where the whole-file blob would live (a chunked version has a
manifest instead of `data`):

```
.oxen/versions/files/{hash[..2]}/{hash[2..]}/manifest
```

Format: msgpack, versioned:

```rust
pub struct ChunkManifest {
    pub version: u8,           // format version (see decision 5 revisit trigger)
    pub file_hash: MerkleHash,
    pub file_size: u64,
    pub chunker_id: ChunkerId,
    pub transform_id: TransformId, // reserved; 0 = identity in v1 (§6.10)
    pub chunks: Vec<ChunkEntry>,   // ordered by offset
}
pub struct ChunkEntry {
    pub hash: u128,    // xxh3-128 of the RAW (uncompressed) chunk bytes
    pub offset: u64,   // offset in the reconstructed file
    pub len: u32,      // raw length
}
```

- ~28 B/chunk ⇒ ~2 MB manifest for a 5 GB file; gzipped on the wire. The
  ordered `len` values must sum exactly to `file_size`.
- Deterministic from content ⇒ manifests dedup themselves and are identical
  across stores.
- `transform_id` reserves the pre-chunking transform slot (§6.10). v1 always
  writes `0` (identity) and rejects any other value on read with a
  structured upgrade-required error.
- **No block placement** — placement is store-local (invariant 3).
- `(offset, len)` per chunk makes `get_version_chunk(hash, offset, size)` a
  binary search + partial chunk reads.
- **Parsed manifests are cached** in a keyed, size-bounded LRU (precedent:
  `merkle_tree_node_cache` — opt-in, enabled on long-running servers). A
  5 GB file's manifest is ~2 MB / ~80k entries; HTTP `Range` requests, CSV
  sniffs, and every `SeekableVersionReader` open would otherwise re-load and
  re-parse it per operation. Manifests are immutable once published, so the
  cache needs no invalidation beyond eviction.
- If terabyte-scale files make whole-manifest loads a measured problem, a
  paged manifest layout (fixed-width chunk-entry pages behind a page
  directory, so a seek reads one page instead of the whole manifest) is a
  recorded future format bump.

### 6.6 Block format

```
.oxen/versions/blocks/{hash[..2]}/{hash[2..]}/data
```

- **Content-addressed**: block ID = xxh3-128 of the block file bytes,
  verifiable after transfer; storage is idempotent.
- **Layout**: concatenated chunk payloads (compressed or raw per chunk),
  followed by a footer, written in one streaming pass:

```
[payload payload payload ...]
[footer: n × (chunk_hash u128, offset u32, stored_len u32, raw_len u32, flags u8)]
[footer_len u32][version u8][magic b"OXBK"]
```

- The footer (~0.04% of a full block) makes blocks **self-describing**: the
  chunk index is rebuildable by scanning footers, and fsck can validate
  blocks standalone.
- The block parser treats input as untrusted: checked arithmetic on all
  offsets/lengths/counts, non-overlapping payload ranges within bounds,
  `raw_len` capped at the policy maximum (128 KiB), decoder output buffer
  fixed to the declared `raw_len`, entry count bounded by object size. This
  hardens against corrupt or malicious block uploads (decompression bombs,
  overflow, OOB).
- **Ingest verifies every chunk against its footer `chunk_hash`**
  (decompress + xxh3) before any index entry is written — decision 16. The
  block hash covers the bytes; only this step makes the footer's *claims*
  trustworthy.
- **Sealing policy**: chunks append in file order to the current open block;
  seal at **64 MiB** (complete encoded size) or when the operation's batch
  ends, then hash → name → store atomically (`AtomicFile` with hash). Never
  append to a published block; there is no mutable tail block. Small commits
  produce small blocks; repack compacts later (legal because blocks are
  store-private).
- Directory-sized operations share one bounded pack session so chunks from
  many files fill blocks efficiently; consecutive chunks of a file land
  consecutively, giving reconstruction locality for free.
- Concurrent writers may race and produce two valid blocks containing the
  same chunk. That is safe: the first indexed location is canonical, and GC
  later removes unreferenced duplicates.

### 6.7 Chunk index (LMDB)

LMDB (`heed`) environment at `.oxen/versions/chunk_index/`:

```
chunk_hash: u128  →  (block_hash: u128, offset: u32, stored_len: u32, raw_len: u32, flags: u8)
```

- Store-local, derived data: rebuildable by scanning block footers (two-pass:
  live manifests first for canonical locations, then remaining block footers).
- Answers dedup probes at add time, `missing_chunks` during negotiation, and
  placement lookups during reconstruction and transfer packing.
- One coherent write batch (LMDB write txn) per validated block, inside a
  single `spawn_blocking` closure per `docs/async_policy.md`; no txn or guard
  crosses `.await`.
- Ordering: a location is inserted only after its block is durable. Crash
  between block publish and index write leaves a safe orphan block that
  rebuild discovers.
- On the S3 backend, blocks and manifests live in S3; the chunk index lives
  with the server's repository metadata on local disk (single authoritative
  server per repository). **Accepted constraint, recorded**: this deepens
  single-server statefulness — a large repo's index is multi-GB local state
  whose loss means an S3 footer scan before dedup probes work again, and it
  makes server failover/horizontal scaling strictly harder. Consistent with
  today's architecture (RocksDB repo metadata is already local); revisit
  alongside the CDN/offload future work (§16).
- The LMDB layer never resizes at runtime — fixed generous map reservation;
  `MapFull` means raising a compile-time constant. Size the chunk-index map
  from the math (~53 B/entry ⇒ ~8.5 GB for a 10 TB repo at 64 KiB average
  chunks, before LMDB overhead), not by copying the merkle store's 256 GiB
  reflexively.

### 6.8 The `VersionStore` seam

**Capability sub-trait; transparent reads; explicit writes.**

- `VersionStore` gains one accessor:

```rust
fn chunked(&self) -> Option<&dyn ChunkedVersionStore>;
```

  A store that returns `None` never stores chunked versions. Both
  `LocalVersionStore` and `S3VersionStore` implement it, sharing the
  chunking/packing/index/reconstruction logic and differing only in raw byte
  IO (local file ranges vs S3 ranged GETs / multipart PUTs).

- `ChunkedVersionStore` (names finalized during implementation):

```rust
#[async_trait]
pub trait ChunkedVersionStore: Send + Sync {
    /// Chunk, compress, pack and index a new version; writes the manifest.
    async fn store_version_chunked(
        &self, hash: &str, data_type: &EntryDataType, extension: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<ChunkManifest, OxenError>;

    async fn get_manifest(&self, hash: &str) -> Result<Option<ChunkManifest>, OxenError>;
    /// Validates (reconstruct + xxh3) before publishing. See §7.2.
    async fn put_manifest(&self, manifest: &ChunkManifest) -> Result<(), OxenError>;

    /// Subset of `hashes` this store does not have (negotiation).
    async fn missing_chunks(&self, hashes: &[u128]) -> Result<Vec<u128>, OxenError>;

    /// Pack the requested chunks into transfer blocks, streamed (§7).
    async fn pack_chunks(&self, hashes: &[u128]) -> Result<BoxedByteStream, OxenError>;

    /// Verify block hash, parse footer, verify every chunk against its
    /// footer hash (decision 16), then store as a local block and index.
    async fn store_block(
        &self, hash: &str, reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), OxenError>;

    /// Sync Read+Seek adapter over manifest + block ranges (§6.9).
    async fn open_seekable(&self, hash: &str) -> Result<SeekableVersionReader, OxenError>;
}
```

- **Reads stay transparent**: `get_version_stream`, `copy_version_to_path`,
  `get_version_size`, `get_version_chunk`, `version_exists`,
  `find_missing_versions` work identically on chunked versions by internally
  loading the manifest and streaming block ranges through the chunk index
  (decompressing per chunk). Checkout, restore, df, workspaces: no caller
  changes.
- **Writes choose explicitly**: every first-party write path (add, branch and
  remote checkout, workspace file/commit, server multipart uploads,
  df-edit-generated versions) routes through one policy-aware ingest that
  calls `should_chunk(...)` and picks whole-file or chunked. Server-originated
  writes use the same ingest locally — no write path may bypass block mode on
  a block-enabled repo. The ingest also closes the existing S3
  `store_version` verification gap (§3): every write path verifies bytes
  against the claimed hash before publishing, on every backend.
- Rejected alternatives: a fat `VersionStore` trait (every impl stubs ~8
  methods) and a standalone coordinator (every read site must route through
  it).

### 6.9 Seekable reads and tabular integration

Block-backed versions have no contiguous file or S3 object, so consumers that
need `Read + Seek` (eager Parquet/IPC readers) get a `SeekableVersionReader`:

- cursor + bounded read-ahead over manifest `(offset, len)` entries, resolving
  placement through the chunk index;
- local blocks use positional file reads; S3 blocks use an async range worker
  connected to the sync reader via bounded request/response channels; the
  consumer runs inside one `spawn_blocking` (sync-core / async-edge policy);
- `seek` moves the cursor without any IO; the next `read` resolves only its
  range; nearby block ranges are coalesced under bounded gap limits.

`core/df/tabular.rs::read_version_df` changes:

- Parquet and IPC use `SeekableVersionReader` (footer/column pruning
  preserved, no materialization);
- CSV/TSV/JSONL use the logical sequential stream, with a bounded head range
  for sniffing;
- path-only consumers keep an explicit materialize fallback — declared, never
  implicit.

HTTP file endpoints stay logical: full GET streams reconstructed bytes,
`Range` requests serve correct 206s via `get_version_chunk`, `Content-Length`
is the logical size, `ETag` is the logical xxh3 hash.

### 6.10 Extensibility model — adding chunkers, codecs, and transforms

Per-file-type compression is the long-term differentiator (§1); this section
is the contract that keeps every future technique an additive, contained
contribution. The encode pipeline has exactly three extension axes:

```
file bytes ─ Transform (reversible; v1: identity) ─ Chunker (boundaries)
           ─ per-chunk Codec ─ block
```

| Axis          | ID recorded in                  | Scope of change when a new one ships                                                                 |
| ------------- | ------------------------------- | ---------------------------------------------------------------------------------------------------- |
| **Chunker**   | manifest `chunker_id`           | Content-visible: same bytes + same chunker ⇒ same manifest. New boundaries = new named policy (§8).  |
| **Codec**     | block footer `flags` byte       | Store-local: never crosses the wire as a durable reference. No manifest or wire change; a store may adopt it for new blocks or via repack. |
| **Transform** | manifest `transform_id`         | Reserved; v1 = identity only. A reversible whole-file transform applied before chunking — e.g. reframing already-compressed containers (parquet pages, gzip members) so CDC sees stable bytes: the parquet-CDC lesson (§1 prior art). A real transform ships as a manifest version bump adding whatever fields it needs (e.g. transformed size); the pipeline slot and ID space are fixed now so it is a format rev, not a redesign. |

Rules, uniform across all three axes:

- **IDs are `u8`, append-only, never reused**, registered in one place:
  `storage/chunked/registry.rs`. An unknown ID on read fails with a
  structured upgrade-required error — never a panic, never a silent
  fallback.
- **One policy function** in `storage/chunked/policy.rs`:
  `(EntryDataType, extension) → (chunker, codec, transform)`. Repo config
  selects a stable *named* policy (§8); a new mapping ships as a new named
  policy ID, so behavior is versioned, not ambient.
- **Invariants no extension may break**: logical file identity stays the
  xxh3-128 of the *original* bytes; reconstruction is byte-exact
  (transforms must be reversible; no parse-and-reserialize); manifests
  never carry placement; decoders for shipped IDs remain available until a
  migration rewrites every object that uses them (§6.4).
- **Contributor checklist**: implement the trait; register the ID; extend
  the policy (or add a named policy); add golden fixtures (boundaries /
  encoded bytes with fixed hashes); pass the shared conformance suite
  (round-trip, range reads, fuzz — §14, which every registered
  implementation runs automatically); add the docs.oxen.ai entry.

## 7. Wire protocol

**Symmetric transfer blocks, stateless endpoints.** Negotiation is
chunk-hash-based; the sender repacks exactly the missing chunks into fresh
transfer blocks (same on-disk format, formed per transfer) and streams them;
the receiver verifies each block hash and every chunk against the footer
(decision 16), and stores it directly as a local block. One `BlockWriter`/`BlockReader` pair serves storage
and transfer in both directions; compressed chunks travel compressed for
free; no chunk is ever re-sent to a peer that has it; block IDs never become
durable wire references.

(Xet's asymmetric shape — wire-visible block IDs + CDN range-GET
reconstruction info — is rejected: oxen-server serves bytes directly, and
durable wire block IDs would freeze block layout. A reconstruction-info read
path is purely additive if a CDN/S3-offload phase arrives. Resumable upload
sessions are likewise recorded future work; idempotent block PUTs already
give coarse resume.)

### 7.1 New endpoints

Under `/api/repos/{namespace}/{repo_name}`:

| Endpoint                                          | Purpose                                                                                                            |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `POST /chunks/missing`                            | Body: gzipped list of chunk hashes. Response: the subset the server is missing. (Push negotiation.)                |
| `PUT /blocks/{block_hash}`                        | Streamed transfer-block upload. Server verifies the block hash, parses the footer, verifies every chunk against its footer hash (decision 16), stores + indexes. Idempotent. |
| `QUERY /chunks`                                   | Body: gzipped list of chunk hashes. Response: a stream of transfer blocks containing exactly those chunks. (Pull.) |
| `PUT /manifests/{file_hash}` / `QUERY /manifests` | Batch manifest upload/download (gzipped msgpack). Upload validates before publish (§7.2).                          |

Legacy endpoints remain registered and serve legacy-format repositories.
Requests from clients below the block-v1 floor against a block-enabled
repository are rejected with a structured minimum-version error before any
mutation (see §8).

All endpoints are repository-authenticated; there is no cross-repository
chunk-presence query (presence responses reveal content existence, so they
must never escape the repo's authorization boundary).

### 7.2 Push flow (chunked files)

1. Capability/version check (§8), then tree walk + commit negotiation as
   today.
2. For versions with a local manifest: collect chunk hashes; batch →
   `POST /chunks/missing` (bounded batches, ~16k hashes per request).
3. `pack_chunks(missing)` → stream transfer blocks → `PUT /blocks/{hash}`
   (parallel, existing retry/backoff machinery).
4. Upload manifests for the pushed file versions. **On `PUT /manifests` the
   server verifies before publishing**: every referenced chunk exists (index
   probes), then a streamed reconstruction of the file from its chunk list
   verifies `file_size` and the xxh3-128 file hash. Publication is atomic;
   failure returns a structured error and publishes nothing. (Deliberate
   cost: one server-side read per pushed file — without it a bad manifest is
   only discovered at checkout. Re-running CDC to police boundary choices is
   skipped: it protects dedup quality, not correctness. Full cost model and
   revisit trigger: decision 11.)
5. Tree nodes + mark-synced, as today.

Ordering guarantee: **blocks → manifests → tree nodes → synced → ref**, so a
synced commit never references chunks the server lacks. Push progress reports
logical bytes, dedup hits, and encoded bytes uploaded separately (a 99%-deduped
push looks stalled if progress tracks logical bytes only).

If a validated manifest already exists for a file hash, an incoming manifest
for that hash is a **no-op success** — the server already has that version's
bytes, and a published manifest is never overwritten. Two *valid* recipes for
the same bytes legitimately differ whenever chunker policies differ (a
`chunker_id` bump, a future format-aware chunker), so a differing recipe must
not be treated as an xxh3 collision — blanket rejection would wedge every
push from a client with a newer chunker policy. The collision-safety goal
(never letting one version's bytes be replaced because xxh3 collided) is
preserved by never overwriting.

### 7.3 Pull flow (chunked files)

1. Tree sync + missing-entry computation as today.
2. For missing versions that the server reports as chunked:
   `QUERY /manifests` → **staged** locally, not published. Manifest presence
   equals version presence on a store (decision 12), so publishing before
   the chunks are durable would make `version_exists` lie and a resumed
   pull skip re-downloading — publish-last (invariant 6) applies on pull
   exactly as on push.
3. Diff staged-manifest chunk hashes against the local chunk index →
   missing set.
4. `QUERY /chunks` with the missing set → stream of transfer blocks →
   `store_block` each (verify block hash + footer bounds + every chunk
   against its footer hash before install; all contained chunks are
   indexed, including co-packed ones — free future dedup).
5. Once all of a manifest's chunks are locally durable, publish the
   manifest. Checkout then reconstructs transparently via the normal read
   path, verifying the file hash as it writes (`AtomicFile` with hash) —
   that write-time check is the client's end-to-end validation; no separate
   reconstruction pass.

Block-granularity idempotency makes interrupted pulls resumable: verified
blocks persist, and the local index removes their chunks from the missing set
on retry.

## 8. Repository format, activation, and version gating

Block storage is a **repository format feature**, not a client preference.

```toml
[storage]
content_format = "block-v1"    # "legacy" before migration

[storage.block]
file_policy   = "generic-fastcdc-v1"   # stable named policy IDs only
min_file_size = 1048576                # 1 MiB
```

- Only stable named policies are accepted in config — arbitrary numeric
  chunker/codec parameters would create unversioned formats.
- Activating `block-v1` raises the repository's minimum compatible client
  version to the release that ships block support. Per the repo convention,
  the gating check inlines the version literal at the comparison site with a
  comment pointing at `docs/deprecations.md`.
- **Hard gate**: an old client or server interacting with a block-enabled
  repository fails early with a structured, actionable error — no silent
  whole-file fallback, no partial mutation. Upgraded software continues to
  read and serve legacy repositories and mixed representations during
  migration. **Enforcement is at the operation level**: legacy mutation
  endpoints on a block-v1 repository reject unconditionally. The existing
  `User-Agent` floor in `params::app_data()` admits requests with a missing
  or unparseable `User-Agent` (§3) — fine for an advisory nudge, not for a
  gate whose failure mode is silent corruption; it must not be the
  enforcement point.
- New repositories may default to `block-v1` only once the release gates
  (§13) pass **and** an ecosystem criterion is met: flipping the default
  makes every new repo unpushable to lagging servers, with no downgrade
  short of a full reverse migration. Gate the flip on server-fleet
  penetration, and/or probe server capability at first push with a clean
  convert-to-legacy escape hatch. Existing repositories activate only
  through explicit migration (§9).
- Small files (< 1 MiB) remain whole-file CAS objects in a block-v1 repo by
  policy; both representations coexist per version.
- Python bindings: no API change expected (reads are transparent); verify
  with `bin/test-rust -p` and add python tests exercising add/push/pull on a
  block-v1 repo.
- docs.oxen.ai needs a docs-repo page covering the config, migration command,
  and old-client rejection behavior when this lands.

## 9. Migration of historical versions

Existing repositories convert via an explicit, resumable, idempotent
maintenance operation. Because manifests are pure content and placement lives
in the index, migration never touches commits, FileNodes, or merkle hashes.

### 9.1 State machine and maintenance lease

Durable state record (atomically updated, outside normal config):

```
Legacy -> Preparing -> Migrating { cursor, counters } -> Verifying -> BlockV1
```

- A repository-wide maintenance lease (in-process lock + advisory lock file +
  durable state), **scoped per operation** — hours of blocked reads on a
  large repo is downtime, and most maintenance doesn't need it:
  - **Migration and index rebuild: writes blocked, reads allowed.** Blocks
    and manifests are immutable once published, and a legacy blob is
    deleted only after its verified manifest is durable, so a read racing a
    blob deletion retries once via the manifest. Index rebuild constructs a
    fresh index beside the live one and swaps atomically; only the swap is
    exclusive.
  - **GC sweep and reverse migration: fully exclusive** (reads + writes).
    GC's mark phase needs only the write block.
  Blocked operations return a structured maintenance response with
  progress. Single authoritative server per repository — no distributed
  lease in v1. Migration docs state expected duration per TB so deployments
  can plan the write-freeze window.
- Interruption at any point resumes idempotently with `--resume`; the state
  record rejects a resume with a different target policy.

### 9.2 Inventory

Snapshot all logical content roots while writes are blocked: every ref, all
retained commits, live workspaces, staged content, pinned sessions.
Deduplicate by file hash. Only **reachable** versions ≥ 1 MiB migrate;
unreachable legacy blobs are reported and left for prune/GC.

### 9.3 Per-version algorithm (bounded batches)

1. Skip and checkpoint any version that already has a validated manifest.
2. Verify each legacy blob matches its logical size/hash.
3. Stream the batch through FastCDC + xxh3, consulting the chunk index; feed
   only missing chunks into the batch's bounded cross-file packer.
4. Publish full blocks as they fill; flush the underfilled tail at batch end;
   index each block only after durable publication.
5. Build each file's manifest, reconstruct through the block reader, verify
   size + xxh3, publish atomically.
6. Only then delete that file's legacy blob and advance the checkpoint.

The old blob is never deleted before a verified manifest is durable. A dry
run reports logical bytes, estimated block bytes, reusable chunks, and the
storage headroom required.

### 9.4 Cutover and reverse migration

After the cursor finishes: re-verify the inventory digest, confirm every
reachable eligible hash has a valid manifest and every small hash a valid
blob, run manifest/block reachable-set checks, rebuild-and-compare the chunk
index, then atomically set `content_format = "block-v1"` and release the
lease. Because writes were blocked, no commit falls between inventory and
cutover.

Reverse migration (block-v1 → legacy) reconstructs each full blob, verifies
xxh3, publishes atomically, then removes manifests/blocks via GC — also under
the lease. The CLI states before forward migration that rollback is a full
reverse migration, not a flag flip.

Clones migrate independently: an upgraded clone of a migrated remote can read
its local legacy blobs, lazily build manifests to push, and run its own local
migration. Block hashes and packing are never required to match across
clones (invariant 3).

## 10. Garbage collection, prune, and repack

- **Mark-and-sweep under the maintenance lease; refcounts are never
  authoritative** (a durable refcount would make the mutable index part of
  correctness).
- Mark: logical hashes reachable from retained refs/commits + workspaces +
  staged/pinned content → their manifests (or legacy blobs) → the **live
  chunk set** → live blocks. Because pure-content manifests don't name
  blocks, the chunk→block mapping for marking comes from scanning block
  footers (authoritative), not the possibly-stale index. Footers are
  immutable, so footer digests are cached locally keyed by block hash
  (never invalidated, only evicted with the block) — on S3 this turns
  marking from ~2 ranged GETs per block *per GC run* into a per-new-block
  cost. `FileNode.chunk_hashes` is ignored; `prune.rs` stops treating it as
  a catalog.
- Sweep: unreferenced manifests, legacy blobs, blocks containing zero live
  chunks, expired temp files, stale index entries. Unreferenced published
  objects get a **24-hour grace period** by default (covers aborted pushes);
  shortening below the default requires a force flag. Rebuild/repair the
  chunk index after sweep.
- v1 keeps a block if **any** chunk in it is live; per-block live-byte
  utilization is reported so the cost is visible. Future repack packs live
  chunks of low-utilization blocks into new blocks, updates the index, and
  deletes the old blocks — manifests are untouched (pure-content payoff).
  **Steady-state erosion is a known force, not a surprise**: receivers
  store transfer blocks as-is and blocks seal at batch end, so every small
  push creates at least one small block and utilization only decays between
  repacks. §14's steady-state release gate measures the erosion rate; if it
  fails, minimal utilization-floor repack moves from §16 into v1.
- `delete_version` removes only the manifest or blob; shared blocks are
  reclaimed by GC.

## 11. Fsck, corruption, save/load

- **Quick fsck**: parse manifests, verify referenced chunks resolve to
  present blocks (via footer scan or verified index), check block
  magic/footer consistency, spot-check blob hashes.
- **Full fsck**: hash every block, decode and hash every chunk, reconstruct
  every manifest-backed file to xxh3, rebuild-and-compare the chunk index.
  Reports affected logical files and paths, not just block hashes.
- Safe auto-repairs: rebuild the index, drop stale locations, discard expired
  temp objects, re-fetch a missing/corrupt block from an authenticated
  remote. A corrupt live block with no valid source is reported as data loss
  for the affected files — never hidden by deleting the manifest.
- `clean_corrupted_versions` and `push --revalidate` become
  representation-aware (distinguish corrupt manifest / missing block /
  corrupt chunk / reconstruction mismatch; a client can re-upload the missing
  pieces).
- **Save/load** (`repositories/save.rs`): blocks and manifests are
  authoritative and included in archives; the LMDB chunk index is derived —
  excluded from the archive and rebuilt on load. A repository in active
  migration cannot be saved without an explicit consistent snapshot.

## 12. Observability and CLI

Metrics separate dedup from compression (never conflate them):

```
logical_bytes, unique_raw_chunk_bytes, encoded_block_bytes, manifest_bytes
dedup_ratio       = logical_bytes / unique_raw_chunk_bytes
compression_ratio = unique_raw_chunk_bytes / encoded_block_bytes
push lookup hits/misses, encoded bytes uploaded
fetch logical bytes requested vs block bytes downloaded
range amplification, index/cache hit rates, orphan + low-utilization block bytes
```

CLI surface (names to follow CLI conventions):

```
oxen storage status | stats
oxen storage migrate --to block-v1 [--dry-run] [--resume]
oxen storage migrate --to legacy   [--dry-run]
oxen storage fsck [--full] [--repair-index]
oxen storage gc [--dry-run] [--grace 24h]
oxen storage rebuild-index
```

Long operations are resumable and observable; they do not rely on one HTTP
connection staying open.

## 13. Implementation phases

Each phase lands independently testable; block-v1 activation stays gated
until the full vertical path exists.

- **Phase 0 — measurements and golden fixtures**: benchmark corpora
  (append/prepend/middle CSV+JSONL edits, parquet rewrites, media, random
  bytes); confirm the 8/64/128 profile and zstd-3 policy (and the parquet
  skip list) with data; benchmark the decision-11 server validation cost on
  the labeling workload (repeated small edits to multi-GB files, both
  backends) so its O(logical size)-per-push cost is a measured, accepted
  number before Phase 5; freeze block/manifest v1 layouts with golden
  fixtures and expected hashes.
- **Phase 1 — format and local core**: `fastcdc` + `zstd` deps; chunker,
  codecs, ID registries + policy function (§6.10), `BlockWriter`/`BlockReader`
  with hardened parser, manifest, LMDB chunk index, reconstruction,
  `should_chunk`. Exit: arbitrary bytes
  round-trip locally; random ranges match a contiguous reference; index
  delete/rebuild preserves behavior.
- **Phase 2 — storage seam and ingest**: `chunked()` capability trait on
  `VersionStore`; local impl; route all first-party write paths through
  policy-aware ingest; transparent reads. Exit: local add/commit/checkout and
  workspace writes chunk every eligible file without changing merkle hashes.
- **Phase 3 — readers**: `SeekableVersionReader` (local + S3 range worker),
  `read_version_df` refactor, HTTP Range behavior. Exit: parquet footer/column
  reads don't materialize; range amplification within gates.
- **Phase 4 — S3 parity**: S3 block/manifest IO, bounded range coalescing,
  conformance suite identical to local.
- **Phase 5 — wire protocol**: version gating, the new endpoints (§7.1), push
  refactor (content before metadata), pull flow, server-side
  reconstruct+xxh3 validation, resume/retry/race coverage. Exit: a small
  insertion into a large pushed file uploads and fetches only new
  chunks/blocks; a fresh clone reconstructs exact files.
- **Phase 6 — migration and maintenance**: format state + lease, inventory,
  checkpointed forward/reverse migration, manifest-aware prune/GC, fsck,
  save/load exclusions. Exit: interruption at every checkpoint resumes
  safely; cutover leaves no reachable eligible legacy blob.
- **Phase 7 — release gate and docs**: benchmarks vs current behavior, soak
  on large-history repos on both backends including a steady-state
  incremental-push soak (§14), docs.oxen.ai pages, then default block-v1
  for new repositories once the §8 ecosystem criterion is met.

## 14. Test strategy and acceptance gates

- **Unit/golden**: FastCDC golden boundaries; boundary stability under
  prefix/middle/suffix edits; codec round-trips + fallback; canonical block
  and manifest bytes with fixed hashes; hardened-parser failures for every
  malformed length/offset/version/codec; unknown chunker/codec/transform IDs
  fail with structured upgrade-required errors; range lookup at first byte,
  chunk boundaries, final byte, EOF; index insert/dup/stale/rebuild. The
  round-trip/range/fuzz cases are a **shared conformance suite
  parameterized over the registry** (§6.10) — every future chunker/codec
  implementation inherits it, not just FastCDC and zstd.
- **Property/fuzz**: arbitrary bytes reconstruct exactly and preserve xxh3;
  arbitrary ranges equal slices of the original; single-bit block mutation is
  detected; parsers tolerate arbitrary untrusted bytes without panic,
  overflow, or excessive allocation; decoder never exceeds declared bounds.
- **Integration (local + S3)**: eligible/small mixed commits; same chunks
  across versions and across different files; insert/delete/append edits;
  incompressible data; cross-file and underfilled blocks; branch, merge,
  restore, workspace, remote upload, df edits; push/pull incremental,
  interrupted, resumed, concurrent duplicate uploads; fresh clone and shallow
  fetch; HTTP full + range downloads; parquet/IPC seeks and CSV/JSONL
  streams; save/load, prune, fsck, index rebuild, GC; forward migration with
  crash at every checkpoint, resume, reverse migration; old-client rejection
  and maintenance-mode responses. Size chunked-path test files via
  `stream_segment_size() + N` and the `OXEN_DEDUP_MIN_FILE_SIZE` override —
  no hardcoded multi-MB fixtures.
- **Failure injection**: kill/fault at every arrow of the durability chain in
  invariant 6 — on push **and on pull** (a crash between block download and
  manifest publication must leave the version reported missing, never
  half-present); every case must leave either the old complete
  representation or the new one, plus at worst reclaimable orphans.
- **Release gates**: exact reconstruction always; incremental network bytes
  after a localized edit ≈ recipe metadata + new chunks; small range reads
  never fetch a whole file or block; ingest memory `O(open blocks)` and
  independent of file size; object count ~1 per 64 MiB, not per 64 KiB, at
  migration time **and** steady-state — after a simulated month of small
  incremental pushes, block count and live-byte utilization stay within
  declared bounds (else minimal repack enters v1); server manifest-validation
  IO per push measured on the labeling workload and explicitly accepted;
  incompressible data does not materially expand; index loss never loses
  data; local/S3 parity; migration preserves every commit ID.

## 15. Cleanup decisions

1. **Delete** `core/v_latest/index/file_chunker.rs` and `io/chunk_reader.rs`
   in the implementation PR — superseded, but **not** zero callers:
   `ChunkReader` is reachable from `oxen df <path> --revision` via
   `core/df/tabular.rs::show_node`, though functionally broken today
   (shards are never written). Deleting is a rewiring job: move `show_node`
   onto the normal transparent read path in the same PR, and check the
   current behavior of `df --revision` first.
2. **Remove** `FileChunkNode` + the `EMerkleTreeNode::FileChunk` arm in a
   separate small tech-debt PR; `u8 = 4` stays permanently reserved in the
   `MerkleTreeNodeType` on-disk contract.
3. **Keep** `FileNode.chunk_hashes` / `chunk_type` / `FileStorageType`
   vestigial and untouched (see decision 12); stop `prune.rs` reading
   `chunk_hashes`; remove the fields at the next FileNode format rev.
   `commit_writer.rs` may keep filling `chunk_hashes` with the whole-file
   hash until that rev, but no new code may read it.
4. **Rename the legacy wire "chunk" APIs to "segment" terminology**
   (`store_version_chunk` → `store_version_segment`, etc.). Internal Rust
   renames are free and happen in the implementation PR; the HTTP routes
   follow the additive-endpoint policy — new `/segments` routes added
   alongside, old `/chunks` routes registered in `docs/deprecations.md` with
   a removal target ~5 minor versions out, client switched to the new paths.

## 16. Out of scope for v1 (recorded future work)

- **Utilization-based block repack** (mark-and-sweep GC ships in v1; repack
  of partially live blocks follows, manifest-free thanks to decision 6).
- **Cross-repo / global dedup** — requires the keyed cryptographic chunk hash
  (decision 5 revisit trigger), tenant namespacing, and a separate threat
  model.
- **CDN / S3-presigned block offload** via an additive reconstruction-info
  read path.
- **Format-aware chunkers** (parquet row groups, JSONL record boundaries) —
  slot into the `(EntryDataType, extension)` policy; must remain
  byte-reversible.
- **Reversible pre-chunking transforms** (`transform_id ≠ 0`, §6.10):
  reframe already-compressed containers (parquet pages, gzip members) so
  CDC sees stable, compressible bytes — the parquet-CDC lesson. Needs a
  manifest version bump for transform-specific fields and transform-aware
  range reads; the ID slot and pipeline stage are already reserved.
- **Resumable upload sessions** with finalize-level accounting (idempotent
  block PUTs cover coarse resume today).
- **Paged manifests** for terabyte-scale files, as a manifest format bump.
- **Sparse fetch** (range-download low-utilization remote transfer blocks).
- Zstd dictionaries / additional codecs behind new stable codec IDs.
