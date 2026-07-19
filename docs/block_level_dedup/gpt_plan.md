# Block-Level Deduplication — Design

Session: `--resume 309b4953-45b1-480f-98a7-60b92d337c97`

Status: **agreed design, pre-implementation** (interview completed 2026-07-14).
This document is the reference for the implementation phase. It records the
motivation, the relevant current state of the codebase, every design decision
with its rationale, and the full Q&A record from the design interview.

## 1. Motivation

Oxen is a version control system that specializes in handling large files. We
want one of Oxen's core benefits to be its compression — specifically of large
files. Certain file types, like tabular data (CSV, JSONL, parquet), have nice
properties for compression: if a user is labeling a dataset, adding rows or
columns, we should be able to smartly compress the file and deduplicate chunks
of the file over time instead of storing and transferring a brand-new copy on
every commit.

Today, dedup is whole-file only: `oxen add` hashes the file (xxh3-128) and
copies it into the content-addressed version store. A one-row change to a 5 GB
CSV stores and pushes a brand-new 5 GB blob.

The solution must be robust, extensible, and customizable per file type —
future contributors should be able to add different chunking and compression
implementations for files, chunks, and blocks. It must work across storage
backends (local filesystem and S3).

### Prior art (Hugging Face / Xet)

- [From files to chunks](https://huggingface.co/blog/from-files-to-chunks):
content-defined chunking (CDC) uses a rolling hash to place chunk boundaries
by *content*, not offset, so insertions/deletions only perturb neighboring
chunks. Xet targets ~64 KB chunks. Measured: ~50% storage/transfer savings
on incrementally updated datasets; 30–85% dedup across fine-tuned model
families.
- [From chunks to blocks](https://huggingface.co/blog/from-chunks-to-blocks):
pure chunk-level CAS does not scale — metadata and request counts that grow
1:1 with chunk count are ruinous. Their fix: dedup at chunk granularity but
**store and transfer at block granularity** (xorbs, ≤64 MB, ~1000× fewer
entries), with *shards* holding file→chunk manifests and reconstruction
expressed as block ranges. Their lesson, which this design adopts:
*deduplication is a performance optimization, not the final goal — never let
chunk count drive your storage or network schema.*



## 2. Glossary

The word "chunk" is already overloaded in this codebase. This design fixes the
vocabulary:


| Term            | Meaning                                                                                                                                                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Chunk**       | A content-defined slice of a file (~64 KB target), identified by its xxh3-128 hash. The unit of *deduplication*.                                                                                                               |
| **Block**       | An immutable pack of chunk payloads (≤64 MB), content-addressed by xxh3-128 of its bytes. The unit of *storage and transfer*.                                                                                                  |
| **Manifest**    | The file→chunk mapping for one file version: an ordered list of `(chunk_hash, offset, len)`. Deterministic from file content.                                                                                                  |
| **Chunk index** | A store-local RocksDB cache mapping `chunk_hash → (block_hash, offset, stored_len, raw_len, flags)`. Rebuildable from block footers.                                                                                           |
| **Segment**     | The existing 10 MiB fixed wire slice used by the legacy large-file upload/download path (today misleadingly named "chunk" in `store_version_chunk` / `combine_version_chunks` / the `/chunks` routes). Being renamed — see §9. |




## 3. Current state of the codebase (findings)

Facts the design builds on, verified 2026-07-14:

- **Merkle tree**: node types `Commit=0, Dir=1, VNode=2, File=3, FileChunk=4`
(`model/merkle_tree/node_type.rs`; the u8 values are a stable on-disk
contract). Nodes are msgpack in a custom node DB
(`core/db/merkle_node/merkle_node_db.rs`), *not* RocksDB. The whole tree is
eagerly tar-synced on fetch.
- `FileNode` (`core/v_latest/model/merkle_tree/node/file_node.rs`) already
carries dormant chunking scaffolding: `chunk_hashes: Vec<u128>` (always
empty), `chunk_type: FileChunkType` (`SingleFile | Chunked`, always
`SingleFile`), `storage_backend: FileStorageType`. It also carries
`data_type: EntryDataType`, `mime_type`, `extension`, and
`metadata: Option<GenericMetadata>` — file-type detection is already in the
tree.
- `FileChunkNode` exists (`data: Vec<u8>` inline!) but nothing constructs
it. A dormant fixed-16 KB shard-based chunker exists at
`core/v_latest/index/file_chunker.rs` with zero callers (its driver is
commented out).
- **Version store** (`storage/version_store.rs`): `VersionStore` async trait,
keyed by content-hash string, layout
`.oxen/versions/files/{hash[..2]}/{hash[2..]}/data`. Impls: local FS
(`storage/local.rs`) and S3 (`storage/s3.rs`). Writes go through
`AtomicFile` with hash verification. Reads used broadly:
`get_version_stream`, `copy_version_to_path` (checkout/restore),
`get_version_chunk(hash, offset, size)` (range reads, e.g. CSV sniffing).
- **Push**: client walks its tree, prunes subtrees the remote has, confirms
missing commit hashes via `POST /commits/missing`. Small files (≤10 MiB) go
gzipped-multipart to `POST /versions`; large files as raw 10 MiB segments to
`PUT /versions/{hash}/chunks?offset=` + `POST /versions/{hash}/complete`.
- **Pull**: tree syncs first; client diffs target tree vs HEAD tree, probes
its own store, batch-downloads small files as a gzip tarball
(`QUERY /versions`) and large files as 10 MiB range segments
(`GET /chunk/{revision}/{path}`).
- **Hashing**: xxh3-128 everywhere (`util/hasher.rs`).
- **Compression on the wire today**: gzip for small-file batches and tree
tarballs; large-file segments are raw. Nothing is compressed at rest.
- Tests can force the chunked-transfer paths by shrinking
`OXEN_STREAM_SEGMENT_SIZE`; the new dedup constants follow the same pattern.



## 4. Design overview

```
oxen add (file ≥ 1 MiB, dedup.enabled)
  │
  ├─ Chunker (per EntryDataType; default FastCDC ~64 KB) ──> chunks
  ├─ Compressor policy (per EntryDataType; default zstd-3, raw fallback)
  ├─ new chunks appended to current block; block sealed ≤64 MB,
  │    content-addressed, footer written
  ├─ Manifest (ordered chunk list) stored keyed by the file hash
  └─ FileNode.chunk_type = Chunked          (tree shape unchanged)

reads (checkout, streams, range reads)      — transparent reconstruction
push/pull                                   — chunk-hash negotiation,
                                              transfer blocks on the wire
```

Design invariants:

1. **The merkle tree does not change shape.** Chunking is a storage concern.
  The tree keeps answering "what changed"; the version store answers "how do
   I get the bytes". The only tree-visible signal is
   `FileNode.chunk_type == Chunked`.
2. **Manifests are pure content.** Same file bytes ⇒ same chunk boundaries ⇒
  same manifest. Manifests never contain block placement.
3. **Blocks are store-private.** Each store (client, server, local, S3) packs
  chunks into blocks independently. Block IDs never cross the wire as durable
   references, so any store may repack/GC its blocks without coordination.
4. **Reads are transparent.** Every existing read API works identically on
  chunked versions. No caller outside the store and the transfer paths knows
   chunking exists.



## 5. Storage design

All new code lives in one module: `crates/liboxen/src/storage/chunked/`
(chunker, compressor, manifest, block format, chunk index, reconstruction).
This is the single place future contributors extend.

### 5.1 Chunker

```rust
pub trait Chunker: Send + Sync {
    fn id(&self) -> ChunkerId;                 // recorded in the manifest
    // yields (offset, len) boundaries + payloads over a streaming reader
    ...
}
```

- Default (and only MVP) implementation: **FastCDC** via the `fastcdc` crate
(v2020 variant, streaming), target **64 KiB**, min 16 KiB, max 256 KiB —
Xet's production-validated parameters.
- Selected per `EntryDataType` by a single policy function, so future
format-aware chunkers (parquet row-group boundaries, JSONL line boundaries)
are additive.
- The dormant fixed-size 16 KB chunker is rejected: fixed boundaries shift on
any mid-file insertion, destroying dedup for exactly the labeling/append
workload this feature targets.



### 5.2 Chunking policy (which files)

- **Size-only in the MVP: chunk every file ≥ 1 MiB, any data type.** Smaller
files keep today's whole-file + gzip-batch path (chunking them is pure
overhead: 1–2 chunks, no dedup opportunity).
- Constant with env override (test-infra pattern, like
`OXEN_STREAM_SEGMENT_SIZE`): `OXEN_DEDUP_MIN_FILE_SIZE`, default 1 MiB.
- Policy isolated in one `should_chunk(data_type, size)` function for future
per-type refinement.
- This threshold is independent of the legacy 10 MiB segment threshold; when
dedup is enabled and the server supports it, files ≥1 MiB move as blocks and
never touch the legacy segment path.



### 5.3 Chunk identity

- **xxh3-128**, consistent with all existing Oxen hashing. Whole-file dedup
already rests on xxh3-128; chunk dedup is the same 128 bits over smaller
inputs, and server-side dedup scope is per-repo (writers already trust each
other). It is also the add-path hot loop — xxh3 is markedly faster than
cryptographic hashes.
- **Revisit trigger (recorded on purpose):** if dedup ever crosses repo/tenant
trust boundaries, chunk hashing must move to a *keyed cryptographic hash*
(per-server key, à la Xet). The manifest format carries a version byte so
this is a format bump, not a redesign.



### 5.4 Compression

```rust
pub trait Compressor: Send + Sync {
    fn id(&self) -> CodecId;                   // recorded in per-chunk flags
    fn compress(&self, raw: &[u8]) -> Vec<u8>;
    fn decompress(&self, stored: &[u8], raw_len: usize) -> Result<Vec<u8>, OxenError>;
}
```

- **Granularity: per-chunk, inside the block.** Keeps every chunk
independently fetchable and decodable (range reads survive); per-block
compression would break mid-block access.
- **Ships in the MVP** with a per-`EntryDataType` codec policy:
  - `Text`, `Tabular` (and other text-like types): try **zstd level 3**.
  - `Binary`, `Image`, `Video`, `Audio`: skip the compression attempt
  (already-compressed data wastes CPU).
- **Raw fallback always applies**: if the compressed chunk is not smaller, the
chunk is stored raw. The per-chunk `flags` byte records the codec actually
used (`0 = raw`, `1 = zstd`); future codecs are additive.
- Rationale for shipping now: CSV/JSONL compress 3–10× under zstd — for the
tabular use case compression likely saves more bytes than dedup itself; the
work is localized to the block writer/reader; and blocks travel the wire
compressed for free.
- Consequence: the chunked write path takes the file's `EntryDataType` as a
parameter (the add path has it; the store does not).



### 5.5 Manifest

Stored in the version store keyed by the **file's content hash**, as a sibling
of where the whole-file blob would live (a chunked version has a manifest
instead of `data`):

```
.oxen/versions/files/{hash[..2]}/{hash[2..]}/manifest
```

Format: msgpack, versioned:

```rust
pub struct ChunkManifest {
    pub version: u8,           // format version (see §5.3 revisit trigger)
    pub file_hash: MerkleHash,
    pub file_size: u64,
    pub chunker_id: ChunkerId,
    pub chunks: Vec<ChunkEntry>,   // ordered by offset
}
pub struct ChunkEntry {
    pub hash: u128,    // xxh3-128 of the RAW (uncompressed) chunk bytes
    pub offset: u64,   // offset in the reconstructed file
    pub len: u32,      // raw length
}
```

- ~28 B/chunk ⇒ ~2 MB manifest for a 5 GB file; gzipped on the wire.
- Deterministic from content ⇒ manifests dedup themselves.
- **No block placement** — placement is store-local (§4 invariant 3).
- `(offset, len)` per chunk makes `get_version_chunk(hash, offset, size)` a
binary search + partial chunk reads, so range-read callers (CSV sniffing)
keep working.

Rejected alternatives: inlining chunk hashes in `FileNode.chunk_hashes`
(bloats tree nodes ~16 B/chunk, paid on every tree sync and walk) and
`FileChunkNode` tree leaves (millions of tree nodes; the node DB and eager
tree sync are not built for that; couples storage layout to tree format).

### 5.6 Block format

```
.oxen/versions/blocks/{hash[..2]}/{hash[2..]}/data
```

- **Content-addressed**: block ID = xxh3-128 of the block file bytes. IDs are
verifiable after transfer and storage is idempotent. (UUID naming rejected:
unverifiable, non-convergent.)
- **Layout**: concatenated chunk payloads (compressed or raw per chunk),
followed by a footer, written in one streaming pass:

```
[payload payload payload ...]
[footer: n × (chunk_hash u128, offset u32, stored_len u32, raw_len u32, flags u8)]
[footer_len u32][version u8][magic b"OXBK"]
```

- Footer cost ≈ 0.04% of a full block. It makes blocks **self-describing**:
the chunk index is a rebuildable cache (fsck/recovery), and future
S3-direct readers can index blocks without a database.
- `flags`: low bits = codec id (0 raw, 1 zstd); remaining bits reserved.
- **Sealing policy**: chunks append in file order to the current open block;
seal when the block reaches **64 MiB** or the add batch ends, then hash →
name → store atomically (`AtomicFile` with hash). Small commits produce
small blocks; that is acceptable in the MVP (a future repack/GC compacts —
legal precisely because blocks are store-private).
- Consecutive chunks of a file land consecutively in a block, so
reconstruction reads long contiguous ranges from few blocks (locality for
free).



### 5.7 Chunk index

RocksDB at `.oxen/versions/chunk_index/`:

```
chunk_hash: u128  →  (block_hash: u128, offset: u32, stored_len: u32, raw_len: u32, flags: u8)
```

- Store-local, derived data: rebuildable by scanning block footers.
- Answers dedup probes at add time (`have chunk?`), `missing_chunks` during
negotiation, and placement lookups during reconstruction and transfer
packing.



### 5.8 The `VersionStore` seam

Decision: **capability sub-trait; transparent reads; explicit writes.**

- `VersionStore` gains one accessor:

```rust
fn chunked(&self) -> Option<&dyn ChunkedVersionStore>;
```

  A store that returns `None` simply never stores chunked versions —
  everything degrades to whole-file. Both `LocalVersionStore` and
  `S3VersionStore` implement it, sharing the chunking/packing/index/
  reconstruction logic and differing only in raw byte IO (local file ranges
  vs S3 ranged GETs / multipart PUTs).

- `ChunkedVersionStore` (names to be finalized in implementation):

```rust
#[async_trait]
pub trait ChunkedVersionStore: Send + Sync {
    /// Chunk, compress, pack and index a new version; writes the manifest.
    async fn store_version_chunked(
        &self, hash: &str, data_type: &EntryDataType,
        reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<ChunkManifest, OxenError>;

    async fn get_manifest(&self, hash: &str) -> Result<Option<ChunkManifest>, OxenError>;
    async fn put_manifest(&self, manifest: &ChunkManifest) -> Result<(), OxenError>;

    /// Subset of `hashes` this store does not have (negotiation).
    async fn missing_chunks(&self, hashes: &[u128]) -> Result<Vec<u128>, OxenError>;

    /// Pack the requested chunks into transfer blocks, streamed (§6).
    async fn pack_chunks(&self, hashes: &[u128]) -> Result<BoxedByteStream, OxenError>;

    /// Verify hash, parse footer, store as a local block, index its chunks.
    async fn store_block(
        &self, hash: &str, reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), OxenError>;
}
```

- **Reads stay transparent**: `get_version_stream`, `copy_version_to_path`,
`get_version_size`, `get_version_chunk`, `version_exists`,
`find_missing_versions` all work identically on chunked versions by
internally loading the manifest and streaming block ranges through the
chunk index (decompressing per chunk). Checkout, restore, df, workspaces:
zero changes.
- **Writes choose explicitly**: the add path calls `should_chunk(...)` and
either the existing whole-file write or `store_version_chunked`, then sets
`FileNode.chunk_type = Chunked`.
- Rejected alternatives: a fat `VersionStore` trait (every impl stubs ~8
methods; callers can't tell capability) and a standalone `ChunkStore` +
coordinator (every existing read site must route through the coordinator).



## 6. Wire protocol

Decision: **symmetric transfer blocks** — negotiation is chunk-hash-based;
the sender repacks exactly the missing chunks into fresh blocks (the same
on-disk format, formed per transfer) and streams them; the receiver verifies
each block hash, parses the footer, and stores it directly as a local block.
One `BlockWriter`/`BlockReader` pair serves storage and transfer in both
directions; compressed chunks travel compressed for free; no chunk is ever
re-sent to a peer that has it; block IDs never become durable wire references.

(Xet's asymmetric shape — upload local blocks wholesale, download
"reconstruction info" of `(block_id, range)` for CDN range-GETs — is rejected
for the MVP: oxen-server serves bytes directly, and wire-visible block IDs
would freeze block layout. Adding a reconstruction-info read path later is
purely additive if a CDN/S3-offload phase arrives.)

### 6.1 New endpoints (all additive, per the deprecation policy)

Under `/api/repos/{namespace}/{repo_name}`:


| Endpoint                                          | Purpose                                                                                                            |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `POST /chunks/missing`                            | Body: gzipped list of chunk hashes. Response: the subset the server is missing. (Push negotiation.)                |
| `PUT /blocks/{block_hash}`                        | Streamed transfer-block upload. Server verifies the hash, parses the footer, stores + indexes. Idempotent.         |
| `QUERY /chunks`                                   | Body: gzipped list of chunk hashes. Response: a stream of transfer blocks containing exactly those chunks. (Pull.) |
| `PUT /manifests/{file_hash}` / `QUERY /manifests` | Batch manifest upload/download (gzipped msgpack).                                                                  |


Existing endpoints are untouched; old clients keep working (§7).

### 6.2 Push flow (chunked files)

1. Tree walk + commit negotiation exactly as today.
2. For files with `chunk_type == Chunked`: collect chunk hashes from local
  manifests; batch → `POST /chunks/missing`.
3. `pack_chunks(missing)` → stream transfer blocks → `PUT /blocks/{hash}`
  (parallel, with the existing retry/backoff machinery).
4. Upload manifests for the pushed file versions.
5. Tree nodes + mark-synced, as today.

Ordering guarantee: **blocks → manifests → tree nodes → synced**, so a synced
commit never references chunks the server lacks. On manifest upload the
server cheaply verifies (index probes) that all referenced chunks exist,
rejecting with a structured error otherwise.

### 6.3 Pull flow (chunked files)

1. Tree sync + missing-entry computation as today.
2. For missing versions with `chunk_type == Chunked`: `QUERY /manifests`,
  store manifests locally.
3. Diff manifest chunk hashes against the local chunk index → missing set.
4. `QUERY /chunks` with the missing set → stream of transfer blocks →
  `store_block` each.
5. Checkout reconstructs transparently via the normal read path.



## 7. Rollout, compatibility, migration

- **Opt-in first.** A repo-level config flag (e.g. `dedup.enabled` in the
repo config, with an env override for tests) gates *writing* chunked
versions. Reading/serving chunked versions is always compiled in and on.
Flip to default-on after a soak period (one or two releases), tracked like
a deprecation entry.
- **Old client ↔ new server: works.** Transparent reads on the server mean
legacy download endpoints stream reconstructed bytes; old clients pushing
whole files produce `SingleFile` versions, which coexist per-FileNode with
chunked ones.
- **New client ↔ old server: works, degraded.** The client detects the
server's capability (server version floor / probe on first negotiation)
and falls back to today's whole-file push — its own transparent reads
stream the reconstructed file into the legacy upload path. Slower, never
wrong.
- **No repo migration.** Existing versions stay whole-file forever; only new
adds chunk. `chunk_type` is per file version.
- **Python bindings**: no API change expected (reads are transparent); verify
with `bin/test-rust -p`, and add a python test exercising add/push/pull
with dedup enabled.
- **docs.oxen.ai**: the config flag and behavior need a docs-repo page when
the implementation lands.



## 8. Out of scope for the MVP (recorded future work)

- **GC / repack**: deleting versions leaves dead chunks in blocks; a repack
compacts. Legal without coordination because blocks are store-private.
- **Cross-repo / global dedup** (Xet's key-chunk index). Requires the keyed
cryptographic chunk hash (§5.3) first.
- **CDN / S3-presigned offload** via a reconstruction-info read path
(additive).
- **Format-aware chunkers** (parquet row groups, JSONL lines) — slot into the
`Chunker`-per-`EntryDataType` policy.
- **Server-side chunking of workspace-committed files** — server writes stay
whole-file initially; adopting the chunked write there is a follow-up.
- **Resumable block upload** and a `oxen dedup stats` CLI surface.



## 9. Cleanup decisions (agreed)

1. **Delete** `core/v_latest/index/file_chunker.rs` (ChunkShardManager,
  ChunkShardDB, shard files) in the implementation PR — superseded, zero
   callers.
2. **Remove** `FileChunkNode` **+ the** `EMerkleTreeNode::FileChunk` **arm** in a
  separate small tech-debt PR; `u8 = 4` stays permanently reserved in the
   `MerkleTreeNodeType` on-disk contract.
3. **Keep** `FileNode.chunk_hashes` (vestigial, empty, ~1 msgpack byte),
  documented as superseded by manifests; remove at the next FileNode format
   rev. `chunk_type` is actively used; `FileStorageType` unchanged.
4. **Rename the legacy wire "chunk" APIs to "segment" terminology** (
  `store_version_chunk` → `store_version_segment`, etc.). Internal Rust
   renames are free; the HTTP routes follow the additive-endpoint policy:
   new `/segments` routes added alongside, old `/chunks` routes registered in
   `docs/deprecations.md` with a removal target ~5 minor versions out, client
   switched to the new paths.

