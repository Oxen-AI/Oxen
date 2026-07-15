# Repository-Scoped Block-Level Deduplication

Status: agreed design, pre-implementation (2026-07-14)

This document is the implementation plan for repository-scoped block-level
deduplication in Oxen. It covers the complete path: local ingestion, immutable
storage, Local and S3 backends, block-aware push and fetch, logical file reads,
efficient ranges and seeks, integrity checking, garbage collection, and
migration of historical versions.

The central design rule is:

> Files remain the logical objects named by commits. Content-defined chunks are
> the unit of deduplication. Immutable blocks are the unit of storage and bulk
> transfer. Manifests connect those layers without changing Merkle or commit
> identity.

## 1. Executive recommendation

Implement a repository-local content layer beneath the existing logical
`VersionStore` interface:

- Keep the existing XXH3-128 hash as the logical file/version identifier used
  by `FileNode`, commits, and existing APIs.
- Use BLAKE3-256 over raw bytes for chunk identifiers.
- Use BLAKE3-256 over the complete canonical encoded block for block
  identifiers.
- Apply streaming FastCDC to every file at or above 10 MiB by default, using an
  8 KiB minimum, 64 KiB target, and 128 KiB maximum chunk size.
- Compress each chunk independently with Zstandard level 1, falling back to raw
  storage when compression saves less than 1% or 64 bytes.
- Pack missing chunks from multiple files into immutable blocks no larger than
  64 MiB encoded, including the header and index.
- Store a range-addressable manifest per logical file version. The manifest
  records the ordered chunk sequence and each chunk's repository-local block
  location.
- Keep the chunk-location index as disposable, rebuildable derived state.
- Make the client perform chunk lookup and block construction for pushes; make
  the server validate blocks and synchronously reconstruct each file before it
  publishes a manifest.
- Fetch manifests first, compare their chunk recipes with the local repository
  index, and download only blocks containing locally missing chunks.
- Put all logical reads through a block-aware facade that supports streams and
  true byte ranges. Do not silently materialize a block-backed file merely to
  satisfy a seek.
- Migrate all reachable historical files that meet the large-file policy under
  repository-wide maintenance mode. Publish and verify each manifest before
  deleting its legacy full blob. The operation is resumable and idempotent.
- Require block-capable clients and servers once a repository activates the new
  format. New code must continue to read legacy full blobs, but old software is
  not allowed to operate on a block-enabled repository.

The first implementation is repository-scoped and assumes one authoritative
server per repository. It deliberately does not introduce global deduplication,
a distributed index, or dynamic third-party plugins.

## 2. Goals, non-goals, and invariants

### 2.1 Goals

1. Store and transfer only newly introduced content when a large file changes
   locally.
2. Deduplicate equal byte ranges across versions and across different files in
   the same repository.
3. Preserve every committed byte exactly. Checkout output and the existing
   XXH3-128 file hash must be unchanged.
4. Work with both `LocalVersionStore` and `S3VersionStore`.
5. Preserve efficient logical byte-range reads and `Read + Seek` behavior,
   especially for Parquet and IPC readers.
6. Avoid one filesystem object, S3 object, HTTP request, or Merkle node per
   approximately 64 KiB chunk.
7. Keep the on-disk format versioned and the Rust extension points small,
   explicit, and testable.
8. Make crashes safe through immutable content, validation, and publish-last
   ordering rather than a distributed transaction.
9. Support migration, fsck, pruning, backups, and operational metrics from the
   first production release.

### 2.2 Non-goals for v1

- Cross-repository or global deduplication.
- Multiple independent server nodes concurrently writing one repository when
  only object storage is shared.
- Dynamic native-code plugins or user-provided codecs.
- A Parquet-, CSV-, or JSONL-specific chunker in the first release.
- Replacing XXH3-128 as Oxen's logical file identity.
- Encrypting blocks independently of the storage backend.
- Presigned direct-to-S3 block upload as a requirement. The design leaves room
  for it, but a server-proxied streaming implementation is sufficient for v1.
- Repacking partially live blocks during the first migration. Mark-and-sweep GC
  is required; utilization-based repacking can follow using the same manifests.

### 2.3 Non-negotiable invariants

- A published manifest reconstructs exactly `logical_size` bytes and hashes to
  its XXH3-128 `logical_hash`.
- A published manifest never references an unpublished or unvalidated block.
- A block is immutable. Bytes at a block hash never change.
- A chunk hash is BLAKE3-256 of the uncompressed, original chunk bytes.
- A block hash is BLAKE3-256 of the complete canonical encoded block bytes.
- A commit and its `FileNode` do not change when a repository migrates,
  compresses, fetches, caches, garbage-collects, or repacks physical content.
- Range reads decompress only chunks intersecting the requested range, apart
  from bounded metadata and read-ahead.
- Loss of the chunk index affects speed, not correctness or recoverability.
- Normal repository operations cannot run while a storage migration or block
  GC holds the repository maintenance lease.

## 3. Prior art and lessons adopted

Hugging Face and Xet provide the closest production precedent:

- [From files to chunks](https://huggingface.co/blog/from-files-to-chunks)
  explains why offset-based chunks lose alignment after insertions and why
  content-defined chunking preserves most boundaries.
- [From chunks to blocks](https://huggingface.co/blog/from-chunks-to-blocks)
  explains why a CAS object per small chunk does not scale and groups chunks
  into objects of roughly 64 MiB instead.
- The official [Xet chunking documentation](https://huggingface.co/docs/xet/chunking)
  describes an 8 KiB minimum, 64 KiB target, and 128 KiB maximum profile.
- Xet's [file reconstruction](https://huggingface.co/docs/xet/file-reconstruction),
  [upload](https://huggingface.co/docs/xet/upload), and
  [download](https://huggingface.co/docs/xet/download) documentation separates
  logical reconstruction metadata from immutable block objects.
- [Parquet content-defined chunking](https://huggingface.co/blog/parquet-cdc)
  demonstrates an important limitation: generic CDC over already compressed
  Parquet bytes cannot recover semantic similarity that serialization and
  compression have destroyed. Format-aware work must happen at a suitable
  logical layer and still be reversibly encoded.
- The [FastCDC paper](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
  motivates a fast, normalized content-defined chunker suitable for the generic
  path.
- The open-source [xet-core repository](https://github.com/huggingface/xet-core)
  is useful implementation reference for separating chunking, block storage,
  reconstruction metadata, deduplication indexes, and caching.

The Oxen design adopts the architecture, not every implementation detail:

- Deduplicate at chunk granularity but store and bulk-transfer at block
  granularity.
- Keep file reconstruction metadata compact and separate from the Git-like
  logical tree.
- Make reads range-aware through per-chunk compression and indexed offsets.
- Treat indexes and caches as derived acceleration structures.
- Keep file-format specialization behind versioned policies.

Oxen differs in several deliberate ways:

- Deduplication is repository-scoped first.
- Existing logical file hashes remain XXH3-128.
- Full server-side reconstruction is required before manifest publication.
- Historical eligible blobs are migrated instead of waiting for future writes.
- Block-enabled repositories require upgraded clients and servers.

## 4. Current Oxen architecture and constraints

The following findings were verified in the current codebase before selecting
the design.

### 4.1 Merkle tree and file identity

- `crates/liboxen/src/core/v_latest/model/merkle_tree/node/file_node.rs`
  stores the file's XXH3-128 hash, byte length, `EntryDataType`, MIME type,
  extension, and generic metadata.
- `FileNodeData` also contains dormant physical-storage fields:
  `chunk_hashes: Vec<u128>`, `FileChunkType`, and `FileStorageType`.
  `commit_writer.rs` currently fills `chunk_hashes` with the whole-file hash;
  it is not a real chunk recipe.
- `MerkleTreeNodeType::FileChunk` and `FileChunkNode` exist but are unused.
  `FileChunkNode` embeds bytes directly, and several traversal paths assume a
  file-like leaf is a `FileNode`. Turning CDC chunks into Merkle nodes would
  both explode node counts and expose those incomplete assumptions.
- File nodes already contain enough type information to select a file policy.

Physical representation must not be added to `FileNode`. The same commit may
be stored as a full blob in one clone and as blocks in another. Migration or
repacking must not rewrite Merkle nodes or commit IDs.

### 4.2 Ingestion and commit

- `crates/liboxen/src/core/v_latest/add.rs` hashes and identifies a file,
  stages a `FileNode`, then calls `VersionStore::store_version_from_reader`.
- Content is stored during add or workspace mutation; commit primarily writes
  Merkle state. The block ingestion seam therefore belongs at version storage,
  not inside commit construction.
- Branch operations, remote checkout, workspaces, client downloads, and server
  upload paths also call `store_version_from_reader`. They must be routed
  through the same policy-aware ingest API so block mode cannot be bypassed.

### 4.3 Version storage

- `crates/liboxen/src/storage/version_store.rs` defines the central async
  `VersionStore` trait.
- The trait exposes logical operations such as existence, size, full stream,
  fixed range, copy-to-path, materialization, listing, and deletion.
- `LocalVersionStore` stores a contiguous object under a hash-derived path.
- `S3VersionStore` stores a contiguous object under a repository/hash-derived
  key and supports multipart writes and range reads.
- `VersionLocation::S3` currently lets Polars scan a contiguous S3 object
  directly. A block-backed logical file has no such single S3 URL, so callers
  that depend on this shortcut need a new logical range/seek abstraction.

The public logical `VersionStore` behavior is the best compatibility seam, but
Local and S3 block object operations should live behind a separate physical
`BlockStore` trait.

### 4.4 Current push and fetch terminology

- `STREAM_SEGMENT_SIZE` is currently 10 MiB.
- Existing methods and routes named `store_version_chunk`, `/chunks`, and
  `combine_version_chunks` describe fixed transfer segments. The receiver
  concatenates those segments into one full blob and deletes them.
- These transfer segments are not content-defined chunks and are not durable
  deduplication objects.
- Push currently divides files at the 10 MiB boundary: smaller versions are
  bundled and larger versions are uploaded as parallel fixed ranges.
- Fetch likewise downloads large logical files in fixed ranges and combines
  them into a contiguous version.

The implementation and documentation must call the old objects **transfer
segments** and reserve **chunk** for FastCDC output. Existing HTTP paths may be
replaced for block-enabled repositories because upgraded clients are required;
an unrelated route rename is not required in the first block PR.

### 4.5 Logical readers and tabular data

- Direct version/file endpoints call `get_version_stream`, which is a useful
  transparent reconstruction seam.
- `crates/liboxen/src/core/df/tabular.rs::read_version_df` uses a local path for
  Local storage and a cloud-aware `s3://.../data` scan for S3.
- Parquet and IPC readers need range/seek access; CSV and JSONL generally need
  sequential streams. Silently materializing every block-backed S3 object would
  regress the large-file behavior this feature is intended to improve.
- The old `io/chunk_reader.rs` uses fixed 16 KiB pieces and dormant FileNode
  fields. It is not suitable for the new storage format.

### 4.6 Maintenance and integrity

- `core/v_latest/prune.rs` marks whole version hashes and the dormant
  `FileNode.chunk_hashes` values, then asks `VersionStore` to delete unmarked
  versions. Shared blocks require manifest-based mark-and-sweep instead.
- Current fsck logic expects a contiguous blob and verifies its XXH3 hash.
- `repositories/save.rs` archives `.oxen`; a derived RocksDB chunk index must be
  snapshotted safely or excluded and rebuilt.
- `docs/async_policy.md` requires a synchronous compute/filesystem core behind
  async network edges, coherent `spawn_blocking` operations, bounded channels,
  and Rayon for CPU-parallel batches. The new implementation must follow that
  policy.

### 4.7 Existing experiments

- `crates/liboxen/src/core/v_latest/index/file_chunker.rs` is an unused older
  fixed-size shard experiment.
- `experiments/block-level-dedup` contains a FastCDC prototype, but it reads
  whole files into memory, uses XXH3 for chunks, and lacks production unpack,
  S3, protocol, and integrity behavior.

Those experiments are useful for benchmarks and API lessons only. The
production design should not revive their persistent formats.

## 5. Agreed decision log

The design interview resolved the following product and architecture choices.
The answer in the final column is binding for the initial implementation.

| Question | Recommendation | Agreed answer |
| --- | --- | --- |
| How broad should the plan be? | Cover ingestion through block-aware push/fetch, reads, maintenance, and migration. | Full end-to-end design. |
| What is the deduplication scope? | Start repository-local. | Repository-scoped. |
| Must old clients or servers interoperate with a block-enabled repo? | Require upgraded software and fail early. | Upgraded clients and servers are required. |
| What happens to historical full blobs? | Migrate every reachable eligible version. | Historical blobs are migrated. |
| May migration block normal repository use? | Use an exclusive, resumable maintenance operation. | Repository-wide maintenance mode is acceptable. |
| Which files use CDC initially? | All sufficiently large files, with future type overrides. | Generic CDC is the default for all large files. |
| May block storage degrade random reads? | No; make manifests and blocks range-addressable. | Efficient range/seek reads are required in v1. |
| Which hashes identify each layer? | Preserve logical XXH3; use strong BLAKE3 for physical CAS. | XXH3-128 file, BLAKE3-256 chunk and block. |
| Does push compare blocks or chunks? | Negotiate true chunk hashes, then transfer blocks. | Chunk-level lookup. |
| May a block contain chunks from different files? | Yes, within a bounded packing session. | Cross-file blocks are allowed. |
| Who constructs pushed blocks? | Client after the server reports missing chunks. | Client-side construction. |
| What must the server validate before publishing a manifest? | Block, chunks, recipe, size, and full reconstructed file hash. | Full synchronous validation. |
| Is the chunk index authoritative? | No; rebuild it from blocks and manifests. | Rebuildable derived state. |
| Must v1 coordinate multiple server nodes? | No; abstract the index but match the current single-server model. | One authoritative server per repository. |
| Which generic CDC algorithm? | Normalized FastCDC with a versioned policy ID. | FastCDC. |
| Which FastCDC sizes? | 8 KiB / 64 KiB / 128 KiB. | Agreed. |
| Maximum block size? | 64 MiB of complete encoded bytes. | Agreed. |
| Compression granularity? | Independent chunks with raw fallback. | Agreed. |
| Initial codec? | Zstandard level 1. | Adopted under the user's instruction to continue with recommendations. |

The user delegated the remaining branches to the recommended choices. Those
choices are recorded throughout this document and summarized here:

- 10 MiB is the initial large-file eligibility threshold. It is distinct from
  transfer-segment terminology even though it starts with the same value.
- Manifests are physical repository metadata outside the Merkle tree.
- The initial policy/codec registry is compile-time Rust code selected by stable
  IDs in repository configuration; it is not a dynamic plugin ABI.
- Block headers and manifests use explicitly versioned canonical binary
  encodings rather than serializing Rust structs as the permanent format.
- Normal fetch stores whole missing blocks. Sparse remote byte-range reads do
  not require whole-block transfer; sparse fetch/repack can be added later.
- GC is exclusive mark-and-sweep with a 24-hour orphan grace period. Refcounting
  is not authoritative.
- New blocks are durable first, the chunk index is updated second, and a
  validated manifest is published last.
- New repositories may default to block-v1 once the release is production
  ready; existing repositories activate it only through explicit migration.

### 5.1 Questions answered by the code rather than the interview

The codebase made several choices unambiguous, so they were researched instead
of presented as product questions:

- **Should chunk nodes be added to the Merkle tree?** No. They would make
  physical layout part of logical history, multiply node counts, and enter
  partially implemented `FileChunkNode` paths.
- **Should `FileNode.chunk_hashes` become the manifest?** No. It is an existing
  `Vec<u128>`, is populated with the full hash today, and would bloat every tree
  transfer. Repository-local manifests can vary without changing commits.
- **Should the existing 10 MiB upload chunks become CDC chunks?** No. They are
  temporary offset-based transfer segments with different lifecycle and
  semantics.
- **Where should ingest happen?** At the version/content storage seam, because
  add and workspace mutation store bytes before commit.
- **Must the logical storage API remain transparent?** Yes. File endpoints,
  checkout, diffs, workspaces, and derived-file consumers already converge on
  `VersionStore` operations.

## 6. Architecture overview

```text
                          logical layer

    Commit -> FileNode -> XXH3-128 logical file hash + size + file type
                              |
                              v
                    RepositoryContentStore
               (logical stream/range/seek/copy API)
                       /                 \
             small or legacy          block-backed
             contiguous blob          manifest by file hash
                                            |
                              ordered ChunkRef records
                                            |
                    +-----------------------+--------------------+
                    |                       |                    |
              BLAKE3 block A          BLAKE3 block B       BLAKE3 block C
              [chunk records]         [chunk records]      [chunk records]

                          physical layer

      Local filesystem or S3 BlockStore + ManifestStore
                              |
                 rebuildable repository chunk index
                 chunk BLAKE3 -> block hash + entry
```

There are four distinct identities and lifecycles:

1. **Logical file/version**: existing XXH3-128 identity referenced by Merkle
   nodes and commits.
2. **Chunk**: a content-defined raw byte slice identified by BLAKE3-256; the
   unit of equality and deduplication.
3. **Block**: an immutable canonical encoding of many chunks, identified by
   BLAKE3-256; the unit of physical storage and normal bulk transfer.
4. **Manifest**: repository-local physical reconstruction metadata keyed by the
   logical file hash. It may be replaced during repacking without changing the
   logical file.

## 7. Hashes and typed identities

Introduce strong Rust newtypes rather than passing hash strings between layers:

```rust
pub struct LogicalFileHash(MerkleHash); // existing XXH3-128
pub struct ChunkHash([u8; 32]);         // BLAKE3-256(raw chunk)
pub struct BlockHash([u8; 32]);         // BLAKE3-256(canonical block)
pub struct ManifestDigest([u8; 32]);    // integrity digest, not file identity
```

Each type owns strict lowercase-hex parsing and display. Logical hashes remain
32 hex characters; chunk and block hashes use 64. APIs should not accept an
untyped `&str` once input has crossed the HTTP/config boundary.

Hash definitions are exact:

```text
chunk_hash = BLAKE3-256(original uncompressed chunk bytes)
block_hash = BLAKE3-256(all canonical encoded block bytes)
file_hash  = existing Oxen XXH3-128 over reconstructed original file bytes
```

The block hash is not computed over the concatenated raw chunks. It covers the
format version, complete index, codec identifiers, encoded lengths, and encoded
payloads, so corruption in any part of the object changes its key.

Typed wrappers provide domain separation in code and storage paths. Hash input
prefixes are unnecessary and would make direct content hashes less portable.

If a manifest already exists for an XXH3 logical hash and a new recipe differs
in size or ordered BLAKE3 chunk sequence, publication fails with an explicit
logical-hash-collision error. A server must never overwrite one logical version
with different bytes merely because XXH3 collided.

## 8. Chunking and compression policy

### 8.1 Eligibility

The initial repository policy is:

```text
size < 10 MiB   -> existing whole-file CAS
size >= 10 MiB  -> generic-fastcdc-v1
```

Whole-file CAS already deduplicates identical small files with much less
metadata. Keeping them contiguous also avoids turning repositories containing
millions of tiny files into manifest-heavy stores.

The threshold is part of the repository's versioned storage policy, not the
existing HTTP transfer segment contract. Tests should use an override rather
than lowering the production constant globally.

### 8.2 FastCDC v1

`generic-fastcdc-v1` is deterministic and contains these frozen parameters:

```text
algorithm: FastCDC v2020, normalized mode
minimum:   8 KiB
average:  64 KiB
maximum: 128 KiB
```

The production implementation must be streaming and keep at most a bounded
window plus the current maximum chunk in memory. The experiment's whole-file
buffering implementation cannot be promoted unchanged.

FastCDC boundary validation is part of server manifest validation. While
reconstructing the proposed file, the server runs the declared policy and
compares the resulting ordered `(chunk_hash, raw_len)` sequence. This prevents
buggy or hostile clients from publishing arbitrary chunk boundaries that erode
deduplication or violate size limits.

### 8.3 Zstandard v1 and raw fallback

Each missing chunk is encoded independently:

1. Compress with Zstandard level 1.
2. Use Zstandard only if it saves at least `max(64 bytes, 1% of raw_len)`.
3. Otherwise store the chunk as `Raw`.

Independent frames deliberately trade a little compression ratio for bounded
random access. A one-byte range never requires decompression of a 64 MiB block.

The block records a stable codec ID and codec format version, for example:

```text
0 = RawV1
1 = ZstdV1
```

Encoder parameters belong to the policy; decoding behavior belongs to the
codec ID. Once a codec ID is written, its decoder must remain available until a
repository migration rewrites every block that uses it.

### 8.4 File-type extension model

Use a small compile-time registry:

```rust
pub trait ChunkingStrategy: Send + Sync {
    fn id(&self) -> ChunkingPolicyId;
    fn boundaries(&self, input: &mut dyn Read, sink: &mut dyn ChunkSink)
        -> Result<(), OxenError>;
}

pub trait ChunkCodec: Send + Sync {
    fn id(&self) -> CodecId;
    fn encode(&self, raw: &[u8], out: &mut Vec<u8>) -> Result<Encoding, OxenError>;
    fn decode(&self, encoded: &[u8], raw_len: u32, out: &mut Vec<u8>)
        -> Result<(), OxenError>;
}

pub trait FileStoragePolicy: Send + Sync {
    fn id(&self) -> FilePolicyId;
    fn applies_to(&self, descriptor: &FileDescriptor) -> bool;
    fn chunker(&self) -> ChunkingPolicyId;
    fn codec(&self) -> CodecId;
}
```

This is intentionally not a trait for every struct. Blocks, manifests, and
recipes have one versioned format. Traits exist only where there are real
backend or file-policy variants.

Policy selection uses the existing `EntryDataType`, MIME type, extension, and
sniffed metadata. Selection order is deterministic: an exact registered
format policy, then a data-type policy, then generic FastCDC. In v1 only the
generic policy is active for all eligible files.

A format-specific policy must still reproduce the original bytes exactly. It
may choose smarter boundaries or a reversible internal codec. It may not parse
and reserialize an arbitrary committed Parquet or CSV file into merely
equivalent content. Writer-integrated semantic chunking is a later feature and
must have an explicit reversible representation.

Because one logical hash can appear at different paths or extensions, the first
published representation for that hash is reused. During migration, conflicting
descriptors fall back to the generic policy unless content inspection proves a
single specialized policy is valid.

## 9. Canonical block format

### 9.1 Properties

A block is:

- immutable and content-addressed;
- at most 64 MiB in its complete encoded form;
- self-describing and independently verifiable;
- composed of independently encoded chunk records;
- indexed at the front so a reader can locate payloads with one small initial
  range request;
- allowed to contain chunks from several files;
- never appended to after publication.

The last underfilled block in an add, push, or migration packing session is
published as-is. Oxen must not maintain a mutable repository tail block: that
would introduce locking, crash recovery, and S3 overwrite complexity into the
CAS layer.

### 9.2 Logical layout

The canonical little-endian v1 layout is:

```text
BlockHeaderV1
  magic[8]                 = "OXBLOCK1"
  format_version: u16      = 1
  flags: u16
  entry_count: u32
  index_entry_size: u16
  reserved: u16
  header_len: u32
  payload_len: u64
  raw_total_len: u64
  policy_hint: u32         # diagnostic only; chunks remain self-describing
  reserved bytes

ChunkRecordV1[entry_count] # fixed-width
  chunk_hash: [u8; 32]
  raw_len: u32
  encoded_len: u32
  payload_offset: u64      # from start of payload area
  codec_id: u16
  codec_version: u16
  flags: u32

payload bytes              # records concatenated in index order
```

The exact reserved sizes should be frozen with the implementation, golden
fixtures, and parser limits. The format should not be generated by serializing
a Rust struct with MessagePack or bincode. Explicit reads and writes avoid
layout changes when fields or dependencies evolve.

The block filename/key is the BLAKE3-256 digest of every byte in the layout;
the hash is therefore not embedded in the hashed bytes. A caller carries the
expected hash from the object key.

### 9.3 Validation

Before a block becomes visible, the receiver verifies:

1. Magic, supported version, flags, fixed record size, and reserved fields.
2. Total object length and the 64 MiB cap.
3. Entry count and all arithmetic with checked integer operations.
4. Non-overlapping payload ranges contained within `payload_len`.
5. `raw_len` is nonzero except where explicitly allowed and no greater than the
   policy's 128 KiB maximum.
6. Codec ID/version is supported.
7. Decoding produces exactly `raw_len` bytes, with an output cap fixed before
   allocation.
8. Every decoded chunk matches its BLAKE3-256 `chunk_hash`.
9. The complete encoded object matches its expected BLAKE3-256 `block_hash`.

These checks make the block parser safe against integer overflow,
out-of-bounds ranges, decompression bombs, and corrupt or malicious clients.

### 9.4 Packing

`BlockPacker` accepts unique raw chunks and performs this bounded pipeline:

```text
raw chunk -> BLAKE3 verify -> encode or raw fallback -> size check -> append
                                                        |
                              next record would exceed 64 MiB
                                                        |
                                      finalize/hash/publish current block
```

The encoded header and payload must fit under the cap. The packer knows encoded
sizes before finalization, so it reserves the complete index cost while deciding
whether another record fits.

The builder may buffer one block in memory, but global concurrency is governed
by an explicit memory budget. A temp-file-backed builder is preferred for
server paths so two or three concurrent 64 MiB blocks do not multiply into
unbounded request memory. Either implementation produces identical canonical
bytes.

Within a packing session, a `HashSet<ChunkHash>` prevents the same missing
chunk from being inserted twice. Across sessions, the repository chunk index
prevents normal duplication; concurrent uploads can still race and produce two
valid blocks containing the same chunk. That is safe. The first indexed
location is canonical for future writes, and GC may later remove an unreferenced
duplicate block.

## 10. Range-addressable file manifest

### 10.1 Why the manifest is outside the Merkle tree

The manifest is physical metadata, not commit metadata:

- a local clone and server can pack the same chunks into different blocks;
- migration creates manifests without changing historical commits;
- fetch can reuse chunks already present in different local blocks;
- repacking can rewrite block references without changing file bytes;
- storing thousands of chunk references in `FileNode` would expand Merkle
  synchronization and node databases.

Manifest existence, not `FileNode.chunk_type`, determines whether a version is
block-backed in a particular repository.

The existing `chunk_hashes`, `FileChunkType`, `FileStorageType`, and
`FileChunkNode` scaffolding must not be repurposed. Initially it can remain for
on-disk compatibility. `prune` must stop treating dormant `chunk_hashes` as a
physical chunk catalog; a later FileNode format migration may remove it.

### 10.2 Required fields

`FileManifestV1` contains:

```rust
struct FileManifestV1 {
    logical_hash: LogicalFileHash,
    logical_size: u64,
    generation: u64,
    file_policy: FilePolicyId,
    chunking_policy: ChunkingPolicyId,
    chunk_count: u64,
    entries_per_page: u32,
    pages: Vec<ManifestPageRefV1>,
    digest: ManifestDigest,
}

struct ManifestPageRefV1 {
    first_chunk_index: u64,
    first_file_offset: u64,
    object_offset: u64,
    page_len: u32,
    entry_count: u32,
    page_digest: [u8; 32],
}

struct ChunkRefV1 {
    chunk_hash: ChunkHash,
    block_hash: BlockHash,
    block_entry_index: u32,
    raw_len: u32,
}
```

The ordered `raw_len` values must sum exactly to `logical_size`. The block entry
must repeat the same chunk hash and length. Recording both allows manifest-only
recipe negotiation and detects a reference to the wrong block entry.

`generation` is physical metadata used for safe replacement during future
repacking. It does not enter the logical file identity.

### 10.3 Paged fixed-width layout and authenticated range index

Manifests for multi-terabyte files can contain millions of chunks, so the
reader must not deserialize the entire object to satisfy a small range. V1 uses
an explicit little-endian binary layout:

```text
ManifestHeaderV1
ManifestPageRefV1[page_count]   # authenticated front directory
ManifestPageV1[page_count]      # up to 1024 fixed-width ChunkRefV1 entries/page
ManifestFooterV1                # whole-manifest BLAKE3 digest + footer magic
```

The header records byte offsets and lengths for all sections, the fixed entry
size, a BLAKE3 digest of the page directory, and a self-digest calculated with
the self-digest field zeroed. Each page-directory entry records its first chunk
index, first logical file offset, object byte range, entry count, and BLAKE3
digest of the complete page. The initial page size is 1,024 chunk references.

To locate a logical byte offset, a reader:

1. Fetches and verifies the header and complete page directory, or uses their
   verified cached form.
2. Binary-searches page `first_file_offset` values.
3. Range-reads the necessary page or pages and verifies each page digest.
4. Scans at most 1,023 raw lengths before reaching the target chunk.

At a 64 KiB average, a 1 TiB file has about 16.8 million chunks. Its sparse
page directory is roughly 1 MiB, and a seek reads a roughly 72 KiB manifest
page instead of a manifest exceeding 1 GiB. The full manifest remains streamable
for checkout and validation.

The trailing `ManifestDigest` is BLAKE3-256 over all preceding canonical
manifest bytes. Full reads and fsck verify it. A small random read verifies the
header, directory, and every page it consumes, then verifies referenced block
records and chunk hashes. It therefore does not need to read the entire manifest
to detect corruption capable of selecting the wrong valid chunk.

### 10.4 Publication and replacement

The manifest key is the logical XXH3-128 file hash. Publication is atomic:

- Local: write a sibling temporary file, sync it, then atomic rename.
- S3: complete one object PUT to the final key; readers observe the old or new
  complete object, never a partial write.

A normal ingest uses create-if-absent semantics. If the key exists, the store
compares logical size and the ordered chunk recipe. A different recipe is a
logical-hash collision and is rejected.

Maintenance repacking may replace a manifest with `generation + 1`. It writes
all new blocks first and keeps old blocks until every affected manifest is
published and verified. Repacking runs under maintenance mode, so normal readers
cannot race deletion of a block referenced by a manifest they already loaded.

## 11. Repository chunk index

### 11.1 Role

The index maps:

```text
ChunkHash -> ChunkLocation {
    block_hash,
    block_entry_index,
    raw_len,
}
```

Its only correctness-sensitive promise is that a returned location points to a
currently present, validated block entry with the same hash and raw length. A
manifest always stores its exact block location, so reads do not consult this
index after publication.

The index accelerates:

- chunk presence negotiation during push;
- reuse during local add and historical migration;
- local recipe resolution after fetch;
- deduplication statistics.

### 11.2 Backend and transactions

Use a repository-local RocksDB with fixed binary keys and values. Oxen already
depends on RocksDB, and one write batch per validated block is a natural fit.
All DB operations follow `docs/async_policy.md`: a complete lookup batch or
write batch runs inside one coherent `spawn_blocking` closure and no DB guard or
transaction crosses `.await`.

The first implementation stores one canonical location per chunk. If duplicate
physical copies exist, deterministic rebuild preference is:

1. A location referenced by a live manifest.
2. Otherwise the lexicographically smallest valid block hash.

The value format is versioned independently of the block format. It can later
hold multiple candidate locations or access statistics without changing
manifests.

### 11.3 Derived-state behavior

Blocks and manifests are authoritative. The index may be deleted at any time.
Rebuild performs two passes:

1. Stream live manifests and register their referenced chunk locations.
2. Scan remaining valid block headers and fill hashes that have no live
   location.

S3 rebuild lists repository block keys and range-reads only their headers before
payload validation is requested. A full rebuild is repository-wide maintenance,
which the product requirements permit.

Write ordering is:

```text
validate block -> publish block -> commit index write batch -> publish manifest
```

If the process crashes after block publication but before the index batch, the
block is a safe orphan and rebuild discovers it. If index data is lost after a
manifest publish, the manifest still reads correctly. The implementation must
never insert an index location before the corresponding block is durable.

## 12. Physical stores and paths

### 12.1 Traits

Use two narrow physical traits:

```rust
#[async_trait]
pub trait BlockStore: Debug + Send + Sync {
    async fn put_if_absent(
        &self,
        expected: BlockHash,
        body: BoxedByteStream,
        encoded_len: u64,
    ) -> Result<PutBlockResult, OxenError>;

    async fn head(&self, hash: BlockHash) -> Result<Option<BlockMeta>, OxenError>;
    async fn read_range(&self, hash: BlockHash, range: Range<u64>)
        -> Result<Bytes, OxenError>;
    async fn stream(&self, hash: BlockHash) -> Result<BoxedByteStream, OxenError>;
    async fn list(&self) -> Result<BoxStream<'static, Result<BlockHash, OxenError>>, OxenError>;
    async fn delete(&self, hash: BlockHash) -> Result<(), OxenError>;
}

#[async_trait]
pub trait ManifestStore: Debug + Send + Sync {
    async fn get_header(&self, file: LogicalFileHash) -> Result<Option<ManifestHeader>, OxenError>;
    async fn read_range(&self, file: LogicalFileHash, range: Range<u64>)
        -> Result<Bytes, OxenError>;
    async fn stream(&self, file: LogicalFileHash) -> Result<BoxedByteStream, OxenError>;
    async fn publish(&self, file: LogicalFileHash, manifest: ManifestSource,
                     mode: PublishMode) -> Result<(), OxenError>;
    async fn list(&self) -> Result<BoxStream<'static, Result<LogicalFileHash, OxenError>>, OxenError>;
    async fn delete(&self, file: LogicalFileHash) -> Result<(), OxenError>;
}
```

These are physical object operations. Full validation belongs to the shared
block/content engine so Local and S3 backends cannot diverge in semantics.

### 12.2 Local layout

Default local paths are siblings beneath `.oxen/versions`:

```text
.oxen/versions/
  files/                         # existing contiguous small/legacy versions
    ab/cdef.../data
  blocks/v1/
    ab/cdef...                   # 64-char BLAKE3 block key, prefix-sharded
  manifests/v1/
    ab/cdef...                   # 32-char XXH3 logical file key
  chunk-index/v1/                # RocksDB; rebuildable
  uploads/                       # resumable session staging
  migration/                     # state/checkpoints
```

`StorageConfig` should gain explicit optional block and manifest roots. When
unset, a `VersionPaths` helper derives the sibling paths from the configured
versions root. This keeps custom data-volume deployments predictable instead
of silently putting large blocks back under the repository's default `.oxen`
directory.

Local publication uses the existing `AtomicFile` conventions, verifies the
expected hash before rename, and never mutates a published file.

### 12.3 S3 layout

Under the existing repository namespace/prefix, use distinct versioned key
spaces:

```text
{repo-prefix}/blocks/v1/{first-two}/{remaining-block-hash}
{repo-prefix}/manifests/v1/{first-two}/{remaining-file-hash}
{repo-prefix}/uploads/v1/{session-id}/...
```

Small and legacy full versions keep their current keys. Block and manifest
objects live in S3; the rebuildable chunk index and maintenance state live with
the server's repository metadata. This matches the agreed single-authoritative-
server model.

The S3 implementation uses native async AWS operations, bounded concurrent
range requests, and idempotent put-if-absent behavior. A harmless duplicate PUT
is acceptable because the key is a verified content hash. Maintenance must not
depend on the index being backed up with S3 objects.

### 12.4 Object cache

The first release needs bounded caches, not another authoritative layer:

- verified manifest header/page-directory LRU;
- block-header LRU;
- optional decoded-chunk memory LRU keyed by `ChunkHash`;
- optional whole-block disk cache for S3, keyed by `BlockHash`.

Every cache has a byte budget, exposes hit/miss metrics, and can be deleted.
Loose cached chunks are not a persistent representation in v1.

## 13. Logical content-store facade

### 13.1 Preserve one logical API

Callers should not branch on full-blob versus block-backed storage. Introduce a
`RepositoryContentStore` (the concrete name can be refined during
implementation) that implements or sits directly behind `VersionStore`:

```rust
pub struct RepositoryContentStore {
    legacy: Arc<dyn VersionStore>,
    blocks: Arc<dyn BlockStore>,
    manifests: Arc<dyn ManifestStore>,
    chunk_index: Arc<dyn ChunkLocationIndex>,
    policies: Arc<PolicyRegistry>,
    caches: Arc<ContentCaches>,
}
```

`create_version_store` constructs the existing Local or S3 full-blob backend,
then composes it with matching block and manifest stores. Existing callers keep
an `Arc<dyn VersionStore>` while behavior becomes representation-aware.

The facade resolves a logical hash in this order:

1. A published, parseable manifest.
2. A legacy/small full blob.
3. Not found.

During a migration both representations may briefly exist. A verified published
manifest wins; the full blob remains an emergency fallback until migration
deletes it.

### 13.2 Policy-aware ingest descriptor

The current `store_version_from_reader(hash, reader, size)` has insufficient
context for type-specific policy. Add a descriptor-bearing entry point and
update every first-party write call site:

```rust
pub struct VersionDescriptor {
    pub logical_hash: LogicalFileHash,
    pub logical_size: u64,
    pub file: FileDescriptor, // data type, MIME, extension, relevant metadata
}

async fn put_version(
    &self,
    descriptor: &VersionDescriptor,
    reader: Box<dyn AsyncRead + Send + Unpin>,
) -> Result<PutVersionResult, OxenError>;
```

Keep a crate-private `put_legacy_blob` only for legacy migration/repair and the
small-file policy. It must not remain an easy public bypass for normal writes.

The important call sites include:

- `core/v_latest/add.rs`;
- branch and remote-checkout storage;
- workspace file and workspace commit paths;
- server multipart/browser uploads;
- client fetch installation;
- any code that synthesizes a new version after data-frame edits.

### 13.3 Range and seek primitives

Make logical ranges explicit rather than continuing to call them chunks:

```rust
async fn read_version_range(
    &self,
    hash: LogicalFileHash,
    range: Range<u64>,
) -> Result<Bytes, OxenError>;

async fn get_version_stream(
    &self,
    hash: LogicalFileHash,
) -> Result<BoxedByteStream, OxenError>;

async fn open_version(
    &self,
    hash: LogicalFileHash,
) -> Result<LogicalVersion, OxenError>;
```

`LogicalVersion` exposes size, async ranges, a sequential stream, and a
seekable adapter. Existing `get_version_chunk(hash, offset, size)` can delegate
temporarily to `read_version_range`, but new code and routes must use range or
segment terminology.

### 13.4 Semantics of existing methods

| Logical method | Block-backed behavior |
| --- | --- |
| `version_exists` | True when a published manifest exists; full closure is guaranteed at publish and audited by fsck. |
| `find_missing_versions` | Probe manifest keys and legacy full keys in bounded batches. |
| `get_version_size` | Read `logical_size` from the manifest header. |
| `get_version_stream` | Stream ordered manifest chunks with bounded prefetch. |
| `read_version_range` | Locate manifest entries, range-read encoded payloads, decode and slice. |
| `get_version` | Collect the logical stream; retain the existing warning that it is unsuitable for large files. |
| `copy_version_to_path` | Stream through `AtomicFile`, verify XXH3 while writing, and publish atomically. |
| `materialize` | Explicitly reconstruct to a temporary local file; never an implicit range fallback. |
| `list_versions` | Return the union of manifest logical hashes and full-blob hashes, deduplicated and sorted. |
| `delete_version` | Remove/tombstone the manifest or full blob only; shared blocks are reclaimed by GC. |
| `clean_corrupted_versions` | Validate representation-aware content and report affected logical files. |

Derived images, thumbnails, and similar artifacts remain under the existing
derived-file methods in v1. They can adopt block storage later if their size
profile warrants it.

### 13.5 Replace `VersionLocation` assumptions

A block-backed file has no contiguous filesystem or S3 location. Replace
callers' assumption with an access enum such as:

```rust
pub enum VersionAccess {
    Contiguous(VersionLocation),
    Logical { hash: LogicalFileHash, size: u64 },
}
```

`version_location` may remain for explicitly contiguous consumers, but it must
return a structured `RequiresLogicalReader` error for block-backed content, not
silently download the entire file. First-party read-only consumers should move
to stream/range/seek APIs. External tools that truly require a path continue to
call the explicit `materialize` method.

## 14. Rust module design

A pragmatic module split is:

```text
crates/liboxen/src/storage/
  version_store.rs             # logical trait and facade construction
  block/
    mod.rs
    hash.rs                    # typed BLAKE3 identities
    format.rs                  # canonical block parser/writer
    manifest.rs                # canonical paged manifest parser/writer
    policy.rs                  # registry and stable IDs
    fastcdc.rs                 # streaming generic chunker
    codec.rs                   # RawV1 and ZstdV1
    packer.rs                  # cross-file immutable block builder
    reader.rs                  # logical stream/range/seek reconstruction
    index.rs                   # ChunkLocationIndex + RocksDB implementation
    store.rs                   # BlockStore and ManifestStore traits
    local.rs                   # local physical implementations
    s3.rs                      # S3 physical implementations
    ingest.rs                  # version ingest and validation orchestration
    migration.rs               # resumable historical conversion
    gc.rs                      # manifest/block reachability
```

Avoid abstractions whose only purpose is predicting a hypothetical future.
The stable extension seams are:

- `BlockStore` / `ManifestStore` for Local and S3;
- `ChunkingStrategy` and `ChunkCodec` for format evolution;
- `ChunkLocationIndex` for a future distributed backend;
- a concrete, versioned block and manifest representation shared everywhere.

Shared code must perform hashing, parsing, encoding, reconstruction, and
validation. Client and server should not grow independent implementations of
the storage format.

## 15. Local add, commit, and server-side ingest

### 15.1 `oxen add`

For each file, as part of an operation-wide bounded ingest batch:

1. Existing add logic computes the XXH3 logical hash, size, type, MIME,
   extension, and metadata.
2. It constructs `VersionDescriptor` and selects the repository policy.
3. Small files call the existing atomic whole-blob path.
4. Eligible files stream through FastCDC. Each chunk is BLAKE3-hashed.
5. The local chunk index resolves reusable chunks.
6. Only locally missing chunks enter a cross-file `BlockPacker`.
7. New blocks are encoded, BLAKE3-verified, published, then indexed.
8. The manifest is built from existing and new locations.
9. A full logical reconstruction verifies size and XXH3 before atomic manifest
   publication.
10. The staged `FileNode` continues to reference only the existing logical hash
    and metadata.

The current add path already performs multiple file passes for hashing,
metadata, and storage. V1 may retain a second streaming pass for chunking if it
keeps the change smaller and memory bounded. A later optimization can combine
hashing and chunking once type-selection dependencies are clear.

Directory add should share one bounded pack session so missing chunks from
several files fill blocks efficiently. A single-file add flushes an underfilled
last block at operation completion. For a multi-file add, all recipes in the
bounded batch are chunked first, the tail block is flushed, and only then are
manifests that depend on that tail validated and published.

### 15.2 Commit

Commit behavior remains logical. It does not create chunks or blocks and does
not embed manifest data in Merkle nodes. It only requires that every staged
file's logical version exists through the content-store facade.

The old line that fills `FileNode.chunk_hashes` with the whole-file hash may
remain until a separately versioned FileNode cleanup, but no new storage code
may rely on it.

### 15.3 Workspaces, browser uploads, and generated versions

Server-originated writes cannot rely on client-side block construction. They
use the same shared `VersionIngestor` locally on the server:

- The HTTP body remains an async network stream.
- A bounded channel hands bytes to a coherent sync chunk/hash/compress worker.
- The worker emits encoded blocks through a bounded channel to the async Local
  or S3 publisher.
- Chunk index DB operations occur in complete blocking batches.
- A staged manifest is fully reconstructed and validated before publication.

This covers workspace file upload, workspace commits, tabular row/column edits,
remote imports, and any other `store_version_from_reader` caller. Leaving one of
these paths as an untracked large full blob would make repository behavior
depend on which API created the file.

## 16. Block-aware push protocol

### 16.1 Capability and repository state

Every push begins with a capability response containing at least:

```text
content_format: block-v1
minimum_client_version: <release that implements block-v1>
active_file_policy: generic-fastcdc-v1
chunk_profile: 8192 / 65536 / 131072
block_max_encoded_bytes: 67108864
supported_codecs: [RawV1, ZstdV1]
repository_state: ready | maintenance | migration-failed
```

An incompatible client or server fails before uploading bytes or changing a
ref. There is no legacy whole-file fallback for a block-enabled repository.
Upgraded software still reads legacy repositories and mixed representations
during controlled migration.

### 16.2 Refactor push into content then metadata

Current push uploads versions while iterating individual commits. Cross-file
packing and dedup negotiation work better with a repository content plan:

1. Compute missing commits and Merkle nodes as today.
2. Collect the unique logical file hashes referenced by all missing commits.
3. Satisfy all missing logical content in one resumable upload session.
4. Upload Merkle nodes and commit metadata.
5. Let the server verify every referenced logical version exists.
6. Advance the branch ref last.

This preserves Git-like object-before-ref ordering and enables packing across
files and commits without making blocks part of commit identity.

### 16.3 Session and recipe protocol

Use versioned endpoints under a distinct namespace; exact HTTP naming can
follow server conventions. The semantic operations are:

```text
POST  block/v1/uploads                         -> create resumable session
POST  block/v1/uploads/{id}/recipes            -> stream file recipes and negotiate chunks
PUT   block/v1/uploads/{id}/blocks/{blockHash} -> upload immutable block
POST  block/v1/uploads/{id}/finalize           -> validate and publish manifests
GET   block/v1/uploads/{id}                    -> resume/status/progress
DELETE block/v1/uploads/{id}                   -> abandon session
```

Control responses can use normal JSON views. Large recipes use a bounded binary
framing format rather than a JSON array containing millions of hashes.

A recipe contains only logical content, not client block locations:

```rust
struct FileRecipe {
    logical_hash: LogicalFileHash,
    logical_size: u64,
    policy: ChunkingPolicyId,
    chunks: Vec<RecipeChunk>, // streamed/batched on the wire
}

struct RecipeChunk {
    hash: ChunkHash,
    raw_len: u32,
}
```

The server stores session recipes in bounded staging files. Chunk lookup runs
in batches—initially around 16,384 hashes per DB operation—and responds with a
bitset or compact list of hashes missing from the repository index. Duplicate
hashes across recipes are requested only once.

The server does not expose or trust its physical block locations during push.
At finalize time it resolves every recipe hash through its own index and builds
the repository's canonical manifest. This cleanly permits different local and
remote packing.

### 16.4 Client behavior

For each server-missing logical file:

1. Ensure the local repository has a recipe under the required policy. An old
   upgraded clone containing only a legacy full blob can build a local manifest
   lazily before push.
2. Stream recipe hash batches to the server.
3. Collect the unique chunks the server reports missing.
4. Read those raw chunks from local blocks or the local logical reader.
5. Pack missing chunks across files into immutable blocks up to 64 MiB encoded.
6. Upload each block idempotently with its expected BLAKE3 hash.
7. Finalize the session after all missing chunks have a server location.

The client reports logical bytes considered, dedup hits, encoded bytes uploaded,
block count, and compression savings separately. Progress based solely on
logical file size would otherwise appear stalled when a mostly deduplicated
file uploads very little data.

### 16.5 Server block validation and registration

For every uploaded block, the server:

1. Streams into a bounded temp object while computing the full BLAKE3 hash.
2. Parses the front index and validates all bounds.
3. Confirms each chunk was requested as missing by that authenticated upload
   session; unrequested payload is rejected rather than accepted as free
   repository storage.
4. Decodes each payload under its raw-length limit and verifies each chunk hash.
5. Atomically publishes the block to Local or S3 storage.
6. Registers chunk locations in one index write batch.

An already present block with the same verified hash is success. A hash mismatch
or malformed block fails the session and never enters the index.

### 16.6 Full synchronous manifest validation

`finalize` resolves each recipe to server block locations, creates a staged
manifest, and then streams the complete logical file in order. During that one
pass the server verifies:

- manifest and block-reference consistency;
- every decoded BLAKE3 chunk hash;
- total reconstructed size;
- the declared FastCDC boundaries and ordered recipe;
- the existing XXH3-128 logical file hash.

Only then is the manifest atomically published. Validation is synchronous in
the correctness sense: publication and successful finalize never precede it.
The HTTP operation may expose progress or be represented as a polled job to
avoid proxy timeouts, but the job is not a deferred best-effort fsck.

The full pass adds server-side I/O. It is intentional: without it, a malformed
recipe could make a commit claim an XXH3 version that the block sequence does
not reproduce.

### 16.7 Failure and race behavior

- If upload stops before a block publish, only staging data exists.
- If it stops after block publish but before indexing, rebuild finds the orphan.
- If it stops after indexing but before manifest publish, valid unreferenced
  blocks remain and can serve a later upload.
- If two clients are told the same chunk is missing, both may upload it in
  different blocks. One location wins index canonicalization; either block can
  be referenced, and unreferenced duplicates are later collected.
- Finalize is idempotent. A retry compares an existing manifest's logical
  recipe and returns success if identical.
- Merkle and branch updates do not begin until content finalization succeeds.

The server is authoritative and singular for a repository, but it may serve
several client sessions concurrently. Block uploads and reads are concurrent;
index registration and manifest publication use short repository-local critical
sections. Migration and GC require the exclusive maintenance lease.

## 17. Block-aware fetch and pull

### 17.1 Fetch plan

Fetch continues to identify missing logical versions from the Merkle diff, but
content installation changes:

1. Query the server for representations of missing logical hashes.
2. Download small/legacy full blobs through the existing logical path.
3. For block-backed versions, fetch manifest headers and chunk-reference
   streams.
4. Compare recipe chunk hashes with the local chunk index.
5. Group only locally missing chunks by their remote block hash.
6. Download each required remote block once.
7. Verify and publish blocks locally, then index all their chunks—including
   useful chunks that were co-packed but not requested.
8. Resolve each downloaded recipe against local chunk locations. Existing local
   chunks may live in entirely different blocks than on the server.
9. Build and fully validate a local manifest before publication.
10. Checkout or materialize through the normal logical reader.

The server's physical manifest is therefore an efficient fetch plan, not a
requirement that every clone use the same block packing.

### 17.2 Transfer unit

Normal v1 pull downloads complete immutable blocks. This keeps request count
low, permits direct CAS installation, and makes every fetched chunk immediately
available for future repository-local deduplication.

Incremental pulls usually request newly created blocks only: chunks reused from
an earlier version are already local, while a small edit creates a small final
block unless it was packed with other changes in the same push. Worst-case
cross-file amplification is bounded by the 64 MiB block limit.

The protocol and `BlockStore::read_range` already permit a later sparse-fetch
mode: when utilization of a remote block is very low, fetch selected encoded
ranges and repack those chunks locally. V1 should collect utilization metrics
before adding that complexity.

### 17.3 Endpoints

The semantic read endpoints are:

```text
QUERY block/v1/fetch-plan       # logical hashes -> manifests/recipes
GET   block/v1/blocks/{hash}    # immutable bytes, ETag, HTTP Range support
GET   block/v1/manifests/{hash} # physical manifest, HTTP Range support
```

All requests remain repository-authenticated. S3-backed servers may proxy the
objects in v1. Presigned responses can be added later without changing block or
manifest formats.

### 17.4 Resume and verification

Block hashes make downloads naturally resumable at object granularity. A temp
block is never installed until its complete BLAKE3 and every decoded chunk hash
verify. An interrupted fetch can retain verified blocks and repeat the plan;
the local index eliminates them from the missing set.

The client performs a complete reconstruction/hash validation before publishing
each local manifest, mirroring the server. Checkout writes remain atomic and
verify the logical XXH3 hash in passing.

## 18. Logical streaming, range, and seek reads

### 18.1 Range algorithm

For logical range `[start, end)`:

1. Validate the range against manifest `logical_size`.
2. Use the verified cached manifest page directory to locate the first and last
   chunk-reference pages.
3. Fetch and verify the necessary fixed-width manifest pages.
4. Group required chunks by block hash.
5. Load/cache each block's front index.
6. Map entries to encoded payload ranges.
7. Coalesce nearby payload ranges within a block under bounded gap and request
   limits.
8. Read Local ranges with positional I/O or issue bounded parallel S3/HTTP
   ranges.
9. Decode complete intersecting chunks, verify BLAKE3, discard prefix/suffix
   bytes outside the logical request, and emit bytes in file order.

Read amplification is bounded by:

- verified manifest header/page-directory metadata on a cold read;
- authenticated fixed-width chunk-reference pages;
- block headers on a cold block;
- at most the two partial boundary chunks plus requested complete chunks;
- configurable, bounded read-ahead and coalescing gaps.

It is never the entire block merely because compression is enabled.

### 18.2 Sequential stream

`get_version_stream` walks manifest entries in order and maintains a bounded
prefetch window. Reused chunks may point to older blocks in nonsequential order,
so it groups near-future reads where possible without buffering an unbounded
portion of the file. Every emitted chunk is verified.

### 18.3 `Read + Seek` adapter

Many Rust consumers, including eager Parquet and IPC readers, expect synchronous
`Read + Seek`. Implement `SeekableVersionReader` with a cursor and bounded
read-ahead cache:

- Local blocks use direct positional file reads.
- S3 blocks use an async range worker connected to the sync reader by bounded
  request/response channels.
- The consumer itself runs in one `spawn_blocking` operation, so blocking while
  the async worker obtains an S3 range does not block a Tokio runtime worker.
- `seek` updates the logical cursor without materialization; the next `read`
  resolves only its range.

This follows the repository's sync-core/async-edge policy more cleanly than
calling `Handle::block_on` for every S3 range.

### 18.4 Tabular integration

`core/df/tabular.rs::read_version_df` must stop assuming every S3 version has a
single `s3://.../data` object:

- Parquet and IPC use `SeekableVersionReader`, preserving footer and column
  range access.
- CSV, TSV, and JSONL use the logical sequential stream, with a bounded head
  range for dialect/schema sniffing.
- JSON retains its explicit full-byte behavior only where the parser requires
  it, with existing size cautions.
- Predicate/projection behavior should be tested against the current Polars
  path. If the eager `Read + Seek` API loses an important pruning capability,
  add an object-store-compatible logical range adapter rather than falling back
  to full materialization.

The current public Polars cloud scan constructs its own object store from a URL,
so it cannot directly consume Oxen's internal block store. The seek adapter is
the pragmatic v1 integration. A logical HTTP endpoint with `Accept-Ranges`,
`Content-Length`, and the XXH3 ETag can additionally serve DuckDB, Polars, and
other external engines that understand authenticated HTTP ranges.

Path-only tools such as some ffmpeg or DuckDB invocation modes may still call
the explicit `materialize` API. That is a declared compatibility fallback, not
the implementation of core range/seek reads.

### 18.5 HTTP file behavior

Existing file and version download endpoints remain logical:

- full GET streams reconstructed bytes;
- `Range` requests call `read_version_range` and return correct 206 responses;
- `Content-Length` is the logical file size;
- `ETag` is the logical XXH3 hash;
- errors identify a corrupt/missing block and affected logical version without
  leaking repository-external data.

Thus callers that do not know about blocks still receive exact file bytes, as
long as they are otherwise compatible with the upgraded server API.

## 19. Repository activation and historical migration

### 19.1 Format and capability state

Block storage is a repository-format feature, not a client preference. Extend
repository configuration with a storage content format and stable policy ID:

```toml
[storage]
kind = "local"                 # or "s3"
content_format = "block-v1"    # "legacy" before migration

[storage.block]
file_policy = "generic-fastcdc-v1"
min_file_size = 10485760
```

Only stable named policies are accepted. Arbitrary FastCDC or codec numbers in
config would create unversioned formats the server cannot reliably validate.
Future policy IDs may coexist in old manifests.

Activating `block-v1` bumps the repository's `MinOxenVersion` to the release
that supports it. The current enum collapses supported repositories to
`LATEST`; implementation needs a distinct block-capable format value and an
actionable unsupported-version error.

New repositories can default to `block-v1` once the feature clears its release
gates. Existing repositories do not flip a flag in place; they run the explicit
migration.

### 19.2 Durable migration state machine

Store an atomically updated state record outside normal config:

```text
Legacy
  -> Preparing { target_policy, inventory_digest }
  -> Migrating { target_policy, cursor, counters, last_error }
  -> Verifying { target_policy, inventory_digest }
  -> BlockV1 { target_policy, completed_at }
```

An interrupted `Preparing`, `Migrating`, or `Verifying` repository remains in
maintenance mode. `--resume` continues idempotently. The state record includes
enough context to reject a resume with a different target policy.

Normal API operations return a structured maintenance response with progress
and recovery instructions. Administrative status, resume, abort-to-repair, and
fsck operations remain available.

### 19.3 Maintenance lease

Add a repository-wide content maintenance lease:

- an in-process `RwLock` or equivalent coordinates handlers in the single
  server;
- an advisory lock file coordinates CLI/server processes sharing the repository
  path;
- a durable state record survives process death;
- migration, full index rebuild, block GC, and repack take the exclusive lease;
- ordinary reads/writes are rejected while the durable state is not ready.

The single-server product decision avoids a distributed S3 lease. The trait and
state boundaries should make that limitation explicit rather than accidentally
appearing multi-node safe.

### 19.4 Inventory

Migration snapshots all logical content roots while writes are blocked:

- every branch, tag, and other ref;
- all commits reachable under the repository's retention rules;
- live workspaces;
- staged/index content that the repository promises to preserve;
- explicitly pinned in-progress imports or upload sessions.

Deduplicate the inventory by logical file hash. Collect file descriptors from
all referencing `FileNode`s. Conflicting type descriptors use the generic policy.

Only reachable eligible versions are migrated. Unreachable legacy blobs are
reported and left for prune/GC rather than spending CPU and storage preserving
garbage. Files below 10 MiB remain valid whole-file CAS objects by policy; “all
historical blobs” means all reachable historical blobs eligible for block-v1.

### 19.5 Per-version migration algorithm

Process unique eligible versions in bounded batches so the packer can combine
chunks across files without retaining unbounded unpublished state:

1. Skip and checkpoint any version with an already validated manifest.
2. Confirm each remaining legacy full blob matches logical size/hash and select
   its deterministic file policy.
3. Stream the batch through FastCDC and BLAKE3, consulting the repository chunk
   index.
4. Feed only missing chunks into the batch's bounded cross-file packer.
5. Publish full blocks as they fill; after the batch is chunked, flush its
   underfilled tail. Index each block only after durable publication.
6. For each file, build a staged manifest now that all of its locations are
   durable.
7. Reconstruct through the block reader and verify manifest digest, every chunk,
   total size, policy boundaries, and XXH3 logical hash.
8. Atomically publish the manifest.
9. Perform one final representation-aware read check.
10. Delete that file's legacy full blob.
11. Atomically advance its checkpoint and counters.

The old blob is never deleted before a verified manifest is durable. Blocks may
be shared with versions migrated earlier or later. A bounded batch flushes its
underfilled tail at checkpoints so a crash never depends on an in-memory block.

Deleting each old blob after successful conversion limits extra storage, but
migration still needs enough headroom for the current file/batch's encoded
blocks before that file can be retired. A dry run reports logical bytes,
estimated block bytes, currently reusable chunks, and required safety margin.

### 19.6 Final verification and cutover

After the inventory cursor finishes:

1. Re-scan roots and verify the inventory digest did not change.
2. Confirm every reachable eligible hash has a valid manifest.
3. Confirm every reachable small hash has a valid full blob.
4. Run full manifest/block closure checks.
5. Rebuild and compare the chunk index.
6. Atomically set repository format/minimum version to block-v1 and state to
   `BlockV1`.
7. Release maintenance mode.

Because writes were blocked, no commit can fall between inventory and cutover.

### 19.7 Recovery and reverse migration

Resume is the normal crash recovery. Published blocks and manifests make every
step idempotent; orphan blocks are harmless.

Once old blobs have been deleted, “abort” cannot simply toggle back to legacy.
A reverse migration command must reconstruct each eligible full blob, verify
its XXH3 hash, publish it atomically, and only then remove manifests/blocks under
GC. This also runs in maintenance mode. The UI must state this clearly before a
forward migration begins.

### 19.8 Server and clone ordering

The authoritative remote migrates independently of existing clones. After
cutover:

- an old client is rejected;
- an upgraded legacy clone can read its full local blobs and lazily create
  block recipes needed to push;
- a normal local migration converts all of that clone's reachable eligible
  history;
- fetch installs local blocks/manifests regardless of how the remote packed
  them.

There is no requirement that block hashes or manifest physical references be
identical across clones.

## 20. Garbage collection and repacking

### 20.1 Why refcounts are not authoritative

Blocks contain chunks shared by multiple versions and files. Updating a durable
refcount transactionally with every manifest publication would make the mutable
index part of correctness and complicate crash recovery. Immutable objects plus
periodic mark-and-sweep are simpler and align with the repository maintenance
model.

### 20.2 Mark roots

Under the exclusive maintenance lease, GC marks:

1. Logical hashes reachable from all retained refs/commits.
2. Staged, workspace, import, and explicitly pinned session hashes.
3. Small/legacy whole blobs for those logical hashes.
4. Manifests for block-backed logical hashes.
5. Every block hash referenced by each marked manifest.
6. Blocks/manifests pinned by active migration or upload checkpoints.

`FileNode.chunk_hashes` is ignored. The manifest graph is the physical source of
truth.

### 20.3 Sweep

GC removes, or reports under `--dry-run`:

- unreferenced logical manifests;
- unreferenced legacy whole blobs;
- blocks not referenced by a marked manifest or active pin;
- expired upload-session staging;
- temp files from interrupted local publication;
- stale index entries.

Unreferenced published objects receive a default 24-hour grace period before
deletion. The grace covers recently aborted pushes and gives operators a window
to diagnose a bad ref update. Maintenance state can configure a longer period,
but shortening it below the documented safe default should require an explicit
force flag.

After sweep, rebuild or repair the derived chunk index. `delete_version` does
not synchronously delete shared blocks.

### 20.4 Partially live blocks

If any marked manifest references a block, v1 retains the complete block. This
can leave dead chunks in otherwise live blocks after history pruning. Expose
per-block live-byte utilization in GC output so the cost is visible.

Future repack uses the existing physical indirection:

1. Select low-utilization blocks under maintenance mode.
2. Pack their live chunks into new blocks.
3. Publish and index new blocks.
4. Rewrite and fully validate affected manifests with incremented generations.
5. Delete old blocks only after no manifest references them.

No commit or `FileNode` changes.

## 21. Fsck, corruption, and repair

### 21.1 Fsck levels

Provide representation-aware levels:

- **Quick**: parse manifest headers/checksums, verify referenced block existence
  and basic header consistency, and validate small/full blob hashes where cheap.
- **Full**: BLAKE3 every complete block, decode and hash every chunk, verify
  every manifest digest and reference, reconstruct every logical file to
  XXH3-128, and compare the chunk index with authoritative data.

Both levels report logical hashes and paths/commits affected, not only a block
hash an operator cannot map back to a file.

### 21.2 Repairable cases

Safe automatic repairs include:

- rebuild a missing or corrupt chunk index;
- remove stale index locations;
- discard expired temp/session objects;
- choose another valid duplicate chunk location and rewrite a manifest under
  maintenance mode;
- restore a missing client block from an authenticated remote and revalidate.

A corrupt live block with no valid duplicate or remote source is data loss.
Fsck must not hide it by deleting the manifest. It reports every affected
logical file and prevents branch validation from claiming the content is
healthy.

### 21.3 Existing clean/revalidate flows

`clean_corrupted_versions` and `push --revalidate` become representation-aware:

- quick/full checks distinguish corrupt manifest, missing block, corrupt block,
  corrupt chunk, and reconstructed logical-hash mismatch;
- a client can re-negotiate and upload missing/corrupt chunks or blocks;
- repaired content is fully validated before a manifest is republished.

## 22. Crash consistency and publication order

The complete durability chain is:

```text
raw bytes
  -> validated immutable blocks
  -> durable block objects
  -> derived chunk-index registration
  -> staged manifest
  -> full reconstructed validation
  -> atomic manifest publication
  -> Merkle/commit publication
  -> branch/ref update
```

This ordering provides the following guarantees without a transaction spanning
RocksDB, filesystem/S3, Merkle storage, and refs:

| Crash point | Durable state | Recovery |
| --- | --- | --- |
| While forming a block | Temp data only | Delete temp or resume session. |
| After block, before index | Valid orphan block | Rebuild index or reuse on retry. |
| After index, before manifest | Valid unreferenced block | Retry finalize or GC after grace. |
| During staged manifest validation | Existing logical state unchanged | Discard stage and retry. |
| After manifest, before commit metadata | Valid orphan logical version | Future push reuses it or GC collects it. |
| After commit, before ref | Valid unreachable commit/content | Existing push recovery applies. |
| After ref | Complete reachable state | Normal operation. |

Local filesystem writes use sync-and-rename semantics consistent with
`AtomicFile`. S3 object PUT completion is the publication boundary. The
manifest is the only physical object whose key may be replaced during
maintenance repack, and that replacement is a complete atomic object.

## 23. Security and resource limits

Block and recipe endpoints process untrusted client input. Enforce limits before
allocation or decompression:

- authenticated repository authorization on lookup, upload, fetch, and range
  endpoints;
- no cross-repository chunk-presence query;
- 64 MiB maximum encoded block including metadata;
- 128 KiB maximum raw chunk for `generic-fastcdc-v1`;
- maximum entry count derived from minimum legal record/object sizes;
- checked offset, length, and sum arithmetic;
- decoder output buffer fixed to declared validated raw length;
- bounded recipe batch and session staging sizes;
- per-user/repository concurrent session, upload-byte, and validation limits;
- session expiration and explicit cancellation;
- BLAKE3 verification before content-addressed publication;
- full XXH3 and policy verification before manifest publication.

Repository-scoped unkeyed BLAKE3 hashes are safe for content integrity, but
presence responses can reveal whether authorized collaborators stored a chunk.
The endpoint must never be callable outside the repository's authorization
boundary. Global deduplication would require a separate threat model, tenant
namespacing, and likely keyed lookup identities.

## 24. Concurrency, async boundaries, and memory

Follow `docs/async_policy.md`:

- FastCDC, BLAKE3, compression, block parsing, local filesystem operations, and
  RocksDB batches form the synchronous core.
- AWS and HTTP calls remain native async operations.
- One coherent `spawn_blocking` wraps a local ingest/validation unit; do not
  spawn one blocking task per 64 KiB chunk.
- CPU-parallel batches use Rayon under a bounded file/block memory budget.
- Streaming sync/async crossings use bounded channels and backpressure.
- No RocksDB handle/transaction guard crosses `.await`.
- S3 range reads use bounded concurrent requests and ordered reassembly.

Initial process budgets should be explicit configuration with conservative
defaults:

```text
maximum encoded bytes per in-memory block: 64 MiB
default simultaneous in-memory block builders: 2
default recipe lookup batch: 16,384 chunk hashes
manifest entries per authenticated page: 1,024 chunks
bounded decoded read-ahead: 8 chunks per logical stream
```

Temp-file-backed builders can raise throughput without multiplying RAM. Metrics
must report time blocked on packer/channel backpressure; otherwise memory limits
will look like unexplained slowness.

## 25. Observability and user-facing operations

### 25.1 Statistics

Expose repository and per-operation metrics that separate deduplication from
compression:

```text
logical_bytes
unique_raw_chunk_bytes
encoded_block_bytes
manifest_bytes
block_count / chunk_count / underfilled_block_count
dedup_ratio       = logical_bytes / unique_raw_chunk_bytes
compression_ratio = unique_raw_chunk_bytes / encoded_block_bytes
physical_ratio    = logical_bytes / (encoded blocks + manifests)
push lookup hits/misses and encoded bytes uploaded
fetch logical bytes requested and block bytes downloaded
range bytes requested vs manifest/header/payload bytes fetched
validation bytes read and duration
index/cache hit rates
orphan and low-utilization block bytes
```

Do not claim CDC savings as compression savings or vice versa.

### 25.2 Commands and administrative surfaces

Names can follow CLI conventions, but v1 needs equivalent operations to:

```text
oxen storage status
oxen storage stats
oxen storage migrate --to block-v1 [--dry-run] [--resume]
oxen storage migrate --to legacy [--dry-run]
oxen storage fsck [--full] [--repair-index]
oxen storage gc [--dry-run] [--grace 24h]
oxen storage rebuild-index
```

Server admin APIs expose the same state machine and progress. Long operations
must be resumable and observable instead of relying on one HTTP connection
remaining open.

### 25.3 Save/load and backup

For default Local storage, authoritative blocks and manifests live under
`.oxen` and should be included in `repositories/save.rs` archives. The RocksDB
chunk index is derived: exclude it from the archive and rebuild on load, or use
a point-in-time snapshot. Exclusion is simpler and avoids archiving live DB
files unsafely.

Upload staging, caches, lock files, and transient migration scratch data are
excluded. A repository in active migration cannot be saved without an explicit
consistent snapshot operation.

Current custom external version roots and S3 storage are not automatically made
self-contained by archiving `.oxen`; block storage should preserve that
existing semantic. A future `save --include-content` can stream external
authoritative objects into a portable archive.

## 26. Implementation plan

The feature should land behind repository-format activation, but each phase
must be independently testable. Do not activate block-v1 until the complete
Local/S3 and push/fetch vertical path is present.

### Phase 0: measurements and golden decisions

1. Add representative benchmark corpora: append/prepend/middle edits for CSV
   and JSONL, Parquet rewrites, already-compressed media, and random bytes.
2. Benchmark the `fastcdc` crate's v2020 implementation in streaming form and
   confirm the 8/64/128 KiB profile.
3. Benchmark Zstd level 1 per chunk and validate the raw-fallback threshold.
4. Freeze block/manifest v1 layouts with hand-written fixtures and expected
   BLAKE3 hashes.

Exit: parameter choices have reproducible data and format fixtures.

### Phase 1: format and local storage core

1. Add BLAKE3, FastCDC, and Zstd as explicit direct dependencies with licenses
   reviewed; do not rely on transitive lockfile presence.
2. Implement typed hashes, bounded parsers/writers, codecs, streaming chunker,
   and `BlockPacker`.
3. Implement fixed-width authenticated paged manifests.
4. Implement Local `BlockStore`/`ManifestStore` and RocksDB index.
5. Implement logical streams and ranges plus exact reconstruction.

Exit: arbitrary bytes round-trip locally; random ranges match a contiguous
reference; index deletion/rebuild preserves behavior.

### Phase 2: logical facade and ingestion

1. Compose `RepositoryContentStore` behind `VersionStore`.
2. Add `VersionDescriptor` and policy-aware `put_version`.
3. Update add, branch, remote checkout, workspace, generated-data, and server
   ingest call sites.
4. Keep small/legacy full representations working in the same repository.
5. Update checkout/copy/materialize/list/delete semantics.

Exit: Local add/commit/checkout and workspace writes use blocks for every
eligible file without changing Merkle hashes.

### Phase 3: efficient readers

1. Implement block-aware logical range APIs and caches.
2. Implement `SeekableVersionReader` with Local and async-worker adapters.
3. Refactor tabular readers, file endpoints, CSV sniffing, diffs, and other
   `version_location` consumers.
4. Add HTTP Range behavior and logical ETags.

Exit: Parquet footer/column reads do not materialize the full file and range
amplification stays within the acceptance bounds.

### Phase 4: S3 parity

1. Implement S3 block and manifest stores, atomic publication, listing, ranges,
   and deletion.
2. Exercise ingest, read, seek, copy, and full validation against the existing
   S3-compatible test fixture.
3. Add bounded range coalescing, retries, and cache metrics.

Exit: the Local/S3 conformance suite has identical logical behavior and
corruption detection.

### Phase 5: block-aware push and fetch

1. Add capability/minimum-version checks and maintenance responses.
2. Add upload sessions, binary recipe batches, chunk lookup, block upload, and
   synchronous finalization.
3. Refactor push into unique-content then metadata/ref phases.
4. Add fetch plans, manifest transfer, missing-chunk comparison, whole-block
   download, and local manifest construction.
5. Cover resume, duplicate/racing clients, shallow histories, missing-files,
   revalidate, and progress reporting.

Exit: a second version with a small insertion uploads and fetches only new
chunks/blocks, while a fresh clone reconstructs exact files.

### Phase 6: migration and maintenance

1. Add repository format/config state and maintenance lease.
2. Implement inventory, dry-run, checkpointed forward/reverse migration, and
   final cutover verification.
3. Make prune/GC manifest-aware with grace periods.
4. Extend fsck/clean/revalidate and index rebuild.
5. Update save/load exclusions and rebuild behavior.

Exit: interruption at every checkpoint resumes safely, and no eligible
historical reachable full blob remains after cutover.

### Phase 7: release gate and documentation

1. Run storage/network/range benchmarks against current full-blob behavior.
2. Soak on repositories with large histories and both storage backends.
3. Document capacity planning, migration headroom, rollback cost, metrics, and
   old-client rejection.
4. Make block-v1 the default for new repositories only after the release gate
   is met.

## 27. Code touchpoint checklist

This is a navigation aid, not an assertion that every file needs a large edit:

- `crates/liboxen/src/storage/version_store.rs`
- `crates/liboxen/src/storage/local.rs`
- `crates/liboxen/src/storage/s3.rs`
- new `crates/liboxen/src/storage/block/*`
- `crates/liboxen/src/model/repository/local_repository.rs`
- `crates/liboxen/src/core/versions.rs`
- `crates/liboxen/src/core/v_latest/add.rs`
- `crates/liboxen/src/repositories/commits/commit_writer.rs`
- `crates/liboxen/src/core/v_latest/push.rs`
- `crates/liboxen/src/core/v_latest/fetch.rs`
- `crates/liboxen/src/api/client/versions.rs`
- `crates/liboxen/src/api/client/entries.rs`
- `crates/oxen-server/src/controllers/versions.rs`
- `crates/oxen-server/src/controllers/file.rs`
- `crates/oxen-server/src/controllers/commits.rs`
- workspace file/commit/upload code paths
- `crates/liboxen/src/core/df/tabular.rs`
- `crates/liboxen/src/core/v_latest/prune.rs`
- `crates/liboxen/src/repositories/fsck.rs`
- `crates/liboxen/src/repositories/save.rs`
- `crates/liboxen/src/repositories/load.rs`

The unused fixed-size `file_chunker.rs`, `io/chunk_reader.rs`, and
`FileChunkNode` should be removed or clearly deprecated in a separate cleanup
once no diagnostic code relies on them. The stable numeric Merkle node type
value for `FileChunk` must remain reserved even if its Rust variant is later
removed from active construction.

## 28. Test strategy and acceptance criteria

### 28.1 Unit and golden tests

- Strict typed-hash parsing and noninterchangeability.
- FastCDC golden boundaries for fixed byte fixtures.
- Boundary stability after prefix, middle, and suffix insertion/deletion.
- Raw/Zstd codec round trips and fallback decisions.
- Canonical block bytes and fixed expected BLAKE3 hash.
- Canonical manifest bytes, paged lookup, per-page verification, and fixed
  digest.
- Checked parser failures for every malformed length/offset/version/codec.
- Manifest range lookup at first byte, chunk boundaries, final byte, and EOF.
- Index insert, duplicate location, stale location, deletion, and rebuild.

### 28.2 Property and fuzz tests

- Arbitrary input bytes reconstruct exactly and preserve XXH3.
- Arbitrary valid logical ranges equal slices of the original bytes.
- Any single-bit block mutation is detected by block and/or chunk validation.
- Manifest mutation cannot silently return wrong bytes.
- Decoder never allocates or emits beyond declared bounded raw size.
- Block and manifest parsers tolerate arbitrary untrusted bytes without panic,
  overflow, or excessive allocation.
- Random sequences of duplicate chunks and cross-file packing resolve correctly.

### 28.3 Integration matrix

Run each applicable scenario against Local and S3:

- add/commit/checkout one eligible file;
- small and large files in one commit;
- same chunks across versions;
- same chunks across different paths/files;
- insert/delete/append/prepend/middle edit;
- incompressible and already-compressed data;
- cross-file blocks and underfilled tail blocks;
- branch, merge, restore, workspace, remote upload, and generated dataframe;
- push to empty remote, incremental push, interrupted/resumed push, and retry;
- concurrent clients reporting/uploading the same missing chunk;
- fresh clone, incremental pull, subtree/shallow fetch, and missing-files repair;
- full HTTP download and HTTP ranges;
- Parquet/IPC seek reads and CSV/JSONL streams;
- save/load, prune, fsck, index deletion/rebuild, and GC;
- forward migration, crash at every step, resume, and reverse migration;
- rejected old client/server and maintenance-mode API behavior.

### 28.4 Failure injection

Inject process failure or returned errors:

- while writing a local temp block;
- after Local/S3 block publish but before index commit;
- after index commit but before manifest stage;
- during full manifest reconstruction;
- after manifest publication but before legacy deletion;
- after legacy deletion but before migration checkpoint;
- before/after Merkle upload and before branch ref update;
- during block fetch and local publication;
- while GC has marked but before sweep.

Every case must result in either the old complete logical representation or the
new complete one, plus at worst reclaimable orphans.

### 28.5 Performance and behavior gates

The first production release should meet these qualitative and measurable
gates:

1. **Exactness:** every reconstructed file has the original byte length and
   XXH3-128 hash.
2. **Incremental network:** after a localized edit to a large file already on
   both sides, client upload/download consists of recipe metadata plus newly
   missing chunks packed into blocks—not the entire logical file.
3. **Range behavior:** a small logical range fetches manifest/block metadata and
   overlapping chunks, never the entire file or 64 MiB block. For a warm-header
   read, payload amplification should be no more than the two boundary chunks,
   configured read-ahead, and configured coalescing gaps.
4. **Bounded memory:** ingest memory is `O(block_builder_count * 64 MiB)` plus
   explicit bounded channels/caches, independent of file size.
5. **Object count:** well-filled sessions approach one data object per 64 MiB
   encoded rather than one object per 64 KiB chunk.
6. **Incompressible behavior:** raw fallback avoids material expansion beyond
   small block/manifest metadata.
7. **Recovery:** index deletion, process interruption, and retried protocol
   operations never make published logical content unrecoverable.
8. **Backend parity:** Local and S3 conformance tests return identical logical
   bytes and error classes.
9. **Migration:** cutover leaves zero reachable eligible versions represented
   only by legacy blobs and preserves every pre-migration commit ID.

Benchmark reports should show client network bytes, S3 bytes/requests, CPU,
wall time, peak RSS, object count, logical/dedup/compression ratios, and range
amplification. A compression ratio alone is insufficient evidence.

## 29. Alternatives considered and rejected

### One CAS object per chunk

Rejected because a 64 KiB average creates enormous filesystem/S3 object counts,
indexes, and HTTP request volume. Blocks preserve chunk-level equality without
chunk-level object overhead.

### Fixed-size chunks

Rejected as the default because an insertion shifts every downstream boundary.
FastCDC preserves alignment around local edits.

### Block-hash-only push lookup

Rejected because equal chunks packed differently produce different blocks.
True chunk lookup is necessary for repository-wide deduplication across time
and files.

### Server-side packing for normal client push

Rejected for v1 because it requires uploading/staging individual raw chunks or
many small requests. Client packing sends only missing bytes in storage-sized
objects. Server-side writers still use the same packer locally.

### Whole-block compression

Rejected because a small range would require downloading and decompressing an
entire block. Independently encoded chunks are the range-preserving compromise.

### Physical references in `FileNode`

Rejected because representation differs by repository and changes under
migration/repack. It would rewrite logical history and bloat Merkle sync.

### Per-chunk Merkle nodes

Rejected because they conflate logical and physical graphs, multiply node count,
and enter unused/incomplete code paths.

### Authoritative mutable chunk index or refcounts

Rejected because correctness would depend on coordinating DB mutations with S3,
manifest, commit, and ref publication. Rebuildable indexes plus mark-and-sweep
have simpler failure states.

### Background-only server validation

Rejected because a commit could become visible before the server knows its
manifest reproduces the claimed logical file. Full validation is in the publish
critical path.

### Always materialize block-backed files

Rejected because it destroys efficient S3/Parquet seeks, adds disk pressure,
and makes multi-gigabyte reads pay full-file I/O for small queries.

### Transparent legacy protocol fallback

Rejected by product decision. A block-enabled repository requires upgraded
client and server behavior; silently pushing full files would weaken the
repository's storage and validation guarantees.

### New-writes-only adoption

Rejected by product decision. Historical eligible versions migrate under
maintenance mode so repository compression and deduplication apply to existing
history.

### Format-aware reserialization in v1

Rejected because equivalent data is not necessarily identical committed bytes.
Generic byte CDC is safe. Future type-aware encodings must be reversible and
versioned.

## 30. Future extensions enabled by the design

- CSV/JSONL boundary policies that prefer complete record boundaries while
  retaining a maximum-size escape hatch.
- Writer-integrated Parquet page/row-group chunking and type-aware reversible
  codecs.
- Zstandard dictionaries or numeric byte-grouping codecs with stable IDs.
- Sparse fetch that range-downloads low-utilization remote blocks and repacks
  selected chunks locally.
- Utilization-based repository block repacking.
- Presigned direct S3 block transfer.
- Key-chunk/Bloom-filter lookup acceleration for enormous recipes.
- Multiple candidate locations per chunk and locality-aware selection.
- A distributed chunk-index/lease backend for multiple server nodes.
- Tenant-aware global deduplication with a separate privacy/security model.
- A future cryptographic logical file identity, introduced as a repository
  format migration rather than conflated with this physical-storage change.

## 31. Definition of done

Block-level deduplication is complete only when all of the following are true:

- Large eligible files ingest into FastCDC chunks and immutable compressed
  blocks on both Local and S3 backends.
- Merkle and commit identities are unchanged by physical conversion.
- Push performs server chunk lookup and client block construction, uploads only
  missing chunks, and publishes content only after full server validation.
- Fetch compares logical recipes against the local chunk index, transfers
  missing blocks, validates them, and creates repository-local manifests.
- Streams, copies, full downloads, HTTP ranges, Parquet/IPC seeks, and checkout
  reproduce exact bytes without implicit full materialization for range-capable
  consumers.
- Historical reachable eligible blobs migrate resumably under maintenance mode
  and old blobs are deleted only after verified manifest publication.
- Prune/GC, fsck, index rebuild, save/load, missing-files, and revalidate
  understand shared blocks.
- Old clients and servers are rejected before mutation on block-enabled repos.
- Crash-injection, Local/S3 integration, fuzz, and benchmark gates pass.
- Operators can observe logical, dedup, compression, network, range, migration,
  and garbage metrics separately.

Until these conditions hold, the feature should remain unavailable for
production repository activation even if the local block encoder itself works.
