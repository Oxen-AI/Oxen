# Block-Level Dedup — Implementation Quiz (Answer Key)

For the quizzing agent: each answer lists **Required** points (all must be
covered, in any wording, for full credit) and **Bonus** points (deeper
understanding; mention them when revealing the answer). Code references are
for you to point at during discussion, not something the quiz-taker must
recite.

---

**A1.**
Required:
- *Chunk*: a content-defined slice of a file (~64 KiB target, 8–128 KiB
  bounds), identified by xxh3-128 of its raw bytes — the unit of
  **deduplication**.
- *Block*: an immutable pack of (possibly compressed) chunk payloads, ≤64 MiB,
  content-addressed by xxh3-128 of the block bytes — the unit of **storage and
  transfer**.
- *Manifest*: the ordered `(chunk_hash, offset, len)` list describing one file
  version, keyed by the file's whole-content hash.
- They differ because per-chunk objects don't scale: metadata, object counts,
  and request counts that grow 1:1 with ~64 KiB chunks are ruinous (the
  Hugging Face/Xet "from chunks to blocks" lesson). Dedup wants small units;
  storage/networking want large ones.
Bonus: quoting the design's summary — "deduplication is a performance
optimization; never let chunk count drive your storage or network schema."

**A2.**
Required: the legacy path moves fixed 10 MiB slices of large files during
upload/download (`stream_segment_size()`); the design reserves the term
**segment** for it. It has nothing to do with content-defined chunks.
Bonus: the plan schedules renaming those APIs (`store_version_segment`, new
`/segments` routes added additively) in a later cleanup.

**A3.**
Required:
- Placement lives only in the **store-local LMDB chunk index**
  (`chunk_hash → block_hash, offset, stored_len, raw_len, codec`).
- Benefits (any two): same bytes ⇒ same manifest everywhere, so manifests
  dedup themselves and are identical across stores; repack/GC can move chunks
  between blocks without rewriting any manifest; blocks stay fully
  store-private (each store packs independently); migration never rewrites
  history.
Bonus: the accepted cost — the index becomes read-critical, but it's
rebuildable, so losing it means rebuild-before-read, never data loss.

**A4.**
Required: **False.** The merkle tree does not change shape or content; no
FileNode field signals chunking (manifest existence does, per store). File
identity stays the xxh3-128 of the original bytes, so commit IDs and node
hashes are identical whether a version is stored whole or chunked.
Bonus: why the dormant `chunk_hashes`/`chunk_type` FileNode fields are left
untouched — serialized node blobs are distributed by hash, and mutating bytes
under an unchanged hash would break the hash→bytes immutability tree sync
relies on.

**A5.**
Required: every block ends in a self-describing **footer** listing each
contained chunk's hash, offset, stored/raw lengths, and codec. Rebuild
(`BlockEngine::rebuild_index` / `rebuild_chunk_index`) clears the index, scans
every block on disk, verifies each block's bytes against its content-hash
name, parses footers, and re-inserts every chunk.
Bonus: the tests assert rebuild reproduces byte-identical placements; the
footer is ~0.04% overhead on a full block.

**A6.**
Required ordering: blocks durable (atomic, hash-verified write) → chunk index
updated → manifest validated and published → merkle/commit metadata → ref
update. Worst crash outcome: either the old complete representation or the
new one, plus at most **reclaimable orphans** (e.g. a published block whose
chunks were never indexed or referenced) — never a torn or half-visible
version.
Bonus: on pull the same rule applies — manifests are staged and published only
after all their chunks are durable, because manifest existence *is* version
existence and publishing early would make `version_exists` lie to a resumed
pull.

**A7.**
Required layout:
```
[payload payload ...]                                 (concatenated chunk payloads)
[footer: n × (chunk_hash u128, offset u32, stored_len u32, raw_len u32, flags u8)]
[footer_len u32][version u8][magic "OXBK"]            (9-byte trailer, LE ints)
```
Codec IDs live in the per-chunk `flags` byte **in the footer**, not the
manifest, because the codec is store-local: a store may adopt a new codec for
new blocks or repack old ones without any manifest or wire change.
Bonus: parser hardening — payloads must tile the payload region exactly,
`raw_len` capped at 128 KiB, checked arithmetic, unknown codec ⇒ structured
upgrade-required error; blocks over 64 MiB rejected.

**A8.**
Required: the block hash proves the *bytes* are what the sender sent — it says
nothing about whether the footer's *claims* are true. A buggy or hostile
writer could upload a block whose footer maps chunk hash H to bytes that don't
hash to H. Without ingest verification that lie gets indexed; negotiation then
tells honest clients "the store already has H," their pushes fail server-side
validation, and the wedge persists until an fsck. Verifying every chunk at
ingest (decompress + xxh3, O(uploaded bytes)) turns that into an immediate
rejection of the bad block.
Bonus: co-packed chunks are indexed with no manifest referencing them, which
is exactly why the lie would otherwise go undetected.

**A9.**
Required: raw fallback — if the compressed form is not strictly smaller, the
chunk is stored raw and the footer's flags byte records `0` (raw) instead of
`1` (zstd). The reader reads the flags byte from the footer (or the chunk
index, which copies it) and dispatches through the codec registry.
Bonus: this guarantees incompressible data never materially expands, and the
empty-input edge stores raw; decoders bound output to the declared `raw_len`,
so a decompression bomb can't over-allocate.

**A10.**
Required: FastCDC boundaries depend on gear tables and normalization
constants inside the `fastcdc` crate. A crate upgrade that shifts boundaries
would still pass every round-trip test while silently destroying dedup between
old and new data fleet-wide (same bytes → different chunks). The golden
fixtures pin exact `(offset, len)` boundaries for fixed seeded data, so such a
change fails loudly; the required process is registering a **new** chunker ID,
never an in-place change of `GENERIC_FASTCDC_V1`.
Bonus: IDs are append-only, never reused; ID 0 is permanently invalid so
zeroed data can't masquerade as a manifest.

**A11.**
Required:
- **Chunker** — ID in the manifest (`chunker_id`).
- **Codec** — ID in the block footer's per-chunk flags byte.
- **Transform** — ID in the manifest (`transform_id`); v1 only writes identity
  (0).
Unknown ID on read ⇒ structured upgrade-required `ChunkedError` ("written by a
newer version of oxen, please upgrade") — never a panic or silent fallback.
Bonus: one deliberate exception — an unknown *chunker* ID in a manifest is
accepted on read, because reconstruction never re-chunks and stores must accept
valid manifests from clients with newer chunker policies.

**A12.**
Required: size-only in v1 — every file ≥ 1 MiB chunks, any data type
(`should_chunk`, `DEDUP_MIN_FILE_SIZE`, test-infra override
`OXEN_DEDUP_MIN_FILE_SIZE`). Parquet is already-compressed inside, so a zstd
pass would be wasted CPU that raw fallback then discards — the codec policy
consults the *extension* (compressed-container list), not just
`EntryDataType`, and parquet is `Tabular` by type.
Bonus: the 1 MiB floor deliberately includes the small-but-hot labeled-CSV
workload; the policy function is the single place the
`(EntryDataType, extension) → (chunker, codec, transform)` mapping lives.

**A13.**
Required, per chunk: hash into the running whole-file xxh3 + record the
manifest entry; then dedup — skip packing if the chunk is already in the index
or already appended to the open block; otherwise encode (zstd/raw fallback)
and append. A block seals (atomic hash-named publish, then index) when the
next append would push the encoded size past 64 MiB, and the tail seals at end
of pass. Memory is O(one open block) because sealed blocks are flushed and
chunks stream through. The claimed hash is verified against the hash computed
*during* the same pass ("hash-while-chunking") — no second read; on mismatch
nothing is published under the claimed hash.
Bonus: blocks already written before a mismatch are unreferenced orphans for
GC — publish-last means the failure leaves no visible version.

**A14.**
Required: the vast majority (the tests assert ≥80%; typically all but the 1–3
chunks overlapping the edit). Content-defined boundaries are chosen by local
byte content, so an insertion only re-cuts chunks near the edit and boundaries
downstream re-synchronize. Fixed-size chunking shifts every boundary after the
insertion point, so every downstream chunk changes identity — zero dedup for
exactly the labeling/append workload this feature targets.

**A15.**
Required: `VersionStore::chunked() -> Option<&dyn ChunkedVersionStore>`
(default `None`; `LocalVersionStore` returns `Some(self)`). Transparent reads
(any three): `get_version`, `get_version_size`, `get_version_stream`,
`get_version_chunk` (range reads), `copy_version_to_path`, `version_exists`,
`find_missing_versions` — all serve chunked versions by internally loading the
manifest and streaming decoded block ranges; no caller changes.
Bonus: writes are the explicit side — every write path must *choose* chunked
vs whole-file through policy; only reads are transparent.

**A16.**
Required: both live under `.oxen/versions/files/{hash[..2]}/{hash[2..]}/` —
a whole-file version has a `data` file there; a chunked version has a
`manifest` file **instead**. Chunk payloads live in
`.oxen/versions/files/blocks/{block_hash[..2]}/{block_hash[2..]}/data`, and the
LMDB index in `.oxen/versions/files/chunk_index/`.
Bonus: block hashes are fixed-width 32-hex in paths (uniform fan-out), unlike
the legacy unpadded version hashes.

**A17.**
Required: validation = structural checks (chunks tile the file exactly,
supported version/transform) + **full streamed reconstruction** through the
index/blocks, verifying both the byte count and the xxh3-128 file hash before
the manifest is atomically published. Cost: O(logical file size) per
manifest — deliberately accepted (decision 11) because existence probes can't
catch a chunk list that exists-but-doesn't-reproduce the claimed file; that
failure would otherwise surface only at checkout. Duplicate-hash manifests are
a no-op success because two *valid* recipes for the same bytes legitimately
differ across chunker policies (a chunker ID bump); rejecting would wedge
every newer client, while never-overwriting still preserves collision safety.
Bonus: the recorded revisit trigger — if the cost bites at scale, a
size-thresholded/async validation mode is future work; re-running CDC to
police boundaries is rejected (protects dedup quality, not correctness).

**A18.**
Required: the reconstruction streams through `AtomicFile` configured
`.with_hash(file_hash).with_mtime(...)` — the bytes are hashed as they're
written to a temp file and the atomic rename happens only if the whole-file
hash matches. Crash safety: the destination only ever contains the old
complete file or the new verified one, never a torn or wrong-mtime file.
Bonus: this write-time check *is* the client's end-to-end pull validation —
no separate reconstruction pass is needed on checkout.

**A19.**
Required:
- Entries with a local manifest split off from the legacy small/large paths in
  `push_entries`.
- **Negotiation is at chunk granularity**: the unique chunk hashes from all
  manifests go to `POST /chunks/missing` (batched), and only the reported
  missing chunks move.
- **Transfer is at block granularity**: missing chunks are repacked into fresh
  transfer blocks (stored payloads copied as-is, so compressed chunks travel
  compressed) and uploaded via idempotent `PUT /blocks/{hash}`.
- **Manifests upload last** (`PUT /manifests/{hash}`) because the server
  validates each by full streamed reconstruction — every referenced chunk must
  already be durable there, and a published manifest is what marks the version
  as present; publishing earlier would let a resumed push/pull believe the
  version exists before its bytes do.
- An old server has none of these routes: the 404 maps to the structured
  `OxenError::ServerLacksBlockSupport` — fail early and actionably, never a
  silent whole-file fallback.
Bonus: client memory stays bounded by packing in ~48 MiB raw batches, and
progress counts encoded bytes actually uploaded (a 99%-deduped push would look
stalled if it tracked logical bytes).

**A20.**
Required: (1) the repository's `content_format` is `block-v1`
(`[storage] content_format = "block-v1"` / `repo.storage_config()`), and
(2) `should_chunk(data_type, size)` — the file is at/above the 1 MiB floor
(also requires the store to expose `chunked()`). A 500 KB file takes the
classic whole-file `store_version_from_reader` path — small files stay blobs
by policy, in both representations' coexistence.
Bonus: the routing lives in one helper (`store_file_version` in
`core/v_latest/add.rs`) used by both the bulk-directory and single-file add
paths, and the staged FileNode supplies the data type/extension that drive
the codec policy.

**A21.**
Required: concurrent writers (or a transfer landing a block that co-packs
chunks the store partly has) can legitimately produce two valid blocks
containing the same chunk. First-wins keeps a single canonical location so
readers are deterministic, and the duplicate payload is simply dead weight
for GC to reclaim later. It's safe precisely because both copies are verified
identical bytes (chunk hash checked at ingest).

**A22.**
Required: LMDB map size is a fixed virtual-address reservation, never resized
at runtime. The chunk index is sized from its math (~53 bytes per entry ⇒
roughly 8.5 GB of entries for a 10 TB repo at 64 KiB average chunks, plus
LMDB overhead → 32 GiB is generous) rather than copying the merkle store's
256 GiB reflexively, because fleet-wide address reservation is additive across
stores × open repos. `MapFull` is not a runtime recovery path: the remedy is
raising the compile-time constant and rebuilding.

**A23.**
Required: every server read API (`get_version_stream`, `get_version_chunk`,
tarball packing, range serving) is transparent over chunked versions — the
server reconstructs through its manifest + chunk index + blocks, so an
unchanged client downloads correct whole-file bytes. The reserved optimization
is chunk-level pull dedup (`QUERY /manifests` + `QUERY /chunks`): the client
would fetch manifests, diff chunk hashes against its local index, download
only missing chunks as blocks, and publish manifests last (publish-last
applies on pull exactly as on push).
Bonus: manifest presence equals version presence, which is why a pulled
manifest must stay *staged* until all its chunks are durable — otherwise
`version_exists` would lie to a resumed pull.

**A24.**
Required: **at every interruption point, every version has at least one
complete representation** — the old representation is deleted only after the
new one is durably published and verified. Enforced by
`delete_whole_file_blob` (refuses unless a published manifest exists) and
`delete_manifest` (refuses unless the whole-file blob exists), so not even a
buggy migration loop can delete a version's last representation. Re-running
skips converted versions and finishes any interrupted deletion.
Bonus: forward flips `content_format` at the end so an interrupted run leaves
a still-legacy repo in a valid mixed state; reverse flips it first so no new
chunked writes land while the conversion is draining chunked versions away.
(Reverse also leaves blocks on disk — other manifests may share their chunks;
reclaiming them is GC's job.)
