# Block-Level Dedup — Implementation Quiz (Questions)

Instructions for the quizzing agent: ask these one at a time, in order or
sampled. The grading key is in `block_level_dedup_quiz_answers.md` — each
answer there lists the **required points** (must be mentioned to count as
correct) and **bonus points** (signs of deeper understanding). Encourage
answers in the quiz-taker's own words; exact file names matter less than the
concepts, except where a question explicitly asks "where in the code."

## Vocabulary and shape

**Q1.** Define *chunk*, *block*, and *manifest* in this design, and name the
unit of (a) deduplication and (b) storage/transfer. Why are these two units
deliberately different?

**Q2.** The word "chunk" was already used in the codebase before this feature.
What does the pre-existing `store_version_chunk` / `/chunks` wire path refer
to, and what term does the design reserve for it?

## Design invariants

**Q3.** A manifest never records which block a chunk lives in. Where does
placement live instead, and name two concrete benefits this "pure content"
property buys.

**Q4.** True or false, and justify: enabling `block-v1` on a repository changes
the merkle tree hashes of subsequent commits for files that get chunked.

**Q5.** The LMDB chunk index is described as "disposable, derived state." What
makes that safe — i.e., if the index directory is deleted entirely, what
exactly restores it, and what property of the block format makes that
possible?

**Q6.** Recite the durability ("publish-last") ordering for a chunked write.
What is the worst possible on-disk state after a crash between any two steps?

## Format details

**Q7.** Sketch the v1 block layout from first byte to last. Where do the codec
IDs live, and why are they *not* in the manifest?

**Q8.** At block-ingest time (`store_block` / `verify_block`), the block's
content hash already matched the name it arrived under. Why does the code
still decompress every chunk and re-hash it against the footer's claimed
chunk hash? Describe the attack/bug this closes (design decision 16).

**Q9.** What does `encode_chunk` do when zstd makes a chunk *larger* (e.g.
random bytes), and how does a reader later know which codec a stored payload
used?

**Q10.** The FastCDC parameters are 8 KiB min / 64 KiB target / 128 KiB max,
frozen as `ChunkerId::GENERIC_FASTCDC_V1`. What specific risk do the golden
boundary fixtures in `chunker.rs` protect against, and what is the required
process if a `fastcdc` crate upgrade shifts boundaries?

**Q11.** Which three extension axes does the encode pipeline have, where is
each one's ID recorded, and what happens today when a reader encounters an ID
it doesn't know?

## Policy and ingest

**Q12.** Which files get chunked in v1, exactly? Name the rule, the constant,
the env override, and why parquet files skip the zstd attempt even though
their `EntryDataType` is `Tabular`.

**Q13.** Walk through `BlockEngine::ingest` for a 3 MiB CSV: what happens per
chunk, when does a block seal, and why is memory usage independent of file
size? Also: how does ingest verify the caller's claimed file hash without a
second read pass (invariant 5)?

**Q14.** Two versions of a file differ by one inserted row in the middle.
Roughly what fraction of chunks do they share and why — i.e., why does
FastCDC beat the old dormant fixed-size chunker for exactly this workload?

## Storage seam and reads

**Q15.** How does a `VersionStore` advertise chunked-storage capability, and
what does "reads stay transparent" mean concretely — name three read APIs
that work identically on chunked versions without the caller knowing.

**Q16.** On disk, how do you tell whether a given version hash is stored
whole-file or chunked in a local store? (Paths, not code.)

**Q17.** `put_manifest` (the server-side/pull publication path) validates a
manifest before publishing. What exactly does validation do, what does it
cost, and why is a cheaper existence-probe-only check rejected? Also: why is
an incoming manifest for an *already-published* hash a silent no-op success
rather than a conflict error?

**Q18.** When `copy_version_to_path` materializes a chunked version into the
working tree, where does end-to-end verification happen and what guarantees
does the crash-safety story give?

## Gates and boundaries

**Q19.** Walk through what `oxen push` does with a version that is stored
chunked locally: what is negotiated, at which granularity does data travel,
and why must manifests upload strictly after blocks? What happens if the
remote server predates block support?

**Q20.** In `oxen add` on a block-v1 repo, which two conditions must both hold
for a file to take the chunked path, and what happens to a 500 KB file?

## Stretch

**Q21.** Why is the first indexed location of a chunk canonical
(`ChunkIndex::insert_block` skips existing keys), and what scenario makes a
duplicate chunk-in-two-blocks legitimate rather than a bug?

**Q22.** The chunk-index LMDB map size is 32 GiB while the merkle node store
reserves 256 GiB. Why is it sized from arithmetic instead of copied, and what
is the remedy when an LMDB store hits `MapFull`?

**Q23.** Why does *pull* of a chunked-on-server version require no client
changes for correctness, and what optimization does the plan still reserve
for the pull direction?

**Q24.** `oxen storage migrate` is interruption-safe in both directions. Name
the invariant that guarantees no data loss at any interruption point, and the
two guarded storage-layer methods that enforce it. Bonus: why does forward
migration flip `content_format` at the *end* while reverse migration flips it
at the *start*?
