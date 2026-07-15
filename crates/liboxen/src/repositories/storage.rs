//! # oxen storage
//!
//! Inspect and migrate a repository's content storage format between `legacy`
//! (whole-file blobs) and `block-v1` (chunked, deduplicated block storage).
//!
//! Migration converts every version stored in the version store, one at a time,
//! and is safe to interrupt and re-run: a version's old representation is deleted
//! only after its new one is durably published and verified, so at every point
//! each version has at least one complete representation. Commits, merkle tree
//! nodes, and hashes are never touched — the format is purely a storage concern.
//!
//! Design reference: `docs/block_level_dedup_plan.md` §9. This is the local,
//! single-machine subset (no maintenance lease or reachable-set inventory yet:
//! every stored version migrates, reachable or not — extra chunks are safe and
//! reclaimable by future GC).

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{EntryDataType, LocalRepository};
use crate::storage::ContentFormat;
use crate::storage::chunked::dedup_min_file_size;

/// The `min_version` marker stamped on repos holding block-v1 chunked versions.
/// Binaries that predate chunked storage don't recognize it and refuse to open
/// the repo with an upgrade hint — without the fence they would misread chunked
/// versions as missing data, and their fsck would delete manifest-only version
/// dirs as corruption.
const BLOCK_V1_MIN_OXEN_VERSION: &str = "0.52.0";

/// Counters reported by a migration run.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationStats {
    /// Versions converted by this run.
    pub migrated: u64,
    /// Versions already in the target representation (resume/no-op).
    pub already_migrated: u64,
    /// Versions below the chunking floor, kept whole-file by policy.
    pub skipped_small: u64,
    /// Dirs in the version store that hold no version (e.g. leftovers of an
    /// interrupted legacy segment upload), stepped over by this run.
    pub skipped_orphaned: u64,
    /// Logical bytes converted by this run.
    pub bytes_migrated: u64,
}

/// A repository's storage-format summary for `oxen storage status`.
#[derive(Debug, Clone)]
pub struct StorageStatus {
    pub content_format: ContentFormat,
    pub total_versions: u64,
    pub chunked_versions: u64,
}

/// Summarize the repository's content format and version representations.
pub async fn status(repo: &LocalRepository) -> Result<StorageStatus, OxenError> {
    let store = repo.version_store();
    let hashes = store.list_versions().await?;
    let mut chunked_versions = 0u64;
    if let Some(chunked) = store.chunked() {
        for hash in &hashes {
            if chunked.get_manifest(hash).await?.is_some() {
                chunked_versions += 1;
            }
        }
    }
    Ok(StorageStatus {
        content_format: repo.storage_config().content_format,
        total_versions: hashes.len() as u64,
        chunked_versions,
    })
}

/// Migrate the repository to the `block-v1` content format: chunk every stored
/// version at/above the policy floor into blocks, publish its manifest, and only
/// then delete its whole-file blob. Small versions keep the whole-file path by
/// policy. Idempotent and resumable — re-running finishes any version whose blob
/// deletion was interrupted and skips versions already converted.
///
/// The repository's `content_format` flips to `block-v1` (persisted) only after
/// every eligible version is converted, so an interrupted run leaves a legacy
/// repo with some versions already chunked — a valid mixed state. The
/// `min_version` fence is stamped before the first conversion, so binaries that
/// predate chunked storage refuse to open the repo in any of these states.
pub async fn migrate_to_block_v1(repo: &mut LocalRepository) -> Result<MigrationStats, OxenError> {
    let store = repo.version_store();
    let Some(chunked) = store.chunked() else {
        return Err(OxenError::basic_str(
            "this repository's storage backend does not support block storage",
        ));
    };

    // Fence first: a pre-block binary opening a mid-migration repo would misread
    // chunked versions as missing data (and its fsck deletes manifest-only
    // version dirs), so refuse it the repo before the first version converts.
    repo.set_min_version_marker(BLOCK_V1_MIN_OXEN_VERSION);
    repo.save()?;

    let mut stats = MigrationStats::default();
    for hash in store.list_versions().await? {
        if chunked.get_manifest(&hash).await?.is_some() {
            // Already chunked; finish an interrupted step by clearing any
            // leftover blob (no-op when it's already gone).
            chunked.delete_whole_file_blob(&hash).await?;
            stats.already_migrated += 1;
            continue;
        }
        if !store.version_exists(&hash).await? {
            // Not a version: e.g. a dir left behind by an interrupted legacy
            // segment upload (a chunks/ subdir with no blob and no manifest).
            // Step over it rather than aborting a resumable migration.
            log::warn!("storage migrate: skipping non-version dir for hash {hash}");
            stats.skipped_orphaned += 1;
            continue;
        }
        let size = store.get_version_size(&hash).await?;
        if size < dedup_min_file_size() {
            stats.skipped_small += 1;
            continue;
        }

        // Chunk the blob through the standard single-pass ingest, verified
        // against the blob's content hash. The store is content-addressed, so
        // the original data type isn't recorded; text policy (zstd with raw
        // fallback) is byte-safe for every content type.
        let reader = Box::new(tokio_util::io::StreamReader::new(
            store.get_version_stream(&hash).await?,
        ));
        chunked
            .store_version_chunked(&hash, &EntryDataType::Text, "", reader)
            .await?;
        // The manifest is durable and verified; the blob is now redundant.
        chunked.delete_whole_file_blob(&hash).await?;
        stats.migrated += 1;
        stats.bytes_migrated += size;
    }

    repo.set_content_format(ContentFormat::BlockV1);
    repo.save()?;
    Ok(stats)
}

/// Migrate the repository back to the `legacy` content format: reconstruct every
/// chunked version into a whole-file blob (hash-verified as it lands), and only
/// then delete its manifest. Blocks are left on disk — other repositories'
/// clones never shared them, but reclaiming them is a GC concern, not a
/// migration one.
///
/// `content_format` flips to `legacy` (persisted) *before* conversion, so no new
/// chunked versions are written while the migration runs. Idempotent and
/// resumable.
pub async fn migrate_to_legacy(repo: &mut LocalRepository) -> Result<MigrationStats, OxenError> {
    repo.set_content_format(ContentFormat::Legacy);
    repo.save()?;

    let store = repo.version_store();
    let Some(chunked) = store.chunked() else {
        // A store that never supported chunking has nothing to convert.
        return Ok(MigrationStats::default());
    };

    let mut stats = MigrationStats::default();
    for hash in store.list_versions().await? {
        if chunked.get_manifest(&hash).await?.is_none() {
            if store.version_exists(&hash).await? {
                stats.already_migrated += 1;
            } else {
                // See migrate_to_block_v1: leftovers of an interrupted legacy
                // segment upload are not versions.
                log::warn!("storage migrate: skipping non-version dir for hash {hash}");
                stats.skipped_orphaned += 1;
            }
            continue;
        }
        let size = store.get_version_size(&hash).await?;

        // Reconstruct through the transparent read path into the canonical blob
        // location; the store verifies the bytes against `hash` before
        // publishing. A blob already present (interrupted run) makes this a
        // no-op.
        let reader = Box::new(tokio_util::io::StreamReader::new(
            store.get_version_stream(&hash).await?,
        ));
        store.store_version_from_reader(&hash, reader, size).await?;
        chunked.delete_manifest(&hash).await?;
        stats.migrated += 1;
        stats.bytes_migrated += size;
    }

    // Every chunked version is whole-file again, so pre-block binaries can
    // safely open the repo: lower the fence stamped by migrate_to_block_v1.
    repo.set_min_version(MinOxenVersion::LATEST);
    repo.save()?;
    Ok(stats)
}

/// Rebuild the store-local chunk index (chunk hash → block location) by scanning
/// every stored block. The index is derived state — blocks and manifests are the
/// durable representation — so a lost or corrupted index is fully recoverable
/// here. Returns the number of chunks indexed.
pub async fn rebuild_chunk_index(repo: &LocalRepository) -> Result<u64, OxenError> {
    let store = repo.version_store();
    let Some(chunked) = store.chunked() else {
        return Err(OxenError::basic_str(
            "this repository's storage backend does not support block storage",
        ));
    };
    chunked.rebuild_chunk_index().await
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::opts::RestoreOpts;
    use crate::storage::chunked::dedup_min_file_size;
    use crate::{repositories, test, util};

    /// Forward migration converts big blobs to manifests+blocks (small ones stay),
    /// reads and restores keep working, re-running is a no-op, and reverse
    /// migration restores whole-file blobs byte-exactly.
    #[tokio::test]
    async fn test_migrate_legacy_repo_to_block_v1_and_back() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // A legacy repo with one big CSV and one small file committed.
            let floor = dedup_min_file_size() as usize;
            let mut csv = String::from("file,label\n");
            let mut row = 0u64;
            while csv.len() < floor * 2 {
                csv.push_str(&format!("images/img_{row}.jpg,label_{}\n", row % 7));
                row += 1;
            }
            let csv_path = repo.path.join("train.csv");
            util::fs::write_to_path(&csv_path, &csv)?;
            let small_path = repo.path.join("README.md");
            util::fs::write_to_path(&small_path, "# small")?;
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "legacy commit")?;

            let store = repo.version_store();
            let chunked = store.chunked().expect("local store supports chunked");
            let big_hash = util::hasher::hash_buffer(csv.as_bytes());
            let small_hash = util::hasher::hash_buffer(b"# small");

            // Forward migration.
            let stats = repositories::storage::migrate_to_block_v1(&mut repo).await?;
            assert_eq!(stats.migrated, 1, "one eligible version");
            assert_eq!(stats.skipped_small, 1);
            assert!(chunked.get_manifest(&big_hash).await?.is_some());
            assert!(chunked.get_manifest(&small_hash).await?.is_none());
            let status = repositories::storage::status(&repo).await?;
            assert_eq!(
                status.content_format,
                crate::storage::ContentFormat::BlockV1
            );
            assert_eq!(status.chunked_versions, 1);

            // Reads still serve exact bytes, and restore works from history.
            assert_eq!(store.get_version(&big_hash).await?, csv.as_bytes());
            util::fs::remove_file(&csv_path)?;
            repositories::restore::restore(
                &repo,
                RestoreOpts::from_path_ref("train.csv", commit.id.clone()),
            )
            .await?;
            assert_eq!(util::fs::read_from_path(&csv_path)?, csv);

            // Re-running is a clean no-op resume.
            let stats = repositories::storage::migrate_to_block_v1(&mut repo).await?;
            assert_eq!(stats.migrated, 0);
            assert_eq!(stats.already_migrated, 1);
            assert_eq!(stats.skipped_small, 1);

            // Reverse migration restores the whole-file blob byte-exactly.
            let stats = repositories::storage::migrate_to_legacy(&mut repo).await?;
            assert_eq!(stats.migrated, 1);
            assert!(chunked.get_manifest(&big_hash).await?.is_none());
            assert_eq!(store.get_version(&big_hash).await?, csv.as_bytes());
            let status = repositories::storage::status(&repo).await?;
            assert_eq!(status.content_format, crate::storage::ContentFormat::Legacy);
            assert_eq!(status.chunked_versions, 0);

            Ok(())
        })
        .await
    }

    /// The chunk index is derived state: after losing it entirely (host
    /// redeploy, partial restore), `rebuild_chunk_index` recovers every chunked
    /// version from block footers.
    #[tokio::test]
    async fn test_rebuild_chunk_index_recovers_reads() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            let floor = dedup_min_file_size() as usize;
            let mut csv = String::from("file,label\n");
            let mut row = 0u64;
            while csv.len() < floor + 1024 {
                csv.push_str(&format!("images/img_{row}.jpg,label_{}\n", row % 7));
                row += 1;
            }
            let csv_path = repo.path.join("train.csv");
            util::fs::write_to_path(&csv_path, &csv)?;
            repositories::add(&repo, &csv_path).await?;
            repositories::commit(&repo, "one big file")?;
            repositories::storage::migrate_to_block_v1(&mut repo).await?;
            let big_hash = util::hasher::hash_buffer(csv.as_bytes());

            // Restore the repo elsewhere without the derived index (a host
            // redeploy or partial restore); blocks and manifests stay durable.
            // A copy is needed because LMDB envs are cached per path in-process,
            // so deleting the index dir under the original repo wouldn't affect
            // its already-open env.
            let restored_path = repo.path.parent().expect("repo has a parent").join(format!(
                "{}_restored",
                repo.path
                    .file_name()
                    .expect("repo dirname")
                    .to_string_lossy()
            ));
            util::fs::copy_dir_all(&repo.path, &restored_path)?;
            let index_dir = crate::util::fs::oxen_hidden_dir(&restored_path)
                .join(crate::constants::VERSIONS_DIR)
                .join(crate::constants::FILES_DIR)
                .join(crate::constants::CHUNK_INDEX_DIR);
            util::fs::remove_dir_all(&index_dir)?;

            // The restored repo can't serve the chunked version...
            let restored = crate::model::LocalRepository::from_dir(&restored_path)?;
            assert!(
                restored
                    .version_store()
                    .get_version(&big_hash)
                    .await
                    .is_err()
            );

            // ...until the index is rebuilt from block footers.
            let chunks = repositories::storage::rebuild_chunk_index(&restored).await?;
            assert!(chunks > 0, "rebuild must re-index the stored chunks");
            assert_eq!(
                restored.version_store().get_version(&big_hash).await?,
                csv.as_bytes()
            );
            util::fs::remove_dir_all(&restored_path)?;
            Ok(())
        })
        .await
    }

    /// Migration steps over non-version dirs left by interrupted legacy segment
    /// uploads instead of aborting, and stamps/lowers the `min_version` fence
    /// that keeps pre-block binaries from opening a repo with chunked versions.
    #[tokio::test]
    async fn test_migrate_to_block_v1_skips_orphans_and_fences_min_version() -> Result<(), OxenError>
    {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            let floor = dedup_min_file_size() as usize;
            let mut csv = String::from("file,label\n");
            let mut row = 0u64;
            while csv.len() < floor + 1024 {
                csv.push_str(&format!("images/img_{row}.jpg,label_{}\n", row % 7));
                row += 1;
            }
            let csv_path = repo.path.join("train.csv");
            util::fs::write_to_path(&csv_path, &csv)?;
            repositories::add(&repo, &csv_path).await?;
            repositories::commit(&repo, "one big file")?;

            // An interrupted legacy segment upload leaves a version dir holding
            // only a chunks/ subdir — no blob, no manifest. Not a version.
            let orphan_chunk_dir = repo
                .path
                .join(".oxen/versions/files/ab/cdef0123456789/chunks/0");
            util::fs::create_dir_all(&orphan_chunk_dir)?;
            util::fs::write_to_path(orphan_chunk_dir.join("chunk"), "partial upload")?;

            let stats = repositories::storage::migrate_to_block_v1(&mut repo).await?;
            assert_eq!(stats.migrated, 1);
            assert_eq!(stats.skipped_orphaned, 1);
            assert!(orphan_chunk_dir.join("chunk").exists());

            // Re-running resumes cleanly past the orphan too.
            let stats = repositories::storage::migrate_to_block_v1(&mut repo).await?;
            assert_eq!(stats.already_migrated, 1);
            assert_eq!(stats.skipped_orphaned, 1);

            // The fence is stamped, and current binaries still open the repo.
            let config = std::fs::read_to_string(util::fs::config_filepath(&repo.path))
                .map_err(OxenError::from)?;
            assert!(
                config.contains("min_version = \"0.52.0\""),
                "expected block-v1 fence in config: {config}"
            );
            let reopened = crate::model::LocalRepository::from_dir(&repo.path)?;
            assert_eq!(
                reopened.min_version(),
                crate::core::versions::MinOxenVersion::LATEST
            );

            // Reverse migration lowers the fence once no chunked versions remain.
            let stats = repositories::storage::migrate_to_legacy(&mut repo).await?;
            assert_eq!(stats.migrated, 1);
            let config = std::fs::read_to_string(util::fs::config_filepath(&repo.path))
                .map_err(OxenError::from)?;
            assert!(
                config.contains("min_version = \"0.36.0\""),
                "expected fence lowered in config: {config}"
            );

            Ok(())
        })
        .await
    }
}
