//! Moving a repository's version files between the local and S3 storage backends.

use std::collections::HashSet;

use futures_util::{StreamExt, stream};
use tokio_util::io::StreamReader;

use crate::config::RepositoryConfig;
use crate::constants::MAX_CONCURRENT_VERSION_PROBES;
use crate::core::repo_locks;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::storage::{StorageConfig, StorageKind, VersionStore, create_version_store};
use crate::util;

/// Move `repo`'s version files to the `kind` backend, switch the repository to it, and delete the
/// old backend's copies. Does nothing when `repo` is already on `kind`.
///
/// Writes to `repo` keep landing while most of the files copy, and are refused only while the move
/// copies what arrived since and switches. A move that stops part-way is finished by running it
/// again, and one that stops while deleting leaves the rest of the old copies in place.
///
/// # Errors
/// [`OxenError::S3BackendMissingServerOpts`] when moving to S3 on a server with no bucket, and
/// [`OxenError::S3RepoWithoutIdentity`] when moving to S3 a repository that records no UUID, and
/// [`OxenError::StorageChangedDuringMove`] when another operation switched `repo`'s storage while
/// this move copied.
#[tracing::instrument(
    skip(repo, kind),
    fields(oxen.repository_path = %repo.path.display(), oxen.target_storage_kind = %kind)
)]
pub async fn move_to(repo: &LocalRepository, kind: StorageKind) -> Result<(), OxenError> {
    if repo.storage_config().kind == kind {
        tracing::info!("The repository is already on this storage");
        return Ok(());
    }
    let to = StorageConfig {
        kind,
        versions_path: None,
    };
    let source = repo.version_store();
    let target = create_version_store(&repo.path, &to, repo.repo_uuid(), repo.server_s3_opts())?;
    let (source, target) = (source.as_ref(), target.as_ref());

    target.init().await?;
    let moved = copy_missing(source, target, Vec::new()).await?;
    let moved =
        repo_locks::with_repo_exclusive(repo, catch_up_and_switch(repo, source, target, moved))
            .await?;
    delete_old_copies(repo, source, &moved).await;
    Ok(())
}

/// Copy what `target` lacks beyond `moved`, the hashes an earlier pass moved, switch `repo` to
/// `target`'s backend, and return the hashes of every version file `target` now holds for `repo`.
/// Refuses when `repo`'s storage has changed since it was loaded. Run with `repo` held exclusively.
async fn catch_up_and_switch(
    repo: &LocalRepository,
    source: &dyn VersionStore,
    target: &dyn VersionStore,
    moved: Vec<String>,
) -> Result<Vec<String>, OxenError> {
    let path = util::fs::config_filepath(&repo.path);
    let mut config = RepositoryConfig::from_file(&path)?;
    if config.storage.clone().unwrap_or_default() != *repo.storage_config() {
        return Err(OxenError::StorageChangedDuringMove(
            repo.path.clone().into(),
        ));
    }

    let moved = copy_missing(source, target, moved).await?;
    let kind = target.storage_kind();
    config.storage = Some(StorageConfig {
        kind,
        versions_path: None,
    });
    config.save(&path)?;
    tracing::info!(oxen.target_storage_kind = %kind, oxen.moved_version_count = moved.len(), "Switched the repository to its new storage");
    Ok(moved)
}

/// Delete `moved` from `source`, the store `repo` has switched away from, while holding a write on
/// `repo`, so pushes continue and a second move or a prune waits for the delete to finish. A copy
/// that is not deleted only costs space.
async fn delete_old_copies(repo: &LocalRepository, source: &dyn VersionStore, moved: &[String]) {
    let from = repo.storage_config().kind;
    let _write = match repo_locks::begin_write(repo) {
        Ok(write) => write,
        Err(err) => {
            tracing::warn!(
                oxen.source_storage_kind = %from,
                exception.message = %err,
                "Left every version file on the storage a repository moved from"
            );
            return;
        }
    };
    tracing::info!(oxen.source_storage_kind = %from, oxen.moved_version_count = moved.len(), "Deleting the old copies of the version files");
    let undeleted = stream::iter(moved.iter().cloned())
        .map(|hash| async move {
            let result = source.delete_version(&hash).await;
            if let Err(err) = &result {
                tracing::warn!(oxen.file_hash = %hash, exception.message = %err, "Failed to delete the old copy of a version");
            }
            result.is_err()
        })
        .buffer_unordered(MAX_CONCURRENT_VERSION_PROBES)
        .fold(0, |count, failed| async move { count + usize::from(failed) })
        .await;
    if undeleted > 0 {
        tracing::error!(
            oxen.undeleted_version_count = undeleted,
            oxen.source_storage_kind = %from,
            "Left version files on the storage a repository moved from"
        );
    }
}

/// Copy into `target` every version file `source` holds that `target` lacks, other than those in
/// `moved`, and return `moved` together with the hashes of every other version file `source`
/// holds, all of which `target` then holds too.
///
/// A version whose data `source` lacks, such as one whose chunked upload has not completed, is
/// left out.
async fn copy_missing(
    source: &dyn VersionStore,
    target: &dyn VersionStore,
    mut moved: Vec<String>,
) -> Result<Vec<String>, OxenError> {
    let skip: HashSet<&str> = moved.iter().map(String::as_str).collect();
    let mut to_move: Vec<String> = source
        .list_versions()
        .await?
        .into_iter()
        .filter(|hash| !skip.contains(hash.as_str()))
        .collect();
    let without_data: HashSet<String> = source
        .find_missing_versions(&to_move)
        .await?
        .into_iter()
        .collect();
    to_move.retain(|hash| !without_data.contains(hash));

    let missing = target.find_missing_versions(&to_move).await?;
    tracing::info!(
        oxen.moved_version_count = moved.len(),
        oxen.found_version_count = to_move.len(),
        oxen.missing_version_count = missing.len(),
        "Copying the version files the target storage lacks"
    );
    // Finish every copy before returning an error, so no upload is dropped part-way.
    stream::iter(missing)
        .map(|hash| async move {
            let size = source.get_version_size(&hash).await?;
            let reader = StreamReader::new(source.get_version_stream(&hash).await?);
            target
                .store_version_from_reader(&hash, Box::new(reader), size)
                .await
        })
        .buffer_unordered(MAX_CONCURRENT_VERSION_PROBES)
        .collect::<Vec<Result<(), OxenError>>>()
        .await
        .into_iter()
        .collect::<Result<(), OxenError>>()?;
    moved.append(&mut to_move);
    Ok(moved)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::storage::s3;
    use crate::test;
    use crate::util::hasher;

    #[tokio::test]
    async fn test_move_versions_to_s3_and_back() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            assert!(
                matches!(
                    move_to(&repo, StorageKind::S3).await,
                    Err(OxenError::S3BackendMissingServerOpts)
                ),
                "a repository on a server with no S3 bucket cannot move to S3"
            );

            let (bucket, _tmp, _server) = s3::tests::setup().await;
            let local = repo.version_store();
            let mut hashes = Vec::new();
            for content in ["first", "second", "third"] {
                let hash = hasher::hash_buffer(content.as_bytes());
                local.store_version(&hash, Bytes::from(content)).await?;
                hashes.push(hash);
            }
            // S3 already holds one, as after an earlier move that stopped part-way.
            bucket
                .store_version(&hashes[0], Bytes::from("first"))
                .await?;
            // A version with chunks and no data yet, as during a chunked upload.
            let unfinished = hasher::hash_buffer(b"unfinished");
            local
                .store_version_chunk(&unfinished, 0, Bytes::from("unfin"))
                .await?;

            let moved = copy_missing(local.as_ref(), &bucket, Vec::new()).await?;
            let mut sorted = moved.clone();
            sorted.sort();
            let mut expected = hashes.clone();
            expected.sort();
            assert_eq!(
                sorted, expected,
                "every version with data is copied, and only those"
            );

            // Lands after the first copy, as a push while the bulk of the files copy would.
            let late = hasher::hash_buffer(b"fourth");
            local.store_version(&late, Bytes::from("fourth")).await?;
            hashes.push(late.clone());

            let moved = catch_up_and_switch(&repo, local.as_ref(), &bucket, moved).await?;
            assert!(
                bucket.version_exists(&late).await?,
                "the final pass copies what arrived after the first"
            );
            assert_eq!(
                LocalRepository::from_dir(&repo.path)?.storage_config().kind,
                StorageKind::S3
            );
            delete_old_copies(&repo, local.as_ref(), &moved).await;
            for hash in &hashes {
                assert!(
                    !local.version_exists(hash).await?,
                    "the local copy of {hash} is deleted once S3 holds it"
                );
            }
            assert!(
                matches!(
                    catch_up_and_switch(&repo, local.as_ref(), &bucket, Vec::new()).await,
                    Err(OxenError::StorageChangedDuringMove(_))
                ),
                "a move begun before another one switched the repository stops"
            );

            let on_s3 = LocalRepository::from_dir(&repo.path)?;
            let moved = copy_missing(&bucket, local.as_ref(), Vec::new()).await?;
            let moved = catch_up_and_switch(&on_s3, &bucket, local.as_ref(), moved).await?;
            delete_old_copies(&on_s3, &bucket, &moved).await;
            let back = LocalRepository::from_dir(&repo.path)?;
            assert_eq!(back.storage_config().kind, StorageKind::Local);
            for hash in &hashes {
                assert!(
                    local.version_exists(hash).await?,
                    "{hash} is back on local storage"
                );
            }
            assert_eq!(
                bucket.find_missing_versions(&hashes).await?.len(),
                hashes.len(),
                "the S3 copies are deleted once local storage holds them"
            );

            move_to(&back, StorageKind::Local).await?;
            Ok(())
        })
        .await
    }
}
