use crate::config::RepositoryConfig;
use crate::config::repository_config::MerkleStoreKind;
use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACES_DIR};
use crate::core::db::merkle_node::lmdb::{OXEN_LMDB_MERKLE_DIR, get_or_open, lmdb_dir_location};
use crate::error::OxenError;
use crate::util::fs::{self as oxen_fs, config_filepath};
use crate::view::fork::{ForkStartResponse, ForkStatus, ForkStatusFile, ForkStatusResponse};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use toml;

pub const FORK_STATUS_FILENAME: &str = "fork_status.toml";

fn write_status(repo_path: &Path, status: &ForkStatus) -> Result<(), OxenError> {
    let status_path = repo_path.join(OXEN_HIDDEN_DIR).join(FORK_STATUS_FILENAME);
    if let Some(parent) = status_path.parent() {
        oxen_fs::create_dir_all(parent)?;
    }
    let status_file: ForkStatusFile = status.clone().into();
    fs::write(status_path, toml::to_string(&status_file)?)?;
    Ok(())
}

fn read_status(repo_path: &Path) -> Result<Option<ForkStatus>, OxenError> {
    let status_path = repo_path.join(OXEN_HIDDEN_DIR).join(FORK_STATUS_FILENAME);
    if !status_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&status_path)?;
    let status_file: ForkStatusFile = toml::from_str(&content).map_err(|e| {
        log::error!("Failed to parse fork status on file: {status_path:?} error: {e}");
        OxenError::basic_str(format!("Failed to parse fork status on file: {e}"))
    })?;

    let status = &status_file.status;

    Ok(Some(match status {
        ForkStatus::Started => ForkStatus::Started,
        ForkStatus::InProgress(_) => ForkStatus::InProgress(status_file.progress.unwrap_or(0.0)),
        ForkStatus::Complete => ForkStatus::Complete,
        ForkStatus::Counting(_) => ForkStatus::Counting(status_file.progress.unwrap_or(0.0) as u32),
        ForkStatus::Failed(_) => ForkStatus::Failed(
            status_file
                .error
                .unwrap_or_else(|| "Unknown error".to_string()),
        ),
    }))
}

/// Evict the process-wide cached RocksDB instances for `repo_path` so their
/// on-disk files are no longer held open. Required before copying a repo's
/// `.oxen/` tree on Windows, where an open file can't be copied (see the call
/// site in [`start_fork`]). Covers every cached RocksDB whose files live inside
/// the copied tree: refs (`.oxen/refs`), staged (`.oxen/staged`), per-commit
/// dir-hashes (`.oxen/history/<id>/dir_hashes`), and the file-backend Merkle
/// node cache. The workspace-name index is omitted on purpose — it lives under
/// `.oxen/workspaces`, which the copy skips. Mirrors the eviction done by
/// [`crate::repositories::transfer_namespace`] before relocating a repo on disk.
fn close_cached_dbs(repo_path: &Path) -> Result<(), OxenError> {
    crate::model::merkle_tree::merkle_tree_node_cache::remove_from_cache(repo_path)?;
    crate::core::staged::remove_from_cache_with_children(repo_path)?;
    crate::core::refs::remove_from_cache(repo_path)?;
    crate::core::db::dir_hashes::dir_hashes_db::remove_from_cache_with_children(repo_path)?;
    Ok(())
}

pub fn start_fork(
    original_path: PathBuf,
    new_path: PathBuf,
) -> Result<ForkStartResponse, OxenError> {
    if new_path.exists() {
        return Err(OxenError::ForkDestinationExists(new_path));
    }

    let og_config = RepositoryConfig::from_file(config_filepath(&original_path))?;

    let using_lmdb = {
        let configured_lmdb = matches!(og_config.merkle_store_kind, MerkleStoreKind::Lmdb);
        if configured_lmdb && !lmdb_dir_location(&original_path).is_dir() {
            return Err(OxenError::MisconfiguredMerkleLmdb(original_path));
        }
        configured_lmdb
    };

    if og_config.vfs.unwrap_or(false) && using_lmdb {
        return Err(OxenError::MerkleStoreLmdbNotSupportedOnVfs);
    }

    oxen_fs::create_dir_all(&new_path)?;
    write_status(&new_path, &ForkStatus::Counting(0))?;

    let new_path_clone = new_path.clone();

    thread::spawn(move || {
        let total_items = match count_items(&original_path, &new_path, 0) {
            Ok(count) => count as f32,
            Err(e) => {
                log::error!("Failed to count items: {e}");
                write_status(&new_path, &ForkStatus::Failed(e.to_string())).unwrap_or_else(|e| {
                    log::error!("Failed to write fork error status: {e}");
                });
                return;
            }
        };

        // if LMDB, then perform a special copy of its database file.
        if using_lmdb {
            match get_or_open(&original_path) {
                Ok(lmdb_backend) => {
                    // Copy **without** compaction (`CompactionOption::Disabled`). A fork is a
                    // verbatim copy of the store and compation only works if the physical
                    // location is unchanged.
                    if let Err(error) =
                        lmdb_backend.copy_on_disk(&new_path, heed::CompactionOption::Disabled)
                    {
                        log::error!(
                            "Repository {} uses LMDB but failed to copy its on-disk file: {error}",
                            original_path.display()
                        );
                        write_status(&new_path, &ForkStatus::Failed(error.to_string()))
                            .unwrap_or_else(|e| {
                                log::error!("Failed to write fork error status: {e}");
                            });
                        return;
                    }
                }
                Err(error) => {
                    log::error!(
                        "Repository {} uses LMDB but failed to open its env: {error}",
                        original_path.display()
                    );
                    write_status(&new_path, &ForkStatus::Failed(error.to_string())).unwrap_or_else(
                        |e| {
                            log::error!("Failed to write fork error status: {e}");
                        },
                    );
                    return;
                }
            }
        };

        // Close any cached RocksDB instances for the source repo before copying its
        // files. These DBs (refs, staged, the file-backend Merkle node cache) are
        // kept open process-wide in LRU caches, and RocksDB holds their files open —
        // notably the directory `LOCK`. On Windows `fs::copy` (CopyFileExW) then fails
        // with a sharing violation ("being used by another process", os error 32) when
        // it reaches a file another handle holds open. Evicting drops the last
        // `Arc<DB>`, closing the DBs and releasing the handles. Mirrors
        // `repositories::transfer_namespace`, which evicts the same caches before
        // relocating a repo on disk. (The LMDB env is unaffected: its dir is skipped by
        // the copy below and was snapshotted above via `copy_on_disk`.)
        if let Err(e) = close_cached_dbs(&original_path) {
            log::error!("Failed to close source repo DB caches before fork copy: {e}");
            write_status(&new_path, &ForkStatus::Failed(e.to_string())).unwrap_or_else(|e| {
                log::error!("Failed to write fork error status: {e}");
            });
            return;
        }

        // ignores workspaces and LMDB dirs
        match copy_dir_recursive(&original_path, &new_path, &new_path, total_items, 0.0) {
            Ok(_) => {
                write_status(&new_path, &ForkStatus::Complete).unwrap_or_else(|e| {
                    log::error!("Failed to write fork completion status: {e}");
                });
            }
            Err(e) => {
                write_status(&new_path, &ForkStatus::Failed(e.to_string())).unwrap_or_else(|e| {
                    log::error!("Failed to write fork error status: {e}");
                });
            }
        }
    });

    Ok(ForkStartResponse {
        repository: new_path_clone.to_string_lossy().to_string(),
        fork_status: ForkStatus::Started.to_string(),
    })
}

pub fn get_fork_status(repo_path: &Path) -> Result<ForkStatusResponse, OxenError> {
    let status = read_status(repo_path)?.ok_or(OxenError::ForkStatusNotFound)?;

    Ok(ForkStatusResponse {
        repository: repo_path.to_string_lossy().to_string(),
        status: match status {
            ForkStatus::Started => ForkStatus::Started.to_string(),
            ForkStatus::Counting(_) => ForkStatus::Counting(0).to_string(),
            ForkStatus::InProgress(_) => ForkStatus::InProgress(0.0).to_string(),
            ForkStatus::Complete => ForkStatus::Complete.to_string(),
            ForkStatus::Failed(_) => ForkStatus::Failed("".to_string()).to_string(),
        },
        progress: match status {
            ForkStatus::InProgress(p) => Some(p),
            ForkStatus::Counting(c) => Some(c as f32),
            _ => None,
        },
        error: match status {
            ForkStatus::Failed(e) => Some(e),
            _ => None,
        },
    })
}

/// Copies everything from `src` to `dst`, except for workspaces & LMDB.
fn copy_dir_recursive(
    src: &Path,
    dst: &Path,
    status_repo: &Path,
    total_items: f32,
    mut progress: f32,
) -> Result<f32, OxenError> {
    let workspaces_path = PathBuf::from(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR);
    let lmdb_path = PathBuf::from(OXEN_HIDDEN_DIR).join(OXEN_LMDB_MERKLE_DIR);

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let dest_path = dst.join(entry.file_name());

        if path.ends_with(&workspaces_path) || path.ends_with(&lmdb_path) {
            continue;
        }

        if path.is_dir() {
            oxen_fs::create_dir_all(&dest_path)?;
            progress = copy_dir_recursive(&path, &dest_path, status_repo, total_items, progress)?;
        } else {
            fs::copy(&path, &dest_path)?;
            progress += 1.0;
        }
    }

    let updated_progress = if total_items > 0.0 {
        (progress / total_items) * 100.0
    } else {
        100.0 // Assume completion if there are no items to copy
    };
    write_status(status_repo, &ForkStatus::InProgress(updated_progress))?;
    Ok(updated_progress)
}

fn count_items(path: &Path, status_repo: &Path, mut current_count: u32) -> Result<u32, OxenError> {
    let workspaces_path = PathBuf::from(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR);
    let lmdb_path = PathBuf::from(OXEN_HIDDEN_DIR).join(OXEN_LMDB_MERKLE_DIR);

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.ends_with(&workspaces_path) || path.ends_with(&lmdb_path) {
            continue;
        }
        if path.is_dir() {
            current_count = count_items(&path, status_repo, current_count)?;
        } else {
            current_count += 1;
        }
    }
    write_status(status_repo, &ForkStatus::Counting(current_count))?;
    Ok(current_count)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::RepositoryConfig;
    use crate::constants::REPO_CONFIG_FILENAME;
    use crate::error::OxenError;
    use crate::{repositories, test};

    #[tokio::test]
    async fn test_fork_operations() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|test_dir| {
            async move {
                let original_repo_path = test_dir.join("original");
                let _original_repo = repositories::init(&original_repo_path)?;
                let forked_repo_path = test_dir.join("forked");

                // Create a directory and add a file to it
                let dir_path = original_repo_path.join("dir");
                oxen_fs::create_dir_all(&dir_path)?;
                let file_path = dir_path.join("test_file.txt");
                std::fs::write(file_path, "test file content")?;

                // Create a workspace directory and add a file to it
                let workspaces_path = original_repo_path
                    .join(OXEN_HIDDEN_DIR)
                    .join(WORKSPACES_DIR);
                oxen_fs::create_dir_all(&workspaces_path)?;
                let workspace_file = workspaces_path.join("test_workspace.txt");
                std::fs::write(workspace_file, "test workspace content")?;

                start_fork(original_repo_path.clone(), forked_repo_path.clone())?;
                let mut current_status = "in_progress".to_string();
                let mut attempts = 0;
                const MAX_ATTEMPTS: u32 = 10; // 10 seconds timeout (10 * 1s)

                while current_status == "in_progress" && attempts < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(1000)).await; // Wait for 1 second
                    current_status = match get_fork_status(&forked_repo_path) {
                        Ok(status) => status.status,
                        Err(e) => {
                            if matches!(e, OxenError::ForkStatusNotFound) {
                                "in_progress".to_string()
                            } else {
                                return Err(e);
                            }
                        }
                    };
                    attempts += 1;
                }

                if attempts >= MAX_ATTEMPTS {
                    return Err(OxenError::basic_str("Fork operation timed out"));
                }

                let file_path = original_repo_path.clone().join("dir/test_file.txt");

                assert!(forked_repo_path.exists());
                // Verify that the content of the file is the same in both repos
                let new_file_path = forked_repo_path.join("dir/test_file.txt");
                let original_content = fs::read_to_string(&file_path)?;
                let mut retries = 10;
                let sleep_time = 100;
                let new_content = loop {
                    if new_file_path.exists() {
                        break fs::read_to_string(&new_file_path)?;
                    }

                    if retries == 0 {
                        return Err(OxenError::basic_str("File not found after retries"));
                    }

                    tokio::time::sleep(Duration::from_millis(sleep_time)).await;
                    retries -= 1;
                };

                assert_eq!(
                    original_content, new_content,
                    "The content of test_file.txt should be the same in both repositories"
                );

                // Verify that workspaces directory was not copied
                let new_workspaces_path =
                    forked_repo_path.join(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR);
                assert!(
                    !new_workspaces_path.exists(),
                    "workspaces directory should not be copied"
                );

                // Verify that storage config was updated to point to forked repo
                let forked_config_path = forked_repo_path
                    .join(OXEN_HIDDEN_DIR)
                    .join(REPO_CONFIG_FILENAME);
                assert!(
                    forked_config_path.exists(),
                    "Forked repo config should exist"
                );

                let forked_config = RepositoryConfig::from_file(&forked_config_path)?;
                if let Some(storage) = forked_config.storage {
                    // `versions_path: None` is the canonical "use the default location" shape:
                    // `LocalRepository::storage_config` round-trips, and `create_version_store`
                    // resolves `None` to `.oxen/versions/files`. If the legacy explicit-path
                    // shape is present, it must match the default.
                    if let Some(storage_path) = storage.versions_path {
                        let expected_path = PathBuf::from(OXEN_HIDDEN_DIR)
                            .join("versions")
                            .join("files");

                        assert_eq!(
                            storage_path, expected_path,
                            "Storage path should default to the relative path"
                        );
                    }
                }

                // Fork fails if repo exists
                let result = start_fork(original_repo_path.clone(), forked_repo_path.clone());
                assert!(
                    result.is_err(),
                    "Expected an error because the repo already exists."
                );

                Ok(())
            }
        })
        .await
    }

    /// Fork an LMDB-backed repository and verify the forked repo is also LMDB-backed,
    /// has the canonical snapshotted `data.mdb` on disk, and exposes the same commits
    /// as the source — i.e. the LMDB merkle store survived the `Env::copy_to_path`
    /// snapshot path inside `start_fork`.
    #[tokio::test]
    async fn test_fork_lmdb_backed_repo_preserves_backend_and_data() -> Result<(), OxenError> {
        use crate::config::repository_config::MerkleStoreKind;
        use crate::model::LocalRepository;
        use crate::test::repo_prep::{init_lmdb_safe_test_repo_with_merkle_store, lmdb_test_base};
        use crate::test::{add_txt_file_to_dir, generate_random_string};

        let source = init_lmdb_safe_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        assert_eq!(source.merkle_store_kind(), MerkleStoreKind::Lmdb);

        // A real commit so the LMDB env actually has merkle nodes to snapshot.
        // Done inline rather than via `apply_one_commit_local_repo` because the latter uses
        // `run_async` (a nested `block_on`), which conflicts with this test's tokio runtime.
        let file_path = add_txt_file_to_dir(&source.path, &generate_random_string(20))?;
        repositories::add(&source, &file_path).await?;
        repositories::commit(&source, "Init commit")?;
        let source_commit_ids: Vec<String> = repositories::commits::list_all(&source)?
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert!(
            !source_commit_ids.is_empty(),
            "fresh one-commit repo must expose at least one commit"
        );

        // Place the fork sibling-of the source under `lmdb_test_base()`, not inside it,
        // so `copy_dir_recursive` doesn't recurse into its own destination.
        let forked_path = lmdb_test_base().join(format!("forked-{}", uuid::Uuid::new_v4()));

        start_fork(source.path.clone(), forked_path.clone())?;

        // Poll until the fork reaches a terminal state (`complete` or `failed`),
        // tolerating the transient `started`/`counting`/`in_progress` states and a
        // not-yet-created status file. Mirrors `test_fork_operations`.
        let mut status = get_fork_status(&forked_path).ok();
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 30;
        while !matches!(
            status.as_ref().map(|s| s.status.as_str()),
            Some("complete" | "failed")
        ) && attempts < MAX_ATTEMPTS
        {
            tokio::time::sleep(Duration::from_millis(500)).await;
            status = match get_fork_status(&forked_path) {
                Ok(s) => Some(s),
                Err(OxenError::ForkStatusNotFound) => None,
                Err(e) => return Err(e),
            };
            attempts += 1;
        }
        let status = status.expect("fork status file should exist after polling");
        assert_eq!(
            status.status, "complete",
            "fork should finish in 'complete' state; got {:?} (error: {:?})",
            status.status, status.error,
        );

        // The snapshot must land at `<forked>/.oxen/lmdb_merkle_tree_store/data.mdb`,
        // since heed's `copy_to_path` is called with that exact path in `LmdbBackend::copy_on_disk`.
        let dst_lmdb_dir = lmdb_dir_location(&forked_path);
        let dst_data_mdb = dst_lmdb_dir.join("data.mdb");
        assert!(
            dst_data_mdb.is_file(),
            "forked LMDB env should contain data.mdb at {}",
            dst_data_mdb.display()
        );
        assert!(
            std::fs::metadata(&dst_data_mdb)?.len() > 0,
            "snapshot data.mdb should be non-empty"
        );

        // The recursive copy must skip the source's LMDB dir on the destination side —
        // otherwise it would have clobbered the snapshot with a half-written `data.mdb`.
        // (We can't easily detect "clobbered", but if the readback below succeeds, it didn't.)

        // Open the forked repo: this exercises the snapshot for real by opening the env.
        let forked = LocalRepository::from_dir(&forked_path)?;
        assert_eq!(
            forked.merkle_store_kind(),
            MerkleStoreKind::Lmdb,
            "forked repo must preserve the source's MerkleStoreKind"
        );
        let forked_commit_ids: Vec<String> = repositories::commits::list_all(&forked)?
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(
            forked_commit_ids, source_commit_ids,
            "forked repo must expose the same commit set as the source"
        );

        // Release the env mmap before removing the dir so Windows can unlink the file.
        drop(forked);
        let _ = std::fs::remove_dir_all(&forked_path);
        Ok(())
    }
}
