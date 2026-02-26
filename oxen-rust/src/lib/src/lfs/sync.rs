use std::path::Path;

use crate::error::OxenError;
use crate::lfs::pointer::PointerFile;
use crate::lfs::status;
use crate::storage::version_store::VersionStore;
use crate::storage::LocalVersionStore;

/// Push large file versions to the configured Oxen remote.
///
/// Called by the pre-push hook. `_args` receives the hook arguments
/// (remote name and URL) passed by Git.
pub async fn push_to_remote(
    repo_root: &Path,
    oxen_dir: &Path,
    _args: &[String],
) -> Result<(), OxenError> {
    let versions_dir = oxen_dir.join("versions");
    let statuses = status::get_status(repo_root, &versions_dir).await?;

    let to_push: Vec<_> = statuses.iter().filter(|s| s.local).collect();
    if to_push.is_empty() {
        log::info!("oxen lfs push: nothing to push");
        return Ok(());
    }

    // TODO (Phase 3): Upload missing versions to the Oxen remote
    // using the api::client infrastructure.
    log::info!(
        "oxen lfs push: {} files would be pushed (remote sync not yet implemented)",
        to_push.len()
    );

    Ok(())
}

/// Pull large file content and restore pointer files in the working tree.
///
/// When `local_only` is true, only restores from the local `.oxen/versions/`
/// store (no network). This is used by post-checkout and post-merge hooks.
pub async fn pull_from_remote(
    repo_root: &Path,
    oxen_dir: &Path,
    local_only: bool,
) -> Result<(), OxenError> {
    let versions_dir = oxen_dir.join("versions");
    let statuses = status::get_status(repo_root, &versions_dir).await?;

    let store = LocalVersionStore::new(&versions_dir);
    let mut restored = 0u64;

    for file_status in &statuses {
        if file_status.local {
            // Content is available locally — restore the actual file.
            let dest = repo_root.join(&file_status.path);
            store
                .copy_version_to_path(&file_status.pointer.oid, &dest)
                .await?;
            restored += 1;
        } else if !local_only {
            // TODO (Phase 3): Fetch from remote, then restore.
            log::warn!(
                "oxen lfs pull: {} not available locally and remote fetch not yet implemented",
                file_status.path.display()
            );
        }
    }

    if restored > 0 {
        log::info!("oxen lfs pull: restored {restored} files");
    }

    Ok(())
}

/// Scan working tree for pointer files and return the list of OIDs
/// that need to be pushed.
pub async fn list_pushable_oids(
    repo_root: &Path,
    oxen_dir: &Path,
) -> Result<Vec<PointerFile>, OxenError> {
    let versions_dir = oxen_dir.join("versions");
    let statuses = status::get_status(repo_root, &versions_dir).await?;
    Ok(statuses
        .into_iter()
        .filter(|s| s.local)
        .map(|s| s.pointer)
        .collect())
}
