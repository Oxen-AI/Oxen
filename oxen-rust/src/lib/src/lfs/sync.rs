use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::OxenError;
use crate::lfs::config::LfsConfig;
use crate::lfs::filter;
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
    std::fs::create_dir_all(&versions_dir).ok();

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
    std::fs::create_dir_all(&versions_dir).ok();

    let statuses = status::get_status(repo_root, &versions_dir).await?;

    let store = LocalVersionStore::new(&versions_dir);
    let lfs_config = LfsConfig::load(oxen_dir)?;
    let mut restored_paths: Vec<PathBuf> = Vec::new();

    for file_status in &statuses {
        if file_status.local {
            // Content is available locally — restore the actual file.
            let dest = repo_root.join(&file_status.path);
            store
                .copy_version_to_path(&file_status.pointer.oid, &dest)
                .await?;
            restored_paths.push(file_status.path.clone());
        } else {
            // Try smudge (which checks origin for local clones).
            let pointer_data = file_status.pointer.encode();
            let result =
                filter::smudge(&versions_dir, repo_root, &lfs_config, &pointer_data).await?;
            if !PointerFile::is_pointer(&result) {
                // Smudge resolved it — write to working tree.
                let dest = repo_root.join(&file_status.path);
                std::fs::write(&dest, &result)?;
                restored_paths.push(file_status.path.clone());
            } else if !local_only {
                // TODO (Phase 3): Fetch from remote, then restore.
                log::warn!(
                    "oxen lfs pull: {} not available locally and remote fetch not yet implemented",
                    file_status.path.display()
                );
            }
        }
    }

    if !restored_paths.is_empty() {
        // Re-add restored files so Git's index stat cache reflects the new
        // on-disk content. The clean filter produces the same pointer blob,
        // so no actual index change occurs — only the stat cache is updated.
        git_add(repo_root, &restored_paths);
        println!("oxen lfs pull: restored {} file(s)", restored_paths.len());
    }

    Ok(())
}

/// Force-synchronize ALL tracked pointer files in the working tree.
///
/// For each pointer file that matches a tracked pattern:
/// 1. Try the local `.oxen/versions/` store.
/// 2. Try the origin's `.oxen/versions/` (for local clones).
/// 3. If any file cannot be resolved, return an error listing all failures.
///
/// This is meant to be run explicitly by the user to guarantee every
/// pointer is replaced with actual content.
pub async fn fetch_all(repo_root: &Path, oxen_dir: &Path) -> Result<(), OxenError> {
    let versions_dir = oxen_dir.join("versions");
    std::fs::create_dir_all(&versions_dir).ok();

    let lfs_config = LfsConfig::load(oxen_dir)?;
    let statuses = status::get_status(repo_root, &versions_dir).await?;

    if statuses.is_empty() {
        println!("oxen lfs fetch-all: no tracked pointer files found");
        return Ok(());
    }

    let store = LocalVersionStore::new(&versions_dir);
    let mut restored_paths: Vec<PathBuf> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    for file_status in &statuses {
        let dest = repo_root.join(&file_status.path);

        if file_status.local {
            // Available in local store — restore directly.
            store
                .copy_version_to_path(&file_status.pointer.oid, &dest)
                .await?;
            restored_paths.push(file_status.path.clone());
            println!("  restored: {}", file_status.path.display());
            continue;
        }

        // Try smudge (which checks origin for local clones).
        let pointer_data = file_status.pointer.encode();
        let result = filter::smudge(&versions_dir, repo_root, &lfs_config, &pointer_data).await?;

        if PointerFile::is_pointer(&result) {
            // Could not resolve this pointer.
            failures.push(format!(
                "{} (oid: {})",
                file_status.path.display(),
                file_status.pointer.oid
            ));
        } else {
            std::fs::write(&dest, &result)?;
            restored_paths.push(file_status.path.clone());
            println!("  restored: {}", file_status.path.display());
        }
    }

    if !failures.is_empty() {
        let msg = format!(
            "oxen lfs fetch-all: {} file(s) could not be resolved:\n  {}",
            failures.len(),
            failures.join("\n  ")
        );
        return Err(OxenError::basic_str(msg));
    }

    // Re-add restored files so Git's index stat cache reflects the new
    // on-disk content. The clean filter produces the same pointer blob,
    // so no actual index change occurs — only the stat cache is updated.
    git_add(repo_root, &restored_paths);

    println!(
        "oxen lfs fetch-all: all {} file(s) restored successfully",
        restored_paths.len()
    );
    Ok(())
}

/// Run `git add` on a list of paths so Git's index stat cache is updated.
///
/// After we replace a pointer file with real content, the on-disk size and
/// mtime change. Without re-adding, `git status` shows the files as modified
/// even though the clean filter produces the identical blob. Re-adding lets
/// Git refresh its stat cache.
fn git_add(repo_root: &Path, paths: &[PathBuf]) {
    if paths.is_empty() {
        return;
    }

    let path_args: Vec<&str> = paths.iter().filter_map(|p| p.to_str()).collect();
    if path_args.is_empty() {
        return;
    }

    let mut cmd = Command::new("git");
    cmd.arg("add").args(&path_args).current_dir(repo_root);

    if let Err(e) = cmd.output() {
        log::warn!("oxen lfs: failed to run git add to refresh index: {e}");
    }
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
