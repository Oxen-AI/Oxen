use std::path::{Path, PathBuf};
use std::process::Command;

use crate::api;
use crate::config::UserConfig;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::error::OxenError;
use crate::lfs::config::LfsConfig;
use crate::lfs::filter;
use crate::lfs::pointer::PointerFile;
use crate::lfs::status;
use crate::model::NewCommitBody;
use crate::storage::version_store::VersionStore;
use crate::storage::LocalVersionStore;

/// Push large file versions to the configured Oxen remote.
///
/// Called by the pre-push hook or the `oxen lfs push` CLI command.
/// `hook_args` receives the hook arguments (remote name and URL) passed
/// by Git; logged for debugging.
pub async fn push_to_remote(
    repo_root: &Path,
    oxen_dir: &Path,
    hook_args: &[String],
) -> Result<(), OxenError> {
    log::debug!("oxen lfs push: hook_args={hook_args:?}");

    let versions_dir = oxen_dir.join("versions");
    std::fs::create_dir_all(&versions_dir).ok();

    let lfs_config = LfsConfig::load(oxen_dir)?;
    let remote_repo = match lfs_config.resolve_remote().await? {
        Some(r) => r,
        None => {
            log::info!("oxen lfs push: no remote configured, skipping");
            return Ok(());
        }
    };

    let statuses = status::get_status(repo_root, &versions_dir).await?;
    let to_push: Vec<_> = statuses.iter().filter(|s| s.local).collect();
    if to_push.is_empty() {
        log::info!("oxen lfs push: nothing to push");
        return Ok(());
    }

    let store = LocalVersionStore::new(&versions_dir);

    // Build a temporary staging directory mirroring the files' real repo-relative paths.
    // `add_files` expects absolute paths rooted under a common base directory.
    let staging_dir = tempfile::tempdir().map_err(|e| {
        OxenError::basic_str(format!("oxen lfs push: failed to create staging dir: {e}"))
    })?;

    let mut staged_paths: Vec<PathBuf> = Vec::new();
    for file_status in &to_push {
        let src = store.get_version_path(&file_status.pointer.oid)?;
        let dest = staging_dir.path().join(&file_status.path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Prefer hard-link to avoid copying; fall back to copy.
        if std::fs::hard_link(&src, &dest).is_err() {
            std::fs::copy(&src, &dest)?;
        }
        staged_paths.push(dest);
    }

    let workspace_id = uuid::Uuid::new_v4().to_string();

    // Create workspace → upload files → commit. On any error, attempt cleanup.
    let result = push_workspace(
        &remote_repo,
        &workspace_id,
        staging_dir.path(),
        &staged_paths,
    )
    .await;

    if let Err(ref e) = result {
        log::warn!("oxen lfs push: push failed ({e}), cleaning up workspace");
        if let Err(del_err) = api::client::workspaces::delete(&remote_repo, &workspace_id).await {
            log::warn!("oxen lfs push: workspace cleanup failed: {del_err}");
        }
    }

    result?;

    println!(
        "oxen lfs push: uploaded {} file(s) to {}",
        to_push.len(),
        remote_repo.url()
    );
    Ok(())
}

/// Inner helper: create workspace, add files, commit.
async fn push_workspace(
    remote_repo: &crate::model::RemoteRepository,
    workspace_id: &str,
    staging_dir: &Path,
    staged_paths: &[PathBuf],
) -> Result<(), OxenError> {
    api::client::workspaces::create(remote_repo, DEFAULT_BRANCH_NAME, workspace_id).await?;

    api::client::workspaces::files::add_files(
        remote_repo,
        workspace_id,
        staging_dir,
        staged_paths.to_vec(),
    )
    .await?;

    let user_config = UserConfig::get()?;
    let body = NewCommitBody {
        message: "oxen lfs push: sync large files".to_string(),
        author: user_config.name,
        email: user_config.email,
    };

    api::client::workspaces::commits::commit(remote_repo, DEFAULT_BRANCH_NAME, workspace_id, &body)
        .await?;

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
    let mut need_remote: Vec<&status::LfsFileStatus> = Vec::new();

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
                need_remote.push(file_status);
            }
        }
    }

    // Batch-download any remaining files from the Oxen remote.
    if !need_remote.is_empty() {
        if let Some(remote_repo) = lfs_config.resolve_remote().await? {
            let hashes: Vec<String> = need_remote.iter().map(|s| s.pointer.oid.clone()).collect();
            api::client::versions::download_versions_to_store(&remote_repo, &hashes, &store)
                .await?;

            // Restore the now-downloaded files to the working tree.
            for file_status in &need_remote {
                let dest = repo_root.join(&file_status.path);
                store
                    .copy_version_to_path(&file_status.pointer.oid, &dest)
                    .await?;
                restored_paths.push(file_status.path.clone());
            }
        } else {
            for s in &need_remote {
                log::warn!(
                    "oxen lfs pull: {} not available locally and no remote configured",
                    s.path.display()
                );
            }
        }
    }

    if !restored_paths.is_empty() {
        // Re-add restored files so Git's index stat cache reflects the new
        // on-disk content. The clean filter produces the same pointer blob,
        // so no actual index change occurs — only the stat cache is updated.
        git_add(repo_root, &restored_paths)?;
        println!("oxen lfs pull: restored {} file(s)", restored_paths.len());
    }

    Ok(())
}

/// Force-synchronize ALL tracked pointer files in the working tree.
///
/// For each pointer file that matches a tracked pattern:
/// 1. Try the local `.oxen/versions/` store.
/// 2. Try the origin's `.oxen/versions/` (for local clones).
/// 3. Try the configured Oxen remote.
/// 4. If any file still cannot be resolved, return an error listing all failures.
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
    let mut unresolved: Vec<&status::LfsFileStatus> = Vec::new();

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
            unresolved.push(file_status);
        } else {
            std::fs::write(&dest, &result)?;
            restored_paths.push(file_status.path.clone());
            println!("  restored: {}", file_status.path.display());
        }
    }

    // Try the configured Oxen remote for any remaining unresolved pointers.
    if !unresolved.is_empty() {
        if let Some(remote_repo) = lfs_config.resolve_remote().await? {
            let hashes: Vec<String> = unresolved.iter().map(|s| s.pointer.oid.clone()).collect();
            api::client::versions::download_versions_to_store(&remote_repo, &hashes, &store)
                .await?;

            for file_status in &unresolved {
                let dest = repo_root.join(&file_status.path);
                store
                    .copy_version_to_path(&file_status.pointer.oid, &dest)
                    .await?;
                restored_paths.push(file_status.path.clone());
                println!("  restored (remote): {}", file_status.path.display());
            }
            unresolved.clear();
        }
    }

    if !unresolved.is_empty() {
        let failures: Vec<String> = unresolved
            .iter()
            .map(|s| format!("{} (oid: {})", s.path.display(), s.pointer.oid))
            .collect();
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
    git_add(repo_root, &restored_paths)?;

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
fn git_add(repo_root: &Path, paths: &[PathBuf]) -> Result<(), OxenError> {
    if paths.is_empty() {
        return Ok(());
    }

    let path_args: Vec<&str> = paths.iter().filter_map(|p| p.to_str()).collect();
    if path_args.is_empty() {
        return Ok(());
    }

    let mut cmd = Command::new("git");
    cmd.arg("add").args(&path_args).current_dir(repo_root);

    let output = cmd
        .output()
        .map_err(|e| OxenError::basic_str(format!("oxen lfs: failed to spawn git add: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        log::warn!("oxen lfs: git add exited with {}: {stderr}", output.status);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lfs::filter;
    use crate::lfs::gitattributes;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_push_no_remote_configured() {
        // With no remote_url in lfs.toml, push should succeed silently.
        let tmp = TempDir::new().unwrap();
        let repo_root = tmp.path();
        let oxen_dir = repo_root.join(".oxen");
        std::fs::create_dir_all(&oxen_dir).unwrap();

        // Save config with no remote.
        let cfg = LfsConfig::default();
        cfg.save(&oxen_dir).unwrap();

        let result = push_to_remote(repo_root, &oxen_dir, &[]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pull_local_only_no_network() {
        // local_only pull should not attempt network calls; it should
        // restore files that are in the local store and skip the rest.
        let tmp = TempDir::new().unwrap();
        let repo_root = tmp.path();
        let oxen_dir = repo_root.join(".oxen");
        let versions_dir = oxen_dir.join("versions");
        std::fs::create_dir_all(&versions_dir).unwrap();

        // Track *.bin and create a pointer file whose content IS local.
        gitattributes::track_pattern(repo_root, "*.bin").unwrap();
        let content = b"local binary content";
        let pointer_bytes = filter::clean(&versions_dir, content).await.unwrap();
        std::fs::write(repo_root.join("data.bin"), &pointer_bytes).unwrap();

        // Save default config (no remote).
        LfsConfig::default().save(&oxen_dir).unwrap();

        let result = pull_from_remote(repo_root, &oxen_dir, true).await;
        assert!(result.is_ok());

        // The file should be restored to real content.
        let on_disk = std::fs::read(repo_root.join("data.bin")).unwrap();
        assert_eq!(on_disk, content);
    }

    #[tokio::test]
    async fn test_git_add_returns_result() {
        // git_add on an empty list should be Ok.
        let tmp = TempDir::new().unwrap();
        let result = git_add(tmp.path(), &[]);
        assert!(result.is_ok());

        // git_add on a path in a non-git dir should still return Ok
        // (git add will fail but we only warn on non-zero exit).
        let result = git_add(tmp.path(), &[PathBuf::from("nonexistent.txt")]);
        assert!(result.is_ok());
    }
}
