use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::OxenError;
use crate::lfs::config::LfsConfig;
use crate::lfs::pointer::PointerFile;
use crate::storage::version_store::VersionStore;
use crate::storage::LocalVersionStore;
use crate::util::hasher;

/// Clean filter: hash content, store in version store, return pointer bytes.
///
/// If the input is already a valid pointer it is returned unchanged (idempotent).
pub async fn clean(versions_dir: &Path, content: &[u8]) -> Result<Vec<u8>, OxenError> {
    // Idempotent: don't re-clean a pointer.
    if PointerFile::is_pointer(content) {
        return Ok(content.to_vec());
    }

    let hash = hasher::hash_buffer(content);

    let store = LocalVersionStore::new(versions_dir);
    store.init().await?;
    store.store_version(&hash, content).await?;

    let pointer = PointerFile::new(&hash, content.len() as u64);
    Ok(pointer.encode())
}

/// Smudge filter: parse pointer, look up content in version store, return content.
///
/// Strategy:
/// 1. Local `.oxen/versions/` store.
/// 2. Origin's `.oxen/versions/` (for local clones — discovered via `git config remote.origin.url`).
/// 3. Fallback: return pointer bytes unchanged with a warning.
pub async fn smudge(
    versions_dir: &Path,
    repo_root: &Path,
    _lfs_config: &LfsConfig,
    pointer_data: &[u8],
) -> Result<Vec<u8>, OxenError> {
    // Not a pointer — return data as-is.
    let pointer = match PointerFile::decode(pointer_data) {
        Some(p) => p,
        None => return Ok(pointer_data.to_vec()),
    };

    // Ensure versions dir exists (may be missing on a fresh clone).
    std::fs::create_dir_all(versions_dir).ok();

    let store = LocalVersionStore::new(versions_dir);

    // 1. Try local store.
    if store.version_exists(&pointer.oid).await? {
        return store.get_version(&pointer.oid).await;
    }

    // 2. Try origin's version store (local clones only).
    if let Some(origin_versions) = origin_versions_dir(repo_root) {
        let origin_store = LocalVersionStore::new(&origin_versions);
        if origin_store.version_exists(&pointer.oid).await? {
            // Copy into our local store for future use.
            let data = origin_store.get_version(&pointer.oid).await?;
            store.init().await?;
            store.store_version(&pointer.oid, &data).await?;
            return Ok(data);
        }
    }

    // 3. TODO (Phase 3): fetch from Oxen remote with timeout.

    // 4. Fallback — return pointer bytes and warn.
    log::warn!(
        "oxen lfs smudge: content for {} not available locally; run `oxen lfs pull`",
        pointer.oid,
    );
    Ok(pointer_data.to_vec())
}

/// Discover the origin's `.oxen/versions/` directory for local clones.
///
/// Returns `None` if the origin is a remote URL or doesn't have an `.oxen/versions/` dir.
fn origin_versions_dir(repo_root: &Path) -> Option<PathBuf> {
    let url = get_origin_url(repo_root)?;
    let origin_path = as_local_path(&url)?;
    let versions = origin_path.join(".oxen").join("versions");
    if versions.is_dir() {
        Some(versions)
    } else {
        None
    }
}

/// Run `git config remote.origin.url` in the given repo directory.
fn get_origin_url(repo_root: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["config", "remote.origin.url"])
        .current_dir(repo_root)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let url = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if url.is_empty() {
        None
    } else {
        Some(url)
    }
}

/// Convert a Git remote URL to a local filesystem path, if it is one.
///
/// Handles:
/// - Absolute paths: `/foo/bar`
/// - `file://` URLs: `file:///foo/bar`
///
/// Returns `None` for remote URLs (ssh://, https://, git@, etc.).
fn as_local_path(url: &str) -> Option<PathBuf> {
    if let Some(stripped) = url.strip_prefix("file://") {
        let path = PathBuf::from(stripped);
        if path.is_dir() {
            return Some(path);
        }
        return None;
    }

    // Reject obvious remote URLs.
    if url.contains("://") || url.contains('@') {
        return None;
    }

    let path = PathBuf::from(url);
    if path.is_absolute() && path.is_dir() {
        Some(path)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_clean_stores_and_returns_pointer() {
        let tmp = TempDir::new().unwrap();
        let versions_dir = tmp.path().join("versions");

        let content = b"hello world, this is a large file";
        let result = clean(&versions_dir, content).await.unwrap();

        // Result should be a valid pointer.
        let ptr = PointerFile::decode(&result).expect("should be a pointer");
        assert_eq!(ptr.size, content.len() as u64);

        // Content should be in the store.
        let store = LocalVersionStore::new(&versions_dir);
        assert!(store.version_exists(&ptr.oid).await.unwrap());
        let stored = store.get_version(&ptr.oid).await.unwrap();
        assert_eq!(stored, content);
    }

    #[tokio::test]
    async fn test_clean_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let versions_dir = tmp.path().join("versions");

        let content = b"some content";
        let pointer_bytes = clean(&versions_dir, content).await.unwrap();

        // Cleaning the pointer again should return it unchanged.
        let double = clean(&versions_dir, &pointer_bytes).await.unwrap();
        assert_eq!(pointer_bytes, double);
    }

    #[tokio::test]
    async fn test_smudge_restores_content() {
        let tmp = TempDir::new().unwrap();
        let repo_root = tmp.path();
        let versions_dir = tmp.path().join("versions");
        let config = LfsConfig::default();

        let content = b"restore me";
        let pointer_bytes = clean(&versions_dir, content).await.unwrap();

        let restored = smudge(&versions_dir, repo_root, &config, &pointer_bytes)
            .await
            .unwrap();
        assert_eq!(restored, content);
    }

    #[tokio::test]
    async fn test_smudge_passthrough_non_pointer() {
        let tmp = TempDir::new().unwrap();
        let repo_root = tmp.path();
        let versions_dir = tmp.path().join("versions");
        let config = LfsConfig::default();

        let data = b"not a pointer";
        let result = smudge(&versions_dir, repo_root, &config, data)
            .await
            .unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_smudge_fallback_when_missing() {
        let tmp = TempDir::new().unwrap();
        let repo_root = tmp.path();
        let versions_dir = tmp.path().join("versions");
        let config = LfsConfig::default();

        // Fabricate a pointer whose content is NOT in the store.
        let ptr = PointerFile::new("a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8", 999);
        let pointer_bytes = ptr.encode();

        let result = smudge(&versions_dir, repo_root, &config, &pointer_bytes)
            .await
            .unwrap();
        // Falls back to returning the pointer bytes.
        assert_eq!(result, pointer_bytes);
    }

    #[test]
    fn test_as_local_path_rejects_ssh() {
        assert!(as_local_path("git@github.com:user/repo.git").is_none());
    }

    #[test]
    fn test_as_local_path_rejects_https() {
        assert!(as_local_path("https://github.com/user/repo.git").is_none());
    }

    #[test]
    fn test_as_local_path_accepts_file_url() {
        let tmp = TempDir::new().unwrap();
        let url = format!("file://{}", tmp.path().display());
        assert_eq!(as_local_path(&url), Some(tmp.path().to_path_buf()));
    }

    #[test]
    fn test_as_local_path_accepts_absolute_path() {
        let tmp = TempDir::new().unwrap();
        let path_str = tmp.path().to_string_lossy().to_string();
        assert_eq!(as_local_path(&path_str), Some(tmp.path().to_path_buf()));
    }
}
