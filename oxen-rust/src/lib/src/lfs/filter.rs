use std::path::Path;

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
/// Strategy: local store first, then remote fetch (if configured), then fall back
/// to returning the pointer bytes unchanged with a warning.
pub async fn smudge(
    versions_dir: &Path,
    _lfs_config: &LfsConfig,
    pointer_data: &[u8],
) -> Result<Vec<u8>, OxenError> {
    // Not a pointer — return data as-is.
    let pointer = match PointerFile::decode(pointer_data) {
        Some(p) => p,
        None => return Ok(pointer_data.to_vec()),
    };

    let store = LocalVersionStore::new(versions_dir);

    // 1. Try local store.
    if store.version_exists(&pointer.oid).await? {
        return store.get_version(&pointer.oid).await;
    }

    // 2. TODO (Phase 3): fetch from Oxen remote with timeout.

    // 3. Fallback — return pointer bytes and warn.
    log::warn!(
        "oxen lfs smudge: content for {} not available locally; run `oxen lfs pull`",
        pointer.oid,
    );
    Ok(pointer_data.to_vec())
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
        let versions_dir = tmp.path().join("versions");
        let config = LfsConfig::default();

        let content = b"restore me";
        let pointer_bytes = clean(&versions_dir, content).await.unwrap();

        let restored = smudge(&versions_dir, &config, &pointer_bytes)
            .await
            .unwrap();
        assert_eq!(restored, content);
    }

    #[tokio::test]
    async fn test_smudge_passthrough_non_pointer() {
        let tmp = TempDir::new().unwrap();
        let versions_dir = tmp.path().join("versions");
        let config = LfsConfig::default();

        let data = b"not a pointer";
        let result = smudge(&versions_dir, &config, data).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_smudge_fallback_when_missing() {
        let tmp = TempDir::new().unwrap();
        let versions_dir = tmp.path().join("versions");
        let config = LfsConfig::default();

        // Fabricate a pointer whose content is NOT in the store.
        let ptr = PointerFile::new("a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8", 999);
        let pointer_bytes = ptr.encode();

        // Create the versions dir so version_exists doesn't fail.
        std::fs::create_dir_all(&versions_dir).unwrap();

        let result = smudge(&versions_dir, &config, &pointer_bytes)
            .await
            .unwrap();
        // Falls back to returning the pointer bytes.
        assert_eq!(result, pointer_bytes);
    }
}
