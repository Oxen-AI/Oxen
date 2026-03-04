use std::path::{Path, PathBuf};

use crate::error::OxenError;
use crate::lfs::gitattributes;
use crate::lfs::pointer::PointerFile;
use crate::storage::version_store::VersionStore;
use crate::storage::LocalVersionStore;

/// Status information for a single LFS-tracked file.
#[derive(Debug)]
pub struct LfsFileStatus {
    /// Path relative to repo root.
    pub path: PathBuf,
    /// The parsed pointer.
    pub pointer: PointerFile,
    /// Whether the actual content is available in the local version store.
    pub local: bool,
}

/// Walk the working tree, find pointer files that match tracked patterns,
/// and report their status.
pub async fn get_status(
    repo_root: &Path,
    versions_dir: &Path,
) -> Result<Vec<LfsFileStatus>, OxenError> {
    let patterns = gitattributes::list_tracked_patterns(repo_root)?;
    if patterns.is_empty() {
        return Ok(Vec::new());
    }

    let store = LocalVersionStore::new(versions_dir);
    let mut results = Vec::new();

    // Build glob matchers.
    let matchers: Vec<glob::Pattern> = patterns
        .iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect();

    // Walk the working tree.
    for entry in walkdir::WalkDir::new(repo_root)
        .into_iter()
        .filter_entry(|e| {
            // Skip .git and .oxen directories.
            let name = e.file_name().to_string_lossy();
            name != ".git" && name != ".oxen"
        })
    {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !entry.file_type().is_file() {
            continue;
        }

        let rel_path = match entry.path().strip_prefix(repo_root) {
            Ok(p) => p,
            Err(_) => continue,
        };

        let rel_str = rel_path.to_string_lossy();

        // Check if the file matches any tracked pattern.
        let matched = matchers.iter().any(|m| m.matches(&rel_str));
        if !matched {
            continue;
        }

        // Read the file and check if it's a pointer.
        let data = match std::fs::read(entry.path()) {
            Ok(d) => d,
            Err(_) => continue,
        };

        if let Some(pointer) = PointerFile::decode(&data) {
            let local = store.version_exists(&pointer.oid).await.unwrap_or(false);
            results.push(LfsFileStatus {
                path: rel_path.to_path_buf(),
                pointer,
                local,
            });
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lfs::filter;
    use crate::lfs::gitattributes;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_status_finds_pointer_files() {
        let tmp = TempDir::new().unwrap();
        let repo_root = tmp.path();
        let oxen_dir = repo_root.join(".oxen");
        let versions_dir = oxen_dir.join("versions");
        std::fs::create_dir_all(&versions_dir).unwrap();

        // Track *.bin
        gitattributes::track_pattern(repo_root, "*.bin").unwrap();

        // Create a pointer file by running clean.
        let content = b"binary content here";
        let pointer_bytes = filter::clean(&versions_dir, content).await.unwrap();
        std::fs::write(repo_root.join("model.bin"), &pointer_bytes).unwrap();

        // Create a non-matching file.
        std::fs::write(repo_root.join("readme.txt"), b"hello").unwrap();

        let statuses = get_status(repo_root, &versions_dir).await.unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].path, PathBuf::from("model.bin"));
        assert!(statuses[0].local);
    }
}
