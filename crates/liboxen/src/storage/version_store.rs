use std::fmt::Debug;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::io::AsyncRead;
use tokio_stream::Stream;
use utoipa::ToSchema;

use crate::constants;
use crate::constants::MAX_CONCURRENT_VERSION_PROBES;
use crate::error::OxenError;
use crate::storage::{LocalVersionStore, S3Opts, S3VersionStore};
use crate::util;
use crate::view::versions::CleanCorruptedVersionsResult;

/// A local filesystem path to a version file.
///
/// The path is guaranteed to be readable on the local filesystem, but callers MUST NOT assume it is
/// stable: non-local backends (e.g. S3) materialize the file into a temporary location that is
/// cleaned up when this value is dropped. Callers must treat the file as read-only.
///
/// - Implements `Deref<Target = Path>` so that `&LocalFilePath` can be passed directly to any
///   function that accepts `&Path`.
/// - Implements `AsRef<Path>` so that `LocalFilePath` can be passed directly to any function that
///   accepts `impl AsRef<Path>`.
///
/// TODO: See how many of our own functions can be updated to accept LocalFilePath directly. Perhaps we can remove the need for `AsRef<Path>`.
pub enum LocalFilePath {
    /// A stable path (e.g. from `LocalVersionStore`) that outlives this value. It points at the
    /// blob inside the content-addressed version store, so callers must treat it as read-only:
    /// mutating it corrupts the stored content for every reference to that hash.
    Stable(PathBuf),
    /// A temporary file that is deleted when this value is dropped. Callers must treat it as
    /// read-only.
    Temp(async_tempfile::TempFile),
}

impl Deref for LocalFilePath {
    type Target = Path;

    fn deref(&self) -> &Path {
        match self {
            LocalFilePath::Stable(p) => p.as_path(),
            LocalFilePath::Temp(t) => t.file_path(),
        }
    }
}

impl AsRef<Path> for LocalFilePath {
    fn as_ref(&self) -> &Path {
        self.deref()
    }
}

impl std::fmt::Debug for LocalFilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalFilePath::Stable(p) => write!(f, "Stable({p:?})"),
            LocalFilePath::Temp(t) => write!(f, "Temp({:?})", t.file_path()),
        }
    }
}

impl std::fmt::Display for LocalFilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref().display())
    }
}

impl LocalFilePath {
    pub fn to_pathbuf(&self) -> PathBuf {
        self.deref().to_path_buf()
    }
}

/// Where a version file's data lives, in a form a cloud-aware reader can consume directly.
///
/// Unlike [`LocalFilePath`], this never materializes a remote version file to a temp file on
/// local disk. Callers that need bytes-on-disk should use `copy_version_to_path` instead.
/// Callers that already use a cloud-aware reader (Polars `scan_*` with `CloudOptions`, DuckDB
/// `read_parquet` via the `httpfs` extension, …) consume `S3` variants directly.
#[derive(Debug, Clone)]
pub enum VersionLocation {
    /// Local-backed store: the version file lives on disk at this path.
    Local(PathBuf),
    /// S3-backed store: enough config for a cloud-aware reader to fetch the version file
    /// directly without round-tripping through local disk.
    S3 {
        /// Fully-qualified S3 URL of the combined version file:
        /// `s3://{bucket}/{prefix}/{hash}/data`.
        url: String,
        region: String,
        /// `Some(host:port)` for non-AWS S3-compatible endpoints (s3s test fixture, MinIO,
        /// etc.). `None` selects the default AWS endpoint.
        endpoint_url: Option<String>,
    },
}

/// Storage backend kind. Serializes as `"local"` / `"s3"` on the wire and on disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum StorageKind {
    #[default]
    Local,
    S3,
}

impl std::fmt::Display for StorageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageKind::Local => f.write_str("local"),
            StorageKind::S3 => f.write_str("s3"),
        }
    }
}

impl std::str::FromStr for StorageKind {
    type Err = OxenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(StorageKind::Local),
            "s3" => Ok(StorageKind::S3),
            other => Err(OxenError::UnsupportedStorageKind(other.to_string())),
        }
    }
}

/// Configuration for version storage backend.
///
/// Persisted in each repo's `config.toml` under `[storage]`. On-disk shape:
///
/// ```toml
/// [storage]
/// kind = "local"
/// versions_path = "/mnt/nfs/..."   # only when set
/// ```
///
/// A custom [`Deserialize`] impl also accepts the pre-rename layout
/// (`[storage.settings] path = "..."`) for backwards compatibility: the legacy
/// value is promoted into `versions_path` on load. Any path — new or legacy — is
/// re-emitted as `versions_path` in `[storage]`; `[storage.settings]` goes away on
/// the first save after upgrade.
#[derive(Serialize, Debug, Clone, Default)]
pub struct StorageConfig {
    pub kind: StorageKind,
    /// For the "local" backend, the directory where version files are stored. If `None`,
    /// defaults to `<repo>/.oxen/versions/files`. A `.oxen`-prefixed relative path is
    /// joined with the repo directory at runtime, so repos stay portable if moved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub versions_path: Option<PathBuf>,
}

impl<'de> Deserialize<'de> for StorageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct LegacySettings {
            #[serde(default)]
            path: Option<PathBuf>,
        }

        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            kind: StorageKind,
            #[serde(default)]
            versions_path: Option<PathBuf>,
            // Pre-rename location; consumed on load for backwards compatibility,
            // never re-emitted.
            #[serde(default)]
            settings: Option<LegacySettings>,
        }

        let raw = Raw::deserialize(deserializer)?;
        let versions_path = raw
            .versions_path
            .or_else(|| raw.settings.and_then(|s| s.path));
        Ok(StorageConfig {
            kind: raw.kind,
            versions_path,
        })
    }
}

/// Trait defining operations for version file storage backends
#[async_trait]
pub trait VersionStore: Debug + Send + Sync + 'static {
    /// Initialize the storage backend
    async fn init(&self) -> Result<(), OxenError>;

    /// Store a version file from an async reader
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `reader` - An owned async reader
    /// * `size` - Total size in bytes, used to tune upload chunk sizes
    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
    ) -> Result<(), OxenError>;

    /// Store a version file from bytes
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `data` - The raw bytes to store
    async fn store_version(&self, hash: &str, data: Bytes) -> Result<(), OxenError>;

    /// Store a chunk of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `offset` - The starting byte position of the chunk
    /// * `data` - The raw bytes to store
    async fn store_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        data: Bytes,
    ) -> Result<(), OxenError>;

    /// Store a derived file (resized image, video thumbnail, etc.) corresponding to a file version
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact (e.g. "200x300.jpg")
    /// * `derived_data` - The raw bytes of the derived artifact to store
    async fn store_version_derived(
        &self,
        orig_hash: &str,
        derived_filename: &str,
        derived_data: Bytes,
    ) -> Result<(), OxenError>;

    /// Retrieve a chunk of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `offset` - The starting byte position of the chunk
    /// * `size` - The chunk size
    async fn get_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OxenError>;

    /// List all chunks for a version file.
    ///
    /// Returns the byte offsets of each chunk within the original file, sorted in
    /// ascending order. Concatenating chunks in this order reconstructs the
    /// complete file.
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    async fn list_version_chunks(&self, hash: &str) -> Result<Vec<u64>, OxenError>;

    /// Combine all the chunks for a version file into a single file, then delete the chunks.
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    async fn combine_version_chunks(&self, hash: &str) -> Result<(), OxenError>;

    /// Get metadata of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    async fn get_version_size(&self, hash: &str) -> Result<u64, OxenError>;

    /// Retrieve a version file's contents as bytes (less efficient for large files)
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    async fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError>;

    /// Get a version file as a stream of bytes
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    async fn get_version_stream(
        &self,
        hash: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>;

    /// Get the size in bytes of a derived file (e.g., resized image, video thumbnail).
    ///
    /// Returns the content length of the derived artifact on disk.
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact
    ///
    /// # Errors
    /// Returns `OxenError` if the derived file does not exist or cannot be read.
    async fn get_version_derived_size(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<u64, OxenError>;

    /// Get a stream of a derived file (resized, video thumbnail, etc.)
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact
    async fn get_version_derived_stream(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>;

    /// Check if a derived file exists
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact
    async fn derived_version_exists(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<bool, OxenError>;

    /// Get a local filesystem path to a version file.
    ///
    /// The returned `LocalFilePath` is guaranteed to be readable on the local
    /// filesystem. For local backends the path points into the version store
    /// directly; for remote backends (e.g. S3) the file is downloaded to a
    /// temporary location that is cleaned up when the `LocalFilePath` is dropped.
    ///
    /// **Callers must keep the returned value alive for as long as they use the
    /// path.**
    ///
    /// **The returned file is read-only: callers MUST NOT alter it in any way.** For local stores
    /// the path points straight at the blob inside the content-addressed version store, so mutating
    /// it corrupts that blob for every reference to `hash` (its bytes would no longer match its
    /// content hash). Copy the file elsewhere first if you need a mutable version.
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version file to retrieve
    async fn get_version_path(&self, hash: &str) -> Result<LocalFilePath, OxenError>;

    /// Return a [`VersionLocation`] describing where the version file lives, in a form a
    /// cloud-aware reader can consume without materializing it to a temp file on local disk.
    ///
    /// Local-backed stores return [`VersionLocation::Local`] with the on-disk path; S3-backed
    /// stores return [`VersionLocation::S3`] carrying the s3:// URL plus the region and endpoint
    /// override the caller needs to configure Polars `CloudOptions` or DuckDB `httpfs`.
    ///
    /// Prefer this over [`Self::get_version_path`] whenever the consumer has a cloud-aware
    /// reader available (Polars `scan_*`, DuckDB `read_parquet`, …): `get_version_path`
    /// materializes the entire version file to a temp file on S3, which is fine for small
    /// files but OOMs at the multi-GB scale customers care about.
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version file
    async fn version_location(&self, hash: &str) -> Result<VersionLocation, OxenError>;

    /// Copy a versioned file from the version store to a destination path on the local filesystem
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    /// * `dest_path` - Destination path to copy the file to
    async fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError>;

    /// Materialize the version file for `hash` to a readable local path, placing any temporary
    /// copy under `dir`.
    ///
    /// **Use this only when you need direct read access to a version file's bytes through a
    /// filesystem path** — typically an external library that takes a path and only reads from it
    /// (e.g. DuckDB `read_*`, an ffmpeg invocation). For remote stores it forces a full local copy
    /// (network download plus a disk write on the data volume), so it is the most expensive way to
    /// reach the data. Under any other condition, find a cheaper path: read via
    /// [`Self::version_location`] with a cloud-aware reader (Polars `scan_*` with `CloudOptions`,
    /// DuckDB `httpfs`) or stream the bytes — and if an internal caller only takes a `&Path` just to
    /// read the file, prefer refactoring it to accept a `Read` handle (or `version_location`)
    /// rather than reaching for `materialize`.
    ///
    /// Local-backed stores return the on-disk version path directly (zero copy); remote stores
    /// (e.g. S3) stream the object into a temp file under `dir`, carried alive by the returned
    /// [`LocalFilePath::Temp`] guard. `dir` must live on the data volume, not the OS temp dir, so
    /// large blobs cannot exhaust a small `/tmp`.
    ///
    /// **The returned file is read-only: callers MUST NOT alter it in any way.** For local stores
    /// the path points straight at the blob inside the content-addressed version store, so mutating
    /// it corrupts that blob for every reference to `hash` (its bytes would no longer match its
    /// content hash). Copy the file elsewhere first if you need a mutable version.
    ///
    /// Prefer this over [`Self::get_version_path`], which always writes to the OS temp dir and
    /// buffers the whole blob in memory.
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version file
    /// * `dir` - Directory on the data volume to hold any temporary copy
    async fn materialize(&self, hash: &str, dir: &Path) -> Result<LocalFilePath, OxenError> {
        match self.version_location(hash).await? {
            VersionLocation::Local(path) => Ok(LocalFilePath::Stable(path)),
            VersionLocation::S3 { .. } => {
                util::fs::create_dir_all(dir)?;
                let temp = async_tempfile::TempFile::new_in(dir).await.map_err(|e| {
                    OxenError::basic_str(format!("Failed to create temp file: {e}"))
                })?;
                self.copy_version_to_path(hash, temp.file_path()).await?;
                Ok(LocalFilePath::Temp(temp))
            }
        }
    }

    /// Check if a version exists
    ///
    /// # Arguments
    /// * `hash` - The content hash to check
    async fn version_exists(&self, hash: &str) -> Result<bool, OxenError>;

    /// Returns the subset of `hashes` whose blobs are absent from this store. Used by
    /// bulk endpoints that need to fail fast on missing content before committing to
    /// a streaming response.
    ///
    /// The default implementation probes each hash with `version_exists` in parallel,
    /// bounded by `MAX_CONCURRENT_VERSION_PROBES`. Backends with cheaper batch-existence
    /// primitives (e.g. a future inventory-backed store) may override.
    ///
    /// # Arguments
    /// * `hashes` - The content hashes to check
    async fn find_missing_versions(&self, hashes: &[String]) -> Result<Vec<String>, OxenError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let max_concurrent = MAX_CONCURRENT_VERSION_PROBES.min(hashes.len());

        let mut probes = futures_util::stream::iter(hashes.iter().cloned())
            .map(|hash| async move {
                let exists = self.version_exists(&hash).await?;
                Ok::<_, OxenError>((hash, exists))
            })
            .buffer_unordered(max_concurrent);

        let mut missing = Vec::new();
        while let Some(result) = probes.next().await {
            let (hash, exists) = result?;
            if !exists {
                missing.push(hash);
            }
        }
        Ok(missing)
    }

    /// Delete a version
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to delete
    async fn delete_version(&self, hash: &str) -> Result<(), OxenError>;

    /// List all versions.
    ///
    /// Results are returned as hash strings, sorted in UTF-8 byte order (equivalent to
    /// lexicographic order for the hex-digit hashes we use). This keeps behavior
    /// deterministic across storage backends — the local filesystem returns entries in
    /// platform- and filesystem-dependent order, which implementations must sort.
    async fn list_versions(&self) -> Result<Vec<String>, OxenError>;

    /// Clean corrupted version files
    /// If `dry_run` is true, only scan and report without deleting
    async fn clean_corrupted_versions(
        &self,
        dry_run: bool,
    ) -> Result<CleanCorruptedVersionsResult, OxenError>;

    /// Which storage backend kind this is.
    fn storage_kind(&self) -> StorageKind;
}

/// Build a `VersionStore` for the given repo according to its persisted `StorageConfig`.
///
/// `server_s3_opts` is the server-wide S3 configuration (bucket name). It must be `Some`
/// whenever `config.kind == StorageKind::S3`; CLI and test paths pass `None` because they never
/// run with the S3 backend enabled. The S3 prefix is derived from the repo path's
/// `<namespace>/<name>` tail at construction time and never written to per-repo config, so the
/// server can rotate buckets without rewriting every repo.
pub fn create_version_store(
    repo_dir: &Path,
    config: &StorageConfig,
    server_s3_opts: Option<&S3Opts>,
) -> Result<Arc<dyn VersionStore>, OxenError> {
    match config.kind {
        StorageKind::Local => {
            let versions_dir = if let Some(path) = &config.versions_path {
                if path.starts_with(".oxen") {
                    // Relative path → join with repo dir so the repo stays portable.
                    repo_dir.join(path)
                } else {
                    path.clone()
                }
            } else {
                util::fs::oxen_hidden_dir(repo_dir)
                    .join(constants::VERSIONS_DIR)
                    .join(constants::FILES_DIR)
            };

            let store = LocalVersionStore::new(versions_dir);
            Ok(Arc::new(store))
        }
        StorageKind::S3 => {
            let opts = server_s3_opts.ok_or(OxenError::S3BackendMissingServerOpts)?;
            // Server repo paths are always `<sync_dir>/<namespace>/<name>` (the only path that
            // reaches the S3 branch), so the tail components should always be present. We still
            // surface a structured error instead of panicking on a malformed caller.
            let name = repo_dir
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| OxenError::S3PrefixUnresolvable(repo_dir.into()))?;
            let namespace = repo_dir
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .ok_or_else(|| OxenError::S3PrefixUnresolvable(repo_dir.into()))?;
            let prefix = format!("{namespace}/{name}");
            let store = S3VersionStore::new(opts.bucket.clone(), prefix);
            Ok(Arc::new(store))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn s3_config() -> StorageConfig {
        StorageConfig {
            kind: StorageKind::S3,
            versions_path: None,
        }
    }

    /// The S3 branch requires server opts. `None` is the runtime tripwire for callers that
    /// somehow built an S3-shaped `StorageConfig` without going through `StoragePolicy::resolve()`.
    #[test]
    fn create_version_store_s3_without_server_opts_errors() {
        let repo_dir = PathBuf::from("/srv/oxen/test-ns/test-repo");
        let result = create_version_store(&repo_dir, &s3_config(), None);
        assert!(
            matches!(result, Err(OxenError::S3BackendMissingServerOpts)),
            "expected S3BackendMissingServerOpts, got {result:?}",
        );
    }

    /// With server opts, the S3 branch returns an S3-kind store.
    #[test]
    fn create_version_store_s3_with_server_opts_builds_s3_store() {
        let repo_dir = PathBuf::from("/srv/oxen/test-ns/test-repo");
        let opts = S3Opts {
            bucket: "my-bucket".to_string(),
        };
        let store = create_version_store(&repo_dir, &s3_config(), Some(&opts))
            .expect("S3 store should construct when server opts are present");
        assert_eq!(store.storage_kind(), StorageKind::S3);
    }
}
