use std;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::constants::{
    BLOCKS_DIR, CHUNK_INDEX_DIR, STREAMING_BUF_SIZE, VERSION_CHUNK_FILE_NAME, VERSION_CHUNKS_DIR,
    VERSION_FILE_NAME, VERSION_MANIFEST_FILE_NAME,
};
use crate::error::OxenError;
use crate::model::{EntryDataType, MerkleHash};
use crate::storage::chunked::manifest::ChunkManifest;
use crate::storage::chunked::{
    BlockEngine, ChunkedVersionStore, ReconstructReader, SealedBlock, SeekableVersionReader, TransformId,
    StorageProfile, encode_policy,
};
use crate::storage::version_store::{
    BoxedByteStream, LocalFilePath, VersionLocation, VersionStore,
};
use crate::util::fs::AtomicFile;
use crate::util::hasher::HashingReader;
use crate::util::{concurrency, hasher};
use crate::view::versions::CleanCorruptedVersionsResult;

use async_trait::async_trait;
use bytes::Bytes;
use log;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use tokio::fs::{self, File, metadata};
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::sync::Semaphore;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, SyncIoBridge};

/// Local filesystem implementation of version storage
#[derive(Debug)]
pub struct LocalVersionStore {
    /// Root path where versions are stored
    root_path: PathBuf,
    /// Lazily-opened block engine (blocks dir + LMDB chunk index) for chunked
    /// versions; repos that never touch a chunked version pay nothing.
    block_engine: OnceLock<Arc<BlockEngine>>,
}

impl LocalVersionStore {
    /// Create a new LocalVersionStore
    ///
    /// The store owns everything under `root_path`: version dirs (two hex-named
    /// levels per hash) plus the reserved chunked-storage dirs — e.g. for the
    /// default `.oxen/versions/files`, blocks land in
    /// `.oxen/versions/files/blocks` and the LMDB chunk index in
    /// `.oxen/versions/files/chunk_index` — so stores with distinct roots (any
    /// custom `versions_path`) never share state.
    ///
    /// # Arguments
    /// * `root_path` - Base directory for version storage
    pub fn new(root_path: impl AsRef<Path>) -> Self {
        Self {
            root_path: root_path.as_ref().to_path_buf(),
            block_engine: OnceLock::new(),
        }
    }

    /// Get the directory containing a version file
    fn version_dir(&self, hash: &str) -> PathBuf {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        self.root_path.join(topdir).join(subdir)
    }

    /// Get the full path for a version file
    fn version_path(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_FILE_NAME)
    }

    /// Get the full path for a chunked version's manifest
    fn manifest_path(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_MANIFEST_FILE_NAME)
    }

    /// The block engine for chunked versions, opened on first use. Blocks and the
    /// chunk index live in reserved dirs inside `root_path` (skipped by the
    /// version scanners), so every store's chunked state is isolated under its
    /// own root — sibling stores under a shared parent never collide.
    fn engine(&self) -> Result<Arc<BlockEngine>, OxenError> {
        if let Some(engine) = self.block_engine.get() {
            return Ok(Arc::clone(engine));
        }
        let engine = Arc::new(BlockEngine::open(
            &self.root_path.join(BLOCKS_DIR),
            &self.root_path.join(CHUNK_INDEX_DIR),
        )?);
        // A concurrent first call may have won the race; either engine is fine —
        // the LMDB env is shared per-path, so both wrap the same underlying store.
        Ok(Arc::clone(self.block_engine.get_or_init(|| engine)))
    }

    /// Get the directory containing all the chunks for a version file
    fn version_chunks_dir(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_CHUNKS_DIR)
    }

    /// Get the directory containing a version file
    /// .oxen/versions/{hash}/chunks
    fn version_chunk_dir(&self, hash: &str, offset: u64) -> PathBuf {
        self.version_chunks_dir(hash).join(offset.to_string())
    }

    /// Get the directory containing a version file
    fn version_chunk_file(&self, hash: &str, offset: u64) -> PathBuf {
        self.version_chunk_dir(hash, offset)
            .join(VERSION_CHUNK_FILE_NAME)
    }
}

/// Directory of manifest lineage-base blobs for the store containing
/// `manifest_path` (= `<store root>/<xx>/<rest>/manifest`).
fn manifest_dicts_dir(manifest_path: &Path) -> Result<PathBuf, OxenError> {
    manifest_path
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .map(|root| root.join(crate::constants::MANIFEST_DICTS_DIR))
        .ok_or_else(|| OxenError::basic_str("manifest path missing store root"))
}

/// Parse already-read manifest bytes, resolving any delta base through the
/// store's lineage-blob dir (derived from `manifest_path`).
fn read_manifest_stored_bytes_at(
    bytes: &[u8],
    manifest_path: &Path,
) -> Result<ChunkManifest, OxenError> {
    let dicts = manifest_dicts_dir(manifest_path)?;
    Ok(ChunkManifest::from_stored_bytes(bytes, |base_hash| {
        let blob = crate::util::fs::read_bytes_from_path(dicts.join(format!("{base_hash:032x}")))
            .map_err(|err| {
            super::chunked::ChunkedError::InvalidManifest(format!(
                "missing manifest lineage base {base_hash:032x}: {err}"
            ))
        })?;
        zstd::bulk::decompress(&blob, 256 * 1024 * 1024)
            .map_err(super::chunked::ChunkedError::Decompress)
    })?)
}

/// Load the published manifest at `path`, if present. Sync on purpose; async
/// callers wrap it in `spawn_blocking` per `docs/async_policy.md`.
fn read_manifest_at(path: &Path) -> Result<Option<ChunkManifest>, OxenError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(read_manifest_stored_bytes_at(&bytes, path)?)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Persist a manifest at rest: a delta against the lineage base for its
/// first-chunk hash when that is smaller, otherwise the full form (which then
/// becomes the new lineage base). Bases are content-addressed blobs under the
/// reserved `manifest_dicts` dir, so deleting manifests never breaks a delta.
fn write_manifest_at(
    engine: &BlockEngine,
    manifest_path: &Path,
    manifest: &ChunkManifest,
) -> Result<(), OxenError> {
    /// Manifests below this at-rest size are stored full: delta bookkeeping
    /// cannot pay for itself.
    const MANIFEST_DELTA_MIN: usize = 2048;

    let full = manifest.to_stored_bytes()?;
    let Some(first_chunk) = manifest.chunks.first().map(|c| c.hash) else {
        return AtomicFile::new(manifest_path).write(&full);
    };
    if full.len() < MANIFEST_DELTA_MIN {
        return AtomicFile::new(manifest_path).write(&full);
    }
    let dicts = manifest_dicts_dir(manifest_path)?;
    if let Some(base_hash) = engine.index().manifest_base(first_chunk)?
        && let Ok(blob) =
            crate::util::fs::read_bytes_from_path(dicts.join(format!("{base_hash:032x}")))
        && let Ok(base) = zstd::bulk::decompress(&blob, 256 * 1024 * 1024)
        && let Some(delta) = manifest.to_delta_bytes(base_hash, &base, full.len())?
    {
        return AtomicFile::new(manifest_path).write(&delta);
    }
    // Full write; publish its logical bytes (zstd-compressed at rest, named by
    // the compressed hash) as the new lineage base — blob first, pointer last.
    let logical = manifest.to_bytes()?;
    let blob =
        zstd::bulk::compress(&logical, 19).map_err(super::chunked::ChunkedError::Compress)?;
    let blob_hash = crate::util::hasher::hash_buffer_128bit(&blob);
    AtomicFile::new(dicts.join(format!("{blob_hash:032x}")))
        .with_hash(crate::model::MerkleHash::new(blob_hash))
        .write(&blob)?;
    engine.index().set_manifest_base(first_chunk, blob_hash)?;
    AtomicFile::new(manifest_path).write(&full)
}

#[async_trait]
impl VersionStore for LocalVersionStore {
    async fn init(&self) -> Result<(), OxenError> {
        if !self.root_path.exists() {
            fs::create_dir_all(&self.root_path).await?;
        }
        Ok(())
    }

    async fn store_version_from_reader(
        &self,
        hash: &str,
        mut reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        _size: u64,
    ) -> Result<(), OxenError> {
        let version_path = self.version_path(hash);

        if !version_path.exists() {
            let expected_hash: MerkleHash = hash.parse()?;
            AtomicFile::new(&version_path)
                .with_hash(expected_hash)
                .stream_async(&mut *reader)
                .await?;
        }

        Ok(())
    }

    async fn store_version(&self, hash: &str, data: Bytes) -> Result<(), OxenError> {
        let version_path = self.version_path(hash);

        if version_path.exists() {
            return Ok(());
        }

        let expected_hash: MerkleHash = hash.parse()?;
        spawn_blocking(move || {
            AtomicFile::new(&version_path)
                .with_hash(expected_hash)
                .write(&data)
        })
        .await??;

        Ok(())
    }

    async fn store_version_derived(
        &self,
        orig_hash: &str,
        derived_filename: &str,
        derived_data: Bytes,
    ) -> Result<(), OxenError> {
        let path = self.version_dir(orig_hash).join(derived_filename);
        let path_for_log = path.clone();
        spawn_blocking(move || AtomicFile::new(&path).write(&derived_data)).await??;
        log::debug!("Saved derived version file {path_for_log:?}");
        Ok(())
    }

    async fn get_version_size(&self, hash: &str) -> Result<u64, OxenError> {
        let path = self.version_path(hash);
        match fs::metadata(&path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // Chunked versions have a manifest instead of a `data` blob; the
                // logical size lives in the manifest.
                let manifest_path = self.manifest_path(hash);
                let manifest = spawn_blocking(move || read_manifest_at(&manifest_path)).await??;
                match manifest {
                    Some(manifest) => Ok(manifest.file_size),
                    None => Err(err.into()),
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError> {
        let path = self.version_path(hash);
        match fs::read(&path).await {
            Ok(data) => Ok(data),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let manifest_path = self.manifest_path(hash);
                let engine = self.engine()?;
                spawn_blocking(move || {
                    let Some(manifest) = read_manifest_at(&manifest_path)? else {
                        return Err(err.into());
                    };
                    let mut data = Vec::with_capacity(manifest.file_size as usize);
                    engine.reconstruct_to(&manifest, &mut data)?;
                    Ok(data)
                })
                .await?
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn get_version_derived_size(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<u64, OxenError> {
        let path = self.version_dir(orig_hash).join(derived_filename);
        let metadata = fs::metadata(&path).await?;
        Ok(metadata.len())
    }

    async fn get_version_stream(&self, hash: &str) -> Result<BoxedByteStream, OxenError> {
        let path = self.version_path(hash);
        match File::open(&path).await {
            Ok(file) => {
                let reader = BufReader::new(file);
                let stream = ReaderStream::new(reader);
                Ok(Box::new(stream))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let manifest_path = self.manifest_path(hash);
                let engine = self.engine()?;
                let manifest = spawn_blocking(move || read_manifest_at(&manifest_path))
                    .await??
                    .ok_or(err)?;
                // Channel hand-off: the sync reconstruction runs on the blocking
                // pool, feeding the async stream through a bounded channel.
                let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(2);
                spawn_blocking(move || {
                    let mut reader = ReconstructReader::new(engine, manifest);
                    let mut buf = vec![0u8; STREAMING_BUF_SIZE];
                    loop {
                        match std::io::Read::read(&mut reader, &mut buf) {
                            Ok(0) => break,
                            Ok(n) => {
                                if tx
                                    .blocking_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                                    .is_err()
                                {
                                    break; // consumer dropped the stream
                                }
                            }
                            Err(err) => {
                                let _ = tx.blocking_send(Err(err));
                                break;
                            }
                        }
                    }
                });
                Ok(Box::new(ReceiverStream::new(rx)))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn get_version_derived_stream(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<BoxedByteStream, OxenError> {
        let path = self.version_dir(orig_hash).join(derived_filename);
        let file = File::open(&path).await?;
        let reader = BufReader::new(file);
        let stream = ReaderStream::new(reader);

        Ok(Box::new(stream))
    }

    async fn derived_version_exists(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<bool, OxenError> {
        let derived_path = self.version_dir(orig_hash).join(derived_filename);
        match metadata(derived_path).await {
            Ok(meta) => Ok(meta.is_file()),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(false),
                _ => Err(err)?,
            },
        }
    }

    async fn version_location(&self, hash: &str) -> Result<VersionLocation, OxenError> {
        Ok(VersionLocation::Local(self.version_path(hash)))
    }

    // TODO: (CleanCut) Do we need to make sure the destination path is outside the version store?
    async fn copy_version_to_path(
        &self,
        hash: &str,
        dest_path: &Path,
        mtime: SystemTime,
    ) -> Result<(), OxenError> {
        let version_path = self.version_path(hash);
        log::debug!("copying version path: {version_path:?} to {dest_path:?}");
        // Distinguish "the version-store blob is missing" from generic copy errors so the
        // caller (oxen restore / merge / etc.) can surface a useful hint pointing the user
        // at `oxen fetch --missing-files`. Real IO errors from the existence check (e.g.
        // permission denied on `.oxen/versions/`) propagate as-is.
        if fs::try_exists(&version_path).await? {
            let dest_path = dest_path.to_path_buf();
            spawn_blocking(move || {
                AtomicFile::new(&dest_path)
                    .with_mtime(mtime)
                    .copy_from(&version_path)
            })
            .await??;
            return Ok(());
        }

        // Chunked representation: reconstruct through the block engine, verifying
        // the whole-file hash as the bytes are published (invariant: a checkout
        // never lands unverified reconstructed bytes in the working tree).
        let manifest_path = self.manifest_path(hash);
        let engine = self.engine()?;
        let expected_hash: MerkleHash = hash.parse()?;
        let dest_path_buf = dest_path.to_path_buf();
        let manifest = spawn_blocking(move || read_manifest_at(&manifest_path)).await??;
        let Some(manifest) = manifest else {
            return Err(OxenError::VersionStoreDataMissing {
                hash: hash.to_string(),
                target_path: dest_path.to_path_buf().into(),
            });
        };
        spawn_blocking(move || {
            let mut reader = ReconstructReader::new(engine, manifest);
            AtomicFile::new(&dest_path_buf)
                .with_hash(expected_hash)
                .with_mtime(mtime)
                .stream(&mut reader)
        })
        .await??;
        Ok(())
    }

    async fn version_exists(&self, hash: &str) -> Result<bool, OxenError> {
        // A version is present as either a whole-file blob or a published manifest
        // (a chunked version has a manifest instead of `data`).
        Ok(self.version_path(hash).exists() || self.manifest_path(hash).exists())
    }

    async fn materialize(&self, hash: &str, dir: &Path) -> Result<LocalFilePath, OxenError> {
        let path = self.version_path(hash);
        if path.exists() {
            return Ok(LocalFilePath::Stable(path));
        }
        // Chunked version: no contiguous on-disk file exists, so materializing means
        // reconstructing into a temp file — the explicit, declared fallback for
        // path-only consumers. A missing version surfaces the usual
        // VersionStoreDataMissing from the copy.
        crate::util::fs::create_dir_all(dir)?;
        let temp = async_tempfile::TempFile::new_in(dir)
            .await
            .map_err(|e| OxenError::basic_str(format!("Failed to create temp file: {e}")))?;
        self.copy_version_to_path(hash, temp.file_path(), SystemTime::now())
            .await?;
        Ok(LocalFilePath::Temp(temp))
    }

    async fn delete_version(&self, hash: &str) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        if version_dir.exists() {
            fs::remove_dir_all(&version_dir).await?;
        }
        Ok(())
    }

    async fn destroy(&self) -> Result<(), OxenError> {
        // The versions root may live outside the repo directory (custom `versions_path`), so it
        // must be removed explicitly rather than relying on the repo-directory removal.
        if self.root_path.exists() {
            fs::remove_dir_all(&self.root_path).await?;
        }
        Ok(())
    }

    async fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        let mut versions = Vec::new();

        // Walk through the two-level directory structure
        let mut top_entries = fs::read_dir(&self.root_path).await?;
        while let Some(top_entry) = top_entries.next_entry().await? {
            let file_type = top_entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }

            let top_name = top_entry.file_name();
            // The reserved chunked-storage dirs share the root; they are not
            // versions (two-hex-char prefix dirs can't collide with them).
            if top_name == BLOCKS_DIR || top_name == CHUNK_INDEX_DIR {
                continue;
            }
            let mut sub_entries = fs::read_dir(top_entry.path()).await?;
            while let Some(sub_entry) = sub_entries.next_entry().await? {
                let file_type = sub_entry.file_type().await?;
                if !file_type.is_dir() {
                    continue;
                }

                let sub_name = sub_entry.file_name();
                let hash = format!(
                    "{}{}",
                    top_name.to_string_lossy(),
                    sub_name.to_string_lossy()
                );
                versions.push(hash);
            }
        }

        // `read_dir` order is platform/filesystem dependent, so sort for a deterministic
        // UTF-8 byte order result as documented on the trait.
        versions.sort();
        Ok(versions)
    }

    async fn store_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        data: Bytes,
    ) -> Result<(), OxenError> {
        let chunk_path = self.version_chunk_file(hash, offset);

        if chunk_path.exists() {
            return Ok(());
        }

        spawn_blocking(move || AtomicFile::new(&chunk_path).write(&data)).await??;

        Ok(())
    }

    async fn get_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        let version_file_path = self.version_path(hash);

        let mut file = match File::open(&version_file_path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // Chunked representation: a range read is a manifest binary search
                // plus partial chunk decodes, with the same EOF semantics as the
                // whole-file path below.
                let manifest_path = self.manifest_path(hash);
                let engine = self.engine()?;
                return spawn_blocking(move || {
                    let Some(manifest) = read_manifest_at(&manifest_path)? else {
                        return Err(err.into());
                    };
                    // checked_add: offset and size are request-controlled, and a
                    // wrapping sum would slip past this guard.
                    if offset >= manifest.file_size
                        || offset
                            .checked_add(size)
                            .is_none_or(|end| end > manifest.file_size)
                    {
                        return Err(OxenError::IO(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "beyond end of file",
                        )));
                    }
                    engine.read_range(&manifest, offset, size)
                })
                .await?;
            }
            Err(err) => return Err(err.into()),
        };
        let metadata = file.metadata().await?;
        let file_len = metadata.len();

        // checked_add: offset and size are request-controlled, and a wrapping
        // sum would slip past this guard.
        if offset >= file_len || offset.checked_add(size).is_none_or(|end| end > file_len) {
            return Err(OxenError::IO(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "beyond end of file",
            )));
        }

        let read_len = std::cmp::min(size, file_len - offset);
        if read_len > usize::MAX as u64 {
            return Err(OxenError::basic_str("requested chunk too large"));
        }

        use tokio::io::{AsyncSeekExt, SeekFrom};
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; read_len as usize];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    async fn list_version_chunks(&self, hash: &str) -> Result<Vec<u64>, OxenError> {
        let chunk_dir = self.version_chunks_dir(hash);
        let mut chunks = Vec::new();

        let mut entries = fs::read_dir(&chunk_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_dir()
                && let Ok(chunk_offset) = entry.file_name().to_string_lossy().parse::<u64>()
            {
                chunks.push(chunk_offset);
            }
        }
        chunks.sort();
        Ok(chunks)
    }

    async fn combine_version_chunks(&self, hash: &str) -> Result<(), OxenError> {
        let version_path = self.version_path(hash);
        let chunks_dir = self.version_chunks_dir(hash);
        let chunk_offsets = self.list_version_chunks(hash).await?;
        let expected_hash: MerkleHash = hash.parse()?;
        log::debug!(
            "combine_version_chunks found {} chunks",
            chunk_offsets.len()
        );
        let chunk_paths: Vec<PathBuf> = chunk_offsets
            .iter()
            .map(|offset| self.version_chunk_file(hash, *offset))
            .collect();

        // Run the full read-chunks + atomic-write + cleanup sequence in one `spawn_blocking`
        // closure: chain each chunk file as a sync `Read` and pipe the concatenation through
        // `AtomicFile::stream` with the expected hash so the rename only happens if the
        // reassembled bytes hash to the file's canonical hash.
        spawn_blocking(move || -> Result<(), OxenError> {
            let mut combined: Box<dyn std::io::Read> = Box::new(std::io::empty());
            for chunk_path in &chunk_paths {
                let chunk_file = std::fs::File::open(chunk_path)?;
                combined = Box::new(std::io::Read::chain(combined, chunk_file));
            }
            AtomicFile::new(&version_path)
                .with_hash(expected_hash)
                .stream(&mut combined)?;

            // Close the chunk file handles before removing their directory. On NFS, unlinking a
            // still-open file leaves a hidden .nfsXXXX entry that fails the rmdir with ENOTEMPTY.
            drop(combined);

            if chunks_dir.exists() {
                std::fs::remove_dir_all(&chunks_dir)?;
            }
            Ok(())
        })
        .await??;

        Ok(())
    }

    // It's left here for a quick fix. TODO: Move the business logic to versions controller.
    async fn clean_corrupted_versions(
        &self,
        dry_run: bool,
    ) -> Result<CleanCorruptedVersionsResult, OxenError> {
        // Keep record of the stats
        #[derive(Default)]
        struct Stats {
            scanned_objects: AtomicUsize,
            corrupted_objects: AtomicUsize,
            io_errors: AtomicUsize,
            deleted_objects: AtomicUsize,
        }

        impl Stats {
            fn incr_scanned(&self) {
                self.scanned_objects.fetch_add(1, Ordering::Relaxed);
            }
            fn incr_corrupted(&self) {
                self.corrupted_objects.fetch_add(1, Ordering::Relaxed);
            }
            fn incr_io_error(&self) {
                self.io_errors.fetch_add(1, Ordering::Relaxed);
            }
            fn incr_deleted(&self) {
                self.deleted_objects.fetch_add(1, Ordering::Relaxed);
            }

            fn snapshot(&self) -> (usize, usize, usize, usize) {
                (
                    self.scanned_objects.load(Ordering::Relaxed),
                    self.corrupted_objects.load(Ordering::Relaxed),
                    self.io_errors.load(Ordering::Relaxed),
                    self.deleted_objects.load(Ordering::Relaxed),
                )
            }
        }

        let start = std::time::Instant::now();
        let stats = Arc::new(Stats::default());

        // Limit concurrency
        let concurrency = concurrency::default_num_threads();
        let semaphore = Arc::new(Semaphore::new(concurrency));

        // Read prefix dirs
        let mut prefix_rd = fs::read_dir(&self.root_path).await?;
        let mut prefix_paths: Vec<PathBuf> = Vec::new();
        while let Some(entry) = prefix_rd.next_entry().await? {
            // The reserved chunked-storage dirs share the root but hold no
            // version dirs; scanning them would misread blocks as corruption.
            let name = entry.file_name();
            if name == BLOCKS_DIR || name == CHUNK_INDEX_DIR {
                continue;
            }
            match entry.file_type().await {
                Ok(ft) if ft.is_dir() => {
                    prefix_paths.push(entry.path());
                }
                _ => {
                    stats.incr_io_error();
                }
            }
        }

        // Spawn one task per prefix (concurrent)
        let mut handles = Vec::with_capacity(prefix_paths.len());

        for prefix_path in prefix_paths {
            let semaphore_cl = semaphore.clone();
            let stats_cl = stats.clone();
            let handle = tokio::spawn(async move {
                let mut suffix_rd = match fs::read_dir(&prefix_path).await {
                    Ok(rd) => rd,
                    Err(_) => {
                        stats_cl.incr_io_error();
                        return;
                    }
                };

                // Process suffix directories sequentially
                while let Ok(Some(entry)) = suffix_rd.next_entry().await {
                    // Only process directories
                    let file_type = match entry.file_type().await {
                        Ok(ft) => ft,
                        Err(_) => {
                            stats_cl.incr_io_error();
                            continue;
                        }
                    };
                    if !file_type.is_dir() {
                        continue;
                    }

                    let suffix_path = entry.path();

                    // hash = prefix+suffix
                    let prefix_name = match prefix_path.file_name().and_then(|s| s.to_str()) {
                        Some(p) => p.to_string(),
                        None => {
                            stats_cl.incr_io_error();
                            continue;
                        }
                    };
                    let suffix_name = match suffix_path.file_name().and_then(|s| s.to_str()) {
                        Some(s) => s.to_string(),
                        None => {
                            stats_cl.incr_io_error();
                            continue;
                        }
                    };
                    let expected_hash = format!("{prefix_name}{suffix_name}");

                    // Acquire concurrency permit for heavy operations (I/O, hashing)
                    let permit = semaphore_cl.clone().acquire_owned().await.unwrap();

                    {
                        // First read the data async
                        let data_path = suffix_path.join(VERSION_FILE_NAME);
                        let data = match fs::read(&data_path).await {
                            Ok(b) => b,
                            Err(_) => {
                                // No whole-file blob. Chunked versions publish a
                                // manifest instead of `data`, and the manifest is
                                // their only representation — never delete a
                                // version dir holding a valid one.
                                let manifest_path = suffix_path.join(VERSION_MANIFEST_FILE_NAME);
                                match fs::read(&manifest_path).await {
                                    Ok(manifest_bytes) => {
                                        stats_cl.incr_scanned();
                                        let valid = read_manifest_stored_bytes_at(
                                            &manifest_bytes,
                                            &manifest_path,
                                        )
                                        .is_ok_and(|m| m.file_hash.to_string() == expected_hash);
                                        if !valid {
                                            stats_cl.incr_corrupted();
                                            if !dry_run
                                                && fs::remove_dir_all(&suffix_path).await.is_ok()
                                            {
                                                stats_cl.incr_deleted();
                                            }
                                        }
                                    }
                                    Err(err) if err.kind() == ErrorKind::NotFound => {
                                        // Neither blob nor manifest: nothing valid
                                        // to preserve - treat as corrupted.
                                        stats_cl.incr_io_error();
                                        if !dry_run
                                            && fs::remove_dir_all(&suffix_path).await.is_ok()
                                        {
                                            stats_cl.incr_deleted();
                                        }
                                    }
                                    Err(_) => {
                                        // Manifest present but unreadable: an IO
                                        // fault, not proof of corruption - leave
                                        // it in place.
                                        stats_cl.incr_io_error();
                                    }
                                }
                                drop(permit);
                                continue;
                            }
                        };

                        // increment scanned count
                        stats_cl.incr_scanned();

                        // Compute hash in blocking thread
                        let actual_hash =
                            match spawn_blocking(move || hasher::hash_buffer(&data)).await {
                                Ok(h) => h,
                                Err(_) => {
                                    log::debug!(
                                        "Failed to compute hash for version file {expected_hash}"
                                    );
                                    stats_cl.incr_io_error();

                                    if !dry_run && fs::remove_dir_all(&suffix_path).await.is_ok() {
                                        stats_cl.incr_deleted();
                                    }
                                    drop(permit);
                                    continue;
                                }
                            };

                        if actual_hash != expected_hash {
                            log::debug!("version file {actual_hash} is corrupted!");
                            stats_cl.incr_corrupted();
                            if !dry_run {
                                // mismatch - delete the corrupted version dir
                                match fs::remove_dir_all(&suffix_path).await {
                                    Ok(_) => {
                                        stats_cl.incr_deleted();
                                        log::debug!("Corrupted version file {actual_hash} deleted");
                                    }
                                    Err(_) => {
                                        stats_cl.incr_io_error();
                                        log::debug!(
                                            "Failed to delete corrupted version file {actual_hash}"
                                        );
                                    }
                                }
                            }
                        }

                        // release permit
                        drop(permit);
                    }
                }
            });

            handles.push(handle);
        }

        // Await all prefix tasks
        for h in handles {
            let _ = h.await;
        }

        // Gather stats and return result
        let (scanned, corrupted, io_errors, deleted) = stats.snapshot();
        let elapsed = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);
        let result = CleanCorruptedVersionsResult {
            scanned: scanned as u64,
            corrupted: corrupted as u64,
            cleaned: deleted as u64,
            errors: io_errors as u64,
            elapsed,
        };

        Ok(result)
    }

    fn storage_kind(&self) -> crate::storage::StorageKind {
        crate::storage::StorageKind::Local
    }

    fn chunked(&self) -> Option<&dyn ChunkedVersionStore> {
        Some(self)
    }
}

#[async_trait]
impl ChunkedVersionStore for LocalVersionStore {
    async fn store_version_chunked(
        &self,
        hash: &str,
        profile: Option<StorageProfile>,
        data_type: &EntryDataType,
        extension: &str,
        reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    ) -> Result<ChunkManifest, OxenError> {
        let manifest_path = self.manifest_path(hash);
        let engine = self.engine()?;
        let expected_hash: MerkleHash = hash.parse()?;
        let policy = encode_policy(profile, data_type, extension);
        // The bridge must be constructed on the async runtime; its reads then run
        // on the blocking pool inside the spawn_blocking below.
        let mut sync_reader = SyncIoBridge::new(reader);
        spawn_blocking(move || {
            // Idempotent: a published manifest is never overwritten.
            if let Some(existing) = read_manifest_at(&manifest_path)? {
                return Ok(existing);
            }
            let manifest = engine.ingest(&mut sync_reader, &policy)?;
            if manifest.file_hash != expected_hash {
                // Blocks already written are unreferenced orphans, reclaimable by
                // GC; nothing is published under the claimed hash.
                return Err(OxenError::HashMismatch {
                    path: manifest_path,
                    expected: expected_hash,
                    actual: manifest.file_hash,
                });
            }
            write_manifest_at(&engine, &manifest_path, &manifest)?;
            Ok(manifest)
        })
        .await?
    }

    async fn get_manifest(&self, hash: &str) -> Result<Option<ChunkManifest>, OxenError> {
        let manifest_path = self.manifest_path(hash);
        spawn_blocking(move || read_manifest_at(&manifest_path)).await?
    }

    async fn put_manifest(&self, manifest: &ChunkManifest) -> Result<(), OxenError> {
        let manifest_path = self.manifest_path(&manifest.file_hash.to_string());
        let engine = self.engine()?;
        let manifest = manifest.clone();
        spawn_blocking(move || {
            manifest.validate()?;
            // No-op success: the store already holds this version's bytes, and two
            // valid recipes for the same bytes may legitimately differ across
            // chunker policies. The published manifest stays canonical.
            if manifest_path.exists() {
                return Ok(());
            }
            // Full streamed reconstruction (decision 11): the chunk list must
            // reproduce exactly `file_size` bytes hashing to the claimed file hash
            // before the manifest is published. A missing chunk fails here too.
            let mut reader = ReconstructReader::new(Arc::clone(&engine), manifest.clone());
            let mut hashing = HashingReader::new(&mut reader);
            let reconstructed_size = std::io::copy(&mut hashing, &mut std::io::sink())?;
            let actual = MerkleHash::new(hashing.digest128());
            // Under a transform, `file_size` is the transformed stream's size while
            // the reader yields original bytes — the hash comparison alone is the
            // end-to-end check there.
            let size_ok = manifest.transform_id != TransformId::IDENTITY
                || reconstructed_size == manifest.file_size;
            if !size_ok || actual != manifest.file_hash {
                return Err(OxenError::HashMismatch {
                    path: manifest_path,
                    expected: manifest.file_hash,
                    actual,
                });
            }
            write_manifest_at(&engine, &manifest_path, &manifest)
        })
        .await?
    }

    async fn missing_chunks(&self, hashes: &[u128]) -> Result<Vec<u128>, OxenError> {
        let engine = self.engine()?;
        let hashes = hashes.to_vec();
        spawn_blocking(move || engine.index().missing(&hashes)).await?
    }

    async fn store_block(&self, hash: &str, data: Bytes) -> Result<(), OxenError> {
        let engine = self.engine()?;
        let block_hash = u128::from_str_radix(hash, 16)
            .map_err(|_| OxenError::basic_str(format!("invalid block hash: {hash}")))?;
        spawn_blocking(move || engine.store_block(block_hash, data)).await?
    }

    async fn pack_chunks(&self, hashes: &[u128]) -> Result<Vec<SealedBlock>, OxenError> {
        let engine = self.engine()?;
        let hashes = hashes.to_vec();
        spawn_blocking(move || engine.pack_chunks(&hashes)).await?
    }

    async fn open_seekable(&self, hash: &str) -> Result<SeekableVersionReader, OxenError> {
        let manifest_path = self.manifest_path(hash);
        let engine = self.engine()?;
        let hash = hash.to_string();
        spawn_blocking(move || {
            let manifest = read_manifest_at(&manifest_path)?.ok_or_else(|| {
                OxenError::basic_str(format!("no chunked version found for hash {hash}"))
            })?;
            SeekableVersionReader::new(engine, manifest)
        })
        .await?
    }

    async fn rebuild_chunk_index(&self) -> Result<u64, OxenError> {
        let engine = self.engine()?;
        spawn_blocking(move || engine.rebuild_index()).await?
    }

    async fn compact_chunk_index(&self) -> Result<(), OxenError> {
        let engine = self.engine()?;
        spawn_blocking(move || engine.index().compact().map(|_| ())).await?
    }

    async fn clear_chunk_index(&self) -> Result<(), OxenError> {
        let engine = self.engine()?;
        spawn_blocking(move || engine.index().clear()).await?
    }

    async fn delete_whole_file_blob(&self, hash: &str) -> Result<(), OxenError> {
        let data_path = self.version_path(hash);
        let manifest_path = self.manifest_path(hash);
        let hash = hash.to_string();
        spawn_blocking(move || {
            if !manifest_path.exists() {
                return Err(OxenError::basic_str(format!(
                    "refusing to delete the whole-file blob for {hash}: no chunked representation exists"
                )));
            }
            if data_path.exists() {
                crate::util::fs::remove_file(&data_path)?;
            }
            Ok(())
        })
        .await?
    }

    async fn delete_manifest(&self, hash: &str) -> Result<(), OxenError> {
        let data_path = self.version_path(hash);
        let manifest_path = self.manifest_path(hash);
        let hash = hash.to_string();
        spawn_blocking(move || {
            if !data_path.exists() {
                return Err(OxenError::basic_str(format!(
                    "refusing to delete the manifest for {hash}: no whole-file blob exists"
                )));
            }
            if manifest_path.exists() {
                crate::util::fs::remove_file(&manifest_path)?;
            }
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    async fn setup() -> (TempDir, LocalVersionStore) {
        let temp_dir = TempDir::new().unwrap();
        // Mirror the production layout (`.oxen/versions/files`): the store owns
        // everything under this `files` dir, chunked storage included.
        let store = LocalVersionStore::new(temp_dir.path().join("files"));
        store.init().await.unwrap();
        (temp_dir, store)
    }

    #[tokio::test]
    async fn test_init() {
        let (_temp_dir, store) = setup().await;
        assert!(store.root_path.exists());
        assert!(store.root_path.is_dir());
    }

    #[tokio::test]
    async fn test_store_and_get_version() {
        let (_temp_dir, store) = setup().await;
        let data = b"test data";
        let hash = hasher::hash_buffer(data);

        // Store the version
        store
            .store_version(&hash, Bytes::from_static(data))
            .await
            .unwrap();

        // Verify the file exists with correct structure
        let version_path = store.version_path(&hash);
        assert!(version_path.exists());
        assert_eq!(version_path.parent().unwrap(), store.version_dir(&hash));

        // Get and verify the data
        let retrieved = store.get_version(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_version_location_returns_local_path() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";

        let location = store.version_location(hash).await.unwrap();
        match location {
            VersionLocation::Local(path) => {
                assert_eq!(path, store.version_path(hash));
            }
            other => panic!("expected Local variant, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_store_from_reader() {
        let (_temp_dir, store) = setup().await;
        let data = b"test data from reader";
        let hash = hasher::hash_buffer(data);

        // Create a cursor with the test data
        let cursor = Cursor::new(data.to_vec());

        // Store using the reader
        store
            .store_version_from_reader(&hash, Box::new(cursor), data.len() as u64)
            .await
            .unwrap();

        // Verify the file exists
        let version_path = store.version_path(&hash);
        assert!(version_path.exists());

        // Get and verify the data
        let retrieved = store.get_version(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_version_exists() {
        let (_temp_dir, store) = setup().await;
        let data = b"test data";
        let hash = hasher::hash_buffer(data);

        // Check non-existent version
        assert!(!store.version_exists(&hash).await.unwrap());

        // Store and check again
        store
            .store_version(&hash, Bytes::from_static(data))
            .await
            .unwrap();
        assert!(store.version_exists(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn test_find_missing_versions_returns_only_absent_hashes() {
        let (_temp_dir, store) = setup().await;
        let present_data = b"x";
        let also_present_data = b"y";
        let present = hasher::hash_buffer(present_data);
        let also_present = hasher::hash_buffer(also_present_data);
        let absent = "cccc3333cccc3333";
        store
            .store_version(&present, Bytes::from_static(present_data))
            .await
            .unwrap();
        store
            .store_version(&also_present, Bytes::from_static(also_present_data))
            .await
            .unwrap();

        let missing = store
            .find_missing_versions(&[present.clone(), absent.to_string(), also_present.clone()])
            .await
            .unwrap();
        assert_eq!(missing, vec![absent.to_string()]);
    }

    #[tokio::test]
    async fn test_find_missing_versions_empty_input_returns_empty() {
        let (_temp_dir, store) = setup().await;
        let missing = store.find_missing_versions(&[]).await.unwrap();
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_delete_version() {
        let (_temp_dir, store) = setup().await;
        let data = b"test data";
        let hash = hasher::hash_buffer(data);

        // Store and verify
        store
            .store_version(&hash, Bytes::from_static(data))
            .await
            .unwrap();
        assert!(store.version_exists(&hash).await.unwrap());

        // Delete and verify
        store.delete_version(&hash).await.unwrap();
        assert!(!store.version_exists(&hash).await.unwrap());
        assert!(!store.version_dir(&hash).exists());
    }

    #[tokio::test]
    async fn test_list_versions() {
        let (_temp_dir, store) = setup().await;
        // Insert with three distinct payloads so we get three distinct content-addressed
        // hashes. `list_versions` is documented to return sorted results.
        let payloads: [&[u8]; 3] = [b"alpha", b"bravo", b"charlie"];
        let mut hashes: Vec<String> = payloads.iter().map(|d| hasher::hash_buffer(d)).collect();

        for (payload, hash) in payloads.iter().zip(&hashes) {
            store
                .store_version(hash, Bytes::from_static(payload))
                .await
                .unwrap();
        }

        hashes.sort();
        let versions = store.list_versions().await.unwrap();
        assert_eq!(versions, hashes);
    }

    #[tokio::test]
    async fn test_get_nonexistent_version() {
        let (_temp_dir, store) = setup().await;
        let hash = "nonexistent";

        match store.get_version(hash).await {
            Ok(_) => panic!("Expected error when getting non-existent version"),
            Err(OxenError::IO(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
            Err(e) => {
                panic!("Unexpected error when getting non-existent version: {e:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_delete_nonexistent_version() {
        let (_temp_dir, store) = setup().await;
        let hash = "nonexistent";

        // Should not error when deleting non-existent version
        store.delete_version(hash).await.unwrap();
    }

    #[tokio::test]
    async fn test_store_and_get_version_chunk() {
        let (_temp_dir, store) = setup().await;
        let data = b"test chunk data";
        let hash = hasher::hash_buffer(data);
        let offset = 0;
        let size = data.len() as u64;

        // Store the chunk
        store
            .store_version(&hash, Bytes::from_static(data))
            .await
            .unwrap();

        // Verify the file exists with correct structure
        let file_path = store.version_path(&hash);
        assert!(file_path.exists());
        assert_eq!(file_path.parent().unwrap(), store.version_dir(&hash));

        // Get and verify the data
        let retrieved = store.get_version_chunk(&hash, offset, size).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_combine_version_chunks_reassembles_and_removes_chunks_dir() {
        let (_temp_dir, store) = setup().await;
        let data = b"chunk-zero-byteschunk-one-bytes";
        let hash = hasher::hash_buffer(data);

        // Split into two chunks stored at their byte offsets.
        let split = 16;
        store
            .store_version_chunk(&hash, 0, Bytes::copy_from_slice(&data[..split]))
            .await
            .unwrap();
        store
            .store_version_chunk(&hash, split as u64, Bytes::copy_from_slice(&data[split..]))
            .await
            .unwrap();

        let chunks_dir = store.version_chunks_dir(&hash);
        assert!(chunks_dir.exists());

        store.combine_version_chunks(&hash).await.unwrap();

        // The reassembled version file matches the original bytes...
        let version_path = store.version_path(&hash);
        assert!(version_path.exists());
        assert_eq!(store.get_version(&hash).await.unwrap(), data);

        // ...and the chunks directory is removed.
        assert!(!chunks_dir.exists());
    }

    #[tokio::test]
    async fn test_get_nonexistent_chunk() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";
        let offset = 0;
        let size = 100;

        match store.get_version_chunk(hash, offset, size).await {
            Ok(_) => panic!("Expected error when getting non-existent chunk"),
            Err(OxenError::IO(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
            Err(e) => {
                panic!("Unexpected error when getting non-existent chunk: {e:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_store_and_get_version_derived() {
        let (_temp_dir, store) = setup().await;
        let orig_hash = "aaaaaaaaaaaaaaaa";
        let derived_filename = "100x200.jpg";
        let derived_data = b"fake resized image bytes for hash aaaaaaaaaaaaaaaa";

        // Store derived
        store
            .store_version_derived(
                orig_hash,
                derived_filename,
                Bytes::from_static(derived_data),
            )
            .await
            .unwrap();

        // Retrieve via stream and verify content
        use futures::StreamExt;
        let mut stream = store
            .get_version_derived_stream(orig_hash, derived_filename)
            .await
            .unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            collected.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(collected, derived_data);
        assert_eq!(
            store
                .get_version_derived_size(orig_hash, derived_filename)
                .await
                .unwrap(),
            derived_data.len() as u64
        );
    }

    #[tokio::test]
    async fn test_derived_version_exists() {
        let (_temp_dir, store) = setup().await;
        let orig_hash = "bbbbbbbbbbbbbbbb";
        let derived_filename = "300x400.jpg";
        let derived_data = b"fake resized image bytes for hash bbbbbbbbbbbbbbbb";

        // Should not exist before store
        assert!(
            !store
                .derived_version_exists(orig_hash, derived_filename)
                .await
                .unwrap()
        );

        // Store and check again
        store
            .store_version_derived(
                orig_hash,
                derived_filename,
                Bytes::from_static(derived_data),
            )
            .await
            .unwrap();
        assert!(
            store
                .derived_version_exists(orig_hash, derived_filename)
                .await
                .unwrap()
        );
    }

    /// `copy_version_to_path` must land bytes + stamp the mtime atomically. This is the
    /// working-tree publish path that `restore_file` (oxen pull/checkout/restore) drives —
    /// losing the mtime here is what defeats the `oxen status` size+mtime fast path.
    #[tokio::test]
    async fn test_copy_version_to_path_with_mtime_stamps_atomically() {
        let (_temp_dir, store) = setup().await;
        let data = b"working-tree publish content";
        let hash = hasher::hash_buffer(data);
        store
            .store_version(&hash, Bytes::from_static(data))
            .await
            .unwrap();

        let dest_dir = TempDir::new().unwrap();
        let dest_path = dest_dir.path().join("subdir/output.bin");
        let mtime = SystemTime::UNIX_EPOCH + std::time::Duration::new(1_700_000_000, 123_456_789);

        store
            .copy_version_to_path(&hash, &dest_path, mtime)
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest_path).unwrap(), data);
        let actual = std::fs::metadata(&dest_path).unwrap().modified().unwrap();
        assert_eq!(actual, mtime);
    }

    /// Missing source hash surfaces as `VersionStoreDataMissing` and the call must not
    /// leave a scratch sibling or zero-byte file next to the destination.
    #[tokio::test]
    async fn test_copy_version_to_path_missing_version() {
        let (_temp_dir, store) = setup().await;
        let dest_dir = TempDir::new().unwrap();
        let dest_path = dest_dir.path().join("out.bin");
        let mtime = SystemTime::UNIX_EPOCH + std::time::Duration::new(1_700_000_000, 0);

        let result = store
            .copy_version_to_path("0000000000000000", &dest_path, mtime)
            .await;
        assert!(matches!(
            result,
            Err(OxenError::VersionStoreDataMissing { .. })
        ));
        assert!(!dest_path.exists());
        let names: Vec<_> = std::fs::read_dir(dest_dir.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .collect();
        assert!(names.is_empty(), "leftover entries: {names:?}");
    }

    mod chunked {
        use super::*;
        use futures::StreamExt;

        /// Deterministic compressible pseudo-CSV, seeded for reproducibility.
        fn csv_bytes(seed: u64, len: usize) -> Vec<u8> {
            let mut out = Vec::with_capacity(len + 64);
            let mut state = seed;
            let mut row = 0u64;
            while out.len() < len {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                out.extend_from_slice(
                    format!("{row},image_{state:x}.jpg,label_{}\n", state % 10).as_bytes(),
                );
                row += 1;
            }
            out.truncate(len);
            out
        }

        async fn store_chunked(
            store: &LocalVersionStore,
            data: &[u8],
        ) -> Result<(String, ChunkManifest), OxenError> {
            let hash = hasher::hash_buffer(data);
            let manifest = store
                .chunked()
                .expect("local store supports chunked versions")
                .store_version_chunked(
                    &hash,
                    None,
                    &EntryDataType::Tabular,
                    "csv",
                    Box::new(Cursor::new(data.to_vec())),
                )
                .await?;
            Ok((hash, manifest))
        }

        /// A chunked version reads identically to a whole-file version through
        /// every transparent read API — no caller can tell the representations
        /// apart.
        #[tokio::test]
        async fn chunked_version_reads_are_transparent() -> Result<(), OxenError> {
            let (_temp_dir, store) = setup().await;
            let data = csv_bytes(3, 2 * 1024 * 1024);
            let (hash, manifest) = store_chunked(&store, &data).await?;

            // The version exists with a manifest instead of a data blob.
            assert!(store.version_exists(&hash).await?);
            assert!(!store.version_path(&hash).exists());
            assert!(store.manifest_path(&hash).exists());
            assert_eq!(manifest.file_size, data.len() as u64);

            // Whole-file reads.
            assert_eq!(store.get_version_size(&hash).await?, data.len() as u64);
            assert_eq!(store.get_version(&hash).await?, data);

            // Streamed read.
            let mut stream = store.get_version_stream(&hash).await?;
            let mut streamed = Vec::new();
            while let Some(chunk) = stream.next().await {
                streamed.extend_from_slice(&chunk?);
            }
            assert_eq!(streamed, data);

            // Range reads, including the same beyond-EOF error the blob path gives.
            let range = store.get_version_chunk(&hash, 1000, 4096).await?;
            assert_eq!(range, &data[1000..5096]);
            assert!(matches!(
                store
                    .get_version_chunk(&hash, data.len() as u64 - 1, 2)
                    .await,
                Err(OxenError::IO(e)) if e.kind() == io::ErrorKind::UnexpectedEof
            ));

            // Working-tree publish: verified against the whole-file hash, mtime
            // stamped.
            let dest_dir = TempDir::new().unwrap();
            let dest_path = dest_dir.path().join("restored.csv");
            let mtime = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
            store.copy_version_to_path(&hash, &dest_path, mtime).await?;
            assert_eq!(std::fs::read(&dest_path).unwrap(), data);
            assert_eq!(
                std::fs::metadata(&dest_path).unwrap().modified().unwrap(),
                mtime
            );

            // find_missing_versions sees chunked versions as present.
            let missing = store
                .find_missing_versions(&[hash.clone(), "0000000000000000".to_string()])
                .await?;
            assert_eq!(missing, vec!["0000000000000000".to_string()]);
            Ok(())
        }

        /// Storing the same version chunked twice is idempotent, and a claimed
        /// hash that doesn't match the bytes is rejected without publishing.
        #[tokio::test]
        async fn chunked_ingest_is_idempotent_and_verified() -> Result<(), OxenError> {
            let (_temp_dir, store) = setup().await;
            let data = csv_bytes(5, 1_500_000);
            let (_hash, manifest) = store_chunked(&store, &data).await?;

            let again = store_chunked(&store, &data).await?.1;
            assert_eq!(again, manifest);

            // A lying hash: ingest completes, verification refuses to publish.
            let wrong_hash = hasher::hash_buffer(b"different bytes");
            let result = store
                .chunked()
                .expect("local store supports chunked versions")
                .store_version_chunked(
                    &wrong_hash,
                    None,
                    &EntryDataType::Tabular,
                    "csv",
                    Box::new(Cursor::new(data.clone())),
                )
                .await;
            assert!(matches!(result, Err(OxenError::HashMismatch { .. })));
            assert!(!store.version_exists(&wrong_hash).await?);
            Ok(())
        }

        /// `put_manifest` validates by full reconstruction: it publishes a good
        /// manifest whose chunks are present, no-ops on a duplicate, and rejects a
        /// manifest whose chunks are missing or whose hash lies.
        #[tokio::test]
        async fn put_manifest_validates_before_publish() -> Result<(), OxenError> {
            let (_temp_dir, store) = setup().await;
            let chunked_store = store.chunked().expect("chunked capability");
            let data = csv_bytes(7, 1_200_000);
            let (hash, manifest) = store_chunked(&store, &data).await?;

            // Re-publishing the same manifest is a no-op success.
            chunked_store.put_manifest(&manifest).await?;

            // Delete the manifest, then re-publish from the still-present chunks:
            // validation reconstructs and passes.
            std::fs::remove_file(store.manifest_path(&hash)).unwrap();
            assert!(!store.version_exists(&hash).await?);
            chunked_store.put_manifest(&manifest).await?;
            assert_eq!(store.get_version(&hash).await?, data);

            // A manifest lying about its file hash is rejected.
            std::fs::remove_file(store.manifest_path(&hash)).unwrap();
            let mut lying = manifest.clone();
            lying.file_hash = MerkleHash::new(lying.file_hash.to_u128() ^ 1);
            assert!(matches!(
                chunked_store.put_manifest(&lying).await,
                Err(OxenError::HashMismatch { .. })
            ));

            // A manifest referencing chunks this store doesn't have is rejected.
            let mut foreign = manifest.clone();
            for chunk in &mut foreign.chunks {
                chunk.hash ^= 0xF00D;
            }
            assert!(chunked_store.put_manifest(&foreign).await.is_err());
            Ok(())
        }

        /// Chunked storage is fully contained in each store's root: sibling
        /// stores under a shared parent (any custom `versions_path` layout)
        /// never share blocks or a chunk index, nothing escapes to the parent,
        /// and the reserved dirs never surface as versions.
        #[tokio::test]
        async fn chunked_storage_is_isolated_per_store_root() -> Result<(), OxenError> {
            let shared = TempDir::new().unwrap();
            let store_a = LocalVersionStore::new(shared.path().join("repo_a"));
            store_a.init().await?;
            let store_b = LocalVersionStore::new(shared.path().join("repo_b"));
            store_b.init().await?;

            let data = csv_bytes(29, 1_200_000);
            let (hash, _manifest) = store_chunked(&store_a, &data).await?;

            assert!(shared.path().join("repo_a").join(BLOCKS_DIR).exists());
            assert!(!shared.path().join(BLOCKS_DIR).exists());
            assert!(!shared.path().join("repo_b").join(BLOCKS_DIR).exists());
            assert!(
                store_b
                    .chunked()
                    .expect("local store supports chunked versions")
                    .get_manifest(&hash)
                    .await?
                    .is_none()
            );

            assert_eq!(store_a.list_versions().await?, vec![hash]);
            Ok(())
        }

        /// Range requests whose `offset + size` overflows must fail with the
        /// beyond-EOF error instead of wrapping past the bounds guard and
        /// returning truncated data (offset and size arrive straight from
        /// request query parameters).
        #[tokio::test]
        async fn get_version_chunk_rejects_overflowing_ranges() -> Result<(), OxenError> {
            let (_temp_dir, store) = setup().await;
            let data = csv_bytes(19, 1_200_000);
            let (chunked_hash, _manifest) = store_chunked(&store, &data).await?;
            let blob = b"0123456789";
            let blob_hash = hasher::hash_buffer(blob);
            store
                .store_version(&blob_hash, Bytes::from_static(blob))
                .await?;

            for hash in [&chunked_hash, &blob_hash] {
                let result = store.get_version_chunk(hash, 1, u64::MAX).await;
                assert!(
                    matches!(
                        &result,
                        Err(OxenError::IO(e)) if e.kind() == io::ErrorKind::UnexpectedEof
                    ),
                    "expected beyond-EOF error for {hash}, got {result:?}"
                );
            }
            Ok(())
        }

        /// `clean_corrupted_versions` must never delete a chunked version: the
        /// manifest (not a `data` blob) is its only representation. Corrupted
        /// whole-file blobs and unparseable manifests are still cleaned.
        #[tokio::test]
        async fn clean_corrupted_versions_preserves_chunked_versions() -> Result<(), OxenError> {
            let (_temp_dir, store) = setup().await;
            let data = csv_bytes(13, 1_200_000);
            let (chunked_hash, _manifest) = store_chunked(&store, &data).await?;

            let good = b"healthy whole-file bytes";
            let good_hash = hasher::hash_buffer(good);
            store
                .store_version(&good_hash, Bytes::from_static(good))
                .await?;

            // A whole-file blob planted under a hash its bytes don't match
            // (store_version verifies, so write the fixture directly).
            let bad_hash = "aaaa5555bbbb6666".to_string();
            let bad_dir = store.version_dir(&bad_hash);
            std::fs::create_dir_all(&bad_dir).unwrap();
            std::fs::write(bad_dir.join(VERSION_FILE_NAME), b"corrupted bytes").unwrap();

            // Dry run: counts but deletes nothing.
            let result = store.clean_corrupted_versions(true).await?;
            assert_eq!(result.scanned, 3);
            assert_eq!(result.corrupted, 1);
            assert_eq!(result.cleaned, 0);
            assert!(store.version_exists(&chunked_hash).await?);
            assert!(store.version_exists(&bad_hash).await?);

            // Real run: the corrupted blob goes, the chunked version stays readable.
            let result = store.clean_corrupted_versions(false).await?;
            assert_eq!(result.corrupted, 1);
            assert_eq!(result.cleaned, 1);
            assert!(!store.version_exists(&bad_hash).await?);
            assert!(store.version_exists(&good_hash).await?);
            assert!(store.version_exists(&chunked_hash).await?);
            assert_eq!(store.get_version(&chunked_hash).await?, data);

            // An unparseable manifest is corruption and is cleaned.
            let junk_hash = "1234abcd5678ef90";
            let junk_dir = store.version_dir(junk_hash);
            std::fs::create_dir_all(&junk_dir).unwrap();
            std::fs::write(junk_dir.join(VERSION_MANIFEST_FILE_NAME), b"not a manifest").unwrap();
            let result = store.clean_corrupted_versions(false).await?;
            assert_eq!(result.corrupted, 1);
            assert_eq!(result.cleaned, 1);
            assert!(!store.version_exists(junk_hash).await?);
            assert!(store.version_exists(&chunked_hash).await?);
            Ok(())
        }

        /// The negotiation and transfer surface: missing_chunks partitions
        /// correctly and store_block installs verified blocks that make a
        /// version reconstructable after a manifest arrives.
        #[tokio::test]
        async fn missing_chunks_and_store_block_round_trip() -> Result<(), OxenError> {
            let (_src_dir, src) = setup().await;
            let (_dst_dir, dst) = setup().await;
            let src_chunked = src.chunked().expect("chunked capability");
            let dst_chunked = dst.chunked().expect("chunked capability");

            let data = csv_bytes(11, 1_200_000);
            let (hash, manifest) = store_chunked(&src, &data).await?;

            let all_hashes: Vec<u128> = manifest.chunks.iter().map(|c| c.hash).collect();
            assert_eq!(
                src_chunked.missing_chunks(&all_hashes).await?,
                Vec::<u128>::new()
            );
            assert_eq!(dst_chunked.missing_chunks(&all_hashes).await?, all_hashes);

            // Transfer through the real wire mechanism: pack the needed chunks
            // into transfer blocks (which re-encodes any store-local codecs) and
            // store them on the receiver.
            for block in src_chunked.pack_chunks(&all_hashes).await? {
                dst_chunked
                    .store_block(&format!("{:032x}", block.hash), block.data)
                    .await?;
            }
            assert_eq!(
                dst_chunked.missing_chunks(&all_hashes).await?,
                Vec::<u128>::new()
            );
            dst_chunked.put_manifest(&manifest).await?;
            assert_eq!(dst.get_version(&hash).await?, data);

            // The chunk index is disposable on the receiving store too.
            dst_chunked.rebuild_chunk_index().await?;
            assert_eq!(dst.get_version(&hash).await?, data);
            Ok(())
        }
    }
}
