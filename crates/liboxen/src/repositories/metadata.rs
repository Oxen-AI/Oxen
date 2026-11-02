//! Helper functions to get metadata from the local filesystem.
//!

use crate::core::df::tabular::stored_size_and_schema;
use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::parsed_resource::ParsedResourceView;
use crate::model::{Commit, LocalRepository, MerkleHash, MetadataEntry};
use crate::repositories;
use crate::storage::VersionStore;
use crate::util;
use crate::util::fs::{
    LineCounts, data_type_from_head, file_extension, mime_type_from_head, read_head,
};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use parking_lot::Mutex;
use polars::io::cloud::CloudOptions;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::CloudScheme;
use polars::prelude::PolarsError;
use std::fmt;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::spawn_blocking;

pub mod audio;
pub mod image;
pub mod tabular;
pub mod text;
pub mod video;

/// Returns the metadata given a file path
#[cfg(test)]
fn get(path: impl AsRef<Path>) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path
        .file_name()
        .ok_or_else(|| OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let (mime_type, data_type, metadata) = inspect_file(path, path)?;
    let extension = util::fs::file_extension(path);

    Ok(MetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        hash: "".to_string(),
        is_dir: path.is_dir(),
        latest_commit: None,
        resource: None,
        size,
        data_type,
        mime_type,
        extension,
        metadata,
        is_queryable: None,
        children: None,
    })
}

pub fn from_file_node(
    _repo: &LocalRepository,
    node: &FileNode,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    Ok(MetadataEntry {
        filename: node.name().to_string(),
        hash: node.hash().to_string(),
        is_dir: false,
        latest_commit: Some(commit.to_owned()),
        resource: Some(ParsedResourceView {
            commit: Some(commit.to_owned()),
            branch: None,
            workspace: None,
            path: PathBuf::from(node.name()),
            version: PathBuf::from(commit.id.to_string()),
            resource: PathBuf::from(commit.id.to_string()).join(node.name()),
        }),
        size: node.num_bytes(),
        data_type: node.data_type().clone(),
        mime_type: node.mime_type().to_string(),
        extension: node.extension().to_string(),
        metadata: node.metadata(),
        is_queryable: None,
        children: None,
    })
}

pub fn from_dir_node(
    _repo: &LocalRepository,
    node: &DirNode,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    Ok(MetadataEntry {
        filename: node.name().to_string(),
        hash: node.hash().to_string(),
        is_dir: true,
        latest_commit: Some(commit.to_owned()),
        resource: None,
        size: node.num_bytes(),
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: "".to_string(),
        metadata: None,
        is_queryable: None,
        children: None,
    })
}

/// Returns metadata with latest commit information.
pub use crate::core::v_latest::metadata::get_cli;

/// Returns CLI metadata for `path` as it existed at `revision`, sourced from the merkle tree
/// rather than the working-tree file. `revision` may be a branch name, a commit id, or `HEAD`.
/// Errors if `revision` does not resolve to a commit, or if `path` is not a file in that commit.
pub fn get_cli_at_revision(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    revision: &str,
) -> Result<CLIMetadataEntry, OxenError> {
    let path = path.as_ref();
    let commit = repositories::revisions::get(repo, revision)?
        .ok_or_else(|| OxenError::RevisionNotFound(revision.into()))?;
    let file_node = repositories::tree::get_file_by_path(repo, &commit, path)?
        .ok_or_else(|| OxenError::entry_does_not_exist_in_commit(path, &commit.id))?;
    let last_commit_id = file_node.last_commit_id().to_string();
    let last_updated = repositories::commits::get_by_id(repo, &last_commit_id)?;
    Ok(CLIMetadataEntry {
        filename: file_node.name().to_string(),
        last_updated,
        hash: file_node.hash().to_string(),
        size: file_node.num_bytes(),
        data_type: file_node.data_type().clone(),
        mime_type: file_node.mime_type().to_string(),
        extension: file_node.extension().to_string(),
    })
}

/// Returns the file size in bytes.
pub fn get_file_size(path: impl AsRef<Path>) -> Result<u64, OxenError> {
    let metadata = util::fs::metadata(path.as_ref())?;
    Ok(metadata.len())
}

/// A file's bytes, in a file at a path, in memory, or in an S3 version store.
pub(crate) enum ByteSource<'a> {
    Path(&'a Path),
    Memory(Bytes),
    Stored(&'a StoredVersion),
}

impl ByteSource<'_> {
    /// A reader of the bytes. A stored version file's reader blocks while it fetches them, so read
    /// it off the async runtime.
    pub(crate) fn open(&self) -> Result<Box<dyn MmapBytesReader>, OxenError> {
        match self {
            ByteSource::Path(path) => Ok(Box::new(
                File::open(path).map_err(|err| OxenError::file_open_error(path, err))?,
            )),
            ByteSource::Memory(data) => Ok(Box::new(Cursor::new(data.clone()))),
            ByteSource::Stored(version) => Ok(Box::new(version.reader())),
        }
    }

    /// Fails a stored version file's inspection with `err`, from a Polars scan of the file, when
    /// the scan could not fetch the file's bytes.
    pub(crate) fn record_scan_error(&self, err: &PolarsError) {
        let ByteSource::Stored(version) = self else {
            return;
        };
        let err = OxenError::PolarsError(err.clone());
        if err.is_polars_io_error() {
            version.bytes.record_error(err);
        }
    }
}

impl fmt::Debug for ByteSource<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ByteSource::Path(path) => write!(f, "{path:?}"),
            ByteSource::Memory(data) => write!(f, "{} bytes in memory", data.len()),
            ByteSource::Stored(version) => {
                write!(f, "version file {} at {}", version.bytes.hash, version.url)
            }
        }
    }
}

/// A version file in an S3 version store.
#[derive(Clone)]
pub(crate) struct StoredVersion {
    bytes: Arc<StoredBytes>,
    pub(crate) url: String,
    pub(crate) cloud_options: CloudOptions,
}

impl StoredVersion {
    /// The version file `hash`, `size` bytes long, which `store` keeps at `url` in `region`, behind
    /// `endpoint_url` when the store is not AWS. Call on the async runtime.
    pub(crate) async fn new(
        store: Arc<dyn VersionStore>,
        hash: MerkleHash,
        size: u64,
        url: String,
        region: &str,
        endpoint_url: Option<&str>,
    ) -> Result<Self, OxenError> {
        let mut config = vec![("aws_region", region)];
        if let Some(endpoint) = endpoint_url {
            config.push(("aws_endpoint_url", endpoint));
            config.push(("aws_allow_http", "true"));
            config.push(("aws_virtual_hosted_style_request", "false"));
        }
        let cloud_options =
            CloudOptions::from_untyped_config(CloudScheme::from_path(&url), config)?;
        Ok(Self {
            bytes: Arc::new(StoredBytes {
                store,
                hash,
                size,
                runtime: Handle::current(),
                head: Mutex::default(),
                fetch_error: Mutex::default(),
            }),
            url,
            cloud_options,
        })
    }

    pub(crate) fn size(&self) -> u64 {
        self.bytes.size
    }

    /// A seekable reader of the file, which fetches the ranges it reads from the store.
    pub(crate) fn reader(&self) -> VersionReader {
        VersionReader {
            bytes: Arc::clone(&self.bytes),
            position: 0,
            fetched: Bytes::new(),
            fetched_at: 0,
        }
    }

    /// The file's first `len` bytes, or all of it when it is shorter, running on past `len` when an
    /// earlier read of its start fetched more. Fetches only what no earlier read of its start did.
    pub(crate) async fn fetch_head(&self, len: u64) -> Result<Bytes, OxenError> {
        self.bytes.fetch_head(len).await
    }

    /// The whole file, fetched in one request. Blocks.
    pub(crate) fn read_all(&self) -> Result<Bytes, OxenError> {
        if self.bytes.size == 0 {
            return Ok(Bytes::new());
        }
        Ok(self.bytes.fetch(0, self.bytes.size)?)
    }

    /// The file's line counts, and its character count when `with_chars` is set, from a download of
    /// the whole file.
    pub(crate) async fn download_line_counts(
        &self,
        with_chars: bool,
    ) -> Result<LineCounts, OxenError> {
        let mut stream = (self.bytes.store)
            .get_version_stream(&self.bytes.hash.to_string())
            .await?;
        let mut lines = LineCounts::new(with_chars);
        while let Some(chunk) = stream.next().await {
            lines.add(&chunk?);
        }
        Ok(lines)
    }
}

/// A stored version file's bytes as its readers fetch them: where the store keeps them, the file's
/// first bytes once a read of its start fetches them, and the first failure fetching any of them.
struct StoredBytes {
    store: Arc<dyn VersionStore>,
    hash: MerkleHash,
    size: u64,
    runtime: Handle,
    head: Mutex<Bytes>,
    fetch_error: Mutex<Option<OxenError>>,
}

impl StoredBytes {
    /// The `len` bytes at `offset`, which must lie before the file's end, fetched from the store.
    /// Blocks.
    fn fetch(&self, offset: u64, len: u64) -> io::Result<Bytes> {
        self.runtime
            .block_on(self.fetch_chunk(offset, len))
            .map_err(|err| self.record_error(err))
    }

    /// What [`StoredBytes::fetch`] gives, without blocking.
    async fn fetch_chunk(&self, offset: u64, len: u64) -> Result<Bytes, OxenError> {
        let chunk = self
            .store
            .get_version_chunk(&self.hash.to_string(), offset, len)
            .await?;
        if chunk.is_empty() {
            return Err(OxenError::internal_error(
                "the version store returned no bytes before the file's end",
            ));
        }
        Ok(Bytes::from(chunk))
    }

    /// What [`StoredVersion::fetch_head`] gives, blocking while it fetches.
    fn head(&self, len: u64) -> io::Result<Bytes> {
        self.runtime
            .block_on(self.fetch_head(len))
            .map_err(|err| self.record_error(err))
    }

    /// See [`StoredVersion::fetch_head`].
    async fn fetch_head(&self, len: u64) -> Result<Bytes, OxenError> {
        let head = self.head.lock().clone();
        let len = len.min(self.size);
        if head.len() as u64 >= len {
            return Ok(head);
        }
        let offset = head.len() as u64;
        let rest = self.fetch_chunk(offset, len - offset).await?;
        let joined = if head.is_empty() {
            rest
        } else {
            // A CSV's head grows toward 64 MiB, too large to copy on the runtime.
            spawn_blocking(move || {
                let mut joined = BytesMut::with_capacity(head.len() + rest.len());
                joined.extend_from_slice(&head);
                joined.extend_from_slice(&rest);
                joined.freeze()
            })
            .await?
        };
        *self.head.lock() = joined.clone();
        Ok(joined)
    }

    /// Keeps `err` unless an earlier failure was kept, and returns an error to fail the read with.
    fn record_error(&self, err: OxenError) -> io::Error {
        let read_error = io::Error::other(err.to_string());
        self.fetch_error.lock().get_or_insert(err);
        read_error
    }

    /// The first failure fetching the file's bytes, which no later call returns again.
    fn take_error(&self) -> Option<OxenError> {
        self.fetch_error.lock().take()
    }
}

/// The fewest bytes a [`VersionReader`] fetches at once, 64 KiB.
pub(crate) const MIN_FETCH: u64 = 64 * 1024;
/// The most bytes a [`VersionReader`] fetches at once to read ahead of a run of reads, 8 MiB.
const MAX_FETCH: u64 = 8 * 1024 * 1024;

/// A blocking, seekable reader of a version file in the store, which fetches the ranges it reads.
pub(crate) struct VersionReader {
    bytes: Arc<StoredBytes>,
    position: u64,
    fetched: Bytes,
    fetched_at: u64,
}

impl Read for VersionReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = self.bytes.size;
        if buf.is_empty() || self.position >= size {
            return Ok(0);
        }
        let fetched_to = self.fetched_at + self.fetched.len() as u64;
        if self.position < self.fetched_at || self.position >= fetched_to {
            let ahead = if self.position == fetched_to && !self.fetched.is_empty() {
                (self.fetched.len() as u64 * 2).min(MAX_FETCH)
            } else {
                MIN_FETCH
            };
            let len = ahead.max(buf.len() as u64).min(size - self.position);
            // Every reader of the file shares the bytes at its start.
            let head = self.bytes.head.lock().clone();
            (self.fetched, self.fetched_at) = if self.position < head.len() as u64 {
                (head, 0)
            } else if self.position == 0 {
                (self.bytes.head(len)?, 0)
            } else {
                (self.bytes.fetch(self.position, len)?, self.position)
            };
        }
        let start = (self.position - self.fetched_at) as usize;
        let n = buf.len().min(self.fetched.len() - start);
        buf[..n].copy_from_slice(&self.fetched[start..start + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl Seek for VersionReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = match pos {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => self.bytes.size.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        };
        self.position = position.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before the start of the file",
            )
        })?;
        Ok(self.position)
    }
}

impl MmapBytesReader for VersionReader {}

/// Reads the MIME type, data type, and metadata of the file at `data_path`, whose name is
/// `file_path`. Fails when the file cannot be opened or read, and gives no metadata for content
/// that cannot be parsed.
pub(crate) fn inspect_file(
    data_path: &Path,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    inspect(&ByteSource::Path(data_path), file_path)
}

/// Reads the MIME type, data type, and metadata of `data`, the bytes of a file named `file_path`.
/// Gives no metadata for content that cannot be parsed.
pub(crate) fn inspect_bytes(
    data: Bytes,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    inspect(&ByteSource::Memory(data), file_path)
}

/// Reads the MIME type, data type, and metadata of `version`, the bytes of a file named
/// `file_path`, fetching what it reads from the store. A text file's or line-counted data frame's
/// line counts come from `lines` when they were taken as it arrived, and otherwise from a download
/// of the file. Fails when the store cannot give it the bytes, and gives no metadata for content
/// that cannot be parsed.
pub(crate) async fn inspect_stored(
    version: StoredVersion,
    file_path: &Path,
    lines: Option<LineCounts>,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    let head = read_head(&mut Cursor::new(version.fetch_head(MIN_FETCH).await?))?;
    let mime_type = mime_type_from_head(&head, file_path);
    let data_type = data_type_from_head(&head, file_path, &mime_type);
    let metadata = match &data_type {
        EntryDataType::Text => {
            let lines = match lines {
                Some(lines) => lines,
                None => version.download_line_counts(true).await?,
            };
            Some(GenericMetadata::MetadataText(text::from_counts(&lines)))
        }
        EntryDataType::Tabular => {
            let extension = file_extension(file_path);
            let tabular = stored_size_and_schema(&version, &extension, lines)
                .await?
                .map(|(size, schema)| {
                    GenericMetadata::MetadataTabular(tabular::from_schema(&schema, size.height))
                });
            settle(
                tabular,
                &ByteSource::Stored(&version),
                file_path,
                "Could not compute tabular metadata",
            )?
        }
        EntryDataType::Image | EntryDataType::Video | EntryDataType::Audio => {
            let version = version.clone();
            let file_path = file_path.to_path_buf();
            let data_type = data_type.clone();
            spawn_blocking(move || {
                let source = ByteSource::Stored(&version);
                get_file_metadata(&mut version.reader(), &source, &file_path, &data_type)
            })
            .await??
        }
        EntryDataType::Dir | EntryDataType::Binary => None,
    };
    version
        .bytes
        .take_error()
        .map_or(Ok((mime_type, data_type, metadata)), Err)
}

fn inspect(
    source: &ByteSource,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    let mut reader = source.open()?;
    let head = read_head(&mut reader).map_err(|err| match source {
        ByteSource::Path(data_path) => OxenError::file_read_error(data_path, err),
        ByteSource::Memory(_) | ByteSource::Stored(_) => err.into(),
    })?;
    let mime_type = mime_type_from_head(&head, file_path);
    let data_type = data_type_from_head(&head, file_path, &mime_type);
    let metadata = get_file_metadata(&mut reader, source, file_path, &data_type)?;
    Ok((mime_type, data_type, metadata))
}

/// Returns the metadata of `data`, the bytes of a file of `data_type` named `file_path`. A tabular
/// file's metadata comes from `source` instead, which holds the same bytes as `data`. Gives no
/// metadata for content that cannot be parsed, and fails when the store cannot give a stored file's
/// bytes.
fn get_file_metadata(
    data: &mut (impl Read + Seek),
    source: &ByteSource,
    file_path: &Path,
    data_type: &EntryDataType,
) -> Result<Option<GenericMetadata>, OxenError> {
    let extension = file_extension(file_path);
    data.rewind()?;
    let (metadata, failure) = match data_type {
        EntryDataType::Text => (
            text::get_metadata(data).map(GenericMetadata::MetadataText),
            "Could not compute text metadata",
        ),
        EntryDataType::Image => (
            image::get_metadata(data).map(GenericMetadata::MetadataImage),
            "Could not compute image metadata",
        ),
        EntryDataType::Video => (
            video::get_metadata(data).map(GenericMetadata::MetadataVideo),
            "Could not compute video metadata",
        ),
        EntryDataType::Audio => (
            audio::get_metadata(data, &extension).map(GenericMetadata::MetadataAudio),
            "Could not compute audio metadata",
        ),
        EntryDataType::Tabular => (
            tabular::get_metadata_with_extension(source, &extension)
                .map(GenericMetadata::MetadataTabular),
            "Could not compute tabular metadata",
        ),
        _ => return Ok(None),
    };
    settle(metadata, source, file_path, failure)
}

/// `metadata`, or `None` with `failure` logged when its content could not be parsed. Fails with the
/// store's error instead when a stored file's bytes could not be fetched.
fn settle(
    metadata: Result<GenericMetadata, OxenError>,
    source: &ByteSource,
    file_path: &Path,
    failure: &str,
) -> Result<Option<GenericMetadata>, OxenError> {
    match metadata {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) => {
            if let ByteSource::Stored(version) = source
                && let Some(fetch_error) = version.bytes.take_error()
            {
                return Err(fetch_error);
            }
            tracing::error!(
                oxen.file_path = %file_path.display(),
                exception.message = %err,
                "{failure}"
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{inspect_bytes, inspect_file};
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::util::hasher::maybe_get_metadata_hash;
    use bytes::Bytes;
    use polars::df;
    use std::fs;
    use std::path::Path;

    #[tokio::test]
    async fn test_get_metadata_audio_flac() {
        let file = test::test_audio_file_with_name("121-121726-0005.flac");
        let metadata = repositories::metadata::get(file).unwrap();

        println!("metadata: {metadata:?}");

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");
    }

    #[test]
    fn test_inspect_bytes_matches_inspect_file() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let arrow = dir.join("frame.arrow");
            tabular::write_df(
                &mut df!("id" => [1, 2, 3], "label" => ["a", "b", "c"])?,
                &arrow,
            )?;
            let files = [
                test::test_csv_file_with_name("mixed_data_types.csv"),
                test::test_spam_ham(),
                test::test_text_jsonl(),
                test::test_text_json(),
                test::test_1k_parquet(),
                arrow,
                test::test_img_file_with_name("cat_1.jpg"),
                test::test_audio_file_with_name("121-121726-0005.wav"),
                test::test_video_file_with_name("basketball.mp4"),
                test::test_text_file_with_name("hello.txt"),
            ];
            for file in files {
                let (mime_type, data_type, metadata) = inspect_file(&file, &file)?;
                assert!(metadata.is_some(), "{file:?} has metadata to compare");
                let (bytes_mime_type, bytes_data_type, bytes_metadata) =
                    inspect_bytes(Bytes::from(fs::read(&file)?), &file)?;
                assert_eq!(
                    (bytes_mime_type, bytes_data_type),
                    (mime_type, data_type),
                    "{file:?}"
                );
                assert_eq!(
                    maybe_get_metadata_hash(&bytes_metadata)?,
                    maybe_get_metadata_hash(&metadata)?,
                    "{file:?} gets the same metadata from memory as from the file"
                );
            }
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_get_cli_at_revision_reads_committed_metadata_not_worktree()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let relative_path = Path::new("hello.txt");
            let full_path = repo.path.join(relative_path);

            util::fs::write_to_path(&full_path, "head version")?;
            repositories::add(&repo, &full_path).await?;
            repositories::commit(&repo, "add hello")?;

            let committed =
                repositories::metadata::get_cli_at_revision(&repo, relative_path, "HEAD")?;

            // Change the working tree to prove the metadata comes from the commit, not disk.
            util::fs::write_to_path(&full_path, "a longer working-tree version with new bytes")?;

            let at_head =
                repositories::metadata::get_cli_at_revision(&repo, relative_path, "HEAD")?;
            assert_eq!(at_head.hash, committed.hash);
            assert_eq!(at_head.size, "head version".len() as u64);

            Ok(())
        })
        .await
    }
}
