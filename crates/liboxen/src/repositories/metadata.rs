//! Helper functions to get metadata from the local filesystem.
//!

use crate::core::df::tabular::s3_cloud_options;
use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::parsed_resource::ParsedResourceView;
use crate::model::{Commit, LocalRepository, MerkleHash, MetadataEntry};
use crate::opts::CountLinesOpts;
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
use polars::prelude::PolarsError;
use std::fmt;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio_util::task::AbortOnDropHandle;

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

    /// What [`util::fs::count_lines`] gives for `data`, a reader of these bytes, which is left
    /// unread for a stored version file. Blocks while a stored file downloads to count its lines.
    pub(crate) fn count_lines(
        &self,
        data: impl Read,
        opts: CountLinesOpts,
    ) -> Result<(usize, Option<usize>), OxenError> {
        self.start_counting_lines(opts).finish(data)
    }

    /// Starts counting the lines of these bytes. A stored version file whose lines were not counted
    /// as it arrived downloads to count them while the caller goes on, until the count finishes or
    /// is dropped.
    pub(crate) fn start_counting_lines(&self, opts: CountLinesOpts) -> LineCount<'_> {
        let counting = match self {
            ByteSource::Stored(version) => match version.lines {
                Some(lines) => Counting::Taken(lines),
                None => Counting::Downloading(
                    &version.bytes,
                    version.bytes.download_line_counts(opts.with_chars),
                ),
            },
            ByteSource::Path(_) | ByteSource::Memory(_) => Counting::Unread,
        };
        LineCount { opts, counting }
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

/// A line count [`ByteSource::start_counting_lines`] started.
pub(crate) struct LineCount<'a> {
    opts: CountLinesOpts,
    counting: Counting<'a>,
}

/// Where a [`LineCount`] gets the counts it finishes with.
enum Counting<'a> {
    /// A reader of the bytes, read when the count finishes.
    Unread,
    /// The counts taken as a stored version file arrived.
    Taken(LineCounts),
    /// A download of a stored version file, which the task runs.
    Downloading(
        &'a StoredBytes,
        AbortOnDropHandle<Result<LineCounts, OxenError>>,
    ),
}

impl LineCount<'_> {
    /// What [`util::fs::count_lines`] gives for `data`, a reader of the bytes, which is left unread
    /// for a stored version file. Blocks until a stored file's download ends.
    pub(crate) fn finish(self, data: impl Read) -> Result<(usize, Option<usize>), OxenError> {
        let lines = match self.counting {
            Counting::Unread => return util::fs::count_lines(data, self.opts),
            Counting::Taken(lines) => lines,
            Counting::Downloading(bytes, download) => bytes
                .runtime
                .block_on(download)?
                .map_err(|err| bytes.record_error(err))?,
        };
        let (line_count, chars) = lines.lines(self.opts.remove_trailing_blank_line);
        Ok((line_count, chars.filter(|_| self.opts.with_chars)))
    }
}

/// A version file in an S3 version store, with its line counts when they were taken as it arrived.
pub(crate) struct StoredVersion {
    bytes: Arc<StoredBytes>,
    pub(crate) url: String,
    pub(crate) cloud_options: CloudOptions,
    lines: Option<LineCounts>,
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
        lines: Option<LineCounts>,
    ) -> Result<Self, OxenError> {
        let cloud_options = s3_cloud_options(&url, region, endpoint_url)?;
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
            lines,
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
    /// earlier read of its start fetched more. Fetches only what no earlier read of its start did,
    /// and blocks while it does.
    pub(crate) fn head(&self, len: u64) -> io::Result<Bytes> {
        self.bytes.head(len)
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
        let hash = self.hash.to_string();
        let fetch = self.store.get_version_chunk(&hash, offset, len);
        let chunk = self
            .runtime
            .block_on(fetch)
            .map_err(|err| self.record_error(err))?;
        if chunk.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the version store returned no bytes before the file's end",
            ));
        }
        Ok(Bytes::from(chunk))
    }

    /// See [`StoredVersion::head`].
    fn head(&self, len: u64) -> io::Result<Bytes> {
        let head = self.head.lock().clone();
        let len = len.min(self.size);
        if head.len() as u64 >= len {
            return Ok(head);
        }
        let rest = self.fetch(head.len() as u64, len - head.len() as u64)?;
        let mut joined = BytesMut::with_capacity(head.len() + rest.len());
        joined.extend_from_slice(&head);
        joined.extend_from_slice(&rest);
        let joined = joined.freeze();
        *self.head.lock() = joined.clone();
        Ok(joined)
    }

    /// Starts counting the file's lines, and its characters when `with_chars` is set, from a
    /// download of it on the async runtime, which dropping the task stops.
    fn download_line_counts(
        self: &Arc<Self>,
        with_chars: bool,
    ) -> AbortOnDropHandle<Result<LineCounts, OxenError>> {
        let bytes = Arc::clone(self);
        AbortOnDropHandle::new(self.runtime.spawn(async move {
            let mut stream = bytes
                .store
                .get_version_stream(&bytes.hash.to_string())
                .await?;
            let mut lines = LineCounts::new(with_chars);
            while let Some(chunk) = stream.next().await {
                lines.add(&chunk?);
            }
            Ok(lines)
        }))
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
/// `file_path`, fetching what it reads from the store. Blocks while it reads them. Fails when the
/// store cannot give it the bytes, and gives no metadata for content that cannot be parsed.
pub(crate) fn inspect_stored(
    version: &StoredVersion,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    let inspected = inspect(&ByteSource::Stored(version), file_path);
    version.bytes.take_error().map_or(inspected, Err)
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
/// file's metadata, and line counts taken as a stored file arrived, come from `source` instead,
/// which holds the same bytes as `data`. Gives no metadata for content that cannot be parsed, and
/// fails when the store cannot give a stored file's bytes.
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
            text::get_metadata(data, source).map(GenericMetadata::MetadataText),
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
