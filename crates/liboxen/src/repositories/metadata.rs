//! Helper functions to get metadata from the local filesystem.
//!

use crate::core::df::tabular::DataFrameSource;
use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::parsed_resource::ParsedResourceView;
use crate::model::{Commit, LocalRepository, MetadataEntry};
use crate::repositories;
use crate::util;
use crate::util::fs::{data_type_from_head, file_extension, mime_type_from_head, read_head};

use bytes::Bytes;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

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

/// Reads the MIME type, data type, and metadata of the file at `data_path`, whose name is
/// `file_path`. Fails when the file cannot be opened or read, and gives no metadata for content
/// that cannot be parsed.
pub(crate) fn inspect_file(
    data_path: &Path,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    inspect(&DataFrameSource::Path(data_path), file_path)
}

/// Reads the MIME type, data type, and metadata of `data`, the bytes of a file named `file_path`.
/// Gives no metadata for content that cannot be parsed.
pub(crate) fn inspect_bytes(
    data: Bytes,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    inspect(&DataFrameSource::Memory(data), file_path)
}

fn inspect(
    source: &DataFrameSource,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    let mut reader = source.open()?;
    let head = read_head(&mut reader).map_err(|err| match source {
        DataFrameSource::Path(data_path) => OxenError::file_read_error(data_path, err),
        DataFrameSource::Memory(_) => err.into(),
    })?;
    let mime_type = mime_type_from_head(&head, file_path);
    let data_type = data_type_from_head(&head, file_path, &mime_type);
    let metadata = get_file_metadata(&mut reader, source, file_path, &data_type)?;
    Ok((mime_type, data_type, metadata))
}

/// Returns the metadata of `data`, the bytes of a file of `data_type` named `file_path`. A tabular
/// file's metadata is read from `data_frame` instead, which holds the same bytes as `data`.
fn get_file_metadata(
    data: &mut (impl Read + Seek),
    data_frame: &DataFrameSource,
    file_path: &Path,
    data_type: &EntryDataType,
) -> Result<Option<GenericMetadata>, OxenError> {
    let extension = file_extension(file_path);
    data.rewind()?;
    match data_type {
        EntryDataType::Text => match text::get_metadata(data) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataText(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute text metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Image => match image::get_metadata(data) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataImage(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute image metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Video => match video::get_metadata(data) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataVideo(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute video metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Audio => match audio::get_metadata(data, &extension) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataAudio(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute audio metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Tabular => {
            match tabular::get_metadata_with_extension(data_frame, &extension) {
                Ok(metadata) => Ok(Some(GenericMetadata::MetadataTabular(metadata))),
                Err(err) => {
                    tracing::error!(
                        oxen.file_path = %file_path.display(),
                        exception.message = %err,
                        "Could not compute tabular metadata"
                    );
                    Ok(None)
                }
            }
        }
        _ => Ok(None),
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
