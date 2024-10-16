//! Helper functions to get metadata from the local filesystem.
//!

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::{Commit, CommitEntry, LocalRepository, MetadataEntry};
use crate::util;

use std::path::Path;

pub mod audio;
pub mod image;
pub mod tabular;
pub mod text;
pub mod video;

/// Returns the metadata given a file path
pub fn get(path: impl AsRef<Path>) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);
    let metadata = get_file_metadata(path, &data_type)?;

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
    })
}

/// Returns the metadata given a file path
pub fn from_path(path: impl AsRef<Path>) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);
    let metadata = get_file_metadata(path, &data_type)?;

    // TODO: how do we get the cached dir info if the entry is a dir?
    // TODO: Should we also be getting the real hash here? Seems like we'd have to calculate it again
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
    })
}

pub fn from_commit_entry(
    repo: &LocalRepository,
    entry: &CommitEntry,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    let path = util::fs::version_path(repo, entry);
    let base_name = entry
        .path
        .file_name()
        .ok_or(OxenError::file_has_no_name(&path))?;
    let size = get_file_size(&path)?;
    let mime_type = util::fs::file_mime_type(&path);
    let data_type = util::fs::datatype_from_mimetype(&path, mime_type.as_str());
    let extension = util::fs::file_extension(&path);
    let metadata = get_file_metadata(&path, &data_type)?;

    Ok(MetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        hash: entry.hash.to_string(),
        is_dir: path.is_dir(),
        latest_commit: Some(commit.to_owned()),
        resource: None,
        size,
        data_type,
        mime_type,
        extension,
        metadata,
        is_queryable: None,
    })
}

pub fn from_file_node(
    _repo: &LocalRepository,
    node: &FileNode,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    Ok(MetadataEntry {
        filename: node.name.clone(),
        hash: node.hash.to_string(),
        is_dir: false,
        latest_commit: Some(commit.to_owned()),
        resource: None,
        size: node.num_bytes,
        data_type: node.data_type.clone(),
        mime_type: node.mime_type.clone(),
        extension: node.extension.clone(),
        metadata: node.metadata.clone(),
        is_queryable: None,
    })
}

pub fn from_dir_node(
    _repo: &LocalRepository,
    node: &DirNode,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    Ok(MetadataEntry {
        filename: node.name.clone(),
        hash: node.hash.to_string(),
        is_dir: true,
        latest_commit: Some(commit.to_owned()),
        resource: None,
        size: node.num_bytes,
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: "".to_string(),
        metadata: None,
        is_queryable: None,
    })
}

/// Returns metadata with latest commit information. Less efficient than get().
pub fn get_cli(
    repo: &LocalRepository,
    entry_path: impl AsRef<Path>,
    data_path: impl AsRef<Path>,
) -> Result<CLIMetadataEntry, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::metadata::get_cli(repo, entry_path, data_path),
        MinOxenVersion::V0_19_0 => core::v0_19_0::metadata::get_cli(repo, entry_path, data_path),
    }
}

/// Returns the file size in bytes.
pub fn get_file_size(path: impl AsRef<Path>) -> Result<u64, OxenError> {
    let metadata = std::fs::metadata(path.as_ref())?;
    Ok(metadata.len())
}

/// Returns metadata based on data_type
pub fn get_file_metadata(
    path: impl AsRef<Path>,
    data_type: &EntryDataType,
) -> Result<Option<GenericMetadata>, OxenError> {
    match data_type {
        // dir should not be passed in here
        EntryDataType::Dir => Ok(Some(GenericMetadata::MetadataDir(MetadataDir::new(vec![])))),
        EntryDataType::Text => match text::get_metadata(path) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataText(metadata))),
            Err(err) => {
                log::warn!("could not compute text metadata: {}", err);
                Ok(None)
            }
        },
        EntryDataType::Image => match image::get_metadata(path) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataImage(metadata))),
            Err(err) => {
                log::warn!("could not compute image metadata: {}", err);
                Ok(None)
            }
        },
        EntryDataType::Video => match video::get_metadata(path) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataVideo(metadata))),
            Err(err) => {
                log::warn!("could not compute video metadata: {}", err);
                Ok(None)
            }
        },
        EntryDataType::Audio => match audio::get_metadata(path) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataAudio(metadata))),
            Err(err) => {
                log::warn!("could not compute audio metadata: {}", err);
                Ok(None)
            }
        },
        EntryDataType::Tabular => match tabular::get_metadata(path) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataTabular(metadata))),
            Err(err) => {
                log::warn!("could not compute tabular metadata: {}", err);
                Ok(None)
            }
        },
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use crate::model::EntryDataType;
    use crate::repositories;
    use crate::test;

    #[test]
    fn test_get_metadata_audio_flac() {
        let file = test::test_audio_file_with_name("121-121726-0005.flac");
        let metadata = repositories::metadata::get(file).unwrap();

        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");
    }
}
