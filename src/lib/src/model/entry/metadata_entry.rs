use serde::{Deserialize, Serialize};

use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::parsed_resource::ParsedResourceView;
use crate::model::{
    Commit, CommitEntry, EntryDataType, LocalRepository, ParsedResource, StagedEntryStatus,
};
use crate::repositories;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CLIMetadataEntry {
    pub filename: String,
    pub last_updated: Option<Commit>,
    // Hash of the file
    pub hash: String,
    // size of the file in bytes
    pub size: u64,
    // high level type of "image", "text", "video", "audio", "tabular"
    pub data_type: EntryDataType,
    // auto detected mime type of the file (e.g. "image/png")
    pub mime_type: String,
    // auto detected extension of the file
    pub extension: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataEntry {
    pub filename: String,
    pub hash: String,
    pub is_dir: bool,
    pub latest_commit: Option<Commit>,
    pub resource: Option<ParsedResource>,
    // size of the file in bytes
    pub size: u64,
    // high level type of "image", "text", "video", "audio", "tabular"
    pub data_type: EntryDataType,
    // auto detected mime type of the file (e.g. "image/png")
    pub mime_type: String,
    // auto detected extension of the file
    pub extension: String,
    // metadata per data type
    pub metadata: Option<GenericMetadata>,
    // If it's a tabular file, is it indexed for querying?
    pub is_queryable: Option<bool>,
    // Workspace changes if the entry is part of a workspace
    pub status: Option<StagedEntryStatus>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataEntryView {
    pub filename: String,
    pub hash: String,
    pub is_dir: bool,
    pub latest_commit: Option<Commit>,
    pub resource: Option<ParsedResourceView>,
    // size of the file in bytes
    pub size: u64,
    // high level type of "image", "text", "video", "audio", "tabular"
    pub data_type: EntryDataType,
    // auto detected mime type of the file (e.g. "image/png")
    pub mime_type: String,
    // auto detected extension of the file
    pub extension: String,
    // metadata per data type
    pub metadata: Option<GenericMetadata>,
    // If it's a tabular file, is it indexed for querying?
    pub is_queryable: Option<bool>,
    // Workspace changes if the entry is part of a workspace
    pub status: Option<StagedEntryStatus>,
}

impl MetadataEntry {
    pub fn from_commit_entry(
        repo: &LocalRepository,
        entry: Option<CommitEntry>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        entry.as_ref()?;
        match repositories::metadata::from_commit_entry(repo, &entry.unwrap(), commit) {
            Ok(metadata) => Some(metadata),
            Err(_) => None,
        }
    }

    pub fn from_file_node(
        repo: &LocalRepository,
        node: Option<FileNode>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        node.as_ref()?;
        match repositories::metadata::from_file_node(repo, &node.unwrap(), commit) {
            Ok(metadata) => Some(metadata),
            Err(_) => None,
        }
    }

    pub fn from_dir_node(
        repo: &LocalRepository,
        node: Option<DirNode>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        node.as_ref()?;
        match repositories::metadata::from_dir_node(repo, &node.unwrap(), commit) {
            Ok(metadata) => Some(metadata),
            Err(_) => None,
        }
    }
}

impl From<MetadataEntry> for MetadataEntryView {
    fn from(metadata_entry: MetadataEntry) -> Self {
        MetadataEntryView {
            filename: metadata_entry.filename,
            hash: metadata_entry.hash,
            is_dir: metadata_entry.is_dir,
            latest_commit: metadata_entry.latest_commit,
            resource: metadata_entry.resource.map(|r| r.into()),
            size: metadata_entry.size,
            data_type: metadata_entry.data_type,
            mime_type: metadata_entry.mime_type,
            extension: metadata_entry.extension,
            metadata: metadata_entry.metadata,
            is_queryable: metadata_entry.is_queryable,
            status: metadata_entry.status,
        }
    }
}
