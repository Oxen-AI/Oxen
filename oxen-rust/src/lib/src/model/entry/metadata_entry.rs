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
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WorkspaceMetadataEntry {
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
    pub changes: Option<WorkspaceChanges>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WorkspaceChanges {
    pub status: StagedEntryStatus,
}

impl MetadataEntry {
    pub async fn from_commit_entry(
        repo: &LocalRepository,
        entry: Option<CommitEntry>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        entry.as_ref()?;
        repositories::metadata::from_commit_entry(repo, &entry.unwrap(), commit)
            .await
            .ok()
    }

    pub fn from_file_node(
        repo: &LocalRepository,
        node: Option<FileNode>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        node.as_ref()?;
        repositories::metadata::from_file_node(repo, &node.unwrap(), commit).ok()
    }

    pub fn from_dir_node(
        repo: &LocalRepository,
        node: Option<DirNode>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        node.as_ref()?;
        repositories::metadata::from_dir_node(repo, &node.unwrap(), commit).ok()
    }
}

impl WorkspaceMetadataEntry {
    /// Converts a `MetadataEntry` into a `WorkspaceMetadataEntry` with `changes` set to `None`.
    pub fn from_metadata_entry(metadata: MetadataEntry) -> Self {
        WorkspaceMetadataEntry {
            filename: metadata.filename,
            hash: metadata.hash,
            is_dir: metadata.is_dir,
            latest_commit: metadata.latest_commit,
            // Convert ParsedResource to ParsedResourceView using the From trait.
            resource: metadata.resource.map(ParsedResourceView::from),
            size: metadata.size,
            data_type: metadata.data_type,
            mime_type: metadata.mime_type,
            extension: metadata.extension,
            metadata: metadata.metadata,
            is_queryable: metadata.is_queryable,
            changes: None,
        }
    }
}
