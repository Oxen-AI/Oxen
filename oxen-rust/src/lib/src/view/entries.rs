use std::path::PathBuf;

use crate::model::{
    entry::metadata_entry::{MetadataEntry, WorkspaceMetadataEntry},
    metadata::MetadataDir,
    parsed_resource::ParsedResourceView,
    Branch, Commit, CommitEntry, EntryDataType, MerkleHash, ParsedResource, RemoteEntry,
};
use serde::{Deserialize, Serialize};

use super::{Pagination, StatusMessage};

#[derive(Deserialize, Serialize, Debug)]
pub struct ListMissingFilesRequest {
    pub file_hashes: Option<std::collections::HashSet<MerkleHash>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListCommitEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entries: Vec<CommitEntry>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: CommitEntry,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteEntryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: RemoteEntry,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ResourceVersion {
    pub path: String,
    pub version: String,
}

impl ResourceVersion {
    pub fn from_parsed_resource(resource: &crate::model::ParsedResource) -> ResourceVersion {
        ResourceVersion {
            path: resource.path.to_string_lossy().to_string(),
            version: resource.version.to_string_lossy().to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedEntries {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entries: Vec<RemoteEntry>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedMetadataEntries {
    pub entries: Vec<MetadataEntry>,
    #[serde(flatten)]
    pub pagination: Pagination,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedMetadataEntriesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub entries: PaginatedMetadataEntries,
}

// ignoring because the size difference isn't that big
// look into updating at some point as a small optimization
#[allow(clippy::large_enum_variant)]
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum EMetadataEntry {
    MetadataEntry(MetadataEntry),
    WorkspaceMetadataEntry(WorkspaceMetadataEntry),
}

impl EMetadataEntry {
    /// Returns the filename from the inner entry.
    pub fn filename(&self) -> &str {
        match self {
            EMetadataEntry::MetadataEntry(entry) => &entry.filename,
            EMetadataEntry::WorkspaceMetadataEntry(entry) => &entry.filename,
        }
    }

    /// Returns whether the entry is a directory or not.
    pub fn is_dir(&self) -> bool {
        match self {
            EMetadataEntry::MetadataEntry(entry) => entry.is_dir,
            EMetadataEntry::WorkspaceMetadataEntry(entry) => entry.is_dir,
        }
    }

    /// Returns the entry's data type.
    pub fn data_type(&self) -> EntryDataType {
        match self {
            EMetadataEntry::MetadataEntry(entry) => entry.data_type.clone(),
            EMetadataEntry::WorkspaceMetadataEntry(entry) => entry.data_type.clone(),
        }
    }

    /// Returns the entry's MIME type.
    pub fn mime_type(&self) -> &str {
        match self {
            EMetadataEntry::MetadataEntry(entry) => &entry.mime_type,
            EMetadataEntry::WorkspaceMetadataEntry(entry) => &entry.mime_type,
        }
    }

    /// Returns an optional reference to the parsed resource.
    pub fn resource(&self) -> Option<ParsedResourceView> {
        match self {
            EMetadataEntry::MetadataEntry(entry) => {
                entry.resource.clone().map(ParsedResourceView::from)
            }
            EMetadataEntry::WorkspaceMetadataEntry(entry) => entry.resource.clone(),
        }
    }

    pub fn set_resource(&mut self, resource: Option<ParsedResource>) {
        match self {
            EMetadataEntry::MetadataEntry(entry) => entry.resource = resource,
            EMetadataEntry::WorkspaceMetadataEntry(entry) => {
                entry.resource = resource.map(ParsedResourceView::from)
            }
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            EMetadataEntry::MetadataEntry(entry) => entry.size,
            EMetadataEntry::WorkspaceMetadataEntry(entry) => entry.size,
        }
    }

    pub fn latest_commit(&self) -> Option<Commit> {
        match self {
            EMetadataEntry::MetadataEntry(entry) => entry.latest_commit.clone(),
            EMetadataEntry::WorkspaceMetadataEntry(entry) => entry.latest_commit.clone(),
        }
    }

    pub fn hash(&self) -> String {
        match self {
            EMetadataEntry::MetadataEntry(entry) => entry.hash.clone(),
            EMetadataEntry::WorkspaceMetadataEntry(entry) => entry.hash.clone(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PaginatedDirEntries {
    pub dir: Option<EMetadataEntry>,
    pub entries: Vec<EMetadataEntry>,
    pub resource: Option<ResourceVersion>,
    pub metadata: Option<MetadataDir>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

impl PaginatedDirEntries {
    pub fn empty() -> PaginatedDirEntries {
        PaginatedDirEntries {
            dir: None,
            entries: vec![],
            resource: None,
            metadata: None,
            page_size: 0,
            page_number: 0,
            total_pages: 0,
            total_entries: 0,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedDirEntriesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub entries: PaginatedDirEntries,
}

impl PaginatedDirEntriesResponse {
    pub fn ok_from(paginated: PaginatedDirEntries) -> Self {
        Self {
            status: StatusMessage::resource_found(),
            entries: paginated,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BranchEntryVersion {
    pub branch: Branch,
    pub resource: ResourceVersion,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitEntryVersion {
    pub commit: crate::model::Commit,
    pub resource: ResourceVersion,
    pub schema_hash: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedEntryVersions {
    pub versions: Vec<CommitEntryVersion>,
    #[serde(flatten)]
    pub pagination: Pagination,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PaginatedEntryVersionsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub versions: PaginatedEntryVersions,
    pub branch: Branch,
    pub path: PathBuf,
}
