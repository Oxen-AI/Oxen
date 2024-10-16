use std::path::PathBuf;

use crate::model::{metadata::MetadataDir, Branch, CommitEntry, MetadataEntry, RemoteEntry};
use serde::{Deserialize, Serialize};

use super::{Pagination, StatusMessage};

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

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PaginatedDirEntries {
    pub dir: Option<MetadataEntry>,
    pub entries: Vec<MetadataEntry>,
    pub resource: Option<ResourceVersion>,
    pub metadata: Option<MetadataDir>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
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
