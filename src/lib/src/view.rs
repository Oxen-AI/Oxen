//! Views are the data structures that are returned by the API endpoints.
//!

pub mod branch;
pub mod commit;
pub mod compare;
pub mod data_frames;
pub mod data_type_count;
pub mod diff;
pub mod entries;
pub mod entry_metadata;
pub mod file_metadata;
pub mod fork;
pub mod health;
pub mod http;
pub mod json_data_frame;
pub mod json_data_frame_view;
pub mod merge;
pub mod message;
pub mod mime_type_count;
pub mod namespace;
pub mod notebook;
pub mod oxen_response;
pub mod oxen_version;
pub mod pagination;
pub mod remote_staged_status;
pub mod repository;
pub mod revision;
pub mod schema;
pub mod sql_parse_error;
pub mod status_message;
pub mod tabular_diff_view;
pub mod tree;
pub mod versions;
pub mod workspaces;

pub use crate::view::compare::CompareEntriesResponse;
pub use crate::view::data_type_count::DataTypeCount;
pub use crate::view::file_metadata::{
    ErrorFileInfo, FileMetadata, FileMetadataResponse, FilePathsResponse, FilesHashResponse,
};
pub use crate::view::mime_type_count::MimeTypeCount;

pub use crate::view::status_message::{StatusMessage, StatusMessageDescription};

pub use crate::view::json_data_frame::JsonDataFrame;
pub use crate::view::json_data_frame_view::{
    JsonDataFrameView, JsonDataFrameViewResponse, JsonDataFrameViews,
};
pub use crate::view::namespace::{ListNamespacesResponse, NamespaceResponse, NamespaceView};
pub use crate::view::schema::ListSchemaResponse;

pub use crate::view::repository::{
    ListRepositoryResponse, RepositoryResolveResponse, RepositoryResponse, RepositoryView,
};

pub use crate::view::entries::{
    CommitEntryVersion, EntryResponse, PaginatedDirEntries, PaginatedDirEntriesResponse,
    PaginatedEntries, PaginatedEntryVersions, PaginatedEntryVersionsResponse, RemoteEntryResponse,
};

pub use crate::view::commit::{
    CommitResponse, CommitStatsResponse, ListCommitResponse, PaginatedCommits, RootCommitResponse,
};

pub use crate::view::branch::{
    BranchLockResponse, BranchNew, BranchNewFromBranchName, BranchNewFromCommitId,
    BranchRemoteMerge, BranchResponse, BranchUpdate, ListBranchesResponse,
};

pub use crate::view::revision::ParseResourceResponse;

pub use crate::view::compare::CompareResult;

pub use crate::view::entry_metadata::MetadataEntryResponse;

pub use crate::view::pagination::Pagination;

pub use crate::view::health::HealthResponse;
pub use crate::view::oxen_response::OxenResponse;

pub use crate::view::remote_staged_status::{
    ListStagedFileModResponseDF, ListStagedFileModResponseRaw, RemoteStagedStatus,
    RemoteStagedStatusResponse, StagedFileModResponse,
};

pub use crate::view::sql_parse_error::SQLParseError;

pub use crate::view::tabular_diff_view::TabularDiffView;
pub use crate::view::workspaces::WorkspaceResponseView;

pub use crate::view::tree::merkle_hashes::MerkleHashesResponse;
