use crate::model::{Commit, CommitStats};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use super::{Pagination, StatusMessage};

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct CommitResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commit: Commit,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct UploadCommitResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commit: Option<Commit>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct RootCommitResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commit: Option<Commit>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct CommitStatsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub stats: CommitStats,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct ListCommitResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commits: Vec<Commit>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct CommitTreeValidationResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub can_merge: bool,
}

impl ListCommitResponse {
    pub fn success(commits: Vec<Commit>) -> ListCommitResponse {
        ListCommitResponse {
            status: StatusMessage::resource_found(),
            commits,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct PaginatedCommits {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub commits: Vec<Commit>,
    #[serde(flatten)]
    pub pagination: Pagination,
}

impl PaginatedCommits {
    pub fn success(commits: Vec<Commit>, pagination: Pagination) -> PaginatedCommits {
        PaginatedCommits {
            status: StatusMessage::resource_found(),
            commits,
            pagination,
        }
    }
}
