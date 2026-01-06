use super::{Pagination, StatusMessage};
use crate::model::{Commit, CommitStats};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "commit": {
            "id": "a1b2c3d4e5f67890abcdef1234567890",
            "parent_ids": ["f1e2d3c4b5a67890fedcba9876543210"],
            "message": "Refactor data loading pipeline.",
            "author": "ox",
            "email": "ox@example.com",
            "timestamp": "2025-01-01T10:00:00Z"
        },
    })
)]
pub struct CommitResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_found"})
    )]
    pub status: StatusMessage,
    pub commit: Commit,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "commit": {
            "id": "a1b2c3d4e5f67890abcdef1234567890",
            "parent_ids": ["f1e2d3c4b5a67890fedcba9876543210"],
            "message": "Initial upload.",
            "author": "ox",
            "email": "ox@example.com",
            "timestamp": "2025-01-01T10:00:00Z"
        },
    })
)]
pub struct UploadCommitResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_created"})
    )]
    pub status: StatusMessage,
    pub commit: Option<Commit>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "commit": {
            "id": "a1b2c3d4e5f67890abcdef1234567890",
            "parent_ids": [],
            "message": "Initial empty repository commit.",
            "author": "ox",
            "email": "ox@example.com",
            "timestamp": "2025-01-01T10:00:00Z"
        },
    })
)]
pub struct RootCommitResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_found"})
    )]
    pub status: StatusMessage,
    pub commit: Option<Commit>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "stats": {
            "commit": {
                "id": "a1b2c3d4e5f67890abcdef1234567890",
                "parent_ids": ["f1e2d3c4b5a67890fedcba9876543210"],
                "message": "Refactor data loading pipeline.",
                "author": "ox",
                "email": "ox@example.com",
                "timestamp": "2025-01-01T10:00:00Z"
            },
            "num_entries": 12000,
            "num_synced_files": 11950,
        },
    })
)]
pub struct CommitStatsResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_found"})
    )]
    pub status: StatusMessage,
    pub stats: CommitStats,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "commits": [
            {
                "id": "a1b2c3d4e5f67890abcdef1234567890",
                "parent_ids": ["f1e2d3c4b5a67890fedcba9876543210"],
                "message": "Refactor data loading pipeline.",
                "author": "ox",
                "email": "ox@example.com",
                "timestamp": "2025-01-01T10:00:00Z"
            },
            {
                "id": "b0a9f8e7d6c543210fedcba987654321",
                "parent_ids": ["a1b2c3d4e5f67890abcdef1234567890"],
                "message": "Update ImageNet-1k labels.",
                "author": "bessie",
                "email": "bessie@example.com",
                "timestamp": "2025-01-02T11:00:00Z"
            }
        ],
    })
)]
pub struct ListCommitResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_found"})
    )]
    pub status: StatusMessage,
    pub commits: Vec<Commit>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "commit_validation_success",
        "can_merge": true,
    })
)]
pub struct CommitTreeValidationResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "commit_validation_success"})
    )]
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
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "commits": [
            {
                "id": "a1b2c3d4e5f67890abcdef1234567890",
                "parent_ids": ["f1e2d3c4b5a67890fedcba9876543210"],
                "message": "Refactor data loading pipeline.",
                "author": "ox",
                "email": "ox@example.com",
                "timestamp": "2025-01-01T10:00:00Z"
            }
        ],
        "page_number": 1,
        "page_size": 10,
        "total_pages": 5,
        "total_entries": 45,
        "count_cached": true
    })
)]
pub struct PaginatedCommits {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_found"})
    )]
    pub status: StatusMessage,
    pub commits: Vec<Commit>,
    #[serde(flatten)]
    #[schema(
        value_type = Pagination,
        example = json!({
            "page_number": 1,
            "page_size": 10,
            "total_pages": 5,
            "total_entries": 45,
            "count_cached": true
        })
    )]
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
