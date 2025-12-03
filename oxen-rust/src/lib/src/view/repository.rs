use super::{DataTypeCount, StatusMessage};
use crate::model::{Commit, EntryDataType, MetadataEntry, RemoteRepository};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "namespace": "ox",
        "name": "ImageNet-1k",
        "is_empty": false,
    })
)]
pub struct RepositoryView {
    pub namespace: String,
    pub name: String,
    pub min_version: Option<String>,
    pub is_empty: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "namespace": "bessie",
        "name": "my-farm-data",
    })
)]
pub struct RepositoryListView {
    pub namespace: String,
    pub name: String,
    pub min_version: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "namespace": "ox",
        "name": "ImageNet-1k",
        "latest_commit": {
            "id": "a1b2c3d4e5f67890abcdef1234567890",
            "parent_ids": ["f1e2d3c4b5a67890fedcba9876543210"],
            "message": "Initial dataset import.",
            "author": "ox",
            "email": "ox@example.com",
            "timestamp": "2025-01-01T10:00:00Z"
        },
    })
)]
pub struct RepositoryCreationView {
    pub namespace: String,
    pub name: String,
    pub latest_commit: Option<Commit>,
    pub min_version: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "namespace": "bessie",
        "name": "my-corn-pics",
        "size": 1073741824,
        "data_types": [
            { "data_type": "image", "count": 1000 },
            { "data_type": "tabular", "count": 5 }
        ],
        "is_empty": false,
    })
)]
pub struct RepositoryDataTypesView {
    pub namespace: String,
    pub name: String,
    pub size: u64,
    pub data_types: Vec<DataTypeCount>,
    pub min_version: Option<String>,
    pub is_empty: bool,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "repository": {
            "namespace": "ox",
            "name": "ImageNet-1k",
            "is_empty": false,
        },
    })
)]
pub struct RepositoryResponse {
    pub status: String,
    pub status_message: String,
    pub repository: RepositoryView,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_created",
        "repository": {
            "namespace": "ox",
            "name": "ImageNet-1k",
            "latest_commit": {
                "id": "a1b2c3d4e5f678902e41",
                "parent_ids": ["f1e2d3c4b5a67890fedc"],
                "message": "Initial dataset import.",
                "author": "ox",
                "email": "ox@example.com",
                "timestamp": "2025-01-01T10:00:00Z"
            },
        },
        "metadata_entries": [
            {
                "path": "data/images/cow.jpg",
                "data_type": "image",
                "content_hash": "a1b2c3d4e5f678902e41",
                "file_size": 1024000,
            }
        ],
    })
)]
pub struct RepositoryCreationResponse {
    pub status: String,
    pub status_message: String,
    pub repository: RepositoryCreationView,
    pub metadata_entries: Option<Vec<MetadataEntry>>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "repository": {
            "namespace": "bessie",
            "name": "my-corn-pics",
            "size": 1074288000,
            "data_types": [
                { "data_type": "image", "count": 1000 },
                { "data_type": "tabular", "count": 5 }
            ],
            "is_empty": false,
        },
    })
)]
pub struct RepositoryDataTypesResponse {
    pub status: String,
    pub status_message: String,
    pub repository: RepositoryDataTypesView,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(example = json!({
    "status": "success",
    "status_message": "resource_found",
    "repositories": [
        { "name": "data-fields", "namespace": "ox" },
        { "name": "my-corn-pics", "namespace": "bessie" }
    ],
}))]
pub struct ListRepositoryResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_found"})
    )]
    pub status: StatusMessage,
    #[schema(example = json!([
        { "name": "data-fields", "namespace": "ox" },
        { "name": "my-corn-pics", "namespace": "bessie" }
    ]))]
    pub repositories: Vec<RepositoryListView>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "repository_api_url": "http://localhost:3000/api/repos/ox/ImageNet-1k",
    })
)]
pub struct RepositoryResolveResponse {
    pub status: String,
    pub status_message: String,
    pub repository_api_url: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(
    example = json!({
        "status": "success",
        "status_message": "resource_found",
        "repository": {
            "data_size": 1074288000,
            "data_types": [
                { "data_type": "image", "data_size": 524288000, "file_count": 500 },
                { "data_type": "tabular", "data_size": 550000000, "file_count": 5 },
            ],
        },
    })
)]
pub struct RepositoryStatsResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!({"status": "success", "status_message": "resource_found"})
    )]
    pub status: StatusMessage,
    pub repository: RepositoryStatsView,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "data_type": "image",
        "data_size": 524288000,
        "file_count": 500,
    })
)]
pub struct DataTypeView {
    pub data_type: EntryDataType,
    pub data_size: u64,
    pub file_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "data_size": 1073741824,
        "data_types": [
            { "data_type": "image", "data_size": 524288000, "file_count": 500 },
            { "data_type": "tabular", "data_size": 550000000, "file_count": 5 },
        ],
    })
)]
pub struct RepositoryStatsView {
    pub data_size: u64,
    pub data_types: Vec<DataTypeView>,
}

impl RepositoryView {
    pub fn from_remote(repository: RemoteRepository) -> RepositoryView {
        RepositoryView {
            namespace: repository.namespace.clone(),
            name: repository.name,
            min_version: repository.min_version,
            is_empty: repository.is_empty,
        }
    }
}

impl RepositoryDataTypesView {
    pub fn total_files(&self) -> usize {
        self.data_types.iter().map(|dt| dt.count).sum()
    }

    pub fn data_types_str(data_type_counts: &Vec<DataTypeCount>) -> String {
        let mut data_types_str = String::new();
        for data_type_count in data_type_counts {
            if data_type_count.count == 0 {
                continue;
            }
            if let Ok(edt) = EntryDataType::from_str(&data_type_count.data_type) {
                let emoji = edt.to_emoji();
                let data = format!(
                    "{} {} ({})\t",
                    emoji, data_type_count.data_type, data_type_count.count
                );
                data_types_str.push_str(&data);
            } else {
                let data = format!(
                    "{} ({})\t",
                    data_type_count.data_type, data_type_count.count
                );
                data_types_str.push_str(&data);
            }
        }
        data_types_str
    }
}
