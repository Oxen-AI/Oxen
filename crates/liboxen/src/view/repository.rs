use super::{DataTypeCount, StatusMessage};
use crate::model::parsed_resource::ParsedResourceView;
use crate::model::{Commit, EntryDataType};
use crate::storage::StorageKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "namespace": "ox",
        "name": "ImageNet-1k",
        "is_empty": false,
        "storage_kind": "s3",
    })
)]
pub struct RepositoryView {
    pub namespace: String,
    pub name: String,
    pub min_version: Option<String>,
    pub is_empty: bool,
    pub storage_kind: StorageKind,
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
        "storage_kind": "s3",
    })
)]
pub struct RepositoryCreationView {
    pub namespace: String,
    pub name: String,
    pub latest_commit: Option<Commit>,
    pub min_version: Option<String>,
    pub storage_kind: StorageKind,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[schema(
    example = json!({
        "namespace": "bessie",
        "name": "my-corn-pics",
        "is_empty": false,
        "storage_kind": "s3",
        "size": 1073741824,
        "data_types": [
            { "data_type": "image", "count": 1000 },
            { "data_type": "tabular", "count": 5 }
        ],
        "branch_count": 3,
    })
)]
pub struct RepositoryDataTypesView {
    #[serde(flatten)]
    pub repository: RepositoryView,
    pub size: u64,
    pub data_types: Vec<DataTypeCount>,
    #[serde(default)]
    pub branch_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_resource: Option<ParsedResourceView>,
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
    })
)]
pub struct RepositoryCreationResponse {
    pub status: String,
    pub status_message: String,
    pub repository: RepositoryCreationView,
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
        "data_size": 1074288000,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_types_view_serializes_flat_with_storage_kind() {
        let view = RepositoryDataTypesView {
            repository: RepositoryView {
                namespace: "ox".to_string(),
                name: "test".to_string(),
                min_version: Some("0.0.0".to_string()),
                is_empty: false,
                storage_kind: StorageKind::S3,
            },
            size: 42,
            data_types: vec![],
            branch_count: 1,
            default_resource: None,
        };
        let json = serde_json::to_value(&view).expect("view should serialize");
        // Flattened: the identity fields (including storage_kind) sit at the top level, not
        // nested under a "repository" key.
        assert_eq!(json["namespace"], "ox");
        assert_eq!(json["storage_kind"], "s3");
        assert_eq!(json["size"], 42);
        assert!(json.get("repository").is_none());
    }

    #[test]
    fn repository_view_reads_storage_kind_from_flattened_payload() {
        // The client parses the (flattened) show-endpoint payload as a RepositoryView.
        let payload = serde_json::json!({
            "namespace": "ox",
            "name": "test",
            "min_version": "0.0.0",
            "is_empty": false,
            "storage_kind": "local",
            "size": 42,
            "data_types": [],
            "branch_count": 1,
        });
        let view: RepositoryView =
            serde_json::from_value(payload).expect("payload should deserialize");
        assert_eq!(view.storage_kind, StorageKind::Local);
    }

    #[test]
    fn repository_view_requires_storage_kind() {
        // A response missing storage_kind must fail to deserialize rather than defaulting.
        let payload = serde_json::json!({
            "namespace": "ox",
            "name": "test",
            "is_empty": false,
        });
        let result = serde_json::from_value::<RepositoryView>(payload);
        assert!(result.is_err());
    }

    #[test]
    fn creation_view_serializes_and_requires_storage_kind() {
        let view = RepositoryCreationView {
            namespace: "ox".to_string(),
            name: "test".to_string(),
            latest_commit: None,
            min_version: Some("0.0.0".to_string()),
            storage_kind: StorageKind::S3,
        };
        let json = serde_json::to_value(&view).expect("view should serialize");
        assert_eq!(json["storage_kind"], "s3");

        // A create response missing storage_kind must fail rather than defaulting.
        let missing = serde_json::json!({ "namespace": "ox", "name": "test" });
        assert!(serde_json::from_value::<RepositoryCreationView>(missing).is_err());
    }

    #[test]
    fn data_types_view_round_trips_through_flatten() {
        // Guards against the serde `flatten` + integer-field deserialization footgun: a struct
        // that both flattens another struct and carries numeric fields (`size`, `branch_count`).
        let view = RepositoryDataTypesView {
            repository: RepositoryView {
                namespace: "ox".to_string(),
                name: "test".to_string(),
                min_version: Some("0.0.0".to_string()),
                is_empty: false,
                storage_kind: StorageKind::S3,
            },
            size: 1_073_741_824,
            data_types: vec![],
            branch_count: 3,
            default_resource: None,
        };
        let json = serde_json::to_string(&view).expect("view should serialize");
        let back: RepositoryDataTypesView =
            serde_json::from_str(&json).expect("view should round-trip through the flatten");
        assert_eq!(back.repository.storage_kind, StorageKind::S3);
        assert_eq!(back.repository.namespace, "ox");
        assert_eq!(back.size, 1_073_741_824);
        assert_eq!(back.branch_count, 3);
    }
}
