use crate::model::Namespace;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct NamespaceView {
    pub namespace: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(
    example = json!({
    "status": "success", 
    "status_message": "resource_found", 
    "namespaces": [
        { "name": "my_namespace" },
        { "name": "oxen" }
    ],
}))]
pub struct ListNamespacesResponse {
    #[serde(flatten)]
    #[schema(
        value_type = StatusMessage,
        example = json!(
            { 
                "status": "success",
                "status_message": "resource_found" 
            }
        )
    )]
    pub status: StatusMessage,
    pub namespaces: Vec<NamespaceView>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct NamespaceResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub namespace: Namespace,
}
