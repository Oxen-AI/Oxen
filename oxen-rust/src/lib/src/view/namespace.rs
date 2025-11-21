use crate::model::Namespace;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct NamespaceView {
    pub namespace: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct ListNamespacesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub namespaces: Vec<NamespaceView>,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct NamespaceResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub namespace: Namespace,
}
