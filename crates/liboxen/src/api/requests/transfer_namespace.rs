//! Request payload for transferring a repository to another namespace.
//!
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The namespace a repository is being transferred into.
#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct TransferNamespaceRequest {
    /// The destination namespace, as addressed on disk. A UUID where a control plane owns
    /// namespaces, and the namespace's name where the server owns its own.
    pub namespace: String,
    /// What the destination namespace is called, where `namespace` addresses it by UUID instead.
    /// Recorded as the repository's namespace hint, and ignored by a server that owns its own
    /// namespaces.
    #[serde(default)]
    pub namespace_name: Option<String>,
}
