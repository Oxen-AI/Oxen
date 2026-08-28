//! Defines payloads that the client sends to the server.
//!
pub mod repo_new;
pub mod transfer_namespace;

pub use repo_new::RepoNew;
pub use transfer_namespace::TransferNamespaceRequest;
