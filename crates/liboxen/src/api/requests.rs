//! Defines payloads that the client sends to the server.
//!
pub mod rename_repo;
pub mod repo_new;
pub mod transfer_namespace;

pub use rename_repo::RenameRepoRequest;
pub use repo_new::RepoNew;
pub use transfer_namespace::TransferNamespaceRequest;
