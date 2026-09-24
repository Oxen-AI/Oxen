//! Request payload for renaming a repository.
//!
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The name a repository is being renamed to.
#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct RenameRepoRequest {
    /// What the repository is called from now on, recorded as its name hint. Refused when it is not
    /// a valid repository name.
    pub repo_name: String,
}
