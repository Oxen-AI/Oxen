use crate::{error::OxenError, model::RemoteRepository, opts::helpers};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct ListOpts {
    pub paths: Vec<PathBuf>,
    pub host: String,
    pub remote: String,
    pub revision: String,
    pub page_num: usize,
    pub page_size: usize,
}

impl ListOpts {
    /// Looks at branch or commit id and resolves to commit id. Falls back to main branch.
    pub async fn remote_commit_id(&self, repo: &RemoteRepository) -> Result<String, OxenError> {
        helpers::remote_commit_id(repo, Some(self.revision.to_owned())).await
    }
}
