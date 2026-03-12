use crate::api;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::error::OxenError;
use crate::model::RemoteRepository;

/// Looks at branch or commit id and resolves to commit id. Falls back to main branch.
pub async fn remote_commit_id(
    repo: &RemoteRepository,
    revision: Option<String>,
) -> Result<String, OxenError> {
    let Some(revision) = revision else {
        // Fallback to main branch
        let main_branch = api::client::branches::get_by_name(repo, DEFAULT_BRANCH_NAME).await?;
        if main_branch.is_none() {
            return Err(OxenError::basic_str("No main branch found on remote."));
        }
        return Ok(main_branch.unwrap().commit_id);
    };

    // First see if the commit exists
    if api::client::commits::get_by_id(repo, &revision)
        .await?
        .is_some()
    {
        return Ok(revision.to_string());
    }

    // If not, see if the branch exists
    if let Some(branch) = api::client::branches::get_by_name(repo, &revision).await? {
        return Ok(branch.commit_id);
    }

    // Should not get here
    Err(OxenError::basic_str("No remote commit specified."))
}
