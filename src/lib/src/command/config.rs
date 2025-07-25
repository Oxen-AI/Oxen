//! # oxen config
//!
//! Configuration commands for Oxen
//!

use crate::error::OxenError;
use crate::model::{LocalRepository, Remote};

/// # Set the remote for a repository
/// Tells the CLI where to push the changes to
pub fn set_remote(repo: &mut LocalRepository, name: &str, url: &str) -> Result<Remote, OxenError> {
    if url::Url::parse(url).is_err() {
        return Err(OxenError::invalid_set_remote_url(url));
    }

    if repo.is_remote_mode() {
        return Err(OxenError::basic_str(
            "Error: Cannot change remote of remote-mode repos",
        ));
    }

    let remote = repo.set_remote(name, url);
    repo.save()?;
    Ok(remote)
}

/// # Remove the remote for a repository
/// If you added a remote you no longer want, can remove it by supplying the name
pub fn delete_remote(repo: &mut LocalRepository, name: &str) -> Result<(), OxenError> {
    if repo.is_remote_mode() {
        return Err(OxenError::basic_str(
            "Error: Cannot delete from remote of remote-mode repos",
        ));
    }

    repo.delete_remote(name);
    repo.save()?;
    Ok(())
}

/// # Set the workspace for a remote-mode repository
/// Tells the CLI which workspace to upload the changes to
pub fn set_workspace(repo: &mut LocalRepository, name: &str) -> Result<String, OxenError> {
    repo.set_workspace(name)?;
    repo.save()?;

    Ok(name.to_string())
}

/// # Remove a workspace for a repository
/// If you added a workspace you no longer want, can remove it by supplying the name
pub fn delete_workspace(repo: &mut LocalRepository, name: &str) -> Result<(), OxenError> {
    repo.delete_workspace(name)?;
    repo.save()?;
    Ok(())
}
