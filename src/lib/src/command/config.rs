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

    let remote = repo.set_remote(name, url);
    repo.save()?;
    Ok(remote)
}

/// # Remove the remote for a repository
/// If you added a remote you no longer want, can remove it by supplying the name
pub fn delete_remote(repo: &mut LocalRepository, name: &str) -> Result<(), OxenError> {
    repo.delete_remote(name);
    repo.save()?;
    Ok(())
}
