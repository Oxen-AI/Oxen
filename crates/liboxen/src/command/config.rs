//! # oxen config
//!
//! Configuration commands for Oxen
//!

use crate::api::client::repositories::get_by_remote;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote};

/// Refuse a URL this repository cannot take a remote from.
fn reject_unusable_remote(repo: &LocalRepository, url: &str) -> Result<(), OxenError> {
    if url::Url::parse(url).is_err() {
        return Err(OxenError::invalid_set_remote_url(url));
    }

    if repo.is_remote_mode() {
        return Err(OxenError::basic_str(
            "Error: Cannot change remote of remote-mode repos",
        ));
    }

    Ok(())
}

/// Attach a remote from a name and URL alone, available only in test / `test-utils` builds.
///
/// Production code uses [`set_remote_by_url`], which records the UUID the repository is addressed
/// by. Tests use this for fixture setup against a remote they created inline.
#[cfg(any(test, feature = "test-utils"))]
pub fn set_remote(repo: &mut LocalRepository, name: &str, url: &str) -> Result<Remote, OxenError> {
    reject_unusable_remote(repo, url)?;

    let remote = repo.set_remote(name, url);
    repo.save()?;
    Ok(remote)
}

/// # Set the remote for a repository from its URL
///
/// Reads the repository at `url` and records the UUID it reports, leaving the remote addressable
/// by UUID rather than only by name. Records nothing when the repository cannot be read. A
/// repository reporting no identity is recorded with its name and URL alone. Errors when a remote
/// already recorded under `name` carries a different UUID than the repository at `url` reports.
pub async fn set_remote_by_url(
    repo: &mut LocalRepository,
    name: &str,
    url: &str,
) -> Result<Remote, OxenError> {
    reject_unusable_remote(repo, url)?;

    // Carry the UUID recorded for `name` so resolution refuses a URL pointing at a different repo.
    let requested = Remote {
        name: name.to_string(),
        url: url.to_string(),
        repo_uuid: repo
            .get_remote(name)
            .and_then(|recorded| recorded.repo_uuid),
    };
    let remote_repo = get_by_remote(&requested).await?;
    let remote = repo.set_remote_repo(name, &remote_repo);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use uuid::Uuid;

    /// Attaching from a URL asks the server who the repository is, so the UUID lands in the config
    /// even though the caller only had a URL to go on.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_set_remote_by_url_records_the_servers_uuid() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut repo, remote_repo| async move {
            let expected = remote_repo
                .remote
                .repo_uuid
                .expect("the server reports a UUID for a repo it created");

            let remote = set_remote_by_url(&mut repo, "origin", &remote_repo.remote.url).await?;

            assert_eq!(remote.repo_uuid, Some(expected));
            Ok(remote_repo)
        })
        .await
    }

    /// Re-attaching a remote whose URL has come to point at a different repository is refused,
    /// since adopting either UUID would leave the remote addressing storage the caller did not
    /// attach to.
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_set_remote_by_url_refuses_a_different_repo() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut repo, remote_repo| async move {
            let mut attached = remote_repo.clone();
            attached.remote.repo_uuid = Some(Uuid::new_v4());
            test::attach_remote_repo(&mut repo, &attached)?;

            let result = set_remote_by_url(&mut repo, "origin", &remote_repo.remote.url).await;

            assert!(
                matches!(result, Err(OxenError::RemotePointsAtDifferentRepo { .. })),
                "expected a refusal, got: {result:?}"
            );
            Ok(remote_repo)
        })
        .await
    }

    /// Attaching a remote is how a repository learns the UUID it is addressed by, so a server that
    /// cannot be read leaves nothing attached rather than a remote reachable only by name.
    #[tokio::test]
    async fn test_set_remote_by_url_errors_when_the_server_cannot_be_read() -> Result<(), OxenError>
    {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // Refused immediately rather than blackholed, so this does not wait out a connect
            // timeout.
            let url = "http://localhost:1/ox/cats";
            let err = set_remote_by_url(&mut repo, "origin", url)
                .await
                .expect_err("nothing accepts connections on port 1");
            assert!(
                matches!(err, OxenError::HTTP(_)),
                "expected a failed read rather than local URL validation: {err}"
            );
            assert!(
                repo.remotes().is_empty(),
                "a failed attach records nothing: {:?}",
                repo.remotes()
            );
            Ok(())
        })
        .await
    }
}
