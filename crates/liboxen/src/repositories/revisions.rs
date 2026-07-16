//! Revisions can either be commits by id or head commits on branches by name

use std::path::Path;

use crate::api;
use crate::api::client::file::GetFileOpts;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use crate::storage::BoxedByteStream;

/// Get a commit object from a commit id or branch name
/// Returns Ok(None) if the revision does not exist
pub fn get(repo: &LocalRepository, revision: impl AsRef<str>) -> Result<Option<Commit>, OxenError> {
    let revision = revision.as_ref();
    if revision == "HEAD" {
        let commit = repositories::commits::head_commit(repo)?;
        return Ok(Some(commit));
    }

    let commit_id = repositories::branches::get_commit_id(repo, revision)?
        .unwrap_or_else(|| revision.to_string());
    let commit = repositories::commits::get_by_id(repo, &commit_id)?;
    Ok(commit)
}

/// Get a commit object from a commit id or branch name, off the async worker.
/// Returns Ok(None) if the revision does not exist
pub async fn get_async(
    repo: &LocalRepository,
    revision: &str,
) -> Result<Option<Commit>, OxenError> {
    let repo = repo.clone();
    let revision = revision.to_string();
    tokio::task::spawn_blocking(move || get(&repo, &revision)).await?
}

/// Streams the raw bytes of `path` as it existed at `revision`.
///
/// `revision` may be a branch name, a commit id, or `HEAD`. May make a network call to the
/// configured `origin` remote when the file's bytes aren't available locally, so a remote must be
/// set and reachable in that case. Errors if `revision` does not resolve to a commit, or if `path`
/// is not a file in that commit.
pub async fn get_version_stream_from_revision(
    repo: &LocalRepository,
    revision: &str,
    path: impl AsRef<Path>,
) -> Result<BoxedByteStream, OxenError> {
    let path = path.as_ref();

    let commit =
        get(repo, revision)?.ok_or_else(|| OxenError::RevisionNotFound(revision.into()))?;
    let file_node = repositories::tree::get_file_by_path(repo, &commit, path)?
        .ok_or_else(|| OxenError::entry_does_not_exist_in_commit(path, &commit.id))?;
    let hash = file_node.hash().to_string();

    let version_store = repo.version_store();
    if version_store.version_exists(&hash).await? {
        return version_store.get_version_stream(&hash).await;
    }

    // The file is known in the merkle tree but its bytes aren't stored locally;
    // stream them from the remote by the resolved commit id and path.
    let remote_repo = api::client::repositories::get_default_remote(repo).await?;
    api::client::file::get_file(&remote_repo, &commit.id, path, GetFileOpts::default()).await
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::{repositories, test, util};
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_get_version_stream_reads_committed_bytes_without_touching_worktree()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let relative_path = PathBuf::from("hello.txt");
            let full_path = repo.path.join(&relative_path);

            util::fs::write_to_path(&full_path, "head version")?;
            repositories::add(&repo, &full_path).await?;
            repositories::commit(&repo, "add hello")?;

            // Modify the working tree so we can prove the read comes from the commit, not from
            // disk.
            util::fs::write_to_path(&full_path, "working version")?;

            let mut stream = repositories::revisions::get_version_stream_from_revision(
                &repo,
                "HEAD",
                &relative_path,
            )
            .await?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend_from_slice(&chunk?);
            }

            assert_eq!(String::from_utf8(bytes).unwrap(), "head version");
            assert_eq!(util::fs::read_from_path(&full_path)?, "working version");

            Ok(())
        })
        .await
    }
}
