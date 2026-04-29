//! Revisions can either be commits by id or head commits on branches by name

use std::path::Path;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use crate::storage::LocalFilePath;
use bytes::Bytes;
use tokio_stream::Stream;

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

/// Get the version file path from a commit id
pub async fn get_version_file_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<LocalFilePath, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::revisions::get_version_file_from_commit_id(repo, commit_id, path).await
        }
    }
}

pub async fn get_version_stream_from_revision(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError> {
    let revision = revision.as_ref();
    let path = path.as_ref();

    let commit =
        get(repo, revision)?.ok_or_else(|| OxenError::RevisionNotFound(revision.into()))?;
    let file_node = repositories::tree::get_file_by_path(repo, &commit, path)?
        .ok_or_else(|| OxenError::entry_does_not_exist_in_commit(path, &commit.id))?;

    let version_store = repo.version_store()?;
    version_store
        .get_version_stream(&file_node.hash().to_string())
        .await
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tokio_stream::StreamExt;

    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::{repositories, test, util};

    async fn read_revision_file(
        repo: &LocalRepository,
        revision: &str,
        path: impl AsRef<Path>,
    ) -> Result<Vec<u8>, OxenError> {
        let mut stream =
            repositories::revisions::get_version_stream_from_revision(repo, revision, path).await?;
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend(chunk?);
        }
        Ok(data)
    }

    #[tokio::test]
    async fn test_get_version_stream_from_head_reads_committed_content_without_touching_worktree()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let relative_path = PathBuf::from("hello.txt");
            let full_path = repo.path.join(&relative_path);

            util::fs::write_to_path(&full_path, "head version")?;
            repositories::add(&repo, &full_path).await?;
            repositories::commit(&repo, "add hello")?;

            util::fs::write_to_path(&full_path, "working version")?;
            let before_read = util::fs::read_from_path(&full_path)?;

            let bytes = read_revision_file(&repo, "HEAD", &relative_path).await?;

            assert_eq!(String::from_utf8(bytes).unwrap(), "head version");
            assert_eq!(util::fs::read_from_path(&full_path)?, before_read);

            Ok(())
        })
        .await
    }
}
