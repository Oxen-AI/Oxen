use crate::core::v_latest::stats;
use crate::error::OxenError;
use crate::model::{LocalRepository, RepoStats};
use crate::tasks;

/// The size and the per-data-type file counts and sizes of `repo`'s `main` branch, read off the
/// async worker.
pub async fn get_stats(repo: &LocalRepository) -> Result<RepoStats, OxenError> {
    let repo = repo.clone();
    tasks::spawn_blocking(move || stats::get_stats(&repo)).await?
}
