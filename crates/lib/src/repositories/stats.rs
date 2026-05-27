use crate::core;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::RepoStats;

pub fn get_stats(repo: &LocalRepository) -> Result<RepoStats, OxenError> {
    core::v_latest::stats::get_stats(repo)
}
