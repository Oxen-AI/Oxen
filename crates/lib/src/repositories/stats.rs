use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::RepoStats;

pub fn get_stats(repo: &LocalRepository) -> Result<RepoStats, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::stats::get_stats(repo),
    }
}
