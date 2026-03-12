use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::RepoStats;

#[tracing::instrument(skip(repo), fields(repo_path = %repo.path.display()))]
pub fn get_stats(repo: &LocalRepository) -> Result<RepoStats, OxenError> {
    metrics::counter!("oxen_repo_stats_get_stats_total").increment(1);
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::stats::get_stats(repo),
    }
}
