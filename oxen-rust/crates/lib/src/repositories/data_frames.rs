use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::data_frame::DataFrameSlice;
use crate::model::{LocalRepository, ParsedResource};
use crate::opts::DFOpts;

use std::path::Path;

pub mod schemas;

#[tracing::instrument(skip(repo, resource, path, opts), fields(repo_path = %repo.path.display()))]
pub async fn get_slice(
    repo: &LocalRepository,
    resource: &ParsedResource,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrameSlice, OxenError> {
    metrics::counter!("oxen_repo_data_frames_get_slice_total").increment(1);
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::data_frames::get_slice(repo, resource, path, opts).await,
    }
}
