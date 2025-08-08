use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::data_frame::DataFrameSlice;
use crate::model::{Commit, LocalRepository};
use crate::opts::DFOpts;

use std::path::Path;

pub mod schemas;

pub async fn get_slice(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrameSlice, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::data_frames::get_slice(repo, commit, path, opts).await,
    }
}
