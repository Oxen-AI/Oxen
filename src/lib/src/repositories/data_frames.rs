use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::data_frame::DataFrameSlice;
use crate::model::{Commit, LocalRepository};
use crate::opts::DFOpts;

use std::path::Path;

pub mod schemas;

pub fn get_slice(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrameSlice, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::data_frames::get_slice(repo, commit, path, opts),
        MinOxenVersion::V0_19_0 => core::v0_19_0::data_frames::get_slice(repo, commit, path, opts),
    }
}
