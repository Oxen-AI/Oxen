use std::path::Path;

use crate::constants::STAGED_DIR;
use crate::core;
use crate::core::db;
use crate::error::OxenError;
use crate::model::{StagedData, Workspace};
use crate::util;

use indicatif::ProgressBar;
use rocksdb::{DBWithThreadMode, SingleThreaded};

pub fn status(workspace: &Workspace, directory: impl AsRef<Path>) -> Result<StagedData, OxenError> {
    let dir = directory.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    log::debug!("status db_path: {:?}", db_path);

    // Check if the db path exists, because read only will not create it
    if !db_path.exists() {
        return Ok(StagedData::empty());
    }

    // Open db for read only
    let db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), true)?;

    let read_progress = ProgressBar::new_spinner();
    let (dir_entries, _) = core::v0_19_0::status::read_staged_entries_below_path(
        &workspace.workspace_repo,
        &db,
        dir,
        &read_progress,
    )?;

    let mut staged_data = StagedData::empty();
    core::v0_19_0::status::status_from_dir_entries(&mut staged_data, dir_entries)
}
