use std::path::Path;

use crate::constants::STAGED_DIR;
use crate::core;
use crate::error::OxenError;
use crate::model::{StagedData, Workspace};
use crate::util;

use indicatif::ProgressBar;

pub fn status(workspace: &Workspace, directory: impl AsRef<Path>) -> Result<StagedData, OxenError> {
    let dir = directory.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    // let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    log::debug!("status db_path: {:?}", db_path);

    // Check if the db path exists, because read only will not create it
    if !db_path.exists() {
        return Ok(StagedData::empty());
    }

    let read_progress = ProgressBar::new_spinner();
    let (dir_entries, _) =
        core::v_latest::status::read_staged_entries_below_path_with_staged_db_manager(
            &workspace.workspace_repo,
            dir,
            &read_progress,
        )?;

    let mut staged_data = StagedData::empty();
    // TODO: for the UI editable workspace polling, we get a No such file or directory (os error 2).
    core::v_latest::status::status_from_dir_entries(&mut staged_data, dir_entries)
}
