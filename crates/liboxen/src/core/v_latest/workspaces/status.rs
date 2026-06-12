use std::io;
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
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    log::debug!("status db_path: {db_path:?}");

    // Treat the workspace as empty if the staged db hasn't been fully initialized:
    //   - the dir doesn't exist at all, or
    //   - the dir exists but is missing the RocksDB `CURRENT` pointer (a never-finished
    //     init or a race between dir creation and DB::open).
    // Without CURRENT the DB has no committed metadata, so there's semantically nothing
    // to read. Checking before DB::open also matters because a failed open with
    // `create_if_missing(true)` can re-create CURRENT before bailing on a different
    // check (e.g. `wal_dir contains existing log file`).
    if !db_path.exists() || !db_path.join("CURRENT").exists() {
        return Ok(StagedData::empty());
    }

    let read_progress = ProgressBar::new_spinner();
    let read_result =
        core::v_latest::status::read_staged_entries_below_path_with_staged_db_manager(
            &workspace.workspace_repo,
            dir,
            &read_progress,
        )
        .and_then(|(dir_entries, _)| {
            let mut staged_data = StagedData::empty();
            core::v_latest::status::status_from_dir_entries(&mut staged_data, dir_entries)
        });

    match read_result {
        Ok(staged) => Ok(staged),
        // A clean workspace can race with on-disk db reads: a file underneath is missing
        // (os error 2) by the time we reach it. Treat that as empty rather than a hard failure.
        Err(err) if is_missing_path(&err) => Ok(StagedData::empty()),
        Err(err) => Err(OxenError::WorkspaceStagedDbCorrupted {
            workspace_id: workspace.id.clone(),
            source: Box::new(err),
        }),
    }
}

fn is_missing_path(err: &OxenError) -> bool {
    match err {
        OxenError::PathDoesNotExist(_) => true,
        OxenError::IO(e) => e.kind() == io::ErrorKind::NotFound,
        _ => false,
    }
}
