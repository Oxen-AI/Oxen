use crate::constants;
use crate::constants::STAGED_DIR;
use crate::core;
use crate::core::db;
use crate::core::refs::RefWriter;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, NewCommitBody, Workspace};
use crate::util;

use indicatif::ProgressBar;
use rocksdb::{DBWithThreadMode, SingleThreaded};
use std::path::Path;

pub mod commit;
pub mod data_frames;
pub mod diff;
pub mod files;
pub mod status;

pub fn init_workspace_repo(
    repo: &LocalRepository,
    workspace_dir: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let workspace_dir = workspace_dir.as_ref();
    let oxen_hidden_dir = repo.path.join(constants::OXEN_HIDDEN_DIR);
    let workspace_hidden_dir = workspace_dir.join(constants::OXEN_HIDDEN_DIR);
    log::debug!("init_workspace_repo {workspace_hidden_dir:?}");
    util::fs::create_dir_all(&workspace_hidden_dir)?;

    // Copy the config file
    let config_file = oxen_hidden_dir.join(constants::REPO_CONFIG_FILENAME);
    let target_config_file = workspace_hidden_dir.join(constants::REPO_CONFIG_FILENAME);
    util::fs::copy(config_file, target_config_file)?;

    let dirs_to_copy = vec![constants::HISTORY_DIR];
    for dir in dirs_to_copy {
        let oxen_dir = oxen_hidden_dir.join(dir);
        let target_dir = workspace_hidden_dir.join(dir);

        log::debug!("init_workspace_repo copying {dir} dir {oxen_dir:?} -> {target_dir:?}");
        if oxen_dir.is_dir() {
            util::fs::copy_dir_all(oxen_dir, target_dir)?;
        } else {
            util::fs::copy(oxen_dir, target_dir)?;
        }
    }

    LocalRepository::new(workspace_dir)
}

pub fn commit(
    workspace: &Workspace,
    new_commit: &NewCommitBody,
    branch_name: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let branch_name = branch_name.as_ref();

    let staged_db_path = util::fs::oxen_hidden_dir(&workspace.workspace_repo.path).join(STAGED_DIR);
    log::debug!(
        "0.19.0::workspaces::commit staged db path: {:?}",
        staged_db_path
    );
    let opts = db::key_val::opts::default();

    // Ensure connection to staged_db is dropped before clearing it
    let commit = {
        let staged_db: DBWithThreadMode<SingleThreaded> =
            DBWithThreadMode::open(&opts, dunce::simplified(&staged_db_path))?;

        let commit_progress_bar = ProgressBar::new_spinner();

        // Read all the staged entries
        let (dir_entries, _) = core::v0_19_0::status::read_staged_entries(
            &workspace.workspace_repo,
            &staged_db,
            &commit_progress_bar,
        )?;

        core::v0_19_0::index::commit_writer::commit_dir_entries(
            &workspace.base_repo,
            dir_entries,
            new_commit,
            &commit_progress_bar,
        )?
    };

    // Update the branch
    let ref_writer = RefWriter::new(&workspace.base_repo)?;
    let commit_id = commit.id.to_owned();
    ref_writer.set_branch_commit_id(branch_name, &commit_id)?;

    Ok(commit)
}
