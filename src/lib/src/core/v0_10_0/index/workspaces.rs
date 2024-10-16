use std::path::{Path, PathBuf};

use time::OffsetDateTime;

use crate::constants;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::core::v0_10_0::cache::commit_cacher;
use crate::core::v0_10_0::index::workspaces;
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::model::NewCommit;
use crate::model::NewCommitBody;
use crate::repositories;
use crate::util;

use super::CommitWriter;

pub mod data_frames;
pub mod diff;
pub mod files;
pub mod stager;

pub fn init_workspace_repo(
    repo: &LocalRepository,
    workspace_dir: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let workspace_dir = workspace_dir.as_ref();
    let oxen_hidden_dir = repo.path.join(OXEN_HIDDEN_DIR);
    let workspace_hidden_dir = workspace_dir.join(OXEN_HIDDEN_DIR);
    log::debug!("init_workspace_repo {workspace_hidden_dir:?}");
    util::fs::create_dir_all(&workspace_hidden_dir)?;

    let dirs_to_copy = vec![
        constants::COMMITS_DIR,
        constants::HISTORY_DIR,
        constants::REFS_DIR,
        constants::HEAD_FILE,
        constants::OBJECTS_DIR,
    ];

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
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let workspace_id = &workspace.id;
    let branch_name = branch_name.as_ref();

    log::debug!(
        "commit_staged started for commit {} on branch: {} in workspace {}",
        commit.id,
        branch_name,
        workspace_id
    );

    // Check if the branch exists, if not created it
    // If it does exist, we can only commit to it if the commit ids match
    // If the commit ids don't match, we need to reject for now
    let mut branch = repositories::branches::get_by_name(repo, branch_name)?;
    log::debug!("commit_staged looking up branch: {:#?}", &branch);

    if branch.is_none() {
        branch = Some(repositories::branches::create(
            repo,
            branch_name,
            &commit.id,
        )?);
    }

    let branch = branch.unwrap();

    log::debug!("commit_staged got branch: {:#?}", &branch);

    if branch.commit_id != commit.id {
        // TODO: Merge and handle conflicts better
        log::error!(
            "Branch '{}' is not up to date with commit ID '{}'",
            branch_name,
            commit.id
        );

        // Return the custom error variant
        return Err(OxenError::workspace_behind(branch));
    }

    let root_path = PathBuf::from("");
    let status = workspaces::stager::status(workspace, &root_path)?;
    status.print();

    log::debug!("got branch status: {:#?}", &status);

    let commit_writer = CommitWriter::new(repo)?;
    let timestamp = OffsetDateTime::now_utc();

    let new_commit = NewCommit {
        parent_ids: vec![commit.id.to_owned()],
        message: new_commit.message.to_owned(),
        author: new_commit.author.to_owned(),
        email: new_commit.email.to_owned(),
        timestamp,
    };
    log::debug!("commit_staged: new_commit: {:#?}", &new_commit);

    // This should copy all the files over from the staging dir to the versions dir
    let commit = commit_writer.commit_workspace(workspace, &branch, &new_commit, &status)?;
    repositories::branches::update(repo, &branch.name, &commit.id)?;

    // Cleanup workspace on commit
    repositories::workspaces::delete(workspace)?;

    // Kick off post commit actions
    let force = false;
    match commit_cacher::run_all(repo, &commit, force) {
        Ok(_) => {
            log::debug!(
                "Success processing commit {:?} on repo {:?}",
                commit,
                repo.path
            );
        }
        Err(err) => {
            log::error!(
                "Could not process commit {:?} on repo {:?}: {}",
                commit,
                repo.path,
                err
            );
        }
    }

    Ok(commit)
}
