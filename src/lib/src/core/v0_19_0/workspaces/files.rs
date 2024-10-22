use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::constants::FILES_DIR;
use crate::constants::STAGED_DIR;
use crate::constants::VERSIONS_DIR;
use crate::core::db;
use crate::core::v0_19_0::add::{add_file_node_to_staged_db, process_add_file};
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::LocalRepository;
use crate::model::{Commit, StagedEntryStatus};
use crate::repositories;
use crate::util;

pub fn add(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_add_file(base_repo, workspace_repo, &Some(commit), filepath)?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

pub fn track_modified_data_frame(
    workspace: &Workspace,
    filepath: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_modify_file(base_repo, workspace_repo, &Some(commit), filepath)?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

pub fn delete(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let workspace_repo = &workspace.workspace_repo;

    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let relative_path_str = path.to_str().unwrap();
    staged_db.delete(relative_path_str)?;
    Ok(())
}

pub fn exists(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let workspace_repo = &workspace.workspace_repo;

    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), false)?;

    let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let relative_path_str = path.to_str().unwrap();
    let result = staged_db.key_may_exist(relative_path_str);
    Ok(result)
}

fn p_add_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let versions_path = util::fs::oxen_hidden_dir(&base_repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node = CommitMerkleTree::dir_with_children(base_repo, head_commit, parent_path)?;
    }

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    process_add_file(
        workspace_repo,
        &workspace_repo.path,
        &versions_path,
        &staged_db,
        &maybe_dir_node,
        path,
        &seen_dirs,
    )
}

fn p_modify_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    log::debug!(
        "p_modify_file path: {:?} staged db_path: {:?}",
        path,
        db_path
    );
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let mut maybe_file_node = None;
    if let Some(head_commit) = maybe_head_commit {
        maybe_file_node = repositories::tree::get_file_by_path(base_repo, head_commit, path)?;
    }

    if let Some(mut file_node) = maybe_file_node {
        file_node.name = path.to_str().unwrap().to_string();
        log::debug!("p_modify_file file_node: {}", file_node);
        add_file_node_to_staged_db(&staged_db, path, StagedEntryStatus::Modified, &file_node)
    } else {
        Err(OxenError::basic_str("file not found in head commit"))
    }
}
