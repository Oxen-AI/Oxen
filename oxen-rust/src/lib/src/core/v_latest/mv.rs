use crate::{core, repositories, util};
use crate::core::staged::staged_db_manager::StagedDBManager;
use crate::core::staged::with_staged_db_manager;
use crate::core::v_latest::add::{FileStatus, generate_file_node};
use crate::model::{LocalRepository, StagedEntryStatus};
use crate::model::merkle_tree::node::{FileNode, MerkleTreeNode};
use crate::error::OxenError;

use std::path::PathBuf;
use std::collections::HashSet;
use std::sync::Arc;
use parking_lot::Mutex;

pub fn mv(
    repo: &LocalRepository,
    source_path: &PathBuf,
    dst_path: &PathBuf, 
) -> Result<(), OxenError> {
    let relative_path = util::fs::path_relative_to_dir(source_path, repo.path.clone())?;
    log::debug!("oxen mv source path {:?} to dst {:?}", source_path, dst_path);

    let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? else {
        return Err(OxenError::basic_str("Error: Cannot use `oxen mv` in empty repo"));
    };

    if let Some(file_node) = 
        repositories::tree::get_file_by_path(repo, &head_commit, &relative_path)? {
            log::debug!("found file {:?} in merkle tree", relative_path);
            move_file(
                repo, source_path, dst_path, &file_node
            )?;
        }
    else if let Some(dir_node) = 
        repositories::tree::get_dir_with_children_recursive(repo, &head_commit, &relative_path)? {
            log::debug!("found dir {:?} in merkle tree", relative_path);
            move_dir(
                repo, source_path, dst_path, &dir_node
            )?;
        }
    else {
        return Err(OxenError::basic_str(format!("Error: Cannot move untracked file {:?}", source_path)))?;
    }

    Ok(())

}

pub fn move_file(
    repo: &LocalRepository,
    source_path: &PathBuf,
    dst_path: &PathBuf,
    file_node: &FileNode,
) -> Result<(), OxenError> {
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    with_staged_db_manager(repo, |staged_db_manager| {  
        // 1. Stage the file node with the new path
        move_file_with_staged_db_manager(
            repo,
            source_path,
            dst_path,
            file_node,
            staged_db_manager,
            &seen_dirs,
        )?;

        // 2. Stage the original path as removed
        let status = StagedEntryStatus::Removed;
        core::v_latest::add::add_file_node_and_parent_dir(
            file_node,
            status,
            source_path,
            staged_db_manager,
            &seen_dirs,
        )?;

        Ok(())
    })?;

    // 3. Rename the file in the working directory
    util::fs::rename(source_path, dst_path)?;

    Ok(())
}

pub fn move_file_with_staged_db_manager(
    repo: &LocalRepository,
    data_path: &PathBuf,
    dst_path: &PathBuf,
    file_node: &FileNode,
    staged_db_manager: &StagedDBManager,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<(), OxenError> {


    let last_modified_seconds = file_node.last_modified_seconds();
    let last_modified_nanoseconds = file_node.last_modified_nanoseconds();
    let last_modified = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(last_modified_seconds as u64)
        + std::time::Duration::from_nanos(last_modified_nanoseconds as u64);
        
    let file_status = FileStatus {
        data_path: data_path.to_path_buf(),
        status: StagedEntryStatus::Added,
        hash: *file_node.hash(),
        num_bytes: file_node.num_bytes(),
        mtime: last_modified.into(),
        previous_metadata: file_node.metadata(),
        previous_file_node: Some(file_node.clone()),
    };

    let file_node = generate_file_node(repo, data_path, dst_path, &file_status)?;
    if let Some(file_node) = file_node {
        let status = file_status.status.clone();
        
        // We could also just implement the logic of this function here
        // TODO: Standardize access methods for staged db
        let relative_path = util::fs::path_relative_to_dir(dst_path, &repo.path.clone())?;
        core::v_latest::add::add_file_node_and_parent_dir(
            &file_node,
            status,
            &relative_path,
            staged_db_manager,
            seen_dirs,
        )?;
    }

    Ok(())
}

pub fn move_dir(
    repo: &LocalRepository,
    source_path: &PathBuf,
    dst_path: &PathBuf,
    dir_node: &MerkleTreeNode,
) -> Result<(), OxenError> {
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    with_staged_db_manager(repo, |staged_db_manager| {  
        // 1. Stage the dir and its entries
        staged_db_manager.add_directory(
            dst_path,
            &seen_dirs,
        )?;
        

        // 2. Stage the original dir as removed
        let status = StagedEntryStatus::Removed;
        staged_db_manager.upsert_dir_node(source_path, status, dir_node)?;

        Ok(())
    })?;
    
    // 3. Rename the dir in the working directory
    util::fs::rename(source_path, dst_path)?;

    Ok(())
}
