//! Module for handing glob path parsing 
//!
use crate::{repositories, util};
use crate::model::{Commit, LocalRepository};
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::error::OxenError;

use std::path::{Component, Path, PathBuf};
use std::collections::HashSet;

use glob::{glob, Pattern};
use glob_match::glob_match;

// TODO: Should ignored paths be ignored for non-glob paths too? 
/// 

// Special cases:

// '.' = whole directory
// '/' = whole directory

// Top level module for parsing glob paths
pub fn parse_glob_path(
    path: &Path,
    repo: Option<&LocalRepository>,
    is_staged: &bool,
) -> Result<HashSet<PathBuf>, OxenError> {

    let path = PathBuf::from(&path);
    let mut paths: HashSet<PathBuf> = HashSet::new();
    
    if util::fs::is_glob_path(&path) {
        log::debug!("parse_glob_paths got glob path: {:?}", path);
        
        if *is_staged {
            // If staged flag set, only match against the staged db
            return search_staged_db(
                &path,
                &repo.expect("Cannot parse staged_db for paths without a repo"),
            );
        } else {
            // If the repo is given, match against the merkle tree
            if let Some(repo) = repo {
                search_merkle_tree(
                    &mut paths,
                    &repo, 
                    &path,
                )?;
            }

            // Match against working dir
            search_working_dir(
                &mut paths,
                &path,
            )?;
        }
    } else {
        paths.insert(path);
    }
        
    log::debug!("parse_glob_paths found paths: {:?}", paths.len());
    Ok(paths)
}

fn search_staged_db(
    path: &Path, 
    repo: &LocalRepository,
) -> Result<HashSet<PathBuf>, OxenError> {
    let mut paths = HashSet::new();

    let path_str = path.to_str().unwrap();
    let glob_pattern = Pattern::new(path_str)?;

    let staged_data = repositories::status::status(repo)?;
    for entry in staged_data.staged_files {
        let entry_path_str = entry.0.to_str().unwrap();
        if glob_pattern.matches(entry_path_str) {
            paths.insert(entry.0.to_owned());
        }
    }

    Ok(paths)
}

// Iterate through the path, expanding glob paths and matching wildcards against the merkle tree
fn search_merkle_tree(
    paths: &mut HashSet<PathBuf>, 
    repo: &LocalRepository,
    glob_path: &Path,
) -> Result<(), OxenError> {

    let glob_path = util::fs::path_relative_to_dir(&glob_path, &repo.path)?;

    if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
        let glob_path_components: Vec<Component> = glob_path.components().collect();
        
        let mut search_index = 0; 
        let mut search_path = PathBuf::from("");

        r_search_merkle_tree(
            repo,
            &head_commit,
            &glob_path_components,
            paths,
            &mut search_path,
            &mut search_index,
        )?;
    }

    Ok(())
}

fn r_search_merkle_tree(
    repo: &LocalRepository,
    head_commit: &Commit,
    glob_path_components: &Vec<Component>,
    paths: &mut HashSet<PathBuf>,
    search_path: &mut PathBuf,
    search_index: &mut usize,
) -> Result<(), OxenError> {

    // Advance to the next wildcard pattern
    let dir_str = glob_path_components[*search_index].as_os_str().to_string_lossy().to_string();
    let mut dir = PathBuf::from(&dir_str);
    while *search_index < glob_path_components.len() && !util::fs::is_glob_path(&dir) {
        *search_index = *search_index + 1;
        *search_path = search_path.join(&dir);

        let dir_str = glob_path_components[*search_index].as_os_str().to_string_lossy().to_string();
        dir = PathBuf::from(&dir_str);
    }
     
    if *search_index < glob_path_components.len() {
        let glob_pattern = dir.to_string_lossy().to_string();

        let is_final = if *search_index == glob_path_components.len() - 1 {
            true
        } else {
            false
        };

        // Match the current glob pattern against the Merkle Tree
        let matched_entries = expand_glob_pattern(
            repo,
            head_commit,
            &glob_pattern,
            &search_path,
            &is_final
        )?;

        // If on the final iteration, extend paths with the matched entries 
        if is_final {
            paths.extend(matched_entries);
            return Ok(());
        }

        // Else, recurse into the matching directories
        let mut new_index = *search_index + 1;
        for mut entry in matched_entries {
            r_search_merkle_tree(
                repo, 
                head_commit,
                glob_path_components,
                paths,
                &mut entry,
                &mut new_index,
            )?;
        }
    }

    Ok(())
}

// Expand a glob pattern with the matching folders from the merkle tree
fn expand_glob_pattern(
    repo: &LocalRepository,
    head_commit: &Commit,
    glob_pattern: &String,
    parent_path: &PathBuf,
    is_final: &bool,
) -> Result<HashSet<PathBuf>, OxenError> {
    let mut paths = HashSet::new(); 

    if let Some(dir_node) =
        repositories::tree::get_dir_with_children(repo, head_commit, parent_path)?
    {
        let dir_children = repositories::tree::list_files_and_folders(&dir_node)?;
        for child in dir_children {

            match &child.node {
                EMerkleTreeNode::Directory(dir_node) => {
                    let child_str = dir_node.name();
                    let child_path = parent_path.join(child_str);
                    if glob_match(&glob_pattern, child_str) {
                        paths.insert(child_path);
                    }
                }
                EMerkleTreeNode::File(file_node) => {
                    let child_str = file_node.name();
                    let child_path = parent_path.join(child_str);
                    // Only include file paths on final iteration
                    if *is_final && glob_match(&glob_pattern, child_str) {
                        paths.insert(child_path);
                    }    
                }
                _ => {
                    return Err(OxenError::basic_str("Unexpected node type"));
                }
            }
        }
    }

    Ok(paths)   
}

fn search_working_dir(
    paths: &mut HashSet<PathBuf>, 
    glob_path: &PathBuf,
) -> Result<(), OxenError> {
    let path_str = glob_path.to_str().unwrap();
 
    for entry in glob(path_str)? {
        paths.insert(entry?);
    }

    Ok(())
}