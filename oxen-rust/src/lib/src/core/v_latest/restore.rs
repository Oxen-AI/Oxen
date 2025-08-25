use std::collections::HashSet;
use std::path::PathBuf;

use crate::core::v_latest::index;
use crate::error::OxenError;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::LocalRepository;
use crate::opts::RestoreOpts;
use crate::repositories;

use glob::Pattern;
use glob_match::glob_match;

use crate::util;

pub async fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let path = &opts.path;
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let root_path = PathBuf::from("");
    let repo_path = repo.path.clone();
    let mut paths: HashSet<PathBuf> = HashSet::new();

    // Quoted wildcard path strings, expand to include present and removed files
    let relative_path = util::fs::path_relative_to_dir(path, &repo_path)?;
    let full_path = repo_path.join(&relative_path);

    if util::fs::is_glob_path(&relative_path) {
        let Some(ref head_commit) = head_commit else {
            return Err(OxenError::basic_str(
                "Error: Cannot use `oxen restore` in empty repository",
            ));
        };

        // If --staged, only operate on staged files
        if opts.staged {
            let path_str = path.to_str().unwrap();
            let pattern = Pattern::new(path_str)?;
            let staged_data = repositories::status::status(repo)?;
            for entry in staged_data.staged_files {
                let entry_path_str = entry.0.to_str().unwrap();
                if pattern.matches(entry_path_str) {
                    paths.insert(entry.0.to_owned());
                }
            }
        } else {
            let glob_pattern = full_path.file_name().unwrap().to_string_lossy().to_string();
            let parent_path = relative_path.parent().unwrap_or(&root_path);

            // Otherwise, traverse the tree for files to restore
            if let Some(dir_node) =
                repositories::tree::get_dir_with_children(repo, head_commit, parent_path)?
            {
                let dir_children = repositories::tree::list_files_and_folders(&dir_node)?;
                println!("dir_node: {dir_node:?}");
                for child in dir_children {
                    if let EMerkleTreeNode::File(file_node) = &child.node {
                        let child_str = file_node.name();
                        let child_path = parent_path.join(child_str);
                        if glob_match(&glob_pattern, child_str) {
                            paths.insert(child_path);
                        }
                    } else if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                        let child_str = dir_node.name();
                        let child_path = parent_path.join(child_str);
                        if glob_match(&glob_pattern, child_str) {
                            // TODO: Method to detect if dirs are modified from the tree
                            paths.insert(child_path);
                        }
                    }
                }
            }
        }
    } else {
        paths.insert(relative_path);
    }

    println!("paths: {paths:?}");

    for path in paths {
        let mut opts = opts.clone();
        opts.path = path;
        index::restore::restore(repo, opts).await?;
    }

    Ok(())
}
