use crate::core::refs::RefReader;

use crate::error::OxenError;
use crate::model::{LocalRepository, ParsedResource};
use crate::repositories;

use crate::util;

use std::path::{Path, PathBuf};

pub fn parse_resource_from_path(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();
    // Try to use the first component as a commit id or a workspace id.
    if let Some(first_component) = components.first() {
        let base_path: &Path = first_component.as_ref();
        let first_str = base_path.to_str().unwrap();

        // First try to interpret the first component as a commit id.
        if let Some(commit) = repositories::commits::get_by_id(repo, first_str)? {
            let mut file_path = PathBuf::new();
            for (i, component) in components.iter().enumerate() {
                if i != 0 {
                    let component_path: &Path = component.as_ref();
                    file_path = file_path.join(component_path);
                }
            }
            log::debug!(
                "parse_resource_from_path got commit.id [{}] and filepath [{:?}]",
                commit.id,
                file_path
            );
            return Ok(Some(ParsedResource {
                commit: Some(commit.clone()),
                branch: None,
                workspace: None,
                path: file_path,
                version: PathBuf::from(commit.id.to_string()),
                resource: path.to_owned(),
            }));
        }

        // If not a commit, try to interpret the first component as a workspace id.
        match repositories::workspaces::get(repo, first_str) {
            Ok(workspace) => {
                let mut file_path = PathBuf::new();
                for (i, component) in components.iter().enumerate() {
                    if i != 0 {
                        let component_path: &Path = component.as_ref();
                        file_path = file_path.join(component_path);
                    }
                }
                log::debug!(
                    "parse_resource_from_path got workspace.id [{}] and filepath [{:?}]",
                    workspace.id,
                    file_path
                );
                return Ok(Some(ParsedResource {
                    commit: None,
                    branch: None,
                    workspace: Some(workspace.clone()),
                    path: file_path,
                    version: PathBuf::from(workspace.id),
                    resource: path.to_owned(),
                }));
            }
            Err(e) => {
                log::debug!(
                    "Workspace lookup failed for '{}' with error: {:?}",
                    first_str,
                    e
                );
                // Continue to branch resolution below if no workspace is found
            }
        }
    }

    // Fallback to branch resolution logic if neither commit nor workspace was found.
    // Create a ref reader to look up branch information.
    let ref_reader = RefReader::new(repo)?;
    let mut file_path = PathBuf::new();
    while let Some(component) = components.pop() {
        let component_path: &Path = component.as_ref();
        if file_path == PathBuf::new() {
            file_path = component_path.to_path_buf();
        } else {
            file_path = component_path.join(file_path);
        }
        // If we have no more components to process, consider this as the branch name.
        if components.is_empty() {
            let branch_name = util::fs::linux_path_str(file_path.to_str().unwrap());
            if let Some(branch) = ref_reader.get_branch_by_name(&branch_name)? {
                log::debug!(
                    "parse_resource_from_path got branch [{}] with no file path",
                    branch_name
                );
                let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?;
                file_path = PathBuf::from("");
                return Ok(Some(ParsedResource {
                    commit,
                    branch: Some(branch.clone()),
                    workspace: None,
                    path: file_path,
                    version: PathBuf::from(branch.name),
                    resource: path.to_owned(),
                }));
            } else {
                return Ok(None);
            }
        }

        // Otherwise, try constructing a branch name from the remaining components.
        let mut branch_path = PathBuf::new();
        for component in components.iter() {
            let component_path: &Path = component.as_ref();
            branch_path = branch_path.join(component_path);
        }
        let branch_name = util::fs::linux_path_str(branch_path.to_str().unwrap());
        if let Some(branch) = ref_reader.get_branch_by_name(&branch_name)? {
            log::debug!(
                "parse_resource_from_path got branch [{}] and filepath [{:?}]",
                branch_name,
                file_path
            );
            let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?;
            return Ok(Some(ParsedResource {
                commit,
                branch: Some(branch.clone()),
                workspace: None,
                path: file_path,
                version: PathBuf::from(branch.name),
                resource: path.to_owned(),
            }));
        }
    }

    Ok(None)
}
