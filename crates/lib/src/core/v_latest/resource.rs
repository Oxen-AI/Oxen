use crate::core::refs::with_ref_manager;

use crate::error::OxenError;
use crate::model::{Branch, Commit, LocalRepository, ParsedResource, Workspace};
use crate::repositories;

use crate::util;

use std::path::{Component, Path, PathBuf};

/// Parse a resource identifier from a URL-style path.
///
/// Resolution order: commits, then branches, then workspaces.
/// Reason: At the moment, workspaces are slow to lookup.
///         We will speed up the lookup, but they are lowest priority.
pub fn parse_resource_from_path(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    let components = path.components().collect::<Vec<_>>();

    let first_str = match components.first() {
        Some(c) => component_str(c),
        None => return Ok(None),
    };

    // 1) Commit — first component is a commit id
    if let Some(commit) = repositories::commits::get_by_id(repo, first_str)? {
        let file_path = remaining_path(&components, 1);
        log::debug!(
            "parse_resource_from_path got commit.id [{}] and filepath [{:?}]",
            commit.id,
            file_path
        );
        return Ok(Some(parsed_from_commit(path, commit, file_path)));
    }

    // 2) Branch — progressively match components as a branch name
    if let Some(resource) = try_parse_as_branch(repo, path, &components)? {
        return Ok(Some(resource));
    }

    // 3) Workspace — first component is a workspace id or name
    //    (`repositories::workspaces::get` accepts either form)
    match repositories::workspaces::get(repo, first_str) {
        Ok(Some(workspace)) => {
            let file_path = remaining_path(&components, 1);
            log::debug!(
                "parse_resource_from_path got workspace.id [{}] and filepath [{:?}]",
                workspace.id,
                file_path
            );
            return Ok(Some(parsed_from_workspace(path, workspace, file_path)));
        }
        Ok(None) => {
            log::debug!("Workspace not found: {first_str}");
        }
        Err(e) => {
            log::debug!("Workspace lookup failed for '{first_str}' with error: {e:?}");
        }
    }

    Ok(None)
}

fn parsed_from_commit(resource_path: &Path, commit: Commit, file_path: PathBuf) -> ParsedResource {
    ParsedResource {
        version: PathBuf::from(commit.id.to_string()),
        commit: Some(commit),
        branch: None,
        workspace: None,
        path: file_path,
        resource: resource_path.to_owned(),
    }
}

fn parsed_from_branch(
    resource_path: &Path,
    branch: Branch,
    commit: Option<Commit>,
    file_path: PathBuf,
) -> ParsedResource {
    ParsedResource {
        version: PathBuf::from(&branch.name),
        commit,
        branch: Some(branch),
        workspace: None,
        path: file_path,
        resource: resource_path.to_owned(),
    }
}

fn parsed_from_workspace(
    resource_path: &Path,
    workspace: Workspace,
    file_path: PathBuf,
) -> ParsedResource {
    ParsedResource {
        version: PathBuf::from(&workspace.id),
        commit: None,
        branch: None,
        workspace: Some(workspace),
        path: file_path,
        resource: resource_path.to_owned(),
    }
}

/// All resource paths originate from HTTP routes, so non-UTF-8 is unreachable.
/// These two helpers make that assumption explicit in one place.
fn component_str<'a>(c: &'a Component<'a>) -> &'a str {
    let p: &Path = c.as_ref();
    path_str(p)
}

fn path_str(p: &Path) -> &str {
    p.to_str()
        .expect("resource path component is not valid UTF-8")
}

fn remaining_path(components: &[Component], skip: usize) -> PathBuf {
    components.iter().skip(skip).fold(PathBuf::new(), |acc, c| {
        let p: &Path = c.as_ref();
        acc.join(p)
    })
}

fn join_components(components: &[Component]) -> PathBuf {
    components
        .iter()
        .fold(PathBuf::new(), |acc, c| acc.join::<&Path>(c.as_ref()))
}

/// Try progressively shorter prefixes of the path as a branch name.
///
/// For a path like `a/b/c/d`, tries:
///   1. `a/b/c` as branch, file = `d`
///   2. `a/b`   as branch, file = `c/d`
///   3. `a`     as branch, file = `b/c/d`
///   4. `a/b/c/d` as branch, file = (empty)
fn try_parse_as_branch(
    repo: &LocalRepository,
    path: &Path,
    components: &[Component],
) -> Result<Option<ParsedResource>, OxenError> {
    let len = components.len();
    if len == 0 {
        return Ok(None);
    }

    // Try every split point: branch = components[..split], file = components[split..]
    // Start with the longest possible branch prefix (all but last component),
    // then shrink, ending with the full path as the branch name (split == 0).
    for split in (0..len).rev() {
        let (branch_components, file_components) = if split == 0 {
            // Entire path is the candidate branch name, no file portion.
            (components, &components[0..0])
        } else {
            (&components[..split], &components[split..])
        };

        let branch_name = util::fs::linux_path_str(path_str(&join_components(branch_components)));

        let maybe_branch =
            with_ref_manager(repo, |manager| manager.get_branch_by_name(&branch_name))?;

        if let Some(branch) = maybe_branch {
            let file_path = if file_components.is_empty() {
                PathBuf::new()
            } else {
                join_components(file_components)
            };

            log::debug!(
                "parse_resource_from_path got branch [{branch_name}] and filepath [{file_path:?}]"
            );
            let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?;
            return Ok(Some(parsed_from_branch(path, branch, commit, file_path)));
        }
    }

    Ok(None)
}
