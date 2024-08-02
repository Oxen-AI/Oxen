use time::OffsetDateTime;

use crate::api;
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::model::NewCommit;
use crate::model::NewCommitBody;
use crate::model::StagedData;
use crate::util;

use super::CommitWriter;

pub mod data_frames;
pub mod files;
pub mod stager;

pub fn get(repo: &LocalRepository, workspace_id: impl AsRef<str>) -> Result<Workspace, OxenError> {
    Workspace::new(repo, workspace_id)
}

pub fn list(repo: &LocalRepository) -> Result<Vec<Workspace>, OxenError> {
    Workspace::list(repo)
}

pub fn create(
    base_repo: &LocalRepository,
    commit: &Commit,
    workspace_id: impl AsRef<str>,
    is_editable: bool,
) -> Result<Workspace, OxenError> {
    // here we set is_editable to true by default because only editable workspaces are created in this endpoint for now
    Workspace::create(base_repo, commit, workspace_id, is_editable)
}

pub fn delete(workspace: &Workspace) -> Result<(), OxenError> {
    let workspace_id = workspace.id.to_string();
    let workspace_dir = workspace.dir();
    if !workspace_dir.exists() {
        return Err(OxenError::workspace_not_found(workspace_id.into()));
    }

    log::debug!(
        "workspace::delete cleaning up workspace dir: {:?}",
        workspace_dir
    );
    match util::fs::remove_dir_all(&workspace_dir) {
        Ok(_) => log::debug!(
            "workspace::delete removed workspace dir: {:?}",
            workspace_dir
        ),
        Err(e) => log::error!("workspace::delete error removing workspace dir: {:?}", e),
    }

    Ok(())
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
    let mut branch = api::local::branches::get_by_name(repo, branch_name)?;
    log::debug!("commit_staged looking up branch: {:#?}", &branch);

    if branch.is_none() {
        branch = Some(api::local::branches::create(repo, branch_name, &commit.id)?);
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


    let status = status_for_workspace(workspace)?;

    // log::debug!("got branch status: {:#?}", &status);

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
    api::local::branches::update(repo, &branch.name, &commit.id)?;

    // Cleanup workspace on commit
    delete(workspace)?;

    Ok(commit)
}

fn status_for_workspace(workspace: &Workspace) -> Result<StagedData, OxenError> {
    let repo = &workspace.base_repo;
    let workspace_repo = &workspace.workspace_repo;
    let commit = &workspace.commit;

    let status = stager::status_for_workspace(workspace_repo)?;
    status.print_stdout();

    Ok(status)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::index;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::test;
    use crate::util;

    #[test]
    fn test_remote_stager_stage_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents
            let commit = api::local::commits::head_commit(&repo)?;
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let workspace_id = UserConfig::identifier()?;
            let workspace = index::workspaces::create(&repo, &commit, workspace_id, true)?;
            let workspaces_dir = workspace.dir();
            let full_dir = workspaces_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;

            index::workspaces::files::add(&workspace, &full_path)?;

            // Verify staged data
            let staged_data = index::workspaces::stager::status(&workspace, directory)?;
            staged_data.print_stdout();
            assert_eq!(staged_data.staged_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_workspace_commit() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents
            let commit = api::local::commits::head_commit(&repo)?;
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let workspace_id = UserConfig::identifier()?;
            let workspace = index::workspaces::create(&repo, &commit, workspace_id, true)?;
            let workspace_dir = workspace.dir();
            let full_dir = workspace_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;
            index::workspaces::files::add(&workspace, &full_path)?;

            let og_commits = api::local::commits::list(&repo)?;
            let new_commit = NewCommitBody {
                author: String::from("Test User"),
                email: String::from("test@oxen.ai"),
                message: String::from("I am committing this workspace's data"),
            };
            index::workspaces::commit(&workspace, &new_commit, DEFAULT_BRANCH_NAME)?;

            for commit in og_commits.iter() {
                println!("OG commit: {commit:#?}");
            }

            let new_commits = api::local::commits::list(&repo)?;
            assert_eq!(og_commits.len() + 1, new_commits.len());

            Ok(())
        })
    }
}
