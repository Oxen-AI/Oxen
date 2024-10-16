//! Based on: <https://git-scm.com/docs/git-rm>
//! Remove files matching pathspec from the index, or from the working tree and the index.
//! `oxen rm` will not remove a file from just your working directory.
//! (There is no option to remove a file only from the working tree and yet keep it in the index; use /bin/rm if you want to do that.)
//! When --staged is given, the staged content has to match either the tip of the branch or the file on disk,
//! allowing the file to be removed from just the index.

use crate::api;
use crate::config::UserConfig;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;

use super::CommitEntryReader;
use super::Stager;

use pluralizer::pluralize;
use std::path::Path;
use std::path::PathBuf;

pub fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    // Check if it is a directory and -r was provided
    let path = &opts.path;
    let dir_exists = dir_is_staged_or_committed(repo, path)?;

    if dir_exists && opts.recursive {
        return rm_dir(repo, opts);
    }

    // Error if is a directory and -r was not provided
    if dir_exists && !opts.recursive {
        let error = format!("`oxen rm` on directory {path:?} requires -r");
        return Err(OxenError::basic_str(error));
    }

    rm_file(repo, opts)
}

fn rm_dir(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();
    if opts.staged {
        return remove_staged(repo, opts);
    }

    // We can only use `oxen rm` on directories that are committed
    if !dir_is_committed(repo, path)? {
        let error = format!("Directory {path:?} does not match any committed directories.");
        return Err(OxenError::basic_str(error));
    }

    // Make sure there are no modified files in directory
    let modifications = list_modified_files_in_dir(repo, path)?;
    if !modifications.is_empty() {
        let num_mods: isize = modifications.len().try_into().unwrap(); // should always be safe to go from usize -> isize
        let error = format!("There are {} with modifications within {path:?}\n\tUse `oxen status` to see the modified files.", pluralize("file", num_mods, true));
        return Err(OxenError::basic_str(error));
    }

    // Remove the directory from disk
    let full_path = repo.path.join(path);
    log::debug!("REMOVING DIRECTORY: {full_path:?}");
    if full_path.exists() {
        // user might have removed dir manually before using `oxen rm`
        util::fs::remove_dir_all(&full_path)?;
    }

    // Stage all the removed files
    repositories::add(repo, &full_path)?;

    Ok(())
}

fn list_modified_files_in_dir(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Vec<PathBuf>, OxenError> {
    let status = repositories::status(repo)?;
    let modified: Vec<PathBuf> = status
        .modified_files
        .into_iter()
        .filter(|p| p.starts_with(path))
        .collect();
    Ok(modified)
}

fn dir_is_staged_or_committed(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    Ok(dir_is_staged(repo, path)? || dir_is_committed(repo, path)?)
}

fn dir_is_staged(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let stager = Stager::new(repo)?;
    Ok(stager.has_staged_dir(path))
}

fn dir_is_committed(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let commit = repositories::commits::head_commit(repo)?;
    let commit_reader = CommitEntryReader::new(repo, &commit)?;
    Ok(commit_reader.has_dir(path))
}

fn file_is_committed(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let commit = repositories::commits::head_commit(repo)?;
    let commit_reader = CommitEntryReader::new(repo, &commit)?;

    Ok(commit_reader.has_file(path))
}

fn rm_file(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();
    if opts.staged {
        return remove_staged_file(repo, path);
    }

    if !file_is_committed(repo, path)? {
        let error = format!("File {path:?} must be committed to use `oxen rm`");
        return Err(OxenError::basic_str(error));
    }

    // Remove file from disk
    let full_path = repo.path.join(path);
    log::debug!("REMOVING FILE: {full_path:?}");
    if full_path.exists() {
        // user might have removed file manually before using `oxen rm`
        util::fs::remove_file(&full_path)?;
    }

    // Stage the removed file
    repositories::add(repo, &full_path)?;

    Ok(())
}

pub async fn remove_remote(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();

    if opts.recursive {
        Err(OxenError::basic_str(
            "`oxen workspace rm` does not support removing directories yet",
        ))
    } else {
        remove_remote_staged_file(repo, path).await
    }
}

async fn remove_remote_staged_file(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let remote_repo = api::client::repositories::get_default_remote(repo).await?;
    let workspace_id = UserConfig::identifier()?;
    api::client::workspaces::files::rm(&remote_repo, &workspace_id, path.to_path_buf()).await
}

fn remove_staged(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    let path = opts.path.as_ref();

    if opts.recursive {
        remove_staged_dir(repo, path)
    } else {
        remove_staged_file(repo, path)
    }
}

fn remove_staged_file(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.remove_staged_file(path)
}

fn remove_staged_dir(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.remove_staged_dir(path)
}
