use std::collections::HashSet;
use std::path::PathBuf;

use crate::core::v0_10_0::index::{self, CommitEntryReader};
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RestoreOpts;
use crate::repositories;

use glob::Pattern;

use crate::util;

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let commit = repositories::commits::head_commit(repo)?;
    let path = &opts.path;
    let mut paths: HashSet<PathBuf> = HashSet::new();

    // Quoted wildcard path strings, expand to include present and removed files
    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            let pattern = Pattern::new(path_str)?;
            let stager = index::Stager::new(repo)?;
            let commit_reader = CommitEntryReader::new(repo, &commit)?;
            let staged_data = stager.status(&commit_reader)?;

            // If --staged, only operate on staged files
            if opts.staged {
                for entry in staged_data.staged_files {
                    let entry_path_str = entry.0.to_str().unwrap();
                    if pattern.matches(entry_path_str) {
                        paths.insert(entry.0.to_owned());
                    }
                }
            // Otherwise, `restore` should operate on unstaged modifications and removals.
            } else {
                let modified_and_removed: Vec<PathBuf> = staged_data
                    .modified_files
                    .into_iter()
                    .chain(staged_data.removed_files)
                    .collect();
                for entry in modified_and_removed {
                    let entry_path_str = entry.to_str().unwrap();
                    if pattern.matches(entry_path_str) {
                        paths.insert(entry.to_owned());
                    }
                }
            }
        } else {
            paths.insert(path.to_owned());
        }
    }

    for path in paths {
        let mut opts = opts.clone();
        opts.path = path;
        index::restore(repo, opts)?;
    }

    Ok(())
}
