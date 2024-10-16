use glob::glob;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::core::oxenignore;
use crate::core::v0_10_0::index::{CommitEntryReader, SchemaReader, Stager};
use crate::util;
use crate::{error::OxenError, model::LocalRepository, repositories};

pub fn add(repo: &LocalRepository, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let stager = Stager::new_with_merge(repo)?;
    let commit = repositories::commits::head_commit(repo)?;
    let reader = CommitEntryReader::new(repo, &commit)?;
    let schema_reader = SchemaReader::new(repo, &commit.id)?;
    let ignore = oxenignore::create(repo);
    log::debug!("---START--- oxen add: {:?}", path);

    // Collect paths that match the glob pattern either:
    // 1. In the repo working directory (untracked or modified files)
    // 2. In the commit entry db (removed files)
    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                paths.insert(entry?);
            }

            let pattern_entries = repositories::commits::search_entries(repo, &commit, path_str)?;
            paths.extend(pattern_entries);
        } else {
            // Non-glob path
            paths.insert(path.to_owned());
        }
    }

    // Get all entries in the head commit
    for path in paths {
        stager.add(path.as_ref(), &reader, &schema_reader, &ignore)?;
    }

    log::debug!("---END--- oxen add: {:?}", path);
    Ok(())
}
