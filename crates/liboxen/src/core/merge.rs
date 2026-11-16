use crate::constants::{MERGE_DIR, MERGE_HEAD_FILE, ORIG_HEAD_FILE};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;
use crate::util::fs::AtomicFile;

pub mod entry_merge_conflict_db_reader;
pub mod entry_merge_conflict_reader;
pub mod entry_merge_conflict_writer;
pub mod node_merge_conflict_db_reader;
pub mod node_merge_conflict_reader;
pub mod node_merge_conflict_writer;

use std::path::{Path, PathBuf};

pub fn db_path(repo: &LocalRepository) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path).join(Path::new(MERGE_DIR))
}

/// Records an unfinished merge's two parents, so the next `oxen commit` makes the merge commit.
pub(crate) fn write_merge_heads(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
) -> Result<(), OxenError> {
    // MERGE_HEAD goes last, since its presence is what marks a merge in progress
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    AtomicFile::new(hidden_dir.join(ORIG_HEAD_FILE)).write(base_commit.id.as_bytes())?;
    AtomicFile::new(hidden_dir.join(MERGE_HEAD_FILE)).write(merge_commit.id.as_bytes())
}
