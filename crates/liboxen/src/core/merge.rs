use crate::util;
use crate::{constants::MERGE_DIR, model::LocalRepository};

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
