//! Writes merge conflicts to database
//!

use crate::core::db;
use crate::core::merge;
use crate::error::OxenError;
use crate::model::{Commit, EntryMergeConflict, LocalRepository};

use rocksdb::DB;

/// Creates the DB object then calls write_conflicts_to_disk
pub fn write_conflicts_to_db(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
    conflicts: &[EntryMergeConflict],
) -> Result<(), OxenError> {
    let db_path = merge::db_path(repo);
    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(&db_path))?;

    write_conflicts_to_disk(repo, &db, merge_commit, base_commit, conflicts)
}

/// Writes merge conflicts to database, and write merge commit and head commit to disk
pub fn write_conflicts_to_disk(
    repo: &LocalRepository,
    db: &DB,
    merge_commit: &Commit,
    base_commit: &Commit,
    conflicts: &[EntryMergeConflict],
) -> Result<(), OxenError> {
    merge::write_merge_heads(repo, merge_commit, base_commit)?;

    for conflict in conflicts.iter() {
        let key = conflict.base_entry.path.to_str().unwrap();
        let key_bytes = key.as_bytes();
        let val_json = serde_json::to_string(&conflict)?;

        db.put(key_bytes, val_json.as_bytes())?;
    }

    Ok(())
}
