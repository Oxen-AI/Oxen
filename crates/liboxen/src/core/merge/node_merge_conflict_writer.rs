use std::path::Path;

use crate::core::db;
use crate::core::merge;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, merge_conflict::NodeMergeConflict};

use rocksdb::DB;

/// Creates the DB object then calls write_conflicts_to_disk
pub fn write_conflicts_to_db(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
    conflicts: &[NodeMergeConflict],
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
    conflicts: &[NodeMergeConflict],
) -> Result<(), OxenError> {
    merge::write_merge_heads(repo, merge_commit, base_commit)?;

    for conflict in conflicts.iter() {
        let (_, base_path) = &conflict.base_entry;
        log::debug!("writing conflict to db: {base_path:?}");
        let key = base_path.to_str().unwrap();
        let key_bytes = key.as_bytes();
        let val_json = serde_json::to_string(&conflict)?;

        db.put(key_bytes, val_json.as_bytes())?;
    }

    Ok(())
}

pub fn mark_conflict_as_resolved_in_db(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let db_path = merge::db_path(repo);
    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(&db_path))?;

    log::debug!(
        "mark_conflict_as_resolved_in_db path: {:?} db: {:?}",
        path.as_ref(),
        db_path
    );

    let key = path.as_ref().to_str().unwrap();
    let key_bytes = key.as_bytes();
    db.delete(key_bytes)?;

    Ok(())
}
