use crate::error::OxenError;
use crate::model::merge_conflict::NodeMergeConflict;

use rocksdb::{IteratorMode, DB};
use std::path::Path;
use std::str;

pub struct NodeMergeConflictDBReader {}

impl NodeMergeConflictDBReader {
    pub fn has_file(db: &DB, path: &Path) -> Result<bool, OxenError> {
        match NodeMergeConflictDBReader::get_conflict(db, path) {
            Ok(Some(_val)) => Ok(true),
            Ok(None) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn get_conflict(db: &DB, path: &Path) -> Result<Option<NodeMergeConflict>, OxenError> {
        let key = path.to_str().unwrap();
        let bytes = key.as_bytes();
        match db.get(bytes) {
            Ok(Some(value)) => match str::from_utf8(&value) {
                Ok(value) => {
                    let entry: NodeMergeConflict = serde_json::from_str(value)?;
                    Ok(Some(entry))
                }
                Err(_) => Err(OxenError::basic_str(
                    "NodeMergeConflictDBReader::get_conflict invalid entry",
                )),
            },
            Ok(None) => Ok(None),
            Err(err) => {
                let err =
                    format!("NodeMergeConflictDBReader::get_conflict Error reading db\nErr: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn has_conflicts(db: &DB) -> Result<bool, OxenError> {
        Ok(db.iterator(IteratorMode::Start).count() > 0)
    }

    pub fn list_conflicts(db: &DB) -> Result<Vec<NodeMergeConflict>, OxenError> {
        let mut conflicts: Vec<NodeMergeConflict> = vec![];
        let iter = db.iterator(IteratorMode::Start);
        for item in iter {
            match item {
                Ok((_, value)) => {
                    let entry: NodeMergeConflict = serde_json::from_str(str::from_utf8(&value)?)?;
                    conflicts.push(entry);
                }
                Err(err) => {
                    let err = format!(
                        "NodeMergeConflictDBReader::list_conflicts Error reading db\nErr: {err}"
                    );
                    return Err(OxenError::basic_str(err));
                }
            }
        }
        Ok(conflicts)
    }
}
