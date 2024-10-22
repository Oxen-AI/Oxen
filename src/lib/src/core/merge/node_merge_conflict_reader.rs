use crate::constants::{MERGE_DIR, MERGE_HEAD_FILE};
use crate::core::db;
use crate::core::merge::node_merge_conflict_db_reader::NodeMergeConflictDBReader;
use crate::error::OxenError;
use crate::model::{merge_conflict::NodeMergeConflict, Commit, LocalRepository};
use crate::{repositories, util};

use rocksdb::DB;
use std::path::Path;

pub struct NodeMergeConflictReader {
    merge_db: DB,
    repository: LocalRepository,
}

impl NodeMergeConflictReader {
    pub fn new(repo: &LocalRepository) -> Result<NodeMergeConflictReader, OxenError> {
        let db_path = util::fs::oxen_hidden_dir(&repo.path).join(Path::new(MERGE_DIR));
        log::debug!("NodeMergeConflictReader::new() DB {:?}", db_path);

        let opts = db::key_val::opts::default();
        if !db_path.exists() {
            util::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db = DB::open(&opts, dunce::simplified(&db_path))?;
        }

        Ok(NodeMergeConflictReader {
            merge_db: DB::open_for_read_only(&opts, dunce::simplified(&db_path), false)?,
            repository: repo.clone(),
        })
    }

    pub fn get_conflict_commit(&self) -> Result<Option<Commit>, OxenError> {
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        let commit_id = util::fs::read_first_line(merge_head_path)?;
        repositories::commits::get_by_id(&self.repository, commit_id)
    }

    pub fn has_conflicts(&self) -> Result<bool, OxenError> {
        NodeMergeConflictDBReader::has_conflicts(&self.merge_db)
    }

    pub fn list_conflicts(&self) -> Result<Vec<NodeMergeConflict>, OxenError> {
        NodeMergeConflictDBReader::list_conflicts(&self.merge_db)
    }

    pub fn get_conflict(&self, path: &Path) -> Result<Option<NodeMergeConflict>, OxenError> {
        NodeMergeConflictDBReader::get_conflict(&self.merge_db, path)
    }
}
