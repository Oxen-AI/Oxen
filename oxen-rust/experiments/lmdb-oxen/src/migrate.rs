use std::path::PathBuf;

use crate::framework::FrameworkResult;

const LMDB_PATH: &str = ".oxen/tree/nodes/lmdb";
use liboxen::core::v_latest::index::CommitMerkleTree;
use liboxen::model::LocalRepository;
use liboxen::repositories::{self, commits};
use std::io::Error;
use std::io::ErrorKind;

pub fn migrate() -> FrameworkResult<()> {
    let lmdb_path = PathBuf::from(LMDB_PATH);
    
    let repo = LocalRepository::from_current_dir().map_err(|e| {
        Error::new(
            ErrorKind::NotFound,
            format!("error loading repository: {}", e),
        )
    })?;

    let head_commit = repositories::commits::head_commit_maybe(repo)?.unwrap();

    let commit_tree = CommitMerkleTree::root_with_children(&repo, &head_commit)
    .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;

    let commits = commits::list(&repo)
    .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;

    for commit in commits {
        
        
    }

    Ok(())
}
