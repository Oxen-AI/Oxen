use std::collections::HashSet;
use std::path::PathBuf;

use crate::framework::FrameworkResult;
use crate::lmdb;

const LMDB_PATH: &str = "./.oxen/tree/lmdb";
use liboxen::core::v_latest::index::CommitMerkleTree;
use liboxen::model::merkle_tree::node::MerkleTreeNode;
use liboxen::model::{LocalRepository, MerkleHash};
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

    let head_commit = repositories::commits::head_commit_maybe(&repo)?.unwrap();

    let commit_tree = CommitMerkleTree::root_with_children(&repo, &head_commit)
    .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;

    let commits = commits::list(&repo)
    .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;

    let mut list_hashes = HashSet::new();
    for commit in commits {
        let mut shared_hashes = HashSet::new();
        let mut unique_hashes = HashSet::new();
        let start_path = PathBuf::new();
        let mut node = MerkleTreeNode::from_hash(&repo, &commit.id.parse::<MerkleHash>().unwrap())?;
        CommitMerkleTree::load_unique_children_list(&repo, &mut node, &start_path, &mut shared_hashes, &mut unique_hashes, &mut list_hashes)
        .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;
        list_hashes.insert((commit.id.parse::<MerkleHash>().unwrap(), node));
    }

    lmdb::insert(&lmdb_path, list_hashes).map_err(|e| Error::new(ErrorKind::Other, format!("error inserting lmdb: {}", e)))?;

    Ok(())
}


pub fn test() -> FrameworkResult<()> {
    lmdb::test().map_err(|e| Error::new(ErrorKind::Other, format!("error testing lmdb: {}", e)))?;
    Ok(())
}
