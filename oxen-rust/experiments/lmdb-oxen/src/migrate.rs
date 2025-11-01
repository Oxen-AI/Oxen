use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use crate::framework::FrameworkResult;
use crate::lmdb;
use crate::lmdb_zerocopy;
use crate::nodes;

pub const LMDB_PATH: &str = "./.oxen/tree/lmdb";
use heed::types::Bytes;
use heed::types::SerdeBincode;
use heed::types::Str;
use heed::Database;
use liboxen::core::v_latest::index::CommitMerkleTree;
use liboxen::model::merkle_tree::node::MerkleTreeNode;
use liboxen::model::{LocalRepository, MerkleHash};
use liboxen::repositories::{self, commits};
use liboxen::util::fs;
use std::io::Error;
use std::error::Error as StdError;
use std::io::ErrorKind;


pub fn migrate() -> Result<(), Box<dyn StdError>> {
    let lmdb_path = PathBuf::from(LMDB_PATH);
    fs::create_dir_all(&lmdb_path).map_err(|e| Error::new(ErrorKind::Other, format!("error creating lmdb directory: {}", e)))?;


    let repo = LocalRepository::from_current_dir().map_err(|e| {
        Error::new(
            ErrorKind::NotFound,
            format!("error loading repository: {}", e),
        )
    })?;

    // let head_commit = repositories::commits::head_commit_maybe(&repo)?.unwrap();

    // let commit_tree = CommitMerkleTree::root_with_children(&repo, &head_commit)
    // .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;

    let commits = commits::list(&repo)
    .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;

    // let mut list_hashes = HashSet::new();
    // println!("{} commits", commits.len());
    // for commit in commits.iter() {
    //     let mut shared_hashes = HashSet::new();
    //     let mut unique_hashes = HashSet::new();
    //     let start_path = PathBuf::new();
    //     let mut node = MerkleTreeNode::from_hash(&repo, &commit.id.parse::<MerkleHash>().unwrap())?;
    //     let start = Instant::now();
    //     CommitMerkleTree::load_unique_children_list(&repo, &mut node, &start_path, &mut shared_hashes, &mut unique_hashes, &mut list_hashes)
    //     .map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;
    //     let duration = start.elapsed();
    //     println!("completed {:<10} {} in {:?}", commit.id, unique_hashes.len(), duration);
    //     list_hashes.insert((commit.id.parse::<MerkleHash>().unwrap(), node));
    // }


    // let list_len = list_hashes.len();
    // let start = Instant::now();
    // lmdb::insert(&lmdb_path, list_hashes).map_err(|e| Error::new(ErrorKind::Other, format!("error inserting lmdb: {}", e)))?;
    // let duration = start.elapsed();
    // println!("inserted {} nodes into lmdb in {:?}", list_len, duration);

    let env = lmdb::env(&lmdb_path).unwrap();
    let rtxn = env.read_txn().unwrap();
    let db:Database<Str, Bytes> = env.open_database(&rtxn, None).unwrap().expect("error opening database");
    for commit in commits.iter() {
        println!("{}", commit.id);
        let mut hashes = HashSet::new();
        // let mut node = nodes::get_node(db, &rtxn, commit.id.parse::<MerkleHash>().unwrap()).unwrap();
        // let parent_ids = node.parent_id.unwrap();
        // let mut file_node = nodes::get_node(parent_ids).unwrap();
        let start = Instant::now();
        let count = lmdb_zerocopy::load_children_zerocopy(
            db, &rtxn, commit.id.parse::<MerkleHash>().unwrap(), &mut hashes
        )?;
        let duration = start.elapsed();
        println!("loaded {} nodes from lmdb in {:?} for commit {}", count, duration, commit.id);
        // break;
    }
    Ok(())
}


pub fn test() -> FrameworkResult<()> {
    lmdb::test().map_err(|e| Error::new(ErrorKind::Other, format!("error testing lmdb: {}", e)))?;
    Ok(())
}
