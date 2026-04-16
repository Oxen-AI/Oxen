use std::path::PathBuf;

use heed::EnvOpenOptions;

use crate::explore::lazy_merkle::{LazyData, MerkleTreeL};
use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;
use crate::explore::merkle_writer::MerkleWriter;
use crate::explore::new_path::AbsolutePath;
use crate::explore::scratch::{Hash, HexHash, Repository};

pub fn main() {
    let tmp_path: PathBuf = std::env::temp_dir().join("lmdb_oxen_explore");
    std::fs::create_dir_all(&tmp_path).expect("failed to create temp dir");

    let repo = Repository::from_walk(tmp_path.clone()).expect("failed to create Repository");

    let db_path = tmp_path.join("lmdb_data");
    std::fs::create_dir_all(&db_path).expect("failed to create db directory");
    let db_location = AbsolutePath::new(db_path).expect("failed to create AbsolutePath");

    let options = EnvOpenOptions::new();
    let merkle_store =
        LmdbMerkleDB::new(repo, db_location, &options).expect("failed to create LmdbMerkleDB");
    check(&merkle_store);

    let hash = Hash::new(&[1, 2, 3, 4, 5]);

    let nodes = [MerkleTreeL::File {
        name: "file.txt".into(),
        parent: None,
        content: LazyData::new(hash),
    }];

    println!("storing: {:?}", nodes);
    println!("-----------------------------------------------------------------");

    merkle_store
        .write(nodes.iter())
        .expect("Failed to store nodes");

    let node = merkle_store
        .node(hash)
        .expect("Failed to retrieve node")
        .expect("Node not found");

    println!("using hash {}, retrieved: {:?}", HexHash::from(hash), node);
    println!("-----------------------------------------------------------------");

    assert_eq!(node.name(), "file.txt");
    assert_eq!(node.hash(), hash);

    println!("SUCCESS!");
}

fn check(_x: &impl MerkleStore) {}
