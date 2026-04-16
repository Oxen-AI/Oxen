use std::path::{Path, PathBuf};

use heed::EnvOpenOptions;

use crate::explore::hash::{Hash, HexHash};
use crate::explore::lazy_merkle::{LazyData, MerkleTreeL};
use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;
use crate::explore::merkle_writer::MerkleWriter;
use crate::explore::paths::AbsolutePath;

pub fn main() {
    let repository_root = {
        let tmp_path: PathBuf = std::env::temp_dir().join("lmdb_oxen_explore");
        std::fs::create_dir_all(&tmp_path).expect("failed to create temp dir");
        AbsolutePath::new(tmp_path).expect("tmp_path is not absolute")
    };

    let db_location = repository_root.join(&Path::new("lmdb_data").try_into().unwrap());
    std::fs::create_dir_all(db_location.as_path()).expect("failed to create db directory");

    let options = EnvOpenOptions::new();
    let merkle_store = LmdbMerkleDB::new(&repository_root, &db_location, &options)
        .expect("failed to create LmdbMerkleDB");
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
