use std::path::{Path, PathBuf};

use heed::EnvOpenOptions;

use crate::explore::hash::{Hash, HexHash};
use crate::explore::lazy_merkle::{LazyData, LazyNode, MerkleTreeL, Root};
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

    fn content_hash(content: &str) -> (&str, Hash) {
        (content, Hash::new(content.as_bytes()))
    }

    let (content_l1, hash_l1) = content_hash("Hello world! How are you today?");
    let (content_l2, hash_l2) =
        content_hash("I am doing wonderful! Thank you for asking -- how are you today?");

    let dir_hash = Hash::hash_of_hashes([hash_l2].iter());

    let commit = Root::new(
        &repository_root,
        &[LazyNode::new(hash_l1), LazyNode::new(dir_hash)],
    );

    let nodes = [
        MerkleTreeL::File {
            name: "level_1.txt".into(),
            parent: Some(LazyNode::new(commit.hash())),
            content: LazyData::new(hash_l1),
        },
        MerkleTreeL::Dir {
            hash: dir_hash,
            name: "a_dir".into(),
            parent: Some(LazyNode::new(commit.hash())),
            children: vec![LazyNode::new(hash_l2)],
        },
        MerkleTreeL::File {
            name: "level_2.txt".into(),
            parent: Some(LazyNode::new(dir_hash)),
            content: LazyData::new(hash_l2),
        },
    ];

    println!("storing {} nodes:\n{:?}", nodes.len(), nodes);
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
