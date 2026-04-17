use std::path::{Path, PathBuf};

use heed::EnvOpenOptions;

use crate::explore::hash::{HasHash, Hash, HexHash};
use crate::explore::lazy_merkle::{HasName, MerkleTreeB, UncomittedRoot};
use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;
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

    let (_, hash_l1) = content_hash("Hello world! How are you today?");
    let (_, hash_l2) =
        content_hash("I am doing wonderful! Thank you for asking -- how are you today?");

    let dir_hash = Hash::hash_of_hashes([hash_l2].iter());

    let to_be_comitted = UncomittedRoot {
        parent: None,
        repository: repository_root.clone(),
        children: vec![
            MerkleTreeB::File {
                hash: hash_l1,
                name: Path::new("level_1.txt").try_into().unwrap(),
            },
            MerkleTreeB::Dir {
                hash: dir_hash,
                name: Path::new("a_dir").try_into().unwrap(),
                children: vec![Box::new(MerkleTreeB::File {
                    hash: hash_l2,
                    name: Path::new("level_2.txt").try_into().unwrap(),
                })],
            },
        ],
    };

    println!("storing:\n");
    println!("--------");
    for c in to_be_comitted.children.iter() {
        println!("{:?}", c);
    }
    println!("-----------------------------------------------------------------");

    let commit = merkle_store
        .commit_changes(to_be_comitted)
        .expect("Failed to store nodes!");

    let retrieved_commit = merkle_store
        .commit(commit.hash())
        .expect("Failed to retrieve commit")
        .expect("Commit not found");

    assert_eq!(commit.hash(), retrieved_commit.hash());

    let hash_names = [
        (hash_l1, "file.txt"),
        (dir_hash, "a_dir"),
        (hash_l2, "level_2.txt"),
    ];
    for (hash, name) in hash_names {
        let retrieved = merkle_store
            .node(hash)
            .expect("Failed to retrieve node")
            .expect("Node not found");

        println!("for hash {} retrieved: {:?}", HexHash::new(hash), retrieved);
        assert_eq!(hash, retrieved.hash());
        assert_eq!(name, retrieved.name().as_str());
    }

    println!("-----------------------------------------------------------------");

    println!("SUCCESS!");
}

fn check(_x: &impl MerkleStore) {}
