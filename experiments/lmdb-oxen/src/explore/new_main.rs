use std::path::{Path, PathBuf};

use heed::EnvOpenOptions;

use crate::explore::hash::{HasHash, Hash, HexHash};
use crate::explore::lazy_merkle::{HasName, MerkleTreeB, MerkleTreeL, UncomittedRoot};
use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;
use crate::explore::paths::AbsolutePath;

struct DeleteOnDrop<'a>(&'a AbsolutePath);

impl<'a> Drop for DeleteOnDrop<'a> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.as_path());
    }
}

#[tokio::main]
pub async fn run() {
    let repository_root = {
        let tmp_path: PathBuf =
            std::env::temp_dir().join(format!("lmdb_oxen_explore_{}", rand::random::<u16>()));
        std::fs::create_dir_all(&tmp_path).expect("failed to create temp dir");
        AbsolutePath::new(tmp_path).expect("tmp_path is not absolute")
    };
    let _r = DeleteOnDrop(&repository_root);

    let db_location = repository_root.join(&Path::new("lmdb_data").try_into().unwrap());
    std::fs::create_dir_all(db_location.as_path()).expect("failed to create db directory");

    let options = EnvOpenOptions::new();
    let merkle_store = LmdbMerkleDB::new(&repository_root, &db_location, &options)
        .expect("failed to create LmdbMerkleDB");
    check(&merkle_store);

    fn content_hash(content: &str) -> (&str, Hash) {
        (content, Hash::new(content.as_bytes()))
    }

    // ./level_1.txt
    let (content_l1, hash_l1) = content_hash("Hello world! How are you today?");
    let path_l1 = repository_root.join(&Path::new("level_1.txt").try_into().unwrap());
    std::fs::write(path_l1.as_path(), content_l1).expect("failed to write level_1.txt");

    // ./a_dir
    let path_a_dir = repository_root.join(&Path::new("a_dir").try_into().unwrap());
    std::fs::create_dir(path_a_dir.as_path()).expect("failed to create a_dir");

    // ./a_dir/level_2.txt
    let (content_l2, hash_l2) =
        content_hash("I am doing wonderful! Thank you for asking -- how are you today?");
    let path_l2 = path_a_dir.join(&Path::new("level_2.txt").try_into().unwrap());
    std::fs::write(path_l2.as_path(), content_l2).expect("failed to write level_2.txt");

    let dir_hash = Hash::hash_of_hashes([hash_l2].iter());

    println!("{} -> level_1.txt -> {}", HexHash::new(hash_l1), content_l1);
    println!("{} -> a_dir", HexHash::new(dir_hash));
    println!("{} -> level_2.txt -> {}", HexHash::new(hash_l2), content_l2);

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
                children: vec![MerkleTreeB::File {
                    hash: hash_l2,
                    name: Path::new("level_2.txt").try_into().unwrap(),
                }],
            },
        ],
    };

    println!("storing:");
    for c in to_be_comitted.children.iter() {
        println!("{:?}", c);
    }
    println!("-----------------------------------------------------------------");

    let commit = merkle_store
        .commit_tree(to_be_comitted)
        .expect("Failed to store nodes!");

    println!("commit: {}", HexHash::new(commit.hash()));
    println!("------------------------------");

    let retrieved_commit = merkle_store
        .commit(commit.hash())
        .expect("Failed to retrieve commit")
        .expect("Commit not found");

    assert_eq!(commit.hash(), retrieved_commit.hash());
    assert_eq!(
        commit.children().count(),
        retrieved_commit.children().count()
    );
    for (c, r) in commit.children().zip(retrieved_commit.children()) {
        assert_eq!(c.hash(), r.hash());
    }

    let hash_names = [
        (hash_l1, "level_1.txt", Some(content_l1)),
        (dir_hash, "a_dir", None),
        (hash_l2, "level_2.txt", Some(content_l2)),
    ];
    for (hash, name, content) in hash_names {
        let retrieved = merkle_store
            .node(hash)
            .expect("Failed to retrieve node")
            .expect("Node not found");

        println!("for hash {} retrieved: {:?}", HexHash::new(hash), retrieved);
        assert_eq!(hash, retrieved.hash());
        assert_eq!(name, retrieved.name().as_str());

        match (content, retrieved) {
            (Some(expected), MerkleTreeL::File { content, .. }) => {
                assert_eq!(
                    expected.to_string(),
                    String::from_utf8(
                        content
                            .load(&merkle_store)
                            .await
                            .expect("Failed to load content")
                    )
                    .expect("Stored non UTF-8 data"),
                );
            }

            (None, MerkleTreeL::Dir { .. }) => { /* expected */ }

            (Some(expected), MerkleTreeL::Dir { name, hash, .. }) => {
                panic!(
                    "Expecting a file ({expected}) but got a directory: {name} ({})",
                    HexHash::new(hash)
                )
            }

            (None, MerkleTreeL::File { name, content, .. }) => {
                panic!(
                    "Expecting a directory but got a file: {name} ({})",
                    HexHash::new(content.hash())
                )
            }
        }
    }

    println!("-----------------------------------------------------------------");

    println!("SUCCESS!");
}

const fn check(_x: &impl MerkleStore) {}
