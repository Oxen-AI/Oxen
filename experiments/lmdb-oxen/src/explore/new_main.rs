use std::path::{Path, PathBuf};

use heed::EnvOpenOptions;

use crate::explore::bench::{self, BenchCommands, common::DEFAULT_LMDB_MAP_GIB};
use crate::explore::hash::{ContentHash, HasContentHash, HasLocationHash, HexHash, LocationHash};
use crate::explore::lazy_merkle::{
    HasName, MerkleTreeB, MerkleTreeL, NodeContent, UncomittedRoot, load_file_bytes,
};
use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;
use crate::explore::paths::{AbsolutePath, Name};

use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum Commands {
    Demo,
    Bench {
        #[command(subcommand)]
        command: BenchCommands,
    },
}

pub async fn run(command: Commands) {
    match command {
        Commands::Demo => {
            demo().await;
        }
        Commands::Bench { command } => {
            bench::run(command).await;
        }
    }
}

async fn demo() {
    let repository_root = {
        let tmp_path: PathBuf =
            std::env::temp_dir().join(format!("lmdb_oxen_explore_{}", rand::random::<u16>()));
        std::fs::create_dir_all(&tmp_path).expect("failed to create temp dir");
        AbsolutePath::new(tmp_path).expect("tmp_path is not absolute")
    };
    let _r = DeleteOnDrop(&repository_root);

    let db_location = repository_root.join(&Path::new("lmdb_data").try_into().unwrap());
    std::fs::create_dir_all(db_location.as_path()).expect("failed to create db directory");

    let merkle_store = LmdbMerkleDB::new(&repository_root, &db_location, {
        let mut options = EnvOpenOptions::new();
        options.map_size((DEFAULT_LMDB_MAP_GIB as usize) * 1024 * 1024 * 1024);
        options
    })
    .expect("failed to create LmdbMerkleDB");
    check(&merkle_store); // compile time check that LmdbMerkleDB implements MerkleReader + MerkleWriter correctly

    fn content_hash(content: &str) -> (&str, ContentHash) {
        (content, ContentHash::new(content.as_bytes()))
    }

    // ./level_1.txt
    let (content_l1, ch_l1) = content_hash("Hello world! How are you today?");
    let path_l1 = repository_root.join(&Path::new("level_1.txt").try_into().unwrap());
    std::fs::write(path_l1.as_path(), content_l1).expect("failed to write level_1.txt");
    let name_l1: Name = Path::new("level_1.txt").try_into().unwrap();

    // ./a_dir
    let path_a_dir = repository_root.join(&Path::new("a_dir").try_into().unwrap());
    std::fs::create_dir(path_a_dir.as_path()).expect("failed to create a_dir");
    let name_a_dir: Name = Path::new("a_dir").try_into().unwrap();

    // ./a_dir/level_2.txt
    let (content_l2, ch_l2) =
        content_hash("I am doing wonderful! Thank you for asking -- how are you today?");
    let path_l2 = path_a_dir.join(&Path::new("level_2.txt").try_into().unwrap());
    std::fs::write(path_l2.as_path(), content_l2).expect("failed to write level_2.txt");
    let name_l2: Name = Path::new("level_2.txt").try_into().unwrap();

    let ch_a_dir = ContentHash::hash_of_hashes([ch_l2].iter());

    println!("{} -> level_1.txt -> {}", HexHash::from(ch_l1), content_l1);
    println!("{} -> a_dir", HexHash::from(ch_a_dir));
    println!("{} -> level_2.txt -> {}", HexHash::from(ch_l2), content_l2);

    let to_be_comitted = UncomittedRoot {
        parent: None,
        repository: repository_root.clone(),
        children: vec![
            MerkleTreeB::File {
                content_hash: ch_l1,
                name: name_l1.clone(),
            },
            MerkleTreeB::Dir {
                content_hash: ch_a_dir,
                name: name_a_dir.clone(),
                children: vec![MerkleTreeB::File {
                    content_hash: ch_l2,
                    name: name_l2.clone(),
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
    let commit_hash = commit.content_hash();

    println!("commit: {}", HexHash::from(commit_hash));
    println!("------------------------------");

    let retrieved_commit = merkle_store
        .commit(commit_hash)
        .expect("Failed to retrieve commit")
        .expect("Commit not found");

    assert_eq!(commit_hash, retrieved_commit.content_hash());
    assert_eq!(
        commit.children().count(),
        retrieved_commit.children().count()
    );
    for (c, r) in commit.children().zip(retrieved_commit.children()) {
        assert_eq!(c.location_hash(), r.location_hash());
    }

    // Compute each node's LocationHash so we can look them up correctly.
    // Root-level children have parent = None.
    let loc_l1 = LocationHash::new(&ch_l1, None, &name_l1);
    let loc_a_dir = LocationHash::new(&ch_a_dir, None, &name_a_dir);
    let loc_l2 = LocationHash::new(&ch_l2, Some(&loc_a_dir), &name_l2);

    let triples = [
        (loc_l1, ch_l1, "level_1.txt", Some(content_l1)),
        (loc_a_dir, ch_a_dir, "a_dir", None),
        (loc_l2, ch_l2, "level_2.txt", Some(content_l2)),
    ];
    for (location, expected_ch, expected_name, expected_content) in triples {
        let retrieved = merkle_store
            .node(location)
            .expect("Failed to retrieve node")
            .expect("Node not found");

        println!(
            "at {} (content {}) retrieved: {:?}",
            HexHash::from(location),
            HexHash::from(expected_ch),
            retrieved
        );
        assert_eq!(expected_ch, retrieved.content_hash());
        assert_eq!(expected_name, retrieved.name().as_str());

        match (expected_content, retrieved) {
            (Some(expected), MerkleTreeL::File { .. }) => {
                let bytes = load_file_bytes(&merkle_store, location)
                    .await
                    .expect("Failed to load file bytes");
                assert_eq!(
                    expected.to_string(),
                    String::from_utf8(bytes).expect("Stored non UTF-8 data"),
                );
            }

            (None, MerkleTreeL::Dir { .. }) => { /* expected */ }

            (
                Some(expected),
                MerkleTreeL::Dir {
                    name, content_hash, ..
                },
            ) => {
                panic!(
                    "Expecting a file ({expected}) but got a directory: {name} (content {})",
                    HexHash::from(content_hash)
                )
            }

            (
                None,
                MerkleTreeL::File {
                    name, content_hash, ..
                },
            ) => {
                panic!(
                    "Expecting a directory but got a file: {name} (content {})",
                    HexHash::from(content_hash)
                )
            }
        }
    }

    // Verify the content table is populated for the dir's intrinsic content.
    let dir_content = merkle_store
        .content(ch_a_dir)
        .expect("Failed to query content table")
        .expect("Dir content record missing");
    match dir_content {
        NodeContent::Dir { children } => {
            assert_eq!(children, vec![ch_l2]);
        }
        NodeContent::File { .. } => panic!("Expected Dir content, got File"),
    }

    println!("-----------------------------------------------------------------");
    println!("duplicate-content-different-path correctness check:");

    // Two files with IDENTICAL bytes at DIFFERENT paths — the scenario the
    // old content-keyed schema silently collapsed. We expect:
    //   * Distinct LocationHashes.
    //   * Each `path()` resolves to its own correct path.
    //   * Exactly one `NodeContent::File`-or-missing content entry for the
    //     shared content hash (dedupe at the content layer).
    let dup_root = repository_root.join(&Path::new("dup_root").try_into().unwrap());
    std::fs::create_dir(dup_root.as_path()).expect("failed to create dup_root");
    let dup_sub = dup_root.join(&Path::new("sub").try_into().unwrap());
    std::fs::create_dir(dup_sub.as_path()).expect("failed to create dup_root/sub");
    let shared_bytes = "hello";
    let dup_top = dup_root.join(&Path::new("same.txt").try_into().unwrap());
    std::fs::write(dup_top.as_path(), shared_bytes).expect("failed to write dup_root/same.txt");
    let dup_nested = dup_sub.join(&Path::new("same.txt").try_into().unwrap());
    std::fs::write(dup_nested.as_path(), shared_bytes)
        .expect("failed to write dup_root/sub/same.txt");

    let ch_shared = ContentHash::new(shared_bytes.as_bytes());
    let name_dup_root: Name = Path::new("dup_root").try_into().unwrap();
    let name_sub: Name = Path::new("sub").try_into().unwrap();
    let name_same: Name = Path::new("same.txt").try_into().unwrap();

    // Build the subtree. `dup_root/` has two children:
    //   same.txt  (content = shared_bytes)
    //   sub/same.txt (content = shared_bytes)
    let nested_same = MerkleTreeB::File {
        content_hash: ch_shared,
        name: name_same.clone(),
    };
    let ch_sub = ContentHash::hash_of_hashes([ch_shared].iter());
    let sub_dir = MerkleTreeB::Dir {
        content_hash: ch_sub,
        name: name_sub.clone(),
        children: vec![nested_same],
    };
    let top_same = MerkleTreeB::File {
        content_hash: ch_shared,
        name: name_same.clone(),
    };
    let ch_dup_root =
        ContentHash::hash_of_hashes([top_same.content_hash(), sub_dir.content_hash()].iter());
    let dup_root_dir = MerkleTreeB::Dir {
        content_hash: ch_dup_root,
        name: name_dup_root.clone(),
        children: vec![top_same, sub_dir],
    };

    let dup_commit = merkle_store
        .commit_tree(UncomittedRoot {
            parent: None,
            repository: repository_root.clone(),
            children: vec![dup_root_dir],
        })
        .expect("Failed to commit dup tree");
    println!("  dup commit: {}", HexHash::from(dup_commit.content_hash()));

    // Compute location hashes of both occurrences.
    let loc_dup_root = LocationHash::new(&ch_dup_root, None, &name_dup_root);
    let loc_top_same = LocationHash::new(&ch_shared, Some(&loc_dup_root), &name_same);
    let loc_sub = LocationHash::new(&ch_sub, Some(&loc_dup_root), &name_sub);
    let loc_nested_same = LocationHash::new(&ch_shared, Some(&loc_sub), &name_same);

    assert_ne!(
        loc_top_same, loc_nested_same,
        "two occurrences of identical content at distinct paths MUST hash to distinct locations"
    );

    // Both records exist in the store.
    let top_node = merkle_store
        .node(loc_top_same)
        .expect("store ok")
        .expect("top same.txt must exist");
    let nested_node = merkle_store
        .node(loc_nested_same)
        .expect("store ok")
        .expect("nested same.txt must exist");
    assert_eq!(top_node.content_hash(), ch_shared);
    assert_eq!(nested_node.content_hash(), ch_shared);
    assert_eq!(top_node.name().as_str(), "same.txt");
    assert_eq!(nested_node.name().as_str(), "same.txt");

    // Each resolves to its own correct path.
    let top_path = merkle_store
        .path(loc_top_same)
        .expect("store ok")
        .expect("top path must resolve");
    let nested_path = merkle_store
        .path(loc_nested_same)
        .expect("store ok")
        .expect("nested path must resolve");
    let top_components: Vec<String> = top_path
        .components()
        .map(|n| n.as_str().to_owned())
        .collect();
    let nested_components: Vec<String> = nested_path
        .components()
        .map(|n| n.as_str().to_owned())
        .collect();
    assert_eq!(top_components, vec!["dup_root", "same.txt"]);
    assert_eq!(nested_components, vec!["dup_root", "sub", "same.txt"]);
    println!("  loc_top    -> {top_components:?}");
    println!("  loc_nested -> {nested_components:?}");

    // Content dedupe: shared content appears once in the content table
    // (files don't write a content record in this implementation — they
    // return None from `content()` — which is fine; the point is that the
    // LOCATION records are distinct).
    let shared_content_record = merkle_store.content(ch_shared).expect("store ok");
    println!(
        "  content({}) = {:?}",
        HexHash::from(ch_shared),
        shared_content_record
    );

    println!("-----------------------------------------------------------------");

    println!("SUCCESS!");
}

const fn check(_x: &impl MerkleStore) {}

struct DeleteOnDrop<'a>(&'a AbsolutePath);

impl<'a> Drop for DeleteOnDrop<'a> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.as_path());
    }
}
