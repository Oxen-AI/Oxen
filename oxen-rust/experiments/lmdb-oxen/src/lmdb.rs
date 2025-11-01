use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use heed::{byteorder, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use heed::types::*;
use liboxen::error::OxenError;
use liboxen::model::merkle_tree::node::MerkleTreeNode;
use liboxen::model::MerkleHash;

use crate::capnp_serde;

// pub fn create() -> Result<Env, Box<dyn std::error::Error>> {
//     let env = unsafe { EnvOpenOptions::new().open("my-first-db")? };
//     let mut wtxn = env.write_txn()?;
//     let db: Database<Str, SerdeBincode<MerkleTreeNode>> = env.create_database(&mut wtxn, None)?;
// }

pub fn env(path: &Path) -> Result<Env, Box<dyn std::error::Error>> {
    let mut env_options = EnvOpenOptions::new();
    env_options.map_size(1024 * 1024 * 1024 * 1024);
    let env = unsafe { env_options.open(path)? };
    Ok(env)
}

/// Insert nodes using Cap'n Proto serialization
pub fn insert(path: &Path, hash_node: HashSet<(MerkleHash, MerkleTreeNode)>) -> Result<(), Box<dyn std::error::Error>> {
    let env = env(path)?;
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, None)?;
    for (hash, node) in hash_node {
        // Serialize using Cap'n Proto instead of MessagePack
        let buf = capnp_serde::serialize_merkle_tree_node(&node)?;
        db.put(&mut wtxn, &hash.to_string(), &buf)?;
    }

    wtxn.commit()?;

    Ok(())
}

/// Insert nodes using MessagePack serialization (for comparison)
pub fn insert_msgpack(path: &Path, hash_node: HashSet<(MerkleHash, MerkleTreeNode)>) -> Result<(), Box<dyn std::error::Error>> {
    let env = env(path)?;
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, None)?;
    for (hash, node) in hash_node {
        let buf = rmp_serde::to_vec(&node).unwrap();
        db.put(&mut wtxn, &hash.to_string(), &buf)?;
    }

    wtxn.commit()?;

    Ok(())
}

pub fn test() -> Result<(), Box<dyn std::error::Error>> {
    let env = unsafe { EnvOpenOptions::new().open("./my-first-db")? };

    // We open the default unnamed database
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, U32<byteorder::NativeEndian>> = env.create_database(&mut wtxn, None)?;

    // We open a write transaction
    db.put(&mut wtxn, "seven", &7)?;
    db.put(&mut wtxn, "zero", &0)?;
    db.put(&mut wtxn, "five", &5)?;
    db.put(&mut wtxn, "three", &3)?;
    wtxn.commit()?;

    // We open a read transaction to check if those values are now available
    let mut rtxn = env.read_txn()?;

    let ret = db.get(&rtxn, "zero")?;
    assert_eq!(ret, Some(0));

    let ret = db.get(&rtxn, "five")?;
    assert_eq!(ret, Some(5));

    Ok(())
}

/// Get node using Cap'n Proto deserialization
pub fn get(db: Database<Str, Bytes>, rtxn: &RoTxn, hash: MerkleHash) -> Result<Option<MerkleTreeNode>, Box<dyn std::error::Error>> {
    let node = db.get(&rtxn, &hash.to_string())?;

    match node {
        Some(bytes) => {
            // Deserialize using Cap'n Proto instead of MessagePack
            let node = capnp_serde::deserialize_merkle_tree_node(bytes)?;
            Ok(Some(node))
        },
        None => Ok(None),
    }
}

/// Get node using MessagePack deserialization (for comparison)
pub fn get_msgpack(db: Database<Str, Bytes>, rtxn: &RoTxn, hash: MerkleHash) -> Result<Option<MerkleTreeNode>, Box<dyn std::error::Error>> {
    let node = db.get(&rtxn, &hash.to_string())?;

    match node {
        Some(bytes) => {
            let node = rmp_serde::from_slice(bytes).unwrap();
            Ok(Some(node))
        },
        None => Ok(None),
    }
}


/// Get many nodes using Cap'n Proto deserialization
pub fn get_many(db: Database<Str, Bytes>, rtxn: &RoTxn, hashes: HashSet<MerkleHash>) -> Result<HashMap<MerkleHash, MerkleTreeNode>, Box<dyn std::error::Error>> {
    let mut nodes = HashMap::new();
    for hash in hashes {
        let node = get(db, rtxn, hash)?;
        match node {
            Some(node) => {
                nodes.insert(hash, node);
            },
            None => return Err(format!("node not found: {}", hash).into()),
        };
    }
    Ok(nodes)
}

/// Get many nodes using MessagePack deserialization (for comparison)
pub fn get_many_msgpack(db: Database<Str, Bytes>, rtxn: &RoTxn, hashes: HashSet<MerkleHash>) -> Result<HashMap<MerkleHash, MerkleTreeNode>, Box<dyn std::error::Error>> {
    let mut nodes = HashMap::new();
    for hash in hashes {
        let node = get_msgpack(db, rtxn, hash)?;
        match node {
            Some(node) => {
                nodes.insert(hash, node);
            },
            None => return Err(format!("node not found: {}", hash).into()),
        };
    }
    Ok(nodes)
}