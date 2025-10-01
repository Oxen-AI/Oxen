use std::collections::HashSet;
use std::fs;
use std::path::Path;
use heed::{byteorder, Database, Env, EnvOpenOptions};
use heed::types::*;
use liboxen::error::OxenError;
use liboxen::model::merkle_tree::node::MerkleTreeNode;
use liboxen::model::MerkleHash;

// pub fn create() -> Result<Env, Box<dyn std::error::Error>> {
//     let env = unsafe { EnvOpenOptions::new().open("my-first-db")? };
//     let mut wtxn = env.write_txn()?;
//     let db: Database<Str, SerdeBincode<MerkleTreeNode>> = env.create_database(&mut wtxn, None)?;
// }

pub fn insert(path: &Path, hash_node: HashSet<(MerkleHash, MerkleTreeNode)>) -> Result<(), Box<dyn std::error::Error>> {
    let env = unsafe { EnvOpenOptions::new().open(path)? };
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, SerdeBincode<MerkleTreeNode>> = env.create_database(&mut wtxn, None)?;

    for (hash, node) in hash_node {
        db.put(&mut wtxn, &hash.to_string(), &node)?;
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

pub fn get(env: &Env, hash: MerkleHash) -> Result<Option<MerkleTreeNode>, Box<dyn std::error::Error>> {
    let mut rtxn = env.write_txn()?;
    let db: Database<Str, SerdeBincode<MerkleTreeNode>> = env.create_database(&mut rtxn, None)?;
    let node = db.get(&rtxn, &hash.to_string())?;
    Ok(node)
}
