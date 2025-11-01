// Benchmark database operations with Cap'n Proto vs MessagePack

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use liboxen::model::merkle_tree::node::MerkleTreeNode;
use liboxen::model::MerkleHash;

use crate::lmdb;

/// Benchmark database insert and retrieval with both serialization formats
pub fn benchmark_db_operations(
    nodes: HashSet<(MerkleHash, MerkleTreeNode)>,
    db_path_capnp: &Path,
    db_path_msgpack: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Database Operations Benchmark ===");
    println!("Number of nodes: {}", nodes.len());
    println!();

    // Benchmark Cap'n Proto
    println!("Cap'n Proto:");
    let start = Instant::now();
    lmdb::insert(db_path_capnp, nodes.clone())?;
    let insert_time_capnp = start.elapsed();
    println!("  Insert time: {:?}", insert_time_capnp);

    // Read back with Cap'n Proto
    let env = lmdb::env(db_path_capnp)?;
    let rtxn = env.read_txn()?;
    let db = env.open_database(&rtxn, None)?.expect("database should exist");
    
    let hashes: HashSet<MerkleHash> = nodes.iter().map(|(h, _)| *h).collect();
    let start = Instant::now();
    let _retrieved = lmdb::get_many(db, &rtxn, hashes.clone())?;
    let read_time_capnp = start.elapsed();
    println!("  Read time:   {:?}", read_time_capnp);
    println!();

    // Benchmark MessagePack
    println!("MessagePack:");
    let start = Instant::now();
    lmdb::insert_msgpack(db_path_msgpack, nodes.clone())?;
    let insert_time_msgpack = start.elapsed();
    println!("  Insert time: {:?}", insert_time_msgpack);

    // Read back with MessagePack
    let env = lmdb::env(db_path_msgpack)?;
    let rtxn = env.read_txn()?;
    let db = env.open_database(&rtxn, None)?.expect("database should exist");
    
    let start = Instant::now();
    let _retrieved = lmdb::get_many_msgpack(db, &rtxn, hashes)?;
    let read_time_msgpack = start.elapsed();
    println!("  Read time:   {:?}", read_time_msgpack);
    println!();

    // Comparison
    println!("=== Comparison ===");
    let insert_speedup = insert_time_msgpack.as_secs_f64() / insert_time_capnp.as_secs_f64();
    let read_speedup = read_time_msgpack.as_secs_f64() / read_time_capnp.as_secs_f64();
    
    println!("Insert speedup (Cap'n Proto vs MessagePack): {:.2}x", insert_speedup);
    println!("Read speedup (Cap'n Proto vs MessagePack):   {:.2}x", read_speedup);

    // Get database sizes
    if let Ok(capnp_size) = std::fs::metadata(db_path_capnp.join("data.mdb")) {
        if let Ok(msgpack_size) = std::fs::metadata(db_path_msgpack.join("data.mdb")) {
            let size_ratio = capnp_size.len() as f64 / msgpack_size.len() as f64;
            println!();
            println!("Database sizes:");
            println!("  Cap'n Proto: {} bytes", capnp_size.len());
            println!("  MessagePack: {} bytes", msgpack_size.len());
            println!("  Size ratio (CP/MP): {:.2}x", size_ratio);
        }
    }

    Ok(())
}
