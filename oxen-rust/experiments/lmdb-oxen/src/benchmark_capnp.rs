// Benchmark Cap'n Proto deserialization vs MessagePack

use liboxen::model::merkle_tree::node::MerkleTreeNode;
use std::time::Instant;

use crate::capnp_serde;

/// Benchmark MessagePack serialization/deserialization
pub fn benchmark_messagepack(node: &MerkleTreeNode, iterations: usize) -> (u64, u64, usize) {
    // Serialize
    let start = Instant::now();
    let mut serialized_data = Vec::new();
    for _ in 0..iterations {
        serialized_data = rmp_serde::to_vec(node).expect("Failed to serialize with MessagePack");
    }
    let serialize_duration = start.elapsed().as_micros() as u64;
    let data_size = serialized_data.len();

    // Deserialize
    let start = Instant::now();
    for _ in 0..iterations {
        let _: MerkleTreeNode =
            rmp_serde::from_slice(&serialized_data).expect("Failed to deserialize with MessagePack");
    }
    let deserialize_duration = start.elapsed().as_micros() as u64;

    (serialize_duration, deserialize_duration, data_size)
}

/// Benchmark Cap'n Proto serialization/deserialization
pub fn benchmark_capnproto(node: &MerkleTreeNode, iterations: usize) -> (u64, u64, usize) {
    // Serialize
    let start = Instant::now();
    let mut serialized_data = Vec::new();
    for _ in 0..iterations {
        serialized_data =
            capnp_serde::serialize_merkle_tree_node(node).expect("Failed to serialize with Cap'n Proto");
    }
    let serialize_duration = start.elapsed().as_micros() as u64;
    let data_size = serialized_data.len();

    // Deserialize
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = capnp_serde::deserialize_merkle_tree_node(&serialized_data)
            .expect("Failed to deserialize with Cap'n Proto");
    }
    let deserialize_duration = start.elapsed().as_micros() as u64;

    (serialize_duration, deserialize_duration, data_size)
}

/// Run comparison benchmark
pub fn run_benchmark(node: &MerkleTreeNode, iterations: usize) {
    println!("=== MerkleTreeNode Serialization Benchmark ===");
    println!("Iterations: {}", iterations);
    println!();

    // MessagePack benchmark
    println!("MessagePack:");
    let (mp_ser, mp_deser, mp_size) = benchmark_messagepack(node, iterations);
    println!("  Serialization:   {} μs total, {} μs/op", mp_ser, mp_ser / iterations as u64);
    println!("  Deserialization: {} μs total, {} μs/op", mp_deser, mp_deser / iterations as u64);
    println!("  Data size:       {} bytes", mp_size);
    println!();

    // Cap'n Proto benchmark
    println!("Cap'n Proto:");
    let (cp_ser, cp_deser, cp_size) = benchmark_capnproto(node, iterations);
    println!("  Serialization:   {} μs total, {} μs/op", cp_ser, cp_ser / iterations as u64);
    println!("  Deserialization: {} μs total, {} μs/op", cp_deser, cp_deser / iterations as u64);
    println!("  Data size:       {} bytes", cp_size);
    println!();

    // Comparison
    println!("=== Comparison ===");
    println!("Serialization speedup:   {:.2}x", mp_ser as f64 / cp_ser as f64);
    println!("Deserialization speedup: {:.2}x", mp_deser as f64 / cp_deser as f64);
    println!("Size ratio (CP/MP):      {:.2}x", cp_size as f64 / mp_size as f64);
}
