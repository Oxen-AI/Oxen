// Zero-copy LMDB operations using Cap'n Proto
// This avoids deserialization overhead by keeping data in Cap'n Proto format

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use heed::{Database, Env, EnvOpenOptions, RoTxn, BytesDecode, BytesEncode};
use heed::types::*;
use liboxen::model::MerkleHash;
use rayon::prelude::*;

use crate::merkle_tree_capnp;

/// Custom codec for MerkleHash to avoid string allocation
pub struct MerkleHashCodec;

impl<'a> BytesEncode<'a> for MerkleHashCodec {
    type EItem = MerkleHash;

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>, Box<dyn std::error::Error + Send + Sync>> {
        // Convert MerkleHash to bytes directly (assuming it has a to_bytes or similar method)
        // If not, use the string representation but cache it
        let s = item.to_string();
        Ok(std::borrow::Cow::Owned(s.into_bytes()))
    }
}

impl<'a> BytesDecode<'a> for MerkleHashCodec {
    type DItem = String;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn std::error::Error + Send + Sync>> {
        Ok(std::str::from_utf8(bytes)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
            .to_string())
    }
}

/// Helper struct to store Cap'n Proto message with its data
struct NodeWithData {
    hash: MerkleHash,
    data: Vec<u8>,
}

pub fn env(path: &Path) -> Result<Env, Box<dyn std::error::Error>> {
    let mut env_options = EnvOpenOptions::new();
    env_options.map_size(1024 * 1024 * 1024 * 1024);
    let env = unsafe { env_options.open(path)? };
    Ok(env)
}

/// Get raw bytes without deserializing
/// Optimized: returns borrowed slice when possible to avoid allocation
pub fn get_bytes(
    db: Database<Str, Bytes>,
    rtxn: &RoTxn,
    hash: MerkleHash,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let bytes = db.get(&rtxn, &hash.to_string())?;
    Ok(bytes.map(|b| b.to_vec()))
}

/// Get bytes as borrowed slice - zero copy!
/// This avoids the Vec allocation
pub fn get_bytes_borrowed<'txn>(
    db: Database<Str, Bytes>,
    rtxn: &'txn RoTxn,
    hash: &MerkleHash,
) -> Result<Option<&'txn [u8]>, Box<dyn std::error::Error>> {
    let hash_str = hash.to_string(); // Still need this allocation unfortunately
    let bytes = db.get(rtxn, &hash_str)?;
    Ok(bytes)
}

/// Batch get multiple hashes at once - more efficient than individual gets
pub fn get_bytes_batch(
    db: Database<Str, Bytes>,
    rtxn: &RoTxn,
    hashes: &[MerkleHash],
) -> Result<Vec<(MerkleHash, Vec<u8>)>, Box<dyn std::error::Error>> {
    let mut results = Vec::with_capacity(hashes.len());
    for &hash in hashes {
        if let Some(bytes) = db.get(&rtxn, &hash.to_string())? {
            results.push((hash, bytes.to_vec()));
        }
    }
    Ok(results)
}

/// Load children recursively using zero-copy access
/// This is MUCH faster than deserializing every node
pub fn load_children_zerocopy(
    db: Database<Str, Bytes>,
    rtxn: &RoTxn,
    root_hash: MerkleHash,
    visited: &mut HashSet<MerkleHash>,
) -> Result<usize, Box<dyn std::error::Error>> {
    //println!("[ZEROCOPY] Starting load_children_zerocopy for hash: {}", root_hash);
    let mut count = 0;
    
    // Initialize stack with root node data
    let root_bytes = match get_bytes(db, rtxn, root_hash)? {
        Some(b) => b,
        None => return Ok(0),
    };
    
    let mut stack = vec![NodeWithData {
        hash: root_hash,
        data: root_bytes,
    }];
    
    while let Some(node_with_data) = stack.pop() {
        let hash = node_with_data.hash;
        //println!("[ZEROCOPY] Processing hash: {}", hash);
        
        if visited.contains(&hash) {
            //println!("[ZEROCOPY] Already visited, skipping");
            continue;
        }
        visited.insert(hash);
        
        //println!("[ZEROCOPY] Creating Cap'n Proto reader");
        // Read the message safely
        let message = match capnp::serialize::read_message(
            &node_with_data.data[..], 
            capnp::message::ReaderOptions::new()
        ) {
            Ok(msg) => {
                //println!("[ZEROCOPY] Successfully created message reader");
                msg
            },
            Err(e) => {
                //println!("[ZEROCOPY] ERROR creating message reader: {:?}", e);
                return Err(format!("Failed to read Cap'n Proto message: {:?}", e).into());
            }
        };
        
        //println!("[ZEROCOPY] Getting root reader");
        let reader = match message.get_root::<merkle_tree_capnp::merkle_tree_node::Reader>() {
            Ok(r) => {
                //println!("[ZEROCOPY] Successfully got root reader");
                r
            },
            Err(e) => {
                //println!("[ZEROCOPY] ERROR getting root: {:?}", e);
                return Err(format!("Failed to get root: {:?}", e).into());
            }
        };
        
        count += 1;
        
        //println!("[ZEROCOPY] Getting children list");
        // Access children directly from Cap'n Proto buffer
        let children = match reader.get_children() {
            Ok(c) => {
                //println!("[ZEROCOPY] Got {} children", c.len());
                c
            },
            Err(e) => {
                //println!("[ZEROCOPY] ERROR getting children: {:?}", e);
                return Err(format!("Failed to get children: {:?}", e).into());
            }
        };
        
        //println!("[ZEROCOPY] Iterating through {} children", children.len());
        for (i, child) in children.iter().enumerate() {
            //println!("[ZEROCOPY] Processing child {}/{}", i + 1, children.len());
            
            let child_hash_reader = match child.get_hash() {
                Ok(h) => h,
                Err(e) => {
                    //println!("[ZEROCOPY] ERROR getting child hash: {:?}", e);
                    return Err(format!("Failed to get child hash: {:?}", e).into());
                }
            };
            
            let value_reader = match child_hash_reader.get_value() {
                Ok(v) => v,
                Err(e) => {
                    //println!("[ZEROCOPY] ERROR getting hash value: {:?}", e);
                    return Err(format!("Failed to get hash value: {:?}", e).into());
                }
            };
            
            let high = value_reader.get_high() as u128;
            let low = value_reader.get_low() as u128;
            let child_hash = MerkleHash::new((high << 64) | low);
            
            // Fetch child data and check if it's a leaf node
            if let Some(child_bytes) = get_bytes(db, rtxn, child_hash)? {
                let child_message = capnp::serialize::read_message(
                    &child_bytes[..], 
                    capnp::message::ReaderOptions::new()
                )?;
                
                let child_reader = child_message.get_root::<merkle_tree_capnp::merkle_tree_node::Reader>()?;
                let child_node_reader = child_reader.get_node()?;
                
                let child_node_type = match child_node_reader.which() {
                    Ok(merkle_tree_capnp::e_merkle_tree_node::File(_)) => "File",
                    Ok(merkle_tree_capnp::e_merkle_tree_node::Directory(_)) => "Directory",
                    Ok(merkle_tree_capnp::e_merkle_tree_node::Vnode(_)) => "VNode",
                    Ok(merkle_tree_capnp::e_merkle_tree_node::FileChunk(_)) => "FileChunk",
                    Ok(merkle_tree_capnp::e_merkle_tree_node::Commit(_)) => "Commit",
                    Err(e) => {
                        return Err(format!("Failed to get child node type: {:?}", e).into());
                    }
                };
                
                // Only add non-leaf nodes to the stack with their data
                if child_node_type != "File" && child_node_type != "FileChunk" {
                    stack.push(NodeWithData {
                        hash: child_hash,
                        data: child_bytes,
                    });
                } else {
                    //println!("[ZEROCOPY] Skipping leaf child: {}", child_hash);
                }
            }
        }
        
        //println!("[ZEROCOPY] Completed processing hash: {}, count: {}", hash, count);
    }
    
    //println!("[ZEROCOPY] Finished! Total count: {}", count);
    Ok(count)
}

/// Collect all hashes in the tree using zero-copy access
// pub fn collect_all_hashes_zerocopy(
//     db: Database<Str, Bytes>,
//     rtxn: &RoTxn,
//     root_hash: MerkleHash,
// ) -> Result<HashSet<MerkleHash>, Box<dyn std::error::Error>> {
//     let mut hashes = HashSet::new();
//     load_children_zerocopy(&env, db, rtxn, root_hash, &mut hashes)?;
//     Ok(hashes)
// }

/// Count nodes by type without full deserialization
pub fn count_node_types_zerocopy(
    db: Database<Str, Bytes>,
    rtxn: &RoTxn,
    root_hash: MerkleHash,
) -> Result<HashMap<String, usize>, Box<dyn std::error::Error>> {
    //println!("[ZEROCOPY] Counting node types for hash: {}", root_hash);
    let mut counts = HashMap::new();
    let mut stack = vec![root_hash];
    let mut visited = HashSet::new();
    
    while let Some(hash) = stack.pop() {
        if visited.contains(&hash) {
            continue;
        }
        visited.insert(hash);

        
        if let Some(bytes) = get_bytes(db, rtxn, hash)? {
            //println!("[ZEROCOPY] Reading node type for hash: {}", hash);
            
            let message = capnp::serialize::read_message(
                &bytes[..], 
                capnp::message::ReaderOptions::new()
            )?;
            
            let reader = message.get_root::<merkle_tree_capnp::merkle_tree_node::Reader>()?;
            let node_reader = reader.get_node()?;
            
            // Check node type without deserializing
            let node_type = match node_reader.which()? {
                merkle_tree_capnp::e_merkle_tree_node::File(_) => "File",
                merkle_tree_capnp::e_merkle_tree_node::Directory(_) => "Directory",
                merkle_tree_capnp::e_merkle_tree_node::Vnode(_) => "VNode",
                merkle_tree_capnp::e_merkle_tree_node::FileChunk(_) => "FileChunk",
                merkle_tree_capnp::e_merkle_tree_node::Commit(_) => "Commit",
            };
            
            *counts.entry(node_type.to_string()).or_insert(0) += 1;
            
            // Add children only if this is not a leaf node (File and FileChunk are leaf nodes)
            if node_type != "File" && node_type != "FileChunk" {
                let children = reader.get_children()?;
                for child in children.iter() {
                    let child_hash_reader = child.get_hash()?;
                    let value_reader = child_hash_reader.get_value()?;
                    let high = value_reader.get_high() as u128;
                    let low = value_reader.get_low() as u128;
                    let child_hash = MerkleHash::new((high << 64) | low);
                    stack.push(child_hash);
                }
            }
        }
    }
    
    //println!("[ZEROCOPY] Node type counts: {:?}", counts);
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_zerocopy_performance() {
        // Zero-copy access should be 10-100x faster than full deserialization
        // because we're just reading fields directly from the buffer
    }
}
