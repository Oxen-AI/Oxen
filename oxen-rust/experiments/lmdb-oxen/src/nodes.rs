use std::error::Error;
use std::time::Instant;

use heed::types::{Bytes, SerdeBincode, Str};
use heed::{Database, Env, RoTxn, RwTxn};
use liboxen::model::{merkle_tree::node::MerkleTreeNode, MerkleHash, MerkleTreeNodeType};
use crate::migrate::LMDB_PATH;
use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use crate::lmdb;


// pub fn get_node_recursive() -> Result<MerkleTreeNode, Box<dyn Error>> {

// }


pub fn get_node(db: Database<Str, Bytes>, rtxn: &RoTxn, hash: MerkleHash) -> Result<MerkleTreeNode, Box<dyn Error>> {

    let node = lmdb::get(db, rtxn, hash)?;
    // //println!("{:?}", node);    

    Ok(node.unwrap())
}

pub fn load_children_mine(
        db: Database<Str, Bytes>,
        rtxn: &RoTxn,
        node: &mut MerkleTreeNode,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<(), Box<dyn Error>> {
        //println!("=== ENTERING load_children_mine ===");
        //println!("Node hash: {:?}", node.hash);
        //println!("Node type: {:?}", node.node.node_type());
        // //println!("Node path: {:?}", node.node.path());
        //println!("Current children count: {}", node.children.len());
        //println!("Total hashes in set: {}", hashes.len());
        
        let dtype = node.node.node_type();
        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            //println!("SKIPPING: Node type {:?} is not Commit/Dir/VNode", dtype);
            //println!("=== EXITING load_children_mine (early return) ===\n");
            return Ok(());
        }

        //println!("Inserting hash into set: {:?}", node.hash);
        hashes.insert(node.hash);
        //println!("node children: {:?}", node.children);

        // let leaf_nodes: HashMap<MerkleHash, MerkleTreeNode> = node.children.clone().into_iter().filter(|x| x.node.node_type() == MerkleTreeNodeType::File || x.node.node_type() == MerkleTreeNodeType::FileChunk).map(|x| (x.hash, x)).collect();
        // let non_leaf_nodes: Vec<MerkleTreeNode> = node.children.clone().into_iter().filter(|x| x.node.node_type() != MerkleTreeNodeType::File && x.node.node_type() != MerkleTreeNodeType::FileChunk).collect();
        
        // let child_hashes: HashSet<MerkleHash> = non_leaf_nodes.clone().into_iter().map(|x| x.hash).collect();
        // //println!("Fetching {} child hashes from DB: {:?}", child_hashes.len(), child_hashes);
        
        // let start = Instant::now();
        // let mut children_nodes = lmdb::get_many(db, rtxn, child_hashes)?;
        // let duration = start.elapsed();
        // println!("loaded {} nodes from lmdb in {:?}", children_nodes.len(), duration);
        // let _ = leaf_nodes.into_iter().map(|(hash, node)| children_nodes.insert(hash, node));
        // //println!("Retrieved {} children from DB", children_nodes.len());

        let children_nodes = lmdb::get(db, rtxn, node.hash)?.unwrap();



        for child in children_nodes.children {
            //println!("\n--- Processing child {}/{} ---", idx + 1, children_nodes.len());
            //println!("Child key: {}", key);
            let mut child = child.to_owned();
            //println!("Child hash: {:?}", child.hash);
            //println!("Child type: {:?}", child.node.node_type());
            // //println!("Child path: {:?}", child.node.path());
            
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    //println!("RECURSING into child: {:?} (type: {:?})", child.hash, child.node.node_type());
                    load_children_mine(db, rtxn, &mut child, hashes)?;
                    //println!("RETURNED from recursion for child: {:?}", child.hash);
                    //println!("Child now has {} children after recursion", child.children.len());
                    node.children.push(child);
                }
                // FileChunks and Files are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    //println!("LEAF NODE: Adding child {:?} (type: {:?}) directly", child.hash, child.node.node_type());
                    node.children.push(child);
                }
            }
        }

        //println!("\n=== EXITING load_children_mine ===");
        //println!("Final children count for node {:?}: {}", node.hash, node.children.len());
        //println!("Total hashes in set: {}\n", hashes.len());
        Ok(())
    }
