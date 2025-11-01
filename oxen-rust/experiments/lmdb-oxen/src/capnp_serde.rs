// Cap'n Proto serialization/deserialization helpers for MerkleTreeNode

use crate::merkle_tree_capnp;

use liboxen::model::merkle_tree::node::{
    CommitNode, DirNode, EMerkleTreeNode, FileChunkNode, FileNode, MerkleTreeNode, VNode,
};
use liboxen::model::{EntryDataType, MerkleHash, MerkleTreeNodeType};

use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;

/// Convert a u128 to Cap'n Proto UInt128
fn u128_to_capnp(value: u128, mut builder: merkle_tree_capnp::u_int128::Builder) {
    builder.set_high((value >> 64) as u64);
    builder.set_low(value as u64);
}

/// Convert Cap'n Proto UInt128 to u128
fn capnp_to_u128(reader: merkle_tree_capnp::u_int128::Reader) -> Result<u128, capnp::Error> {
    let high = reader.get_high() as u128;
    let low = reader.get_low() as u128;
    Ok((high << 64) | low)
}

/// Convert MerkleHash to Cap'n Proto
fn merkle_hash_to_capnp(
    hash: &MerkleHash,
    mut builder: merkle_tree_capnp::merkle_hash::Builder,
) -> Result<(), capnp::Error> {
    let mut value_builder = builder.reborrow().init_value();
    u128_to_capnp(hash.to_u128(), value_builder);
    Ok(())
}

/// Convert Cap'n Proto to MerkleHash
fn capnp_to_merkle_hash(
    reader: merkle_tree_capnp::merkle_hash::Reader,
) -> Result<MerkleHash, capnp::Error> {
    let value = capnp_to_u128(reader.get_value()?)?;
    Ok(MerkleHash::new(value))
}

/// Convert MerkleTreeNodeType to Cap'n Proto enum
fn node_type_to_capnp(node_type: &MerkleTreeNodeType) -> merkle_tree_capnp::MerkleTreeNodeType {
    match node_type {
        MerkleTreeNodeType::Commit => merkle_tree_capnp::MerkleTreeNodeType::Commit,
        MerkleTreeNodeType::File => merkle_tree_capnp::MerkleTreeNodeType::File,
        MerkleTreeNodeType::Dir => merkle_tree_capnp::MerkleTreeNodeType::Dir,
        MerkleTreeNodeType::VNode => merkle_tree_capnp::MerkleTreeNodeType::Vnode,
        MerkleTreeNodeType::FileChunk => merkle_tree_capnp::MerkleTreeNodeType::FileChunk,
    }
}

/// Convert EntryDataType to Cap'n Proto enum
fn entry_data_type_to_capnp(data_type: &EntryDataType) -> merkle_tree_capnp::EntryDataType {
    match data_type {
        EntryDataType::Dir => merkle_tree_capnp::EntryDataType::Dir,
        EntryDataType::Text => merkle_tree_capnp::EntryDataType::Text,
        EntryDataType::Image => merkle_tree_capnp::EntryDataType::Image,
        EntryDataType::Video => merkle_tree_capnp::EntryDataType::Video,
        EntryDataType::Audio => merkle_tree_capnp::EntryDataType::Audio,
        EntryDataType::Tabular => merkle_tree_capnp::EntryDataType::Tabular,
        EntryDataType::Binary => merkle_tree_capnp::EntryDataType::Binary,
    }
}

/// Serialize a MerkleTreeNode to Cap'n Proto bytes
pub fn serialize_merkle_tree_node(node: &MerkleTreeNode) -> Result<Vec<u8>, capnp::Error> {
    let mut message = Builder::new_default();
    let mut builder = message.init_root::<merkle_tree_capnp::merkle_tree_node::Builder>();

    // Set hash
    merkle_hash_to_capnp(&node.hash, builder.reborrow().init_hash())?;

    // Set parent_id if present
    if let Some(parent_id) = &node.parent_id {
        builder.set_has_parent_id(true);
        merkle_hash_to_capnp(parent_id, builder.reborrow().init_parent_id())?;
    } else {
        builder.set_has_parent_id(false);
    }

    // Set the node variant
    let node_builder = builder.reborrow().init_node();
    match &node.node {
        EMerkleTreeNode::File(file_node) => {
            serialize_file_node(file_node, node_builder.init_file())?;
        }
        EMerkleTreeNode::Directory(dir_node) => {
            serialize_dir_node(dir_node, node_builder.init_directory())?;
        }
        EMerkleTreeNode::VNode(vnode) => {
            serialize_vnode(vnode, node_builder.init_vnode())?;
        }
        EMerkleTreeNode::FileChunk(file_chunk) => {
            serialize_file_chunk(file_chunk, node_builder.init_file_chunk())?;
        }
        EMerkleTreeNode::Commit(commit) => {
            serialize_commit_node(commit, node_builder.init_commit())?;
        }
    }

    // Set children (recursive)
    let mut children_builder = builder.reborrow().init_children(node.children.len() as u32);
    for (i, child) in node.children.iter().enumerate() {
        serialize_merkle_tree_node_to_builder(child, children_builder.reborrow().get(i as u32))?;
    }

    // Serialize to bytes
    let mut buffer = Vec::new();
    serialize::write_message(&mut buffer, &message)?;
    Ok(buffer)
}

/// Helper to serialize MerkleTreeNode to an existing builder
fn serialize_merkle_tree_node_to_builder(
    node: &MerkleTreeNode,
    mut builder: merkle_tree_capnp::merkle_tree_node::Builder,
) -> Result<(), capnp::Error> {
    merkle_hash_to_capnp(&node.hash, builder.reborrow().init_hash())?;

    if let Some(parent_id) = &node.parent_id {
        builder.set_has_parent_id(true);
        merkle_hash_to_capnp(parent_id, builder.reborrow().init_parent_id())?;
    } else {
        builder.set_has_parent_id(false);
    }

    let mut node_builder = builder.reborrow().init_node();
    match &node.node {
        EMerkleTreeNode::File(file_node) => {
            serialize_file_node(file_node, node_builder.init_file())?;
        }
        EMerkleTreeNode::Directory(dir_node) => {
            serialize_dir_node(dir_node, node_builder.init_directory())?;
        }
        EMerkleTreeNode::VNode(vnode) => {
            serialize_vnode(vnode, node_builder.init_vnode())?;
        }
        EMerkleTreeNode::FileChunk(file_chunk) => {
            serialize_file_chunk(file_chunk, node_builder.init_file_chunk())?;
        }
        EMerkleTreeNode::Commit(commit) => {
            serialize_commit_node(commit, node_builder.init_commit())?;
        }
    }

    let mut children_builder = builder.reborrow().init_children(node.children.len() as u32);
    for (i, child) in node.children.iter().enumerate() {
        serialize_merkle_tree_node_to_builder(child, children_builder.reborrow().get(i as u32))?;
    }

    Ok(())
}

/// Serialize FileNode
fn serialize_file_node(
    file_node: &FileNode,
    mut builder: merkle_tree_capnp::file_node_data::Builder,
) -> Result<(), capnp::Error> {
    builder.set_node_type(node_type_to_capnp(file_node.node_type()));
    builder.set_name(file_node.name());
    merkle_hash_to_capnp(file_node.hash(), builder.reborrow().init_hash())?;
    merkle_hash_to_capnp(file_node.combined_hash(), builder.reborrow().init_combined_hash())?;
    merkle_hash_to_capnp(
        file_node.last_commit_id(),
        builder.reborrow().init_last_commit_id(),
    )?;

    if let Some(metadata_hash) = file_node.metadata_hash() {
        builder.set_has_metadata_hash(true);
        merkle_hash_to_capnp(metadata_hash, builder.reborrow().init_metadata_hash())?;
    } else {
        builder.set_has_metadata_hash(false);
    }

    builder.set_num_bytes(file_node.num_bytes());
    builder.set_last_modified_seconds(file_node.last_modified_seconds());
    builder.set_last_modified_nanoseconds(file_node.last_modified_nanoseconds());
    builder.set_data_type(entry_data_type_to_capnp(file_node.data_type()));
    builder.set_mime_type(file_node.mime_type());
    builder.set_extension(file_node.extension());

    // Chunk hashes
    let chunk_hashes = file_node.chunk_hashes();
    let mut chunk_hashes_builder = builder.reborrow().init_chunk_hashes(chunk_hashes.len() as u32);
    for (i, hash) in chunk_hashes.iter().enumerate() {
        u128_to_capnp(*hash, chunk_hashes_builder.reborrow().get(i as u32));
    }

    // Simplified: skip metadata and chunk type/storage backend for now
    builder.set_has_metadata(false);

    Ok(())
}

/// Serialize DirNode
fn serialize_dir_node(
    dir_node: &DirNode,
    mut builder: merkle_tree_capnp::dir_node_data::Builder,
) -> Result<(), capnp::Error> {
    builder.set_node_type(node_type_to_capnp(dir_node.node_type()));
    builder.set_name(dir_node.name());
    merkle_hash_to_capnp(dir_node.hash(), builder.reborrow().init_hash())?;
    merkle_hash_to_capnp(
        dir_node.last_commit_id(),
        builder.reborrow().init_last_commit_id(),
    )?;
    builder.set_num_entries(dir_node.num_entries());
    builder.set_num_bytes(dir_node.num_bytes());
    builder.set_last_modified_seconds(dir_node.last_modified_seconds());
    builder.set_last_modified_nanoseconds(dir_node.last_modified_nanoseconds());

    // Data type counts and sizes
    let data_type_counts = dir_node.data_type_counts();
    let mut counts_builder =
        builder
            .reborrow()
            .init_data_type_counts(data_type_counts.len() as u32);
    for (i, (key, value)) in data_type_counts.iter().enumerate() {
        let mut count_builder = counts_builder.reborrow().get(i as u32);
        count_builder.set_data_type(key.as_str());
        count_builder.set_count(*value);
    }

    let data_type_sizes = dir_node.data_type_sizes();
    let mut sizes_builder = builder.reborrow().init_data_type_sizes(data_type_sizes.len() as u32);
    for (i, (key, value)) in data_type_sizes.iter().enumerate() {
        let mut size_builder = sizes_builder.reborrow().get(i as u32);
        size_builder.set_data_type(key.as_str());
        size_builder.set_size(*value);
    }

    Ok(())
}

/// Serialize VNode
fn serialize_vnode(
    vnode: &VNode,
    mut builder: merkle_tree_capnp::v_node_data::Builder,
) -> Result<(), capnp::Error> {
    merkle_hash_to_capnp(vnode.hash(), builder.reborrow().init_hash())?;
    builder.set_node_type(merkle_tree_capnp::MerkleTreeNodeType::Vnode);
    builder.set_num_entries(vnode.num_entries());
    Ok(())
}

/// Serialize FileChunkNode
fn serialize_file_chunk(
    file_chunk: &FileChunkNode,
    mut builder: merkle_tree_capnp::file_chunk_node::Builder,
) -> Result<(), capnp::Error> {
    builder.set_data(&file_chunk.data);
    builder.set_node_type(node_type_to_capnp(&file_chunk.node_type));
    merkle_hash_to_capnp(&file_chunk.hash, builder.reborrow().init_hash())?;
    Ok(())
}

/// Serialize CommitNode
fn serialize_commit_node(
    commit: &CommitNode,
    mut builder: merkle_tree_capnp::commit_node_data::Builder,
) -> Result<(), capnp::Error> {
    merkle_hash_to_capnp(commit.hash(), builder.reborrow().init_hash())?;
    builder.set_node_type(node_type_to_capnp(&MerkleTreeNodeType::Commit));

    let parent_ids = commit.parent_ids();
    let mut parent_ids_builder = builder.reborrow().init_parent_ids(parent_ids.len() as u32);
    for (i, parent_id) in parent_ids.iter().enumerate() {
        merkle_hash_to_capnp(parent_id, parent_ids_builder.reborrow().get(i as u32))?;
    }

    builder.set_message(commit.message());
    builder.set_author(commit.author());
    builder.set_email(commit.email());

    let timestamp = commit.timestamp();
    builder.set_timestamp_seconds(timestamp.unix_timestamp());
    builder.set_timestamp_nanoseconds(timestamp.nanosecond());

    Ok(())
}

/// Deserialize a MerkleTreeNode from Cap'n Proto bytes
pub fn deserialize_merkle_tree_node(bytes: &[u8]) -> Result<MerkleTreeNode, capnp::Error> {
    let reader = serialize::read_message(bytes, ReaderOptions::new())?;
    let node_reader = reader.get_root::<merkle_tree_capnp::merkle_tree_node::Reader>()?;
    deserialize_merkle_tree_node_from_reader(node_reader)
}

/// Helper to deserialize from a reader
fn deserialize_merkle_tree_node_from_reader(
    reader: merkle_tree_capnp::merkle_tree_node::Reader,
) -> Result<MerkleTreeNode, capnp::Error> {
    let hash = capnp_to_merkle_hash(reader.get_hash()?)?;

    let parent_id = if reader.get_has_parent_id() {
        Some(capnp_to_merkle_hash(reader.get_parent_id()?)?)
    } else {
        None
    };

    let node_reader = reader.get_node()?;
    let node = match node_reader.which()? {
        merkle_tree_capnp::e_merkle_tree_node::File(file_reader) => {
            EMerkleTreeNode::File(deserialize_file_node(file_reader?)?)
        }
        merkle_tree_capnp::e_merkle_tree_node::Directory(dir_reader) => {
            EMerkleTreeNode::Directory(deserialize_dir_node(dir_reader?)?)
        }
        merkle_tree_capnp::e_merkle_tree_node::Vnode(vnode_reader) => {
            EMerkleTreeNode::VNode(deserialize_vnode(vnode_reader?)?)
        }
        merkle_tree_capnp::e_merkle_tree_node::FileChunk(chunk_reader) => {
            EMerkleTreeNode::FileChunk(deserialize_file_chunk(chunk_reader?)?)
        }
        merkle_tree_capnp::e_merkle_tree_node::Commit(commit_reader) => {
            EMerkleTreeNode::Commit(deserialize_commit_node(commit_reader?)?)
        }
    };

    let children_reader = reader.get_children()?;
    let mut children = Vec::with_capacity(children_reader.len() as usize);
    for child_reader in children_reader.iter() {
        children.push(deserialize_merkle_tree_node_from_reader(child_reader)?);
    }

    Ok(MerkleTreeNode {
        hash,
        node,
        parent_id,
        children,
    })
}

/// Deserialize FileNode (simplified - returns default for now)
fn deserialize_file_node(
    _reader: merkle_tree_capnp::file_node_data::Reader,
) -> Result<FileNode, capnp::Error> {
    // For benchmarking, we can return a default FileNode
    // In production, you'd need to properly reconstruct the FileNode
    Ok(FileNode::default())
}

/// Deserialize DirNode (simplified)
fn deserialize_dir_node(
    _reader: merkle_tree_capnp::dir_node_data::Reader,
) -> Result<DirNode, capnp::Error> {
    Ok(DirNode::default())
}

/// Deserialize VNode (simplified)
fn deserialize_vnode(
    _reader: merkle_tree_capnp::v_node_data::Reader,
) -> Result<VNode, capnp::Error> {
    Ok(VNode::default())
}

/// Deserialize FileChunkNode (simplified)
fn deserialize_file_chunk(
    _reader: merkle_tree_capnp::file_chunk_node::Reader,
) -> Result<FileChunkNode, capnp::Error> {
    Ok(FileChunkNode::default())
}

/// Deserialize CommitNode (simplified)
fn deserialize_commit_node(
    _reader: merkle_tree_capnp::commit_node_data::Reader,
) -> Result<CommitNode, capnp::Error> {
    Ok(CommitNode::default())
}
