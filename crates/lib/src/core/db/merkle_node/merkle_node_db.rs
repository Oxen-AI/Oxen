/*
Write a db that is optimized for opening, finding by hash, listing.

Rocks db is too slow. It was taking ~100ms to open a db, and if we have > 10 vnodes,
that means we are taking > 1 second to open before doing any operations.

We can make this faster by using a simple file format.

Writing happens once at commit, then we read many times from the server and status.

Is also already sharded and optimized in the tree structure.
Reading, find by hash, listing is high throughput.

On Disk Format:

All nodes are stored in .oxen/tree/{NODE_HASH} and contain two files:
- node: the metadata for the node and a lookup table for all the children
- data: the serialized nodes

node file format:
- node data
- data-type,hash-int,data-offset,data-length

children file format:
- data blobs


For example, data for a vnode of hash 1234 with two children:

.oxen/tree/1234/node
    0 # data length
    4 # data

    0 # file data type
    1235 # hash
    0 # data offset
    100 # data length

    1 # dir data type
    1236 # hash
    100 # data offset
    100 # data length

.oxen/tree/1234/children
    {file data node}
    {dir data node}
*/

use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::constants;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::MerkleHash;
use crate::model::merkle_tree::node_type::InvalidMerkleTreeNodeType;
use crate::util;

use crate::model::merkle_tree::node::{
    EMerkleTreeNode, MerkleTreeNode, MerkleTreeNodeType, TMerkleTreeNode,
};

const NODE_FILE: &str = "node";
const CHILDREN_FILE: &str = "children";

/// An absolute path to the directory for the Merkle node's `node` and `children` files.
pub fn node_db_path(repo: &LocalRepository, hash: &MerkleHash) -> PathBuf {
    let dir_prefix = hash.to_hex_hash().node_db_prefix();
    repo.path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(constants::NODES_DIR)
        .join(dir_prefix)
}

/// Errors that the Merkle node database can encounter when reading and writing nodes.
#[derive(Debug, thiserror::Error)]
pub enum MerkleDbError {
    // Errors encountered in the operation of the custom file format based Merkle tree store.
    #[error("Must call open before closing")]
    CloseBeforeOpen,
    #[error("Cannot write to read-only db")]
    ReadOnly,
    #[error("Cannot write size after writing data")]
    IllegalOperationWriteSizeFirst,
    #[error("Must call open before writing")]
    WriteBeforeOpen,
    #[error("Must call open before reading")]
    ReadBeforeOpen,
    // wrappers
    #[error("Error writing to a node or children file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Cannot encode a Merkle node: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("Cannot decode a Merkle node: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("{0}")]
    TypeMismatch(#[from] InvalidMerkleTreeNodeType),
    #[error("Failed to create directory: {0}")]
    DirCreate(Box<OxenError>), // TODO: replace with FsError from upcoming refactoring PR
    #[error("Failed to open file: {0}")]
    Open(Box<OxenError>), // TODO: replace with FsError from upcoming refactoring PR
    #[error("Invalid hash in merkle tarball path: {0}")]
    ParseHash(#[from] std::num::ParseIntError),
    #[error("Filesystem operation failed during merkle transport: {0}")]
    FsTransport(Box<OxenError>), // TODO: replace with FsError from upcoming refactoring PR
    #[error("Could not read entries from merkle tree tar archive: {0}")]
    CannotReadMerkle(std::io::Error),
}

impl MerkleDbError {
    fn dir_create(err: OxenError) -> Self {
        Self::DirCreate(Box::new(err))
    }

    fn open(err: OxenError) -> Self {
        Self::Open(Box::new(err))
    }
}

pub struct MerkleNodeLookup {
    pub data_type: u8,
    pub parent_id: u128,
    pub data: Vec<u8>,
    pub num_children: u64,
    // hash -> (dtype, offset, length)
    pub offsets: Vec<(u128, (u8, u64, u64))>,
}

impl MerkleNodeLookup {
    pub fn load(node_table_file: &mut File) -> Result<Self, MerkleDbError> {
        // log::debug!("MerkleNodeLookup.load() {:?}", node_table_file);
        // Read the whole node into memory
        let mut file_data = Vec::new();
        node_table_file.read_to_end(&mut file_data)?;
        // log::debug!(
        //     "MerkleNodeLookup.load() read file_data: {}",
        //     file_data.len()
        // );

        // Create a cursor to iterate over data
        let mut cursor = std::io::Cursor::new(file_data);

        // Read the data type
        let mut buffer = [0u8; 1]; // u8 is 1 byte
        cursor.read_exact(&mut buffer)?;
        let node_data_type = u8::from_le_bytes(buffer);
        // log::debug!(
        //     "MerkleNodeLookup.load() data_type: {:?}",
        //     MerkleTreeNodeType::from_u8(node_data_type)
        // );

        // Read the parent id
        let mut buffer = [0u8; 16]; // u128 is 16 bytes
        cursor.read_exact(&mut buffer)?;
        let parent_id = u128::from_le_bytes(buffer);
        // log::debug!("MerkleNodeLookup.load() parent_id: {:x}", parent_id);

        // Read the length of the node data
        let mut buffer = [0u8; 4]; // u32 is 4 bytes
        cursor.read_exact(&mut buffer)?;
        let data_len = u32::from_le_bytes(buffer);
        // log::debug!("MerkleNodeLookup.load() data_len: {}", data_len);

        // Read the length of the data and save buffer
        let mut buffer = vec![0u8; data_len as usize];
        cursor.read_exact(&mut buffer)?;
        let data = buffer;
        // log::debug!("MerkleNodeLookup.load() read data: {}", data.len());

        // Read the map of offsets
        let mut offsets: Vec<(u128, (u8, u64, u64))> = Vec::new();
        let mut dtype_buffer = [0u8; 1]; // data-type u8 is 1 byte
        let mut hash_buffer = [0u8; 16]; // hash u128 is 16 bytes
        let mut offset_buffer = [0u8; 8]; // data-offset u64 is 8 bytes
        let mut len_buffer = [0u8; 8]; // data-length u64 is 8 bytes

        // Will loop until we hit an EOF error
        // let mut i = 0;
        while cursor.read_exact(&mut dtype_buffer).is_ok() {
            // log::debug!("MerkleNodeLookup.load() --reading-- {}", i);

            let data_type = u8::from_le_bytes(dtype_buffer);
            // log::debug!(
            //     "MerkleNodeLookup.load() got data_type {:?}",
            //     MerkleTreeNodeType::from_u8(data_type)
            // );

            // Read the hash
            cursor.read_exact(&mut hash_buffer)?;
            let hash = u128::from_le_bytes(hash_buffer);
            // log::debug!("MerkleNodeLookup.load() got hash {:x}", hash);

            // Read the offset
            cursor.read_exact(&mut offset_buffer)?;
            let data_offset = u64::from_le_bytes(offset_buffer);
            // log::debug!("MerkleNodeLookup.load() got data_offset {}", data_offset);

            // Read the length
            cursor.read_exact(&mut len_buffer)?;
            let data_len = u64::from_le_bytes(len_buffer);
            // log::debug!("MerkleNodeLookup.load() got data_len {}", data_len);

            offsets.push((hash, (data_type, data_offset, data_len)));
            // i += 1;
        }

        let num_children = offsets.len() as u64;
        // log::debug!(
        //     "MerkleNodeLookup.load() parent_id {:x} num_children {}",
        //     parent_id,
        //     num_children
        // );
        Ok(Self {
            data_type: node_data_type,
            parent_id,
            data,
            num_children,
            offsets,
        })
    }
}

pub struct MerkleNodeDB {
    pub dtype: MerkleTreeNodeType,
    pub node_id: MerkleHash,
    pub parent_id: Option<MerkleHash>,
    read_only: bool,
    path: PathBuf,
    node_file: Option<File>,
    children_file: Option<File>,
    lookup: Option<MerkleNodeLookup>,
    data: Vec<u8>,
    num_children: u64,
    data_offset: u64,
}

impl MerkleNodeDB {
    pub fn num_children(&self) -> u64 {
        if let Some(lookup) = &self.lookup {
            return lookup.num_children;
        }

        self.num_children
    }

    pub fn data(&self) -> Vec<u8> {
        if let Some(lookup) = &self.lookup {
            return lookup.data.to_owned();
        }

        self.data.to_owned()
    }

    pub fn node(&self) -> Result<EMerkleTreeNode, MerkleDbError> {
        let node = Self::to_node(self.dtype, &self.data())?;
        Ok(node)
    }

    pub fn to_node(
        dtype: MerkleTreeNodeType,
        data: &[u8],
    ) -> Result<EMerkleTreeNode, rmp_serde::decode::Error> {
        EMerkleTreeNode::from_type_and_bytes(dtype, data)
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_owned()
    }

    pub fn exists(repo: &LocalRepository, hash: &MerkleHash) -> bool {
        let db_path = node_db_path(repo, hash);
        db_path.join(NODE_FILE).exists() && db_path.join(CHILDREN_FILE).exists()
    }

    pub fn open_read_only(
        repo: &LocalRepository,
        hash: &MerkleHash,
    ) -> Result<Self, MerkleDbError> {
        let path = node_db_path(repo, hash);
        Self::open(path, true)
    }

    pub fn open_read_write<N: TMerkleTreeNode>(
        repo: &LocalRepository,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self, MerkleDbError> {
        let path = node_db_path(repo, &node.hash());
        if !path.exists() {
            util::fs::create_dir_all(&path).map_err(MerkleDbError::dir_create)?;
        }
        log::debug!("open_read_write merkle node db at {}", path.display());
        let mut db = Self::open(path, false)?;
        db.write_node(node, parent_id)?;
        Ok(db)
    }

    pub fn open(path: impl AsRef<Path>, read_only: bool) -> Result<Self, MerkleDbError> {
        let path = path.as_ref();

        // mkdir if not exists
        if !path.exists() {
            util::fs::create_dir_all(path).map_err(MerkleDbError::dir_create)?;
        }

        let node_path = path.join(NODE_FILE);
        let children_path = path.join(CHILDREN_FILE);

        // log::debug!(
        //     "Opening merkle node db read_only {} at {}",
        //     read_only,
        //     path.display()
        // );
        let (lookup, node_file, children_file): (
            Option<MerkleNodeLookup>,
            Option<File>,
            Option<File>,
        ) = if read_only {
            let mut node_file = util::fs::open_file(node_path).map_err(MerkleDbError::open)?;
            let children_file = util::fs::open_file(children_path).map_err(MerkleDbError::open)?;
            // log::debug!("Opened merkle node db read_only at {}", path.display());
            (
                Some(MerkleNodeLookup::load(&mut node_file)?),
                Some(node_file),
                Some(children_file),
            )
        } else {
            // self.lookup does not exist yet if we are writing (only write once)
            let node_file = File::create(node_path)?;
            let children_file = File::create(children_path)?;
            (None, Some(node_file), Some(children_file))
        };

        let dtype = match lookup.as_ref() {
            Some(l) => MerkleTreeNodeType::from_u8(l.data_type)?,
            None => MerkleTreeNodeType::Commit,
        };

        let parent_id = lookup.as_ref().map(|l| l.parent_id);
        Ok(Self {
            read_only,
            path: path.to_path_buf(),
            node_file,
            children_file,
            lookup,
            data: vec![],
            num_children: 0,
            dtype,
            node_id: MerkleHash::new(0),
            parent_id: parent_id.map(MerkleHash::new),
            data_offset: 0,
        })
    }

    /// Closes the open node and children file handles.
    /// WARNING: Sets the internal node_file, children_file, and lookup to None.
    pub fn close(&mut self) -> Result<(), MerkleDbError> {
        if let Some(node_file) = &mut self.node_file {
            node_file.flush()?;
            node_file.sync_data()?;
        } else {
            return Err(MerkleDbError::CloseBeforeOpen);
        }

        if let Some(children_file) = &mut self.children_file {
            children_file.flush()?;
            children_file.sync_data()?;
        } else {
            return Err(MerkleDbError::CloseBeforeOpen);
        }

        self.node_file = None;
        self.children_file = None;
        self.lookup = None;
        Ok(())
    }

    /// Write the base node info.
    /// WARNING: Sets the internal dtype, node_id, parent_id of `self` to the values from `node`.
    fn write_node<N: TMerkleTreeNode>(
        &mut self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<(), MerkleDbError> {
        if self.read_only {
            return Err(MerkleDbError::ReadOnly);
        }

        if self.data_offset > 0 {
            return Err(MerkleDbError::IllegalOperationWriteSizeFirst);
        }

        let Some(node_file) = self.node_file.as_mut() else {
            return Err(MerkleDbError::WriteBeforeOpen);
        };
        // log::debug!("write_node node: {}", node);

        // Write data type
        node_file.write_all(&node.node_type().to_u8().to_le_bytes())?;

        // Write parent id
        if let Some(parent_id) = parent_id {
            node_file.write_all(&parent_id.to_le_bytes())?;
        } else {
            node_file.write_all(&[0u8; 16])?;
        }

        // Write data length
        let buf = node.to_msgpack_bytes()?;
        let data_len = buf.len() as u32;
        node_file.write_all(&data_len.to_le_bytes())?;
        // log::debug!("write_node Wrote data length {}", data_len);

        // Write data
        node_file.write_all(&buf)?;

        self.dtype = node.node_type();
        self.node_id = node.hash();
        self.parent_id = parent_id;
        // log::debug!(
        //     "write_node wrote id {} dtype: {:?}",
        //     node.hash(),
        //     node.node_type()
        // );
        Ok(())
    }

    pub fn add_child<N: TMerkleTreeNode>(&mut self, item: &N) -> Result<(), MerkleDbError> {
        if self.read_only {
            return Err(MerkleDbError::ReadOnly);
        }

        let Some(node_file) = self.node_file.as_mut() else {
            return Err(MerkleDbError::WriteBeforeOpen);
        };
        let Some(children_file) = self.children_file.as_mut() else {
            return Err(MerkleDbError::WriteBeforeOpen);
        };

        let buf = item.to_msgpack_bytes()?;
        let data_len = buf.len() as u64;
        // log::debug!("--add_child-- node_file {:?}", node_file);
        // log::debug!("--add_child-- dtype {:?}", item.dtype());
        // log::debug!("--add_child-- hash {:x}", item.id());
        // log::debug!("--add_child-- data_offset {}", self.data_offset);
        // log::debug!("--add_child-- data_len {}", data_len);
        // log::debug!("--add_child-- child {}", item);

        node_file.write_all(&item.node_type().to_u8().to_le_bytes())?;
        node_file.write_all(&item.hash().to_le_bytes())?; // id of child
        node_file.write_all(&self.data_offset.to_le_bytes())?;
        node_file.write_all(&data_len.to_le_bytes())?;

        // log::debug!("--add_child-- children_file {:?}", children_file);
        // log::debug!("--add_child-- buf.len() {}", buf.len());
        children_file.write_all(&buf)?;
        self.data_offset += data_len;

        Ok(())
    }

    /*
    pub fn get<D>(&self, hash: u128) -> Result<D, OxenError>
    where
        D: TMerkleTreeNode + de::DeserializeOwned,
    {
        let Some(lookup) = self.lookup.as_ref() else {
            return Err(MerkleError::ReadBeforeOpen);
        };

        let Some(mut children_file) = self.children_file.as_ref() else {
            return Err(MerkleError::WriteBeforeOpen);
        };

        // Find the offset and length of the data
        let Some(offset) = lookup.offsets.get(&hash) else {
            let err_str = format!(
                "Cannot find hash in merkle node db: {:x} in {} offsets",
                hash,
                lookup.offsets.len()
            );
            return Err(OxenError::basic_str(err_str));
        };

        // Read from the data table at the offset
        // Allocate the exact amount of data
        let mut data = vec![0; offset.2 as usize];
        children_file.seek(SeekFrom::Start(offset.1))?;
        children_file.read_exact(&mut data)?;

        let val: D = rmp_serde::from_slice(&data).map_err(|e| {
            OxenError::basic_str(format!(
                "MerkleNodeDB.get({}): Error deserializing data: {:?}",
                hash, e
            ))
        })?;
        Ok(val)
    }
    */

    pub fn map(&mut self) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, MerkleDbError> {
        // log::debug!("Loading merkle node db map");
        let Some(lookup) = self.lookup.as_ref() else {
            return Err(MerkleDbError::ReadBeforeOpen);
        };
        let Some(children_file) = self.children_file.as_mut() else {
            return Err(MerkleDbError::WriteBeforeOpen);
        };

        // Parse the node parent id
        let data_type = MerkleTreeNodeType::from_u8(lookup.data_type)?;
        let parent_id = MerkleTreeNode::deserialize_id(&lookup.data, data_type)?;

        let mut file_data = Vec::new();
        children_file.read_to_end(&mut file_data)?;
        // log::debug!("Loading merkle node db map got {} bytes", file_data.len());

        let mut ret: Vec<(MerkleHash, MerkleTreeNode)> =
            Vec::with_capacity(lookup.num_children as usize);

        let mut cursor = std::io::Cursor::new(file_data);
        // Iterate over offsets and read the data
        for (hash, (dtype, offset, len)) in lookup.offsets.iter() {
            // log::debug!("Loading dtype {:?}", MerkleTreeNodeType::from_u8(*dtype));
            // log::debug!("Loading offset {}", offset);
            // log::debug!("Loading len {}", len);
            cursor.seek(SeekFrom::Start(*offset))?;
            let mut data = vec![0; *len as usize];
            cursor.read_exact(&mut data)?;
            let dtype = MerkleTreeNodeType::from_u8(*dtype)?;
            let node = MerkleTreeNode {
                parent_id: Some(parent_id),
                hash: MerkleHash::new(*hash),
                node: Self::to_node(dtype, &data)?,
                children: Vec::new(),
            };
            // log::debug!("Loaded node {:?}", node);
            ret.push((MerkleHash::new(*hash), node));
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test;

    #[test]
    fn test_merkle_node_db() -> Result<(), OxenError> {
        test::run_empty_dir_test(|_dir| {
            /*
            let vnode = VNode {
                id: 1234,
                dtype: MerkleTreeNodeType::VNode,
            };
            let mut writer_db = MerkleNodeDB::open_read_write(dir, &vnode, 2)?;

            let node_1 = DirNode {
                path: "test".to_string(),
            };
            writer_db.add_child(1234, MerkleTreeNodeType::Dir, &node_1)?;

            let node_2 = DirNode {
                path: "image".to_string(),
            };
            writer_db.add_child(5678, MerkleTreeNodeType::Dir, &node_2)?;
            writer_db.close()?;

            let reader_db = MerkleNodeDB::open_read_only(dir)?;

            let size = reader_db.num_children();
            assert_eq!(size, 2);

            let data: DirNode = reader_db.get(1234)?;
            assert_eq!(data, node_1);

            let data: DirNode = reader_db.get(5678)?;
            assert_eq!(data, node_2);
            */
            Ok(())
        })
    }
}
