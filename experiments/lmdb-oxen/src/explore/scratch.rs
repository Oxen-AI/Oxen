use std::path::Path;

use std::path::PathBuf;

use liboxen::error::OxenError;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error as ThisError;

use xxhash_rust::xxh3::xxh3_128;

use crate::explore::paths::AbsolutePath;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Oxen(#[from] OxenError),
}

// ==== DESIGN ====

enum MerkleError {
    HashDoesNotExist,
    HashDoesNotEqualContent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hash(u128);

/// Unwraps a hash into its unsigned 128 bit value.
impl From<Hash> for u128 {
    fn from(value: Hash) -> Self {
        value.0
    }
}

pub trait HasHash {
    fn hash(&self) -> Hash;
}

impl HasHash for Hash {
    fn hash(&self) -> Hash {
        *self
    }
}

impl Hash {
    /// Only way to create a new `Hash` instance is to calculate a hash value from file content.
    pub fn new(contents: &[u8]) -> Self {
        Hash(xxh3_128(contents))
    }

    /// Things that have Hashes can be combined into a new Hash value.
    pub fn hash_of_hashes<'a, N: HasHash + 'a>(children: impl Iterator<Item = &'a N>) -> Hash {
        let hashes: Vec<u8> = children
            .map(|h| h.hash().0)
            .flat_map(|h| h.to_le_bytes())
            .collect();
        Self::new(&hashes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HexHash(String);

#[derive(Debug, ThisError)]
#[error("{0} is not a valid hex-encoded u128 value")]
pub struct NotAValidHexHash<'a>(&'a str);

impl<'a> TryFrom<&'a str> for Hash {
    type Error = NotAValidHexHash<'a>;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let hash = u128::from_str_radix(value, 16).map_err(|_| NotAValidHexHash(value))?;
        Ok(Hash(hash))
    }
}

/// Only valid way to create a hex-encoded hash value is from an existing `Hash` instance.
impl From<Hash> for HexHash {
    fn from(value: Hash) -> Self {
        HexHash(format!("{:032x}", value.0))
    }
}

/// Safe conversion since we can only create a `HexHash` from a `Hash`.
impl From<&HexHash> for Hash {
    fn from(value: &HexHash) -> Self {
        value
            .0
            .as_str()
            .try_into()
            .expect("Invariant violated! A HexHash was made that bypassed safe creation!")
    }
}

/// Writes as a hex-encoded string.
impl std::fmt::Display for HexHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

trait Node {
    fn hash(&self) -> Hash;
    fn name(&self) -> &str;
}

pub trait MerkleMetadataStore {
    type Node;

    /// If true, then there is a node in the Merkle tree that has this hash.
    fn exists(&self, hash: Hash) -> bool;

    /// Obtains a reference to the Merkle tree node for the given hash.
    /// None means there is no node with that hash.
    fn node(&self, hash: Hash) -> Option<&Self::Node>;

    /// Ensures that `content` is a child of `parent`.
    /// If content is already a child of parent, then this does not change the Merkle tree.
    /// It always returns the hash of the child, unless the parent does not exist, in
    /// which case it will return None.
    fn insert(&self, parent: Hash, content: &Self::Node) -> Option<Hash>;

    fn store_tree(&self, commit: Root) -> Hash;

    fn path(&self, hash: Hash) -> Option<Vec<Hash>>;
}

#[derive(Debug)]
pub enum RepositoryTree {
    Dir {
        name: String,
        children: Vec<Box<Self>>,
    },
    File {
        name: String,
        content: Vec<u8>,
    },
}

impl RepositoryTree {
    pub fn name(&self) -> &str {
        match self {
            RepositoryTree::Dir { name, .. } => name,
            RepositoryTree::File { name, .. } => name,
        }
    }

    pub fn from_walk(path: &Path) -> std::io::Result<Box<Self>> {
        if path.is_dir() {
            let name = path
                .file_name()
                .expect("directory does not have a name")
                .to_string_lossy()
                .to_string();
            let mut children = Vec::new();
            let dir = std::fs::read_dir(path)?;
            for entry in dir {
                let entry = entry?;
                let child = Self::from_walk(&entry.path())?;
                children.push(child);
            }
            Ok(Box::new(Self::Dir { name, children }))
        } else if path.is_file() {
            let name = path
                .file_name()
                .expect("file does not have a name")
                .to_string_lossy()
                .to_string();
            let content = std::fs::read(path)?;
            Ok(Box::new(Self::File { name, content }))
        } else {
            Err(std::io::Error::other("unsupported file type"))
        }
    }
}

pub struct Repository {
    pub root: AbsolutePath,
    pub top_level: Vec<Box<RepositoryTree>>,
}

impl Repository {
    pub fn from_walk(root: PathBuf) -> std::io::Result<Repository> {
        let root = AbsolutePath::new(root)?;
        match *RepositoryTree::from_walk(root.as_path())? {
            RepositoryTree::Dir { name: _, children } => Ok(Self {
                root,
                top_level: children,
            }),
            RepositoryTree::File { name, content: _ } => Err(std::io::Error::other(format!(
                "unsupported file type: {}",
                name
            ))),
        }
    }
}

pub enum MerkleTree {
    Dir {
        hash: Hash,
        name: String,
        children: Vec<Box<Self>>,
    },
    File {
        hash: Hash,
        name: String,
        content: Vec<u8>,
    },
}

impl MerkleTree {
    pub fn hash(&self) -> Hash {
        match self {
            MerkleTree::Dir { hash, .. } => *hash,
            MerkleTree::File { hash, .. } => *hash,
        }
    }
}

impl HasHash for MerkleTree {
    fn hash(&self) -> Hash {
        self.hash()
    }
}

impl HasHash for Box<MerkleTree> {
    fn hash(&self) -> Hash {
        self.hash()
    }
}

pub struct Root {
    pub root: AbsolutePath,
    pub hash: Hash,
    pub children: Vec<Box<MerkleTree>>,
}

pub fn convert_repository_into_merkle_tree(repo: Repository) -> Root {
    let Repository { root, top_level } = repo;
    let children = top_level.into_iter().map(hash_convert).collect::<Vec<_>>();
    Root {
        root,
        hash: Hash::hash_of_hashes(children.iter()),
        children,
    }
}

fn hash_convert(node: Box<RepositoryTree>) -> Box<MerkleTree> {
    match *node {
        RepositoryTree::Dir { name, children } => {
            let merkle_children: Vec<Box<MerkleTree>> = {
                let mut merkle_children = Vec::new();
                for child in children {
                    let merkle_child = hash_convert(child);
                    merkle_children.push(merkle_child);
                }
                merkle_children
            };
            Box::new(MerkleTree::Dir {
                hash: Hash::hash_of_hashes(merkle_children.iter()),
                name,
                children: merkle_children,
            })
        }
        RepositoryTree::File { name, content } => Box::new(MerkleTree::File {
            hash: Hash::new(&content),
            name,
            content,
        }),
    }
}

pub trait MerkleTreePlatform {
    type N: Node;
    type S: MerkleMetadataStore<Node = Self::N>;
}
