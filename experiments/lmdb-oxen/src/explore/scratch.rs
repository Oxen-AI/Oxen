use std::{ops::Deref, path::Path};

use std::path::{PathBuf, StripPrefixError};

use liboxen::{error::OxenError, model::TMerkleTreeNode};
use thiserror::Error as ThisError;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hash(u128);

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

    fn path(&self, hash: Hash) -> Option<Vec<Hash>>;
}

pub struct Name(String);

#[derive(Debug, ThisError)]
pub enum NameError {
    #[error("No name found for path: '{0}'")]
    PathHasNoName(PathBuf),
    #[error("Path has non UTF-8 name: '{0}'")]
    NonUtf8Name(PathBuf),
}

/// Gets the name of the file or directory only.
impl TryFrom<PathBuf> for Name {
    type Error = NameError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        match path.file_name() {
            Some(name) => match name.to_str() {
                Some(name) => Ok(Name(name.to_string())),
                None => Err(NameError::NonUtf8Name(path)),
            },
            None => Err(NameError::PathHasNoName(path)),
        }
    }
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
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unsupported file type",
            ));
        }
    }
}

pub struct AbsolutePath(PathBuf);

pub struct RelativePath(Vec<String>);

impl RelativePath {
    pub fn new(repo: &Repository, path: &Path) -> Result<Self, StripPrefixError> {
        let relative = path.strip_prefix(&repo.root)?;
        let components = relative
            .components()
            .into_iter()
            .map(|c| c.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        Ok(Self(components))
    }
}

pub trait Builder<B> {
    fn build(self) -> B;
}

pub trait Accumulator<Ingest> {
    fn accumulate(&mut self, x: Ingest) -> &mut Self;
}

pub struct Navigator(RelativePath);

impl Accumulator<&RepositoryTree> for Navigator {
    fn accumulate(&mut self, node: &RepositoryTree) -> &mut Self {
        self.0.0.push(node.name().to_string());
        self
    }
}

impl Builder<RelativePath> for Navigator {
    fn build(self) -> RelativePath {
        self.0
    }
}

pub struct Repository {
    pub root: PathBuf,
    pub top_level: Vec<Box<RepositoryTree>>,
}

impl Repository {
    pub fn from_walk(root: PathBuf) -> std::io::Result<Repository> {
        let root = root.canonicalize()?;
        match *RepositoryTree::from_walk(root.as_path())? {
            RepositoryTree::Dir { name: _, children } => Ok(Self {
                root,
                top_level: children,
            }),
            RepositoryTree::File { name, content: _ } => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("unsupported file type: {}", name),
                ));
            }
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

pub struct Root {
    pub hash: Hash,
    pub children: Vec<Box<MerkleTree>>,
}

impl Root {
    pub fn hash(&self) -> Hash {
        self.hash
    }
}

pub trait MerkleTreePlatform {
    type N: Node;
    type S: MerkleMetadataStore<Node = Self::N>;
}
