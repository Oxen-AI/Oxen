use std::{ops::Deref};

use liboxen::error::OxenError;
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

#[derive(Debug, Clone)]
pub struct Hash(u128);

#[derive(Debug, Clone)]
pub struct HexHash(String);

#[derive(Debug, ThisError)]
#[error("{0} is not a valid hex-encoded u128 value")]
pub struct NotAValidHexHash<'a>(&'a str);

impl <'a> TryFrom<&'a str> for Hash {
    type Error = NotAValidHexHash<'a>;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let hash = u128::from_str_radix(value, 16)
            .map_err(|_| NotAValidHexHash(value))?;
        Ok(Hash(hash))
    }
}


impl From<Hash> for HexHash {
    fn from(value: Hash) -> Self {
        HexHash(format!("{:032x}", value.0))
    }
}

/// Safe conversion since we can only create a `HexHash` from a `Hash`.
impl From<&HexHash> for Hash {
    fn from(value: &HexHash) -> Self {
        value.0.try_from()
            .expect("Invariant violated! A HexHash was made that bypassed safe creation!")
    }
}

/// Writes as a hex-encoded string.
impl std::fmt::Display for HexHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Allows a `HexHash` to be dereferenced as a `str`.
impl Deref for HexHash {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


pub trait MerkleTreePlatform {

trait Node {
    fn hash(&self) -> Hash;
    fn name(&self) -> &str;
}
type N : Node;

trait Store {}
type S : Store;

}

pub trait MerkleMetadataStore<'db> {

/// If true, then there is a node in the Merkle tree that has this hash.
fn exists(&'db self, hash: Hash) -> bool;

/// Obtains a reference to the Merkle tree node for the given hash.
/// None means there is no node with that hash.
fn node(&'db self, hash: Hash) -> Option<&'db MerkleNode>;

/// Ensures that `content` is a child of `parent`.
/// If content is already a child of parent, then this does not change the Merkle tree.
/// It always returns the hash of the child, unless the parent does not exist, in
/// which case it will return None.
fn insert(&'db self, parent: Hash, content: impl Iterator<Item=u8>) -> Option<Hash>;

fn path(&'db self, hash: Hash) -> Option<Vec<Hash>>;
}

pub(crate) trait RawMerkleMetadataStore<'db>: MerkleMetadataStore<'db> {

/// Like `insert`, but forces the caller to compute the hash of the content directly.
/// This function can be dangerous: it does not check that the content's hash matches
/// the provided hash value. If this occurs, the tree will be in an invalid state.
fn insert_with_hash(&'db self, parent: Hash, content: impl Iterator<Item=u8>, hash: Hash) -> Option<Hash>;
}
