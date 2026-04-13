mod existing;

use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Oxen(#[from] OxenError),
}

fn main() -> Result<(), Error> {

  // create new lmdb database

  // ==== MIGRATION SCRIPT ====
  // walk an oxen repo directory: .oxen/tree/nodes/
  //  for each {hash_prefix}
  //    for each {hash_suffix}
  //      hash = {prefix} + {suffix}
  //      look at ./{ha/sh}/nodes and ./{ha/sh}/children
  //      create a new entry in nodes table: {hash} -> serialized contents of node file
  //      create a new entry in children table: {hash} -> serialized contents of children file
  //      create a new entry in dir_hashes table: {hash} -> []each child hash


  // ==== DESIGN ====


  enum MerkleError {
    HashDoesNotExist,
    HashDoesNotEqualContent,
  }

  #[derive(Debug, Error)]
  #[error("{0} is not a valid hex-encoded u128 value")]
  pub struct NotAValidHexHash<'a>(&'a str)

  pub struct Hash(u128);
  impl Hash {
    pub fn from_hex<'a>(hex_encoded_hash: &'a str) -> Result<Self, NotAValidHexHash<'a>>;

    pub fn to_hex(&self) -> String;

    pub fn new(content: impl Iterator<Item=u8>) -> Self;
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

  Ok(())
}
