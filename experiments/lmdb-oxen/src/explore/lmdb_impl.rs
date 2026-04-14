use crate::explore::{
    lazy_merkle_lmdb::{MerkleMetadataStore, MerkleTreeL, Root},
    scratch::{Hash, Repository},
};

pub struct LmdbMerkleDB {
    repo: Repository,
}

impl MerkleMetadataStore for LmdbMerkleDB {
    fn repository(&self) -> &Repository {
        &self.repo
    }

    /// If true, then there is a node in the Merkle tree that has this hash.
    ///
    /// This always means that either `self.node()` xor `self.commit()` will return a
    /// non-`None` value. Note that this is a mutually exclusive relationship: exactly
    /// one of `node` or `commit` is non-`None` if `exists` is `true`.
    fn exists(&self, hash: Hash) -> bool {
        unimplemented!()
    }

    /// Obtains a reference to the Merkle tree node for the given hash.
    ///
    /// Corresponds to a real file or directory under version control.
    /// None means there is no node with that hash.
    fn node(&self, hash: Hash) -> Option<&MerkleTreeL<Self>> {
        unimplemented!()
    }

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Option<&Root<Self>> {
        unimplemented!()
    }
}
