use std::{fmt, path::Path};

use crate::model::{MerkleHash, TMerkleTreeNode};

/// Helper: create a content-name hash from a Merkle tree node.
pub(super) fn hash_cn_from(node: &dyn TMerkleTreeNode) -> HashCN {
    HashCN::new(
        &node.hash(),
        node.name()
            .and_then(Filename::new_assume_invariants)
            .as_ref(),
    )
}

// A [`MerkleHash`] of a file's contents and the name of a file.
//
// Can only be constructed with an existing [`MerkleHash`] from a file's
// contents and the non-empty filename — except for [`HashCN::from_raw_u128`],
// which rehydrates an already-computed value read back out of LMDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct HashCN(u128);

impl HashCN {
    #[inline(always)]
    pub fn new(content: &MerkleHash, file: Option<&Filename>) -> Self {
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(&content.to_le_bytes());
        if let Some(file) = file {
            hasher.update(file.name().as_bytes());
        }
        Self(hasher.digest128())
    }

    /// Rehydrate a [`HashCN`] from its raw `u128` as stored on disk.
    ///
    /// Production construction must go through [`HashCN::new`] (content +
    /// filename). This bypass exists solely for the LMDB value codecs in
    /// [`super::value_structs`], which read an already-computed `HashCN` back
    /// out of a `merkle_links` / `merkle_node_dupes` row. The bytes were
    /// produced by `new` when first written, so they are trusted here.
    #[inline(always)]
    pub(in crate::core::db::merkle_node::lmdb) fn from_raw_u128(value: u128) -> Self {
        Self(value)
    }

    /// The little-endian 16-byte representation, for writing into an LMDB value.
    #[inline(always)]
    pub(in crate::core::db::merkle_node::lmdb) fn to_le_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    /// Unwraps the name-content hash into its raw u128 XXH3 hash value.
    pub fn unwrap(self) -> u128 {
        self.0
    }

    pub fn as_u128(&self) -> &u128 {
        &self.0
    }

    /// The hexidecimal representation of the name-content hash.
    pub fn to_hex_hash(&self) -> HexHashCN {
        HexHashCN::new(self)
    }
}

/// The name of a file. Can only be constructed from a valid filepath.
pub(crate) struct Filename(String);

impl Filename {
    /// Get the name of the file. Returns None if the path has no file name or if it is not a file.
    pub fn new(file: &Path) -> Option<Self> {
        if file.is_file() {
            file.file_name()
                .and_then(|n| n.to_str())
                .filter(|n| !n.is_empty())
                .map(|n| Self(n.to_string()))
        } else {
            None
        }
    }

    /// The name of the file.
    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.0
    }

    /// LMDB package private direct constructor.
    #[inline(always)]
    pub(in crate::core::db::merkle_node::lmdb) fn new_assume_invariants<S: Into<String>>(
        filename: S,
    ) -> Option<Self> {
        let f: String = filename.into();
        if f.is_empty() { None } else { Some(Self(f)) }
    }
}

impl fmt::Display for HashCN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_hex_hash())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HexHashCN(String);

impl HexHashCN {
    /// The 32-character hex-encoded string of the [`u128`] [`HashCN`] value.
    pub fn new(hash_nc: &HashCN) -> Self {
        Self(format!("{:032x}", hash_nc.0))
    }
}

impl fmt::Display for HexHashCN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_hash_exactly_32_characters() {
        for v in [810787125761027, 0, u128::MAX, 5, 6491] {
            let hex = HexHashCN::new(&HashCN::from_raw_u128(v));
            assert_eq!(
                hex.to_string().len(),
                32,
                "{hex} is not 32 characters long!"
            );
        }
    }
}
