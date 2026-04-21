use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use utoipa::ToSchema;

use crate::error::OxenError;

// The derived serializer here will serialize the hash as a u128. This is used
// in the binary representation on disk. We define a custom serializer that uses
// the string representation of the hash below.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Deserialize, Serialize, ToSchema)]
#[schema(value_type = String)]
pub struct MerkleHash(u128);

impl MerkleHash {
    #[inline(always)]
    pub fn new(hash: u128) -> Self {
        Self(hash)
    }

    #[inline(always)]
    pub fn to_le_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    #[inline(always)]
    pub fn to_u128(&self) -> u128 {
        self.0
    }

    // only print the first N characters of the hash
    pub fn to_short_str(&self) -> String {
        const SHORT_STR_LEN: usize = 10;
        let str = format!("{self}");
        if str.len() > SHORT_STR_LEN {
            str[..SHORT_STR_LEN].to_string()
        } else {
            str
        }
    }

    /// Encode the hash value as a hexadecimal string and wrap in zero-sized struct.
    #[allow(clippy::wrong_self_convention)]
    #[inline(always)]
    pub(crate) fn to_hex_hash(&self) -> HexHash {
        HexHash::new(self)
    }
}

/// Parses a hexadecimal string into a `MerkleHash`.
impl FromStr for MerkleHash {
    type Err = OxenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hash = u128::from_str_radix(s, 16)?;
        Ok(Self(hash))
    }
}

/// Parses a hexadecimal string into a `MerkleHash`.
impl TryFrom<String> for MerkleHash {
    type Error = OxenError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

/// Writes the hash value in hexadecimal format.
impl fmt::Display for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl fmt::Debug for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MerkleHash({self})")
    }
}

impl Hash for MerkleHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl From<HexHash> for MerkleHash {
    fn from(value: HexHash) -> Self {
        Self::from_str(&value.0)
            .expect("Invariant violation: HexHash was not constructed from a valid MerkleHash!")
    }
}

// This builds a custom serializer for MerkleHash that serializes to a string.
// We use this format in the API responses.
// The serializer it creates is compatible with serde's "with" attribute and
// "serde_as" and exposes it as a module called "MerkleHashAsString"
// See: https://docs.rs/serde_with/latest/serde_with/macro.serde_conv.html
serde_with::serde_conv!(
    pub MerkleHashAsString,
    MerkleHash,
    |hash: &MerkleHash| hash.to_string(),
    |s: String| MerkleHash::try_from(s)
);

/// A hexadecimal representation of a 128-bit [`MerkleHash`] value.
///
/// Is a zero-sized struct around an owned `String`. Can only be created from a [`MerkleHash`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HexHash(String);

impl HexHash {
    #[inline(always)]
    pub fn new(hash: &MerkleHash) -> Self {
        Self(hash.to_string())
    }

    /// Produces a relative path for the 2-level directory structure used to store Merkle nodes.
    /// The first directory name is the first 3 characters of the hex-encoded hash. The second
    /// is the remaining characters.
    pub fn node_db_prefix(&self) -> PathBuf {
        let hash_str = &self.0;
        const DIR_PREFIX_LEN: usize = 3;
        let dir_prefix = &hash_str[0..DIR_PREFIX_LEN];
        let dir_suffix = &hash_str[DIR_PREFIX_LEN..];
        Path::new(dir_prefix).join(dir_suffix)
    }
}

/// Writes the hexadecimal representation of the hash only.
impl std::fmt::Display for HexHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Converts a `MerkleHash` into a `HexHash`.
impl From<MerkleHash> for HexHash {
    fn from(value: MerkleHash) -> Self {
        Self::new(&value)
    }
}

/// Convert a reference to a `MerkleHash` into a `HexHash`.
impl<'a> From<&'a MerkleHash> for HexHash {
    fn from(value: &'a MerkleHash) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_hash_conversions_and_node_db_prefix() {
        for _ in [0..1000] {
            let random_value: u128 = rand::random();
            let hash = MerkleHash::new(random_value);

            let hex = hash.to_hex_hash();
            assert_eq!(hex.to_string(), hash.to_string());

            let convert_back_to_hash: MerkleHash = hex.clone().into();
            assert_eq!(convert_back_to_hash, hash);

            let convert_back_to_hex: HexHash = hash.into();
            assert_eq!(convert_back_to_hex, hex);

            let dir = hex.node_db_prefix();
            let prefix = dir
                .parent()
                .expect("dir should have a parent")
                .to_str()
                .expect("should have utf-8 name");
            let suffix = dir
                .file_name()
                .expect("dir should have a file name")
                .to_str()
                .expect("should have utf-8 name");
            assert_eq!(suffix.len(), hex.0.len() - 3);
            assert_eq!(prefix.len(), 3);
            assert_eq!(format!("{prefix}{suffix}"), hex.to_string());
        }
    }
}
