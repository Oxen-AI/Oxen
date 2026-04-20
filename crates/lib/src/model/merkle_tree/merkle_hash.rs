use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
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

/// A hexadecimal representation of a `MerkleHash`. Can only be created from a `MerkleHash`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HexHash(String);

impl HexHash {
    #[inline(always)]
    pub fn new(hash: &MerkleHash) -> Self {
        Self(format!("{hash}"))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
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
