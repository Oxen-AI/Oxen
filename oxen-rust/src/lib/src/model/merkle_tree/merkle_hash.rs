use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use crate::error::OxenError;

// The derived serializer here will serialize the hash as a u128. This is used
// in the binary representation on disk. We define a custom serializer that uses
// the string representation of the hash below.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct MerkleHash(u128);

impl MerkleHash {
    pub fn new(hash: u128) -> Self {
        Self(hash)
    }

    pub fn to_le_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    pub fn to_u128(&self) -> u128 {
        self.0
    }

    // only print the first N characters of the hash
    pub fn to_short_str(&self) -> String {
        const SHORT_STR_LEN: usize = 10;
        let str = format!("{}", self);
        if str.len() > SHORT_STR_LEN {
            str[..SHORT_STR_LEN].to_string()
        } else {
            str
        }
    }
}

impl FromStr for MerkleHash {
    type Err = OxenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hash = u128::from_str_radix(s, 16)?;
        Ok(Self(hash))
    }
}

impl TryFrom<String> for MerkleHash {
    type Error = OxenError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

impl fmt::Display for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl fmt::Debug for MerkleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MerkleHash({})", self)
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
