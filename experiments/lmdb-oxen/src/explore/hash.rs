use serde::Deserialize;
use serde::Serialize;
use thiserror::Error as ThisError;

use xxhash_rust::xxh3::xxh3_128;

/// A 128-bit unsigned integer hash value that uniquely identifies some piece of data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hash(u128);

/// Unwraps a hash into its unsigned 128 bit value.
impl From<Hash> for u128 {
    fn from(value: Hash) -> Self {
        value.0
    }
}

/// Indicates that a type has some sort of content and has an associated computed hash value
/// identifying that content.
pub trait HasHash {
    fn hash(&self) -> Hash;
}

impl HasHash for Hash {
    fn hash(&self) -> Hash {
        *self
    }
}

impl Hash {
    /// Only way to create a new `Hash` instance is to calculate a hash value from some content.
    #[inline(always)]
    pub fn new(contents: &[u8]) -> Self {
        Hash(xxh3_128(contents))
    }

    /// Compute the hash of the concatonation of several hashes.
    ///
    /// The order of the children is important. Changing order changes the resulting hash value,
    /// even for identical children.
    #[inline(always)]
    pub fn hash_of_hashes<'a, N: HasHash + 'a>(children: impl Iterator<Item = &'a N>) -> Hash {
        let hashes: Vec<u8> = children.flat_map(|h| h.hash().as_bytes()).collect();
        Self::new(&hashes)
    }

    /// The platform-independent byte layout of a [Hash] value.
    ///
    /// The byte layout of a hash value needs to be consistent across architectures.
    /// On the common architectures we use, it's little endian, so it will be a/close to a
    /// no-op in the majority of cases.
    #[inline(always)]
    pub(crate) fn as_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }
}

/// A hex-encoded value of a 128-bit unsigned integer hash value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HexHash(String);

#[derive(Debug, ThisError)]
#[error("{0} is not a valid hex-encoded u128 value")]
pub struct NotAValidHexHash<'a>(&'a str);

/// Convert a hex-encoded value into a [Hash] instance.
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
