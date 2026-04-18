use serde::Deserialize;
use serde::Serialize;

use xxhash_rust::xxh3::xxh3_128;

use crate::explore::paths::Name;

/// A 128-bit hash that identifies a piece of content by its bytes.
///
/// Two ways to construct one:
/// - [`ContentHash::new`] hashes a byte slice directly (the content itself).
/// - [`ContentHash::hash_of_hashes`] folds an iterator of child content
///   hashes — used for directory content hashes and commit hashes.
///
/// A `ContentHash` can **never** be turned into a [`LocationHash`]: they're
/// different types in the type system with no `From`/`Into` between them.
/// Use the right one at the right place or it won't compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ContentHash(u128);

impl ContentHash {
    /// Hash a byte slice (a file's contents, or any other bytes that represent
    /// "content"). The only way to make a `ContentHash` from raw data.
    #[inline(always)]
    pub fn new(contents: &[u8]) -> Self {
        Self(xxh3_128(contents))
    }

    /// Hash-of-hashes over children's content hashes. Order matters.
    /// Used for dir content hashes and commit hashes.
    #[inline(always)]
    pub fn hash_of_hashes<'a, N: HasContentHash + 'a>(
        children: impl Iterator<Item = &'a N>,
    ) -> ContentHash {
        let hashes: Vec<u8> = children.flat_map(|h| h.content_hash().as_bytes()).collect();
        Self::new(&hashes)
    }

    /// Little-endian bytes of the underlying 128-bit value. Platform-
    /// independent layout used when a `ContentHash` participates in another
    /// hash computation.
    #[inline(always)]
    pub(crate) fn as_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }
}

/// LMDB key coercion. Heed's `U128<LE>` key type needs a `u128`.
impl From<ContentHash> for u128 {
    fn from(value: ContentHash) -> Self {
        value.0
    }
}

/// A 128-bit hash that identifies a specific **position** in a tree, i.e. a
/// particular occurrence of a file or directory under a particular parent
/// with a particular name.
///
/// The only way to construct one is [`LocationHash::new`]. No other public
/// constructor; in particular, **not** from raw bytes, **not** from a bare
/// `u128`, and **not** from a [`ContentHash`]. A commit's root-level
/// children are constructed with `parent = None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LocationHash(u128);

impl LocationHash {
    /// Deterministic location hash for a node at `(content, parent, name)`.
    ///
    /// Same `(content, parent, name)` across different commits produces the
    /// same location hash — desirable: same logical path in two commits is
    /// one stored record. Different parents or names produce different
    /// location hashes even when the content is identical, which is how
    /// duplicate-content nodes at distinct paths keep distinct identities.
    ///
    /// `parent = None` is used for nodes directly under a commit's root.
    /// Their stored record's `parent` is also `None`, which is what the
    /// `path()` walker uses to terminate.
    pub fn new(content: &ContentHash, parent: Option<&LocationHash>, name: &Name) -> Self {
        let name_bytes = name.as_str().as_bytes();
        let mut buf = Vec::with_capacity(16 + 16 + name_bytes.len());
        buf.extend_from_slice(&content.as_bytes());
        match parent {
            Some(p) => buf.extend_from_slice(&p.as_bytes()),
            None => buf.extend_from_slice(&[0u8; 16]),
        }
        buf.extend_from_slice(name_bytes);
        Self(xxh3_128(&buf))
    }

    /// Little-endian bytes of the underlying 128-bit value.
    #[inline(always)]
    pub(crate) fn as_bytes(&self) -> [u8; 16] {
        self.0.to_le_bytes()
    }
}

/// LMDB key coercion.
impl From<LocationHash> for u128 {
    fn from(value: LocationHash) -> Self {
        value.0
    }
}

/// A type that carries (or is) a [`ContentHash`].
pub trait HasContentHash {
    fn content_hash(&self) -> ContentHash;
}

impl HasContentHash for ContentHash {
    #[inline(always)]
    fn content_hash(&self) -> ContentHash {
        *self
    }
}

/// A type that carries (or is) a [`LocationHash`].
pub trait HasLocationHash {
    fn location_hash(&self) -> LocationHash;
}

impl HasLocationHash for LocationHash {
    #[inline(always)]
    fn location_hash(&self) -> LocationHash {
        *self
    }
}

/// A hex-encoded hash value, used for display. Carries no type distinction
/// between content and location — once you're rendering hex you've left the
/// type system's jurisdiction. If the caller cares which kind of hash the
/// hex came from, they should label it in surrounding context.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HexHash(String);

impl HexHash {
    fn from_u128(value: u128) -> Self {
        Self(format!("{value:032x}"))
    }
}

impl std::fmt::Display for HexHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

macro_rules! impl_into_hexhash {
    ($type:ty) => {
        impl From<$type> for HexHash {
            fn from(value: $type) -> Self {
                HexHash::from_u128(value.0)
            }
        }

        impl From<&$type> for HexHash {
            fn from(value: &$type) -> Self {
                HexHash::from_u128(value.0)
            }
        }
    };
}

impl_into_hexhash!(ContentHash);
impl_into_hexhash!(LocationHash);
