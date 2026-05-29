//! Zero-copy on-disk byte layouts for the LMDB Merkle tree backend.
//!
//! Every persisted value is `magic + version + fixed header + variable tail`.
//! The fixed headers are `#[repr(C)]` with alignment-1 fields so they can be
//! cast directly from LMDB-returned `&[u8]` regardless of pointer alignment;
//! the variable tail is interpreted via `<[T]>::ref_from_bytes`.
//!
//! Reads return [`LmdbNodeRef`] / [`LmdbLinkRef`] borrowed into the LMDB read
//! transaction's mmap region — no copying, no parsing. Writes go through the
//! owned [`LmdbNode`] / [`LmdbLink`] convenience types which encode the bytes
//! contiguously for [`heed::Database::put`].
//!
//! Schema evolution is by versioning: never mutate `LmdbNodeHeaderV1` /
//! `LmdbLinkHeaderV1` after release. Add a `V2` variant to [`NodeVersion`] /
//! [`LinkVersion`] alongside a `LmdbNodeHeaderV2` / `LmdbLinkHeaderV2` and
//! dispatch on the version byte.

use std::mem::size_of;

// NOTE: All numeric values stored in LMDB are always in little-endian format. This ensures that
//       LMDB files always contain the exact same data. Nearly every single processor architecture
//       that we expect oxen to run on will be in little endian, so using these values means that
//       all conversion operations are no-ops.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
use zerocopy::byteorder::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::core::db::merkle_node::lmdb::LmdbError;
use crate::core::db::merkle_node::lmdb::hash_content_name::HashCN;
use crate::model::{MerkleHash, MerkleTreeNodeType};

/// 4-byte magic prefix for `merkle_tree_nodes` values.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
pub(super) const NODE_MAGIC: [u8; 4] = *b"OXNV";

/// 4-byte magic prefix for `merkle_links` values.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
pub(super) const LINK_MAGIC: [u8; 4] = *b"OXLN";

/// 4-byte magic prefix for `merkle_node_dupes` values.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
pub(super) const DUPES_MAGIC: [u8; 4] = *b"OXDP";

// ─────────────────────────────────────────────────────────────────────────────
// Version enums. These are real Rust enums in code, single-byte on disk.
//
// `#[repr(u8)]` pins the discriminant size to 1 byte and avoids endianness
// concerns (a 2-byte+ repr would use native endianness for the discriminant,
// which is a cross-platform hazard for persisted data). 256 versions is far
// more headroom than we'll ever exhaust.
//
// `TryFromBytes` validates that the byte matches a declared variant during
// `try_ref_from_prefix`. `IntoBytes` lets `header.as_bytes()` work.
// ─────────────────────────────────────────────────────────────────────────────

/// On-disk version tag for [`LmdbNodeHeaderV1`].
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
#[derive(
    TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Copy, Clone, Debug, PartialEq, Eq,
)]
#[repr(u8)]
pub enum NodeVersion {
    V1 = 1,
}

/// On-disk version tag for [`LmdbLinkHeaderV1`].
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
#[derive(
    TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Copy, Clone, Debug, PartialEq, Eq,
)]
#[repr(u8)]
pub enum LinkVersion {
    V1 = 1,
}

/// On-disk version tag for [`LmdbDupesHeaderV1`].
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
#[derive(
    TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Copy, Clone, Debug, PartialEq, Eq,
)]
#[repr(u8)]
pub enum DupesVersion {
    V1 = 1,
}

// ─────────────────────────────────────────────────────────────────────────────
// Node header — 8 bytes total, kind discriminant in the header, msgpack tail.
// ─────────────────────────────────────────────────────────────────────────────

/// Fixed header of a `merkle_tree_nodes` value.
///
/// Followed in storage by the msgpack-serialized
/// [`crate::model::merkle_tree::node::EMerkleTreeNode`] payload. Size and
/// offsets are pinned by tests so a future field reorder breaks CI immediately
/// rather than silently corrupting on-disk data.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
// ****************** CHANGING STRUCT LAYOUT IS A BREAKING CHANGE!!! ******************
// ****************** REMOVING `repr(c)` IS A BREAKING CHANGE!!!!!!! ******************
#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Copy, Clone, Debug)]
#[repr(C)]
pub(super) struct LmdbNodeHeaderV1 {
    pub magic: [u8; 4],
    pub version: NodeVersion,
    pub kind: u8,
    pub _reserved: [u8; 2],
}

impl LmdbNodeHeaderV1 {
    pub const SIZE: usize = size_of::<Self>();
}

// ─────────────────────────────────────────────────────────────────────────────
// Link header — 32 bytes, parent slot is always reserved (zeros if unused),
// children follow as `[[u8; 16]; num_children]` in little-endian.
// ─────────────────────────────────────────────────────────────────────────────

/// Fixed header of a `merkle_links` value.
///
/// Followed in storage by `num_children` children, each a 16-byte little-endian
/// merkle hash. The 16-byte `parent` slot is always written; consumers must
/// only read it when `has_parent == 1`.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
// ****************** CHANGING STRUCT LAYOUT IS A BREAKING CHANGE!!! ******************
// ****************** REMOVING `repr(c)` IS A BREAKING CHANGE!!!!!!! ******************
#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Copy, Clone, Debug)]
#[repr(C)]
pub(super) struct LmdbLinkHeaderV1 {
    pub magic: [u8; 4],
    pub version: LinkVersion,
    pub has_parent: u8,
    pub _reserved: [u8; 2],
    pub num_children: U64,
    pub parent: [u8; 16],
}

impl LmdbLinkHeaderV1 {
    pub const SIZE: usize = size_of::<Self>();
}

// ─────────────────────────────────────────────────────────────────────────────
// Dupes header — 16 bytes. The keyed content hash maps to the set of `HashCN`s
// (one per uniquely-named file) that share that content. Entries follow as
// `[[u8; 16]; num_dupes]` in little-endian.
// ─────────────────────────────────────────────────────────────────────────────

/// Fixed header of a `merkle_node_dupes` value.
///
/// Followed in storage by `num_dupes` entries, each a 16-byte little-endian
/// [`HashCN`]. Two byte-identical files with different names share one content
/// [`MerkleHash`] (the table key) but get distinct `HashCN`s — this row records
/// every such `HashCN` so the duplicates can all be recovered.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
// ****************** CHANGING STRUCT LAYOUT IS A BREAKING CHANGE!!! ******************
// ****************** REMOVING `repr(c)` IS A BREAKING CHANGE!!!!!!! ******************
#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Copy, Clone, Debug)]
#[repr(C)]
pub(super) struct LmdbDupesHeaderV1 {
    pub magic: [u8; 4],
    pub version: DupesVersion,
    pub _reserved: [u8; 3],
    pub num_dupes: U64,
}

impl LmdbDupesHeaderV1 {
    pub const SIZE: usize = size_of::<Self>();
}

// ─────────────────────────────────────────────────────────────────────────────
// Zero-copy reader views.
// `'a` is the LMDB read transaction's lifetime: the borrows end when the rtxn
// drops, which the borrow checker enforces.
// ─────────────────────────────────────────────────────────────────────────────

#[inline(always)]
fn read_hash_cn_from_ref(bytes: &[u8; 16]) -> HashCN {
    HashCN::from_raw_u128(u128::from_le_bytes(*bytes))
}

/// Zero-copy view over a `merkle_tree_nodes` value.
///
/// `header` is a typed view into the first [`LmdbNodeHeaderV1::SIZE`] bytes;
/// `data` is the msgpack tail still residing in the LMDB mmap. The msgpack
/// decoder runs against `data` directly when an owned node is needed.
#[derive(Debug)]
pub(crate) struct LmdbNodeRef<'a> {
    pub header: &'a LmdbNodeHeaderV1,
    pub data: &'a [u8],
}

impl<'a> LmdbNodeRef<'a> {
    /// Parse a stored `merkle_tree_nodes` value with no copying.
    ///
    /// Validation order is chosen for precise error reporting:
    /// 1. Length check → [`LmdbError::NodeHeaderTruncated`].
    /// 2. Magic check  → [`LmdbError::NodeBadMagic`].
    /// 3. `try_ref_from_prefix` → its only remaining failure mode is an
    ///    invalid `NodeVersion` byte, which we map to
    ///    [`LmdbError::NodeUnsupportedVersion`].
    #[inline]
    pub(crate) fn from_bytes(bytes: &'a [u8]) -> Result<Self, LmdbError> {
        if bytes.len() < LmdbNodeHeaderV1::SIZE {
            return Err(LmdbError::NodeHeaderTruncated { len: bytes.len() });
        }
        let magic: [u8; 4] = bytes[0..4].try_into().expect("len pre-checked");
        if magic != NODE_MAGIC {
            return Err(LmdbError::NodeBadMagic { actual: magic });
        }
        let (header, tail) = LmdbNodeHeaderV1::try_ref_from_prefix(bytes).map_err(|_| {
            // Length and magic pre-checked; only `NodeVersion` validity can fail.
            LmdbError::NodeUnsupportedVersion(bytes[4])
        })?;
        Ok(Self { header, data: tail })
    }

    /// Decode the kind discriminant. Errors if the byte isn't a known
    /// [`MerkleTreeNodeType`].
    #[inline(always)]
    pub(crate) fn kind(&self) -> Result<MerkleTreeNodeType, LmdbError> {
        Ok(MerkleTreeNodeType::from_u8(self.header.kind)?)
    }
}

/// Zero-copy view over a `merkle_links` value.
#[derive(Debug)]
pub(crate) struct LmdbLinkRef<'a> {
    pub header: &'a LmdbLinkHeaderV1,
    /// Children stored as `[u8; 16]` little-endian [`HashCN`]s (content+name
    /// keys into `merkle_node_store`). Length is validated against
    /// `header.num_children` at parse time.
    pub child_hashes: &'a [[u8; 16]],
}

impl<'a> LmdbLinkRef<'a> {
    /// Parse a stored `merkle_links` value with no copying. See
    /// [`LmdbNodeRef::from_bytes`] for the validation-order rationale.
    #[inline]
    pub(crate) fn from_bytes(bytes: &'a [u8]) -> Result<Self, LmdbError> {
        if bytes.len() < LmdbLinkHeaderV1::SIZE {
            return Err(LmdbError::LinkHeaderTruncated { len: bytes.len() });
        }
        let magic: [u8; 4] = bytes[0..4].try_into().expect("len pre-checked");
        if magic != LINK_MAGIC {
            return Err(LmdbError::LinkBadMagic { actual: magic });
        }
        let (header, tail) = LmdbLinkHeaderV1::try_ref_from_prefix(bytes).map_err(|_| {
            // Length and magic pre-checked; only `LinkVersion` validity can fail
            // structurally — `has_parent` is `u8` so it's always a valid bit
            // pattern (we validate its semantic range below).
            LmdbError::LinkUnsupportedVersion(bytes[4])
        })?;
        if header.has_parent > 1 {
            return Err(LmdbError::InvalidIsParent(header.has_parent));
        }
        let child_hashes =
            <[[u8; 16]]>::ref_from_bytes(tail).map_err(|_| LmdbError::ChildrenTailMisaligned {
                tail_len: tail.len(),
            })?;
        let claimed = header.num_children.get() as usize;
        if child_hashes.len() != claimed {
            return Err(LmdbError::ChildrenCountMismatch {
                claimed,
                actual: child_hashes.len(),
            });
        }
        Ok(Self {
            header,
            child_hashes,
        })
    }

    #[inline(always)]
    pub(crate) fn parent_id(&self) -> Option<MerkleHash> {
        if self.header.has_parent == 1 {
            Some(MerkleHash::new(u128::from_le_bytes(self.header.parent)))
        } else {
            None
        }
    }

    #[inline(always)]
    pub(crate) fn children_iter(&self) -> impl Iterator<Item = HashCN> + '_ {
        self.child_hashes.iter().map(read_hash_cn_from_ref)
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn get_child(&self, index: usize) -> Option<HashCN> {
        self.child_hashes.get(index).map(read_hash_cn_from_ref)
    }

    #[inline(always)]
    pub(crate) fn num_children(&self) -> usize {
        self.child_hashes.len()
    }
}

/// Zero-copy view over a `merkle_node_dupes` value: the set of [`HashCN`]s
/// (one per uniquely-named file) that all share the keyed content [`MerkleHash`].
#[derive(Debug)]
pub(crate) struct LmdbDupesRef<'a> {
    #[allow(dead_code)]
    pub header: &'a LmdbDupesHeaderV1,
    /// Each entry is a 16-byte little-endian [`HashCN`]. Length is validated
    /// against `header.num_dupes` at parse time.
    pub hash_cns: &'a [[u8; 16]],
}

impl<'a> LmdbDupesRef<'a> {
    /// Parse a stored `merkle_node_dupes` value with no copying. See
    /// [`LmdbNodeRef::from_bytes`] for the validation-order rationale.
    #[inline]
    pub(crate) fn from_bytes(bytes: &'a [u8]) -> Result<Self, LmdbError> {
        if bytes.len() < LmdbDupesHeaderV1::SIZE {
            return Err(LmdbError::DupesHeaderTruncated { len: bytes.len() });
        }
        let magic: [u8; 4] = bytes[0..4].try_into().expect("len pre-checked");
        if magic != DUPES_MAGIC {
            return Err(LmdbError::DupesBadMagic { actual: magic });
        }
        let (header, tail) = LmdbDupesHeaderV1::try_ref_from_prefix(bytes).map_err(|_| {
            // Length and magic pre-checked; only `DupesVersion` validity can fail.
            LmdbError::DupesUnsupportedVersion(bytes[4])
        })?;
        let hash_cns =
            <[[u8; 16]]>::ref_from_bytes(tail).map_err(|_| LmdbError::DupesTailMisaligned {
                tail_len: tail.len(),
            })?;
        let claimed = header.num_dupes.get() as usize;
        if hash_cns.len() != claimed {
            return Err(LmdbError::DupesCountMismatch {
                claimed,
                actual: hash_cns.len(),
            });
        }
        Ok(Self { header, hash_cns })
    }

    #[inline]
    pub(crate) fn hash_cns_iter(&self) -> impl Iterator<Item = HashCN> + '_ {
        self.hash_cns.iter().map(read_hash_cn_from_ref)
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn get(&self, index: usize) -> Option<HashCN> {
        self.hash_cns.get(index).map(read_hash_cn_from_ref)
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn num_dupes(&self) -> usize {
        // self.header.get() as usize
        self.hash_cns.len()
    }

    /// If this refers to a unique [`MerkleHash`], then return it. Otherwise return None.
    #[inline(always)]
    pub(super) fn unique(&self) -> Result<DupeUnqResult, LmdbError> {
        match self.hash_cns.len() {
            0 => Ok(DupeUnqResult::InvariantViolation),
            1 => Ok(DupeUnqResult::Some(read_hash_cn_from_ref(
                &self.hash_cns[0],
            ))),
            _ => Ok(DupeUnqResult::None),
        }
    }
}

pub(super) enum DupeUnqResult {
    /// Must have 1 or more. This [`MerkleHash`] was found with 0 unique hashes.
    InvariantViolation,
    /// A [`MerkleHash`] is unique.
    Some(HashCN),
    /// > 1 case is normal => means that this is a duplicate file
    /// > we only want `MerkleHash`es that are unique, so ignore
    None,
}

// ─────────────────────────────────────────────────────────────────────────────
// Owned write-side types.
// These exist for write paths that build values in memory; encode produces the
// on-disk byte format. Reads should prefer the `Ref` types.
// ─────────────────────────────────────────────────────────────────────────────

/// In-memory representation of a `merkle_tree_nodes` row.
///
/// Properties:
///   - File nodes do not have children (those live in `merkle_links`).
#[derive(Debug, Clone)]
pub struct LmdbNode {
    pub(crate) kind: MerkleTreeNodeType,
    /// msgpack-serialized [`crate::model::merkle_tree::node::EMerkleTreeNode`].
    pub(crate) data: Vec<u8>,
}

impl LmdbNode {
    /// Encode for storage in `merkle_tree_nodes`. Produces a single contiguous
    /// `Vec<u8>` of `[header][msgpack_tail]`. The header is a [`LmdbNodeHeaderV1`].
    #[inline]
    pub(super) fn encode(self) -> Vec<u8> {
        let header = LmdbNodeHeaderV1 {
            magic: NODE_MAGIC,
            version: NodeVersion::V1,
            kind: self.kind.to_u8(),
            _reserved: [0; 2],
        };
        let mut out = Vec::with_capacity(LmdbNodeHeaderV1::SIZE + self.data.len());
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(&self.data);
        out
    }

    /// Eager-decode from on-disk bytes — copies the msgpack tail into a fresh
    /// `Vec`. Prefer [`LmdbNodeRef::from_bytes`] when zero-copy is possible.
    #[inline]
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LmdbError> {
        let r = LmdbNodeRef::from_bytes(bytes)?;
        Ok(Self {
            kind: r.kind()?,
            data: r.data.to_vec(),
        })
    }

    /// The type of Merkle tree node that's serialized.
    #[inline(always)]
    pub fn kind(&self) -> MerkleTreeNodeType {
        self.kind
    }

    /// A reference to the msgpack-encoded bytes of the node.
    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// In-memory representation of a `merkle_links` row.
///
/// Properties:
///   - Commit nodes do not have a parent (so their `parent_id` is None).
///   - File nodes do not have children.
#[derive(Debug, Clone)]
pub struct LmdbLink {
    pub(crate) parent_id: Option<MerkleHash>,
    /// The children of this node, if it has any. Each child is a [`HashCN`]
    /// (content+name key into `merkle_node_store`) so that two same-content,
    /// differently-named files are distinct entries here rather than colliding.
    pub(crate) children: Vec<HashCN>,
}

impl LmdbLink {
    /// Encode for storage in `merkle_links`. Produces `[header][child_hashes]`.
    /// The header is a [`LmdbLinkHeaderV1`].
    #[inline]
    pub(super) fn encode(self) -> Vec<u8> {
        let (has_parent, parent_bytes) = match self.parent_id {
            Some(p) => (1u8, p.to_le_bytes()),
            None => (0u8, [0u8; 16]),
        };
        let header = LmdbLinkHeaderV1 {
            magic: LINK_MAGIC,
            version: LinkVersion::V1,
            has_parent,
            _reserved: [0; 2],
            num_children: U64::new(self.children.len() as u64),
            parent: parent_bytes,
        };
        let mut out =
            Vec::with_capacity(LmdbLinkHeaderV1::SIZE + self.children.len() * size_of::<u128>());
        out.extend_from_slice(header.as_bytes());
        for c in &self.children {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    /// Eager-decode from on-disk bytes. Prefer [`LmdbLinkRef::from_bytes`] when
    /// zero-copy is possible.
    #[inline]
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LmdbError> {
        let r = LmdbLinkRef::from_bytes(bytes)?;
        Ok(Self {
            parent_id: r.parent_id(),
            children: r.children_iter().collect(),
        })
    }

    /// A reference to this node's optional parent hash.
    #[inline(always)]
    pub fn parent_id(&self) -> Option<&MerkleHash> {
        self.parent_id.as_ref()
    }

    /// A reference to this node's children, each a [`HashCN`].
    #[inline(always)]
    pub(super) fn children(&self) -> &[HashCN] {
        &self.children
    }
}

/// In-memory representation of a `merkle_node_dupes` row.
///
/// Records the set of [`HashCN`]s — one per uniquely-named file — that all share
/// a single content [`MerkleHash`] (the table key). Lets the backend recover
/// every duplicate when only the content hash is known (e.g. packing by content
/// hash for the cross-backend wire format).
#[derive(Debug, Clone)]
pub struct LmdbDupes {
    pub(crate) hash_cns: Vec<HashCN>,
}

impl LmdbDupes {
    /// Build a dupes row from the set of [`HashCN`]s sharing one content hash.
    #[inline]
    #[allow(dead_code)]
    pub(super) fn new(hash_cns: Vec<HashCN>) -> Self {
        Self { hash_cns }
    }

    /// Encode for storage in `merkle_node_dupes`. Produces `[header][hash_cns]`.
    /// The header is a [`LmdbDupesHeaderV1`].
    #[inline]
    pub(super) fn encode(self) -> Vec<u8> {
        let header = LmdbDupesHeaderV1 {
            magic: DUPES_MAGIC,
            version: DupesVersion::V1,
            _reserved: [0; 3],
            num_dupes: U64::new(self.hash_cns.len() as u64),
        };
        let mut out =
            Vec::with_capacity(LmdbDupesHeaderV1::SIZE + self.hash_cns.len() * size_of::<u128>());
        out.extend_from_slice(header.as_bytes());
        for h in &self.hash_cns {
            out.extend_from_slice(&h.to_le_bytes());
        }
        out
    }

    /// Eager-decode from on-disk bytes. Prefer [`LmdbDupesRef::from_bytes`] when
    /// zero-copy is possible.
    #[inline]
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LmdbError> {
        let r = LmdbDupesRef::from_bytes(bytes)?;
        Ok(Self {
            hash_cns: r.hash_cns_iter().collect(),
        })
    }

    /// The [`HashCN`]s recorded for this content hash.
    #[inline(always)]
    pub(super) fn hash_cns(&self) -> &[HashCN] {
        &self.hash_cns
    }
}

#[cfg(test)]
mod tests {
    use super::super::hash_content_name::Filename;
    use super::*;
    use std::mem::offset_of;

    // ── LmdbNodeHeaderV1 layout ──────────────────────────────────────────────

    #[test]
    fn node_header_v1_size_is_stable() {
        assert_eq!(size_of::<LmdbNodeHeaderV1>(), 8);
        assert_eq!(LmdbNodeHeaderV1::SIZE, 8);
    }

    #[test]
    fn node_header_v1_offsets_are_stable() {
        assert_eq!(offset_of!(LmdbNodeHeaderV1, magic), 0);
        assert_eq!(offset_of!(LmdbNodeHeaderV1, version), 4);
        assert_eq!(offset_of!(LmdbNodeHeaderV1, kind), 5);
        assert_eq!(offset_of!(LmdbNodeHeaderV1, _reserved), 6);
    }

    #[test]
    fn node_header_v1_golden_bytes() {
        let h = LmdbNodeHeaderV1 {
            magic: NODE_MAGIC,
            version: NodeVersion::V1,
            kind: 7,
            _reserved: [0; 2],
        };
        let expected: &[u8] = &[
            // magic "OXNV"
            0x4f, 0x58, 0x4e, 0x56, //
            // version = NodeVersion::V1 (discriminant = 1)
            0x01, //
            // kind = 7
            0x07, //
            // _reserved
            0x00, 0x00,
        ];
        assert_eq!(h.as_bytes(), expected);
    }

    // ── LmdbLinkHeaderV1 layout ──────────────────────────────────────────────

    #[test]
    fn link_header_v1_size_is_stable() {
        assert_eq!(size_of::<LmdbLinkHeaderV1>(), 32);
        assert_eq!(LmdbLinkHeaderV1::SIZE, 32);
    }

    #[test]
    fn link_header_v1_offsets_are_stable() {
        assert_eq!(offset_of!(LmdbLinkHeaderV1, magic), 0);
        assert_eq!(offset_of!(LmdbLinkHeaderV1, version), 4);
        assert_eq!(offset_of!(LmdbLinkHeaderV1, has_parent), 5);
        assert_eq!(offset_of!(LmdbLinkHeaderV1, _reserved), 6);
        assert_eq!(offset_of!(LmdbLinkHeaderV1, num_children), 8);
        assert_eq!(offset_of!(LmdbLinkHeaderV1, parent), 16);
    }

    #[test]
    fn link_header_v1_golden_bytes_with_parent() {
        let h = LmdbLinkHeaderV1 {
            magic: LINK_MAGIC,
            version: LinkVersion::V1,
            has_parent: 1,
            _reserved: [0; 2],
            num_children: U64::new(3),
            parent: [0xab; 16],
        };
        let expected: &[u8] = &[
            // magic "OXLN"
            0x4f, 0x58, 0x4c, 0x4e, //
            // version = LinkVersion::V1 (discriminant = 1)
            0x01, //
            // has_parent = 1
            0x01, //
            // _reserved
            0x00, 0x00, //
            // num_children = 3 LE
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
            // parent [0xab; 16]
            0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, //
            0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
        ];
        assert_eq!(h.as_bytes(), expected);
    }

    // ── Version enums ────────────────────────────────────────────────────────

    /// Sanity check that the version enums occupy exactly 1 byte. Catches a
    /// future repr change before it silently breaks the header layout.
    #[test]
    fn version_enums_are_one_byte() {
        assert_eq!(size_of::<NodeVersion>(), 1);
        assert_eq!(size_of::<LinkVersion>(), 1);
        assert_eq!(size_of::<DupesVersion>(), 1);
        assert_eq!((NodeVersion::V1 as u8), 1);
        assert_eq!((LinkVersion::V1 as u8), 1);
        assert_eq!((DupesVersion::V1 as u8), 1);
    }

    // ── Round-trips ──────────────────────────────────────────────────────────

    #[test]
    fn lmdb_node_round_trip() {
        let n = LmdbNode {
            kind: MerkleTreeNodeType::Commit,
            data: vec![1, 2, 3, 4, 5],
        };
        let bytes = n.clone().encode();
        let r = LmdbNodeRef::from_bytes(&bytes).expect("ref parse");
        assert_eq!(r.kind().expect("kind"), MerkleTreeNodeType::Commit);
        assert_eq!(r.header.version, NodeVersion::V1);
        assert_eq!(r.data, &[1, 2, 3, 4, 5][..]);

        let owned = LmdbNode::decode(&bytes).expect("owned decode");
        assert_eq!(owned.kind, MerkleTreeNodeType::Commit);
        assert_eq!(owned.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn lmdb_link_round_trip_no_parent_no_children() {
        let l = LmdbLink {
            parent_id: None,
            children: vec![],
        };
        let bytes = l.encode();
        assert_eq!(bytes.len(), LmdbLinkHeaderV1::SIZE);
        let r = LmdbLinkRef::from_bytes(&bytes).expect("ref parse");
        assert_eq!(r.header.version, LinkVersion::V1);
        assert_eq!(r.parent_id(), None);
        assert_eq!(r.num_children(), 0);
    }

    #[test]
    fn lmdb_link_round_trip_with_parent_and_many_children() {
        let parent = MerkleHash::new(0xDEAD_BEEF);
        let children: Vec<HashCN> = (0..7)
            .map(|i| HashCN::from_raw_u128(0xC0DE_0000 + i as u128))
            .collect();
        let l = LmdbLink {
            parent_id: Some(parent),
            children: children.clone(),
        };
        let bytes = l.encode();
        let r = LmdbLinkRef::from_bytes(&bytes).expect("ref parse");
        assert_eq!(r.parent_id(), Some(parent));
        assert_eq!(r.num_children(), 7);
        let returned: Vec<HashCN> = r.children_iter().collect();
        assert_eq!(returned, children);
    }

    /// Two byte-identical files with different names share one content
    /// `MerkleHash` but get distinct `HashCN`s. As children they must both
    /// survive — this is the regression that the whole `HashCN` keying fixes.
    #[test]
    fn lmdb_link_round_trip_duplicate_content_distinct_hashcns() {
        // Same content hash for both, but distinct HashCN (different names).
        let content = MerkleHash::new(0x1234);
        let cat_2 = HashCN::new(
            &content,
            Filename::new_assume_invariants("cat_2.jpg").as_ref(),
        );
        let cat_3 = HashCN::new(
            &content,
            Filename::new_assume_invariants("cat_3.jpg").as_ref(),
        );
        assert_ne!(cat_2, cat_3, "different names must yield different HashCN");
        let children = vec![cat_2, cat_3];
        let l = LmdbLink {
            parent_id: Some(MerkleHash::new(7)),
            children: children.clone(),
        };
        let r_bytes = l.encode();
        let r = LmdbLinkRef::from_bytes(&r_bytes).expect("ref parse");
        assert_eq!(r.num_children(), 2);
        assert_eq!(r.children_iter().collect::<Vec<_>>(), children);
        assert_eq!(r.get_child(0).as_ref(), children.first());
        assert_eq!(r.get_child(1).as_ref(), children.get(1));
    }

    #[test]
    fn lmdb_node_ref_round_trip_unaligned() {
        // Force a misaligned source pointer by prepending a byte; the ref
        // parse must still succeed because every header field is alignment-1.
        let n = LmdbNode {
            kind: MerkleTreeNodeType::Dir,
            data: vec![0xCD; 17],
        };
        let canonical = n.encode();
        let mut buf = vec![0u8];
        buf.extend_from_slice(&canonical);
        let unaligned = &buf[1..];
        let r = LmdbNodeRef::from_bytes(unaligned).expect("must parse unaligned");
        assert_eq!(r.kind().expect("kind"), MerkleTreeNodeType::Dir);
        assert_eq!(r.data, &vec![0xCD; 17][..]);
    }

    #[test]
    fn lmdb_link_ref_round_trip_unaligned() {
        let l = LmdbLink {
            parent_id: Some(MerkleHash::new(42)),
            children: vec![HashCN::from_raw_u128(1), HashCN::from_raw_u128(2)],
        };
        let canonical = l.encode();
        let mut buf = vec![0u8];
        buf.extend_from_slice(&canonical);
        let unaligned = &buf[1..];
        let r = LmdbLinkRef::from_bytes(unaligned).expect("must parse unaligned");
        assert_eq!(r.parent_id(), Some(MerkleHash::new(42)));
        assert_eq!(r.num_children(), 2);
    }

    #[test]
    fn lmdb_node_bad_magic_rejected() {
        let mut bytes = LmdbNode {
            kind: MerkleTreeNodeType::Commit,
            data: vec![],
        }
        .encode();
        bytes[0] = b'X';
        let err = LmdbNodeRef::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, LmdbError::NodeBadMagic { .. }), "{err:?}");
    }

    #[test]
    fn lmdb_node_unsupported_version_rejected() {
        let mut bytes = LmdbNode {
            kind: MerkleTreeNodeType::Commit,
            data: vec![],
        }
        .encode();
        bytes[4] = 99; // not a known NodeVersion discriminant
        let err = LmdbNodeRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::NodeUnsupportedVersion(99)),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_link_unsupported_version_rejected() {
        let mut bytes = LmdbLink {
            parent_id: None,
            children: vec![],
        }
        .encode();
        bytes[4] = 42; // not a known LinkVersion discriminant
        let err = LmdbLinkRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::LinkUnsupportedVersion(42)),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_node_truncated_header_rejected() {
        let bytes = vec![0u8; 5]; // too small for the 8-byte header
        let err = LmdbNodeRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::NodeHeaderTruncated { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_link_truncated_header_rejected() {
        let bytes = vec![0u8; 10]; // too small for the 32-byte header
        let err = LmdbLinkRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::LinkHeaderTruncated { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_link_misaligned_tail_rejected() {
        // Build a valid header that claims 1 child, then put 15 bytes of tail
        // (not divisible by 16) — must fail with ChildrenTailMisaligned.
        let header = LmdbLinkHeaderV1 {
            magic: LINK_MAGIC,
            version: LinkVersion::V1,
            has_parent: 0,
            _reserved: [0; 2],
            num_children: U64::new(1),
            parent: [0; 16],
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(&[0u8; 15]);
        let err = LmdbLinkRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::ChildrenTailMisaligned { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_link_count_mismatch_rejected() {
        // Header claims 5 children but only 1 follows.
        let header = LmdbLinkHeaderV1 {
            magic: LINK_MAGIC,
            version: LinkVersion::V1,
            has_parent: 0,
            _reserved: [0; 2],
            num_children: U64::new(5),
            parent: [0; 16],
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(&[0u8; 16]);
        let err = LmdbLinkRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(
                err,
                LmdbError::ChildrenCountMismatch {
                    claimed: 5,
                    actual: 1
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_link_invalid_is_parent_rejected() {
        let mut bytes = LmdbLink {
            parent_id: None,
            children: vec![],
        }
        .encode();
        bytes[5] = 2; // has_parent must be 0 or 1 (offset is 5 now, post-shrink)
        let err = LmdbLinkRef::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, LmdbError::InvalidIsParent(2)), "{err:?}");
    }

    // ── LmdbDupesHeaderV1 layout ─────────────────────────────────────────────

    #[test]
    fn dupes_header_v1_size_is_stable() {
        assert_eq!(size_of::<LmdbDupesHeaderV1>(), 16);
        assert_eq!(LmdbDupesHeaderV1::SIZE, 16);
    }

    #[test]
    fn dupes_header_v1_offsets_are_stable() {
        assert_eq!(offset_of!(LmdbDupesHeaderV1, magic), 0);
        assert_eq!(offset_of!(LmdbDupesHeaderV1, version), 4);
        assert_eq!(offset_of!(LmdbDupesHeaderV1, _reserved), 5);
        assert_eq!(offset_of!(LmdbDupesHeaderV1, num_dupes), 8);
    }

    #[test]
    fn dupes_header_v1_golden_bytes() {
        let h = LmdbDupesHeaderV1 {
            magic: DUPES_MAGIC,
            version: DupesVersion::V1,
            _reserved: [0; 3],
            num_dupes: U64::new(2),
        };
        let expected: &[u8] = &[
            // magic "OXDP"
            0x4f, 0x58, 0x44, 0x50, //
            // version = DupesVersion::V1 (discriminant = 1)
            0x01, //
            // _reserved
            0x00, 0x00, 0x00, //
            // num_dupes = 2 LE
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(h.as_bytes(), expected);
    }

    // ── LmdbDupes round-trips ────────────────────────────────────────────────

    #[test]
    fn lmdb_dupes_round_trip_empty() {
        let bytes = LmdbDupes::new(vec![]).encode();
        assert_eq!(bytes.len(), LmdbDupesHeaderV1::SIZE);
        let r = LmdbDupesRef::from_bytes(&bytes).expect("ref parse");
        assert_eq!(r.header.version, DupesVersion::V1);
        assert_eq!(r.num_dupes(), 0);
    }

    #[test]
    fn lmdb_dupes_round_trip_many() {
        let hash_cns: Vec<HashCN> = (0..4).map(|i| HashCN::from_raw_u128(0xAB00 + i)).collect();
        let bytes = LmdbDupes::new(hash_cns.clone()).encode();
        let r = LmdbDupesRef::from_bytes(&bytes).expect("ref parse");
        assert_eq!(r.num_dupes(), 4);
        assert_eq!(r.hash_cns_iter().collect::<Vec<_>>(), hash_cns);

        let owned = LmdbDupes::decode(&bytes).expect("owned decode");
        assert_eq!(owned.hash_cns(), hash_cns.as_slice());
    }

    #[test]
    fn lmdb_dupes_ref_round_trip_unaligned() {
        let hash_cns = vec![HashCN::from_raw_u128(7), HashCN::from_raw_u128(9)];
        let canonical = LmdbDupes::new(hash_cns.clone()).encode();
        let mut buf = vec![0u8];
        buf.extend_from_slice(&canonical);
        let unaligned = &buf[1..];
        let r = LmdbDupesRef::from_bytes(unaligned).expect("must parse unaligned");
        assert_eq!(r.num_dupes(), 2);
        assert_eq!(r.hash_cns_iter().collect::<Vec<_>>(), hash_cns);
    }

    #[test]
    fn lmdb_dupes_bad_magic_rejected() {
        let mut bytes = LmdbDupes::new(vec![]).encode();
        bytes[0] = b'X';
        let err = LmdbDupesRef::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, LmdbError::DupesBadMagic { .. }), "{err:?}");
    }

    #[test]
    fn lmdb_dupes_unsupported_version_rejected() {
        let mut bytes = LmdbDupes::new(vec![]).encode();
        bytes[4] = 99; // not a known DupesVersion discriminant
        let err = LmdbDupesRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::DupesUnsupportedVersion(99)),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_dupes_truncated_header_rejected() {
        let bytes = vec![0u8; 5]; // too small for the 16-byte header
        let err = LmdbDupesRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::DupesHeaderTruncated { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_dupes_misaligned_tail_rejected() {
        // Valid header claiming 1 dupe, then 15 tail bytes (not a multiple of 16).
        let header = LmdbDupesHeaderV1 {
            magic: DUPES_MAGIC,
            version: DupesVersion::V1,
            _reserved: [0; 3],
            num_dupes: U64::new(1),
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(&[0u8; 15]);
        let err = LmdbDupesRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, LmdbError::DupesTailMisaligned { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn lmdb_dupes_count_mismatch_rejected() {
        // Header claims 5 dupes but only 1 follows.
        let header = LmdbDupesHeaderV1 {
            magic: DUPES_MAGIC,
            version: DupesVersion::V1,
            _reserved: [0; 3],
            num_dupes: U64::new(5),
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(&[0u8; 16]);
        let err = LmdbDupesRef::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(
                err,
                LmdbError::DupesCountMismatch {
                    claimed: 5,
                    actual: 1
                }
            ),
            "{err:?}"
        );
    }
}
