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
use crate::model::{MerkleHash, MerkleTreeNodeType};

/// 4-byte magic prefix for `merkle_tree_nodes` values.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
pub(super) const NODE_MAGIC: [u8; 4] = *b"OXNV";

/// 4-byte magic prefix for `merkle_links` values.
// WARNING: DO NOT CHANGE THIS!!!!
// MIGRATION WARNING: CHANGING THIS **REQUIRES** A MIGRATION!
pub(super) const LINK_MAGIC: [u8; 4] = *b"OXLN";

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
// Zero-copy reader views.
// `'a` is the LMDB read transaction's lifetime: the borrows end when the rtxn
// drops, which the borrow checker enforces.
// ─────────────────────────────────────────────────────────────────────────────

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
    pub(crate) fn kind(&self) -> Result<MerkleTreeNodeType, LmdbError> {
        Ok(MerkleTreeNodeType::from_u8(self.header.kind)?)
    }
}

/// Zero-copy view over a `merkle_links` value.
#[derive(Debug)]
pub(crate) struct LmdbLinkRef<'a> {
    pub header: &'a LmdbLinkHeaderV1,
    /// Children stored as `[u8; 16]` little-endian merkle hashes. Length is
    /// validated against `header.num_children` at parse time.
    pub child_hashes: &'a [[u8; 16]],
}

impl<'a> LmdbLinkRef<'a> {
    /// Parse a stored `merkle_links` value with no copying. See
    /// [`LmdbNodeRef::from_bytes`] for the validation-order rationale.
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

    pub(crate) fn parent_id(&self) -> Option<MerkleHash> {
        if self.header.has_parent == 1 {
            Some(MerkleHash::new(u128::from_le_bytes(self.header.parent)))
        } else {
            None
        }
    }

    pub(crate) fn children_iter(&self) -> impl Iterator<Item = MerkleHash> + '_ {
        self.child_hashes
            .iter()
            .map(|h| MerkleHash::new(u128::from_le_bytes(*h)))
    }

    pub(crate) fn num_children(&self) -> usize {
        self.child_hashes.len()
    }
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
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LmdbError> {
        let r = LmdbNodeRef::from_bytes(bytes)?;
        Ok(Self {
            kind: r.kind()?,
            data: r.data.to_vec(),
        })
    }

    /// The type of Merkle tree node that's serialized.
    pub fn kind(&self) -> MerkleTreeNodeType {
        self.kind
    }

    /// A reference to the msgpack-encoded bytes of the node.
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
    /// The children of this node, if it has any.
    pub(crate) children: Vec<MerkleHash>,
}

impl LmdbLink {
    /// Encode for storage in `merkle_links`. Produces `[header][child_hashes]`.
    /// The header is a [`LmdbLinkHeaderV1`].
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
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LmdbError> {
        let r = LmdbLinkRef::from_bytes(bytes)?;
        Ok(Self {
            parent_id: r.parent_id(),
            children: r.children_iter().collect(),
        })
    }

    /// A reference to this node's optional parent hash.
    pub fn parent_id(&self) -> Option<&MerkleHash> {
        self.parent_id.as_ref()
    }

    /// A reference to this node's children.
    pub fn children(&self) -> &[MerkleHash] {
        &self.children
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!((NodeVersion::V1 as u8), 1);
        assert_eq!((LinkVersion::V1 as u8), 1);
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
        let children: Vec<MerkleHash> = (0..7)
            .map(|i| MerkleHash::new(0xC0DE_0000 + i as u128))
            .collect();
        let l = LmdbLink {
            parent_id: Some(parent),
            children: children.clone(),
        };
        let bytes = l.encode();
        let r = LmdbLinkRef::from_bytes(&bytes).expect("ref parse");
        assert_eq!(r.parent_id(), Some(parent));
        assert_eq!(r.num_children(), 7);
        let returned: Vec<MerkleHash> = r.children_iter().collect();
        assert_eq!(returned, children);
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
            children: vec![MerkleHash::new(1), MerkleHash::new(2)],
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
}
