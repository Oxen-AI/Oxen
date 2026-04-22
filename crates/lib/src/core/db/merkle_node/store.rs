use std::marker::PhantomData;

use crate::error::OxenError;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{MerkleHash, TMerkleTreeNode};

use super::file_backend::{FileBackend, FileNodeSession, FileWriteSession};

/// Pluggable merkle node storage backend.
///
/// Dispatch is via enum variant, not `Box<dyn MerkleStore>` (the writer trait's
/// GATs rule that out). Phase 2 will add an `Lmdb(LmdbBackend)` variant; adding
/// a variant requires no changes to any caller.
pub enum MerkleNodeStore<'repo> {
    File(FileBackend<'repo>),
    // Phase 2: Lmdb(LmdbBackend),
}

/// Top-level write session dispatched to a variant's session.
///
/// The `_Lifetime` variant exists to keep the `'a` lifetime parameter
/// load-bearing so that Phase 2's `Lmdb(LmdbWriteSession<'a>)` fits the same
/// enum type signature. It is never constructed.
pub enum MerkleNodeStoreSession<'a, 'repo> {
    File(FileWriteSession<'repo>),
    #[doc(hidden)]
    _Lifetime(PhantomData<&'a ()>),
}

/// Per-node write handle dispatched to a variant's session.
///
/// See [`MerkleNodeStoreSession`] for the rationale behind `_Lifetime`. The
/// large-variant lint fires because the placeholder variant is empty; boxing
/// the real variant would cost an allocation per node write with no benefit.
#[allow(clippy::large_enum_variant)]
pub enum MerkleNodeStoreNodeSession<'a, 'repo> {
    File(FileNodeSession),
    #[doc(hidden)]
    _Lifetime(PhantomData<(&'a (), &'repo ())>),
}

impl<'repo> MerkleReader for MerkleNodeStore<'repo> {
    type Error = OxenError;

    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        match self {
            Self::File(b) => b.exists(hash).map_err(Into::into),
        }
    }

    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, OxenError> {
        match self {
            Self::File(b) => b.get_node(hash).map_err(Into::into),
        }
    }

    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError> {
        match self {
            Self::File(b) => b.get_children(hash).map_err(Into::into),
        }
    }
}

impl<'repo> MerkleWriter for MerkleNodeStore<'repo> {
    type Error = OxenError;
    type Session<'a>
        = MerkleNodeStoreSession<'a, 'repo>
    where
        Self: 'a;

    fn begin(&self) -> Result<MerkleNodeStoreSession<'_, 'repo>, OxenError> {
        match self {
            Self::File(b) => b
                .begin()
                .map(MerkleNodeStoreSession::File)
                .map_err(Into::into),
        }
    }
}

impl<'a, 'repo> MerkleWriteSession<'a> for MerkleNodeStoreSession<'a, 'repo> {
    type Error = OxenError;
    type NodeSession<'b>
        = MerkleNodeStoreNodeSession<'b, 'repo>
    where
        Self: 'b;

    fn create_node<'b, N: TMerkleTreeNode>(
        &'b self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<MerkleNodeStoreNodeSession<'b, 'repo>, OxenError> {
        match self {
            Self::File(s) => s
                .create_node(node, parent_id)
                .map(MerkleNodeStoreNodeSession::File)
                .map_err(Into::into),
            Self::_Lifetime(_) => unreachable!(),
        }
    }

    fn finish(self) -> Result<(), OxenError> {
        match self {
            Self::File(s) => s.finish().map_err(Into::into),
            Self::_Lifetime(_) => unreachable!(),
        }
    }
}

impl<'a, 'repo> NodeWriteSession for MerkleNodeStoreNodeSession<'a, 'repo> {
    type Error = OxenError;

    fn node_id(&self) -> &MerkleHash {
        match self {
            Self::File(s) => s.node_id(),
            Self::_Lifetime(_) => unreachable!(),
        }
    }

    fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), OxenError> {
        match self {
            Self::File(s) => s.add_child(child).map_err(Into::into),
            Self::_Lifetime(_) => unreachable!(),
        }
    }

    fn finish(self) -> Result<(), OxenError> {
        match self {
            Self::File(s) => s.finish().map_err(Into::into),
            Self::_Lifetime(_) => unreachable!(),
        }
    }
}
