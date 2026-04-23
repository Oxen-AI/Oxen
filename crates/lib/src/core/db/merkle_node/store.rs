//! Type-erased dispatch enums for the Merkle node store.
//!
//! The core merkle-store traits ([`MerkleReader`], [`MerkleWriter`],
//! [`MerkleWriteSession`], [`NodeWriteSession`]) use GATs to name the
//! session/node-session types returned by `begin()` and `create_node()`. GATs
//! make the traits non-object-safe, and returning an opaque `impl MerkleWriter`
//! from [`LocalRepository::merkle_store`] interacts badly with the borrow
//! checker's dropck — `let s = store.begin()?; ...; s.finish()?;` fails to
//! compile because the opaque types' projections keep the borrow alive past
//! the move into `finish()`.
//!
//! These three enums give us one concrete, named type per trait layer that
//! dispatches at each call to the chosen backend. Today there is only one
//! variant ([`MerkleNodeStore::File`]). The `_Lifetime` variants on the two
//! session enums are intentional placeholders that keep their lifetime
//! parameters load-bearing until the LMDB variant is added in Phase 2.
//!
//! [`LocalRepository::merkle_store`]: crate::model::LocalRepository::merkle_store
//! [`MerkleReader`]: crate::model::merkle_tree::merkle_reader::MerkleReader
//! [`MerkleWriter`]: crate::model::merkle_tree::merkle_writer::MerkleWriter
//! [`MerkleWriteSession`]: crate::model::merkle_tree::merkle_writer::MerkleWriteSession
//! [`NodeWriteSession`]: crate::model::merkle_tree::merkle_writer::NodeWriteSession

use std::marker::PhantomData;

use crate::core::db::merkle_node::file_backend::{FileBackend, FileNodeSession, FileWriteSession};
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{MerkleHash, TMerkleTreeNode};

/// Dispatch enum over every Merkle node store backend.
///
/// Returned by [`LocalRepository::merkle_store`]. Callers interact with this
/// via the merkle-store traits; the `Error` associated type is [`OxenError`]
/// on all layers, so `?` works in functions returning `Result<_, OxenError>`.
///
/// [`LocalRepository::merkle_store`]: crate::model::LocalRepository::merkle_store
pub enum MerkleNodeStore<'repo> {
    File(FileBackend<'repo>),
}

impl<'repo> MerkleNodeStore<'repo> {
    pub fn file(repo: &'repo LocalRepository) -> Self {
        Self::File(FileBackend::new(repo))
    }
}

/// Dispatch enum over every backend's write session.
///
/// The `'a` parameter mirrors the GAT lifetime on [`MerkleWriter::Session`]
/// and keeps the session's borrow of the parent store load-bearing.
/// `_Lifetime` is a placeholder variant that keeps `'a` used until Phase 2's
/// LMDB variant is added.
pub enum MerkleNodeStoreSession<'a, 'repo> {
    File(FileWriteSession<'repo>),
    #[doc(hidden)]
    _Lifetime(PhantomData<&'a ()>),
}

/// Dispatch enum over every backend's per-node write session.
///
/// See [`MerkleNodeStoreSession`] for the rationale on the `_Lifetime`
/// placeholder variant.
///
/// The `File` variant is large today (the phantom variant is zero-sized);
/// that imbalance goes away once the LMDB variant lands.
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
            MerkleNodeStore::File(b) => b.exists(hash).map_err(Into::into),
        }
    }

    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, OxenError> {
        match self {
            MerkleNodeStore::File(b) => b.get_node(hash).map_err(Into::into),
        }
    }

    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError> {
        match self {
            MerkleNodeStore::File(b) => b.get_children(hash).map_err(Into::into),
        }
    }
}

impl<'repo> MerkleWriter for MerkleNodeStore<'repo> {
    type Error = OxenError;
    #[rustfmt::skip]
    type Session<'a> = MerkleNodeStoreSession<'a, 'repo> where Self: 'a;

    fn begin(&self) -> Result<MerkleNodeStoreSession<'_, 'repo>, OxenError> {
        match self {
            MerkleNodeStore::File(b) => {
                let session = b.begin().map_err(OxenError::from)?;
                Ok(MerkleNodeStoreSession::File(session))
            }
        }
    }
}

impl<'a, 'repo: 'a> MerkleWriteSession<'a> for MerkleNodeStoreSession<'a, 'repo> {
    type Error = OxenError;
    #[rustfmt::skip]
    type NodeSession<'b> = MerkleNodeStoreNodeSession<'b, 'repo> where Self: 'b;

    fn create_node<'b, N: TMerkleTreeNode>(
        &'b self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<MerkleNodeStoreNodeSession<'b, 'repo>, OxenError> {
        match self {
            MerkleNodeStoreSession::File(s) => {
                let ns = s.create_node(node, parent_id).map_err(OxenError::from)?;
                Ok(MerkleNodeStoreNodeSession::File(ns))
            }
            MerkleNodeStoreSession::_Lifetime(_) => {
                unreachable!("_Lifetime variant is a phantom placeholder")
            }
        }
    }

    fn finish(self) -> Result<(), OxenError> {
        match self {
            MerkleNodeStoreSession::File(s) => s.finish().map_err(Into::into),
            MerkleNodeStoreSession::_Lifetime(_) => {
                unreachable!("_Lifetime variant is a phantom placeholder")
            }
        }
    }
}

impl<'a, 'repo> NodeWriteSession for MerkleNodeStoreNodeSession<'a, 'repo> {
    type Error = OxenError;

    fn node_id(&self) -> &MerkleHash {
        match self {
            MerkleNodeStoreNodeSession::File(ns) => ns.node_id(),
            MerkleNodeStoreNodeSession::_Lifetime(_) => {
                unreachable!("_Lifetime variant is a phantom placeholder")
            }
        }
    }

    fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), OxenError> {
        match self {
            MerkleNodeStoreNodeSession::File(ns) => ns.add_child(child).map_err(Into::into),
            MerkleNodeStoreNodeSession::_Lifetime(_) => {
                unreachable!("_Lifetime variant is a phantom placeholder")
            }
        }
    }

    fn finish(self) -> Result<(), OxenError> {
        match self {
            MerkleNodeStoreNodeSession::File(ns) => ns.finish().map_err(Into::into),
            MerkleNodeStoreNodeSession::_Lifetime(_) => {
                unreachable!("_Lifetime variant is a phantom placeholder")
            }
        }
    }
}
