//! Private enum-dispatch for merkle-store backends.
//!
//! [`LocalRepository::merkle_store`] returns an opaque `impl MerkleStore`
//! backed by the [`StoreEnum`] generated here. Callers never name the enum —
//! it is `pub(super)` and only the parent `local_repository` module wraps
//! construction.
//!
//! To add a new backend, add one line to the
//! [`define_merkle_store_dispatch!`] invocation at the bottom of this file.
//! The macro expands every delegating trait impl and the unified error enum;
//! no hand-written match arms need to be edited.
//!
//! [`LocalRepository::merkle_store`]: super::LocalRepository::merkle_store

use crate::core::db::merkle_node::FileBackend;
use crate::core::db::merkle_node::merkle_node_db::MerkleDbError;
use crate::error::{IntoOxenError, OxenError};
use crate::model::MerkleHash;
use crate::model::TMerkleTreeNode;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_transport::{
    MerklePacker, MerkleUnpacker, PackOptions, UnpackOptions,
};
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::merkle_tree::node::MerkleTreeNode;

/// Declare the merkle-store dispatch enum and every delegating trait impl
/// from a single list of backends. Each entry is
/// `VariantName => Backend<'repo>, error = BackendError`.
///
/// The macro also generates a unified `StoreError` enum with one variant per
/// backend's native error. It derives `IntoOxenError` by delegating to each
/// backend's own `IntoOxenError` impl, so errors funnel cleanly into
/// `OxenError` via the crate-wide blanket `From<E: IntoOxenError> for OxenError`.
///
/// Implementation note: none of this should be visible to users depending on
/// `liboxen`. We use `pub(super)` so that `local_repository` is the only
/// module that can use this macro and associated dispatch-enum.
macro_rules! define_merkle_store_dispatch {
    ( $( $variant:ident => $backend:ty, error = $err:ty ),* $(,)? ) => {
        /// Unified error for the dispatch enum. One variant per backend.
        #[derive(Debug, thiserror::Error)]
        pub(super) enum StoreError {
            $(
                #[error(transparent)]
                $variant($err),
            )*
        }

        impl IntoOxenError for StoreError {
            fn into_oxen(self) -> OxenError {
                match self {
                    $( Self::$variant(e) => e.into_oxen() ),*
                }
            }
        }

        /// Dispatch enum over merkle-store backends.
        pub(super) enum StoreEnum<'repo> {
            $( $variant($backend) ),*
        }

        /// Dispatch enum over per-backend [`MerkleWriteSession`] types. Each
        /// variant type is named via GAT projection from the backend type, so
        /// adding a backend doesn't require naming its session type.
        pub(super) enum SessionEnum<'repo> {
            $( $variant(<$backend as MerkleWriter>::Session<'repo>) ),*
        }

        /// Dispatch enum over per-backend [`NodeWriteSession`] types.
        pub(super) enum NodeSessionEnum<'a, 'repo: 'a> {
            $(
                $variant(
                    <<$backend as MerkleWriter>::Session<'repo>
                        as MerkleWriteSession>::NodeSession<'a>
                )
            ),*
        }

        impl<'repo> MerkleReader for StoreEnum<'repo> {
            type Error = StoreError;

            fn exists(&self, hash: &MerkleHash) -> Result<bool, StoreError> {
                match self {
                    $( Self::$variant(b) => b.exists(hash).map_err(StoreError::$variant) ),*
                }
            }

            fn get_node(
                &self,
                hash: &MerkleHash,
            ) -> Result<Option<MerkleNodeRecord>, StoreError> {
                match self {
                    $( Self::$variant(b) => b.get_node(hash).map_err(StoreError::$variant) ),*
                }
            }

            fn get_children(
                &self,
                hash: &MerkleHash,
            ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, StoreError> {
                match self {
                    $( Self::$variant(b) => b.get_children(hash).map_err(StoreError::$variant) ),*
                }
            }
        }

        impl<'repo> MerkleWriter for StoreEnum<'repo> {
            type Error = StoreError;
            type Session<'a> = SessionEnum<'repo> where Self: 'a;

            fn begin(&self) -> Result<SessionEnum<'repo>, StoreError> {
                match self {
                    $(
                        Self::$variant(b) => b
                            .begin()
                            .map(SessionEnum::$variant)
                            .map_err(StoreError::$variant)
                    ),*
                }
            }
        }

        impl<'repo> MerkleWriteSession for SessionEnum<'repo> {
            type Error = StoreError;
            type NodeSession<'a>
                = NodeSessionEnum<'a, 'repo>
            where
                Self: 'a;

            fn create_node<'a, N: TMerkleTreeNode>(
                &'a self,
                node: &N,
                parent_id: Option<MerkleHash>,
            ) -> Result<NodeSessionEnum<'a, 'repo>, StoreError> {
                match self {
                    $(
                        Self::$variant(s) => s
                            .create_node(node, parent_id)
                            .map(NodeSessionEnum::$variant)
                            .map_err(StoreError::$variant)
                    ),*
                }
            }

            fn finish(self) -> Result<(), StoreError> {
                match self {
                    $( Self::$variant(s) => s.finish().map_err(StoreError::$variant) ),*
                }
            }
        }

        impl<'a, 'repo: 'a> NodeWriteSession for NodeSessionEnum<'a, 'repo> {
            type Error = StoreError;

            fn node_id(&self) -> &MerkleHash {
                match self {
                    $( Self::$variant(n) => n.node_id() ),*
                }
            }

            fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), StoreError> {
                match self {
                    $( Self::$variant(n) => n.add_child(child).map_err(StoreError::$variant) ),*
                }
            }

            fn finish(self) -> Result<(), StoreError> {
                match self {
                    $( Self::$variant(n) => n.finish().map_err(StoreError::$variant) ),*
                }
            }
        }

        impl<'repo> MerklePacker for StoreEnum<'repo> {
            type Error = StoreError;

            fn pack_nodes<W: std::io::Write>(
                &self,
                hashes: &std::collections::HashSet<MerkleHash>,
                opts: PackOptions,
                out: W,
            ) -> Result<(), StoreError> {
                match self {
                    $( Self::$variant(b) => b.pack_nodes(hashes, opts, out).map_err(StoreError::$variant) ),*
                }
            }

            fn pack_all<W: std::io::Write>(&self, out: W) -> Result<(), StoreError> {
                match self {
                    $( Self::$variant(b) => b.pack_all(out).map_err(StoreError::$variant) ),*
                }
            }
        }

        impl<'repo> MerkleUnpacker for StoreEnum<'repo> {
            type Error = StoreError;

            fn unpack<R: std::io::Read>(
                &self,
                reader: R,
                opts: UnpackOptions,
            ) -> Result<std::collections::HashSet<MerkleHash>, StoreError> {
                match self {
                    $( Self::$variant(b) => b.unpack(reader, opts).map_err(StoreError::$variant) ),*
                }
            }
        }
    };
}

// === Backend registry. ===
// Adding a new backend is a single line: `Variant => Backend<'repo>, error = Backend's Error type`.
// Every dispatch impl and the `StoreError` variant are regenerated by the macro.
define_merkle_store_dispatch! {
    File => FileBackend<'repo>, error = MerkleDbError,
}
