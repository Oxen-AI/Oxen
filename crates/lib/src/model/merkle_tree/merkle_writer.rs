use crate::error::IntoOxenError;
use crate::model::{MerkleHash, TMerkleTreeNode};

/// Interface for writing to a Merkle tree store.
pub trait MerkleWriter: Send + Sync {
    /// The error type for the Merkle tree's underlying storage layer.
    ///
    /// Backends may use whichever error type is natural for their storage
    /// (e.g. [`MerkleDbError`] for the [`FileBackend`]). The `Into<OxenError>`
    /// bound on the associated type propagates as an implied bound at every
    /// use site, so generic callers can convert errors via
    /// `?` with no additional `where` clauses.
    type Error: std::error::Error + IntoOxenError;

    /// The write session that manages writing multiple nodes to the store.
    type Session<'a>: MerkleWriteSession<Error = Self::Error>
    where
        Self: 'a;

    /// Create a new write session to write many changed Merkle tree nodes to the store.
    ///
    /// To ensure that changes are persisted, callers must call [`MerkleWriteSession::finish`] on the returned session.
    /// Correct use of the returned session is to create a [`NodeWriteSession`] for each node to be
    /// written, and then call [`NodeWriteSession::finish`] on that session when complete with the node.
    fn begin(&self) -> Result<Self::Session<'_>, Self::Error>;
}

/// A write session for writing multiple nodes to the Merkle tree store.
///
/// A [`MerkleWriteSession`] is used to create multiple [`NodeWriteSession`]s, each of which
/// represents a single node being written to the store. Typical usage is to create a single
/// [`MerkleWriteSession`] when committing repository changes. From this one write session,
/// callers will create multiple [`NodeWriteSession`]s to write the nodes they need to store.
/// Each [`NodeWriteSession`] must have its [`finish`] called to finalize the written node
/// information. Once all nodes have been written, the [`finish`] method of the [`MerkleWriteSession`]
/// must be called to persist the changes to the store.
///
/// Persistence and eagerness of writes are implementation details. Implementations may choose
/// to buffer writes or write immediately to the store when [`create_node`] and [`add_child`]
/// are called. The invariant is that [`finish`] must be called to **ensure** that writes are
/// persisted. An implementation may choose to e.g. have a transaction mechanism to roll-back
/// changes on `Err`. However, implementations are not required to support this.
pub trait MerkleWriteSession {
    /// The error type for the Merkle tree's underlying storage layer.
    /// Must be convertible into an [`OxenError`].
    type Error: std::error::Error + IntoOxenError;

    /// The write session that manages writing a single node's information to the store.
    type NodeSession<'b>: NodeWriteSession<Error = Self::Error>
    where
        Self: 'b;

    /// Begin the process of writing the node to the Merkle tree store.
    ///
    /// The returned node write session is used to add children, if the node is a directory
    /// or vnode. Callers are responsible for calling `finish` on the returned session
    /// to ensure that their writes will be made available to the Merkle tree store.
    ///
    /// Note that any written nodes are not required to be persisted to the store until
    /// _this_ write session's [`finish`] is called.
    fn create_node<'b, N: TMerkleTreeNode>(
        &'b self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self::NodeSession<'b>, Self::Error>;

    /// Ensure that all content from all finished node write sessions have been written to the
    /// Merkle tree store. Consumes the session: any active [`NodeWriteSession`]s borrowing from
    /// this one must already have been finished (and thus dropped) before this is called — the
    /// borrow checker enforces that invariant.
    fn finish(self) -> Result<(), Self::Error>;
}

/// A write session for a single node being constructed.
///
/// Implementations may buffer the `node` and `children` information in memory or choose to write
/// the data to the store eagerly. However, if [`finish`] is called and returns `Ok`, then the
/// guarantee is that all node and child information must be persisted to the store.
pub trait NodeWriteSession {
    /// The error type for the Merkle tree's underlying storage layer.
    /// Must be convertible into an [`OxenError`].
    type Error: std::error::Error + IntoOxenError;

    /// The hash of the node being written in this session.
    fn node_id(&self) -> &MerkleHash;

    /// Add a child to the current node.
    fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), Self::Error>;

    /// Ensure the node and its children have been written to the Merkle tree store. Consumes
    /// the node session; releases the borrow on the parent [`MerkleWriteSession`].
    fn finish(self) -> Result<(), Self::Error>;
}
