use crate::error::OxenError;
use crate::model::{MerkleHash, TMerkleTreeNode};

/// Interface for writing to a Merkle tree store.
///
/// Dyn-compatible: callers can store this as `Box<dyn MerkleWriter + '_>`
/// or `&dyn MerkleWriter`.
pub trait MerkleWriter: Send + Sync {
    /// Create a new write session to write many changed Merkle tree nodes to the store.
    ///
    /// The returned session is boxed and tied to `&self`'s lifetime. Callers must call
    /// [`MerkleWriteSession::finish`] on it to ensure their writes have been persisted —
    /// because `finish` consumes a `Box<Self>`, the natural shape `session.finish()?`
    /// works directly on the box returned here.
    ///
    /// Correct use of the returned session is to create a [`NodeWriteSession`] for each
    /// node to be written, and then call [`NodeWriteSession::finish`] on that session
    /// when complete with the node.
    fn begin<'a>(&'a self) -> Result<Box<dyn MerkleWriteSession + 'a>, OxenError>;
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
///
/// Object-safe: lives behind `Box<dyn MerkleWriteSession + '_>`. `finish` takes
/// `self: Box<Self>` so the trait is dyn-callable; the natural usage
/// `session.finish()?` on a `Box<dyn ...>` value works directly.
pub trait MerkleWriteSession {
    /// Begin the process of writing the node to the Merkle tree store.
    ///
    /// The returned node write session is used to add children, if the node is a directory
    /// or vnode. Callers are responsible for calling `finish` on the returned session
    /// to ensure that their writes will be made available to the Merkle tree store.
    ///
    /// Note that any written nodes are not required to be persisted to the store until
    /// _this_ write session's [`finish`] is called.
    fn create_node<'a>(
        &'a self,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Box<dyn NodeWriteSession + 'a>, OxenError>;

    /// Ensure that all content from all finished node write sessions have been written to the
    /// Merkle tree store.
    ///
    /// Consumes the boxed session via `self: Box<Self>` so the trait is object-safe.
    /// Any active [`NodeWriteSession`]s borrowing from this one must already have been
    /// finished before this is called — the borrow checker enforces that invariant.
    fn finish(self: Box<Self>) -> Result<(), OxenError>;
}

/// A write session for a single node being constructed.
///
/// Implementations may buffer the `node` and `children` information in memory or choose to write
/// the data to the store eagerly. However, if [`finish`] is called and returns `Ok`, then the
/// guarantee is that all node and child information must be persisted to the store.
///
/// Object-safe: lives behind `Box<dyn NodeWriteSession + '_>`. `finish` takes
/// `self: Box<Self>` for the same reason as [`MerkleWriteSession::finish`].
pub trait NodeWriteSession {
    /// The hash of the node being written in this session.
    fn node_id(&self) -> &MerkleHash;

    /// Add a child to the current node.
    fn add_child(&mut self, child: &dyn TMerkleTreeNode) -> Result<(), OxenError>;

    /// Ensure the node and its children have been written to the Merkle tree store.
    /// Consumes the boxed session; releases the borrow on the parent
    /// [`MerkleWriteSession`].
    fn finish(self: Box<Self>) -> Result<(), OxenError>;
}
