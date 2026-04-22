use crate::core::db::merkle_node::merkle_node_db::{MerkleDbError, MerkleNodeDB};
use crate::model::LocalRepository;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{MerkleHash, TMerkleTreeNode};

/// File-based Merkle node store backend. Implements the [`MerkleStore`] trait.
///
/// Holds a borrowed `&LocalRepository` so it can delegate straight to
/// [`MerkleNodeDB`]'s existing repository-based methods without any modification.
/// Construction is O(1); feel free to call `LocalRepository::merkle_store` on
/// each operation.
pub struct FileBackend<'repo> {
    repo: &'repo LocalRepository,
}

impl<'repo> FileBackend<'repo> {
    pub fn new(repo: &'repo LocalRepository) -> Self {
        Self { repo }
    }
}

/// Merkle reader implementation for the [`FileBackend`].
impl<'repo> MerkleReader for FileBackend<'repo> {
    type Error = MerkleDbError;

    /// Checks if a node with the given `hash` exists in the store.
    ///
    /// Alias for [`MerkleNodeDB::exists`].
    fn exists(&self, hash: &MerkleHash) -> Result<bool, MerkleDbError> {
        Ok(MerkleNodeDB::exists(self.repo, hash))
    }

    /// Retrieves the node with the given `hash` from the store. `None` means no such node exists.
    ///
    /// Alias for [`MerkleNodeDB::open_read_only`].
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, MerkleDbError> {
        if !MerkleNodeDB::exists(self.repo, hash) {
            return Ok(None);
        }
        let db = MerkleNodeDB::open_read_only(self.repo, hash)?;
        let node = db.node()?;
        Ok(Some(MerkleNodeRecord::new(
            db.node_id,
            db.dtype,
            db.parent_id,
            node,
            db.num_children(),
        )))
    }

    /// Retrieves the children of the node with the given `hash` from the store.
    /// An empty vec means that either the node is a not a directory or virtual node or it is one
    /// but has no files.
    ///
    /// Alias for [`MerkleNodeDB::open_read_only`] & a `.map()` call..
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, MerkleDbError> {
        let mut db = MerkleNodeDB::open_read_only(self.repo, hash)?;
        db.map()
    }
}

/// Merkle writer implementation for the [`FileBackend`].
impl<'repo> MerkleWriter for FileBackend<'repo> {
    type Error = MerkleDbError;
    #[rustfmt::skip]
    type Session<'a> = FileWriteSession<'repo> where Self: 'a;

    fn begin(&self) -> Result<FileWriteSession<'repo>, MerkleDbError> {
        Ok(FileWriteSession { repo: self.repo })
    }
}

/// Write session for the file backend. Used to write multiple nodes & their children.
///
/// Writes happen eagerly through each [`FileNodeSession`]; this session's
/// [`finish`] is a no-op. The `Drop` impl below exists for symmetry with
/// [`FileNodeSession`] — the compiler elides it in release builds.
pub struct FileWriteSession<'repo> {
    repo: &'repo LocalRepository,
}

/// Merkle write session implementation that the [`FileBackend`] uses.
impl<'a, 'repo: 'a> MerkleWriteSession<'a> for FileWriteSession<'repo> {
    type Error = MerkleDbError;
    #[rustfmt::skip]
    type NodeSession<'b> = FileNodeSession where Self: 'b;

    /// Creates a new session for writing a `node` and `children` file.
    /// Calls [`MerkleNodeDb::open_read_write`] internally.
    fn create_node<N: TMerkleTreeNode>(
        &self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<FileNodeSession, MerkleDbError> {
        Ok(FileNodeSession::new(MerkleNodeDB::open_read_write(
            self.repo, node, parent_id,
        )?))
    }

    /// A no-op -- the node write session from [`create_node`] eagerly writes its files.
    /// The [`FileNodeSession::finish`] method flushes and closes open file handles.
    fn finish(self) -> Result<(), MerkleDbError> {
        Ok(())
    }
}

/// Per-node write handle for the file backend. Writes exactly 1 `node` and 1 `children` file.
///
/// Acts as a newtype around [`MerkleNodeDB`] with a `finished` sentinel that guards [`Drop`]
/// against double-closing the underlying file handles. When required, the drop implementation
/// will call [`FileNodeSession::finish`].
pub struct FileNodeSession {
    db: MerkleNodeDB,
    finished: bool,
}

impl FileNodeSession {
    /// The [`MerkleNodeDb`] **MUST** be opened in read-write mode.
    fn new(db: MerkleNodeDB) -> Self {
        Self {
            db,
            finished: false,
        }
    }

    /// The `finish` implementation, but using `&mut self` so that it can be used in `Drop`.
    fn idempotent_finish(&mut self) -> Result<(), MerkleDbError> {
        if self.finished {
            Ok(())
        } else {
            self.finished = true;
            MerkleNodeDB::close(&mut self.db)
        }
    }
}

/// Ensure that the `node` and `children` file handles are flushed and closed when dropped.
impl Drop for FileNodeSession {
    fn drop(&mut self) {
        self.idempotent_finish()
            .expect("Did not explicitly call finish() and encountered an error.");
    }
}

/// Merkle node write session that the [`FileBackend`] uses.
impl NodeWriteSession for FileNodeSession {
    type Error = MerkleDbError;

    /// The node currently being written.
    fn node_id(&self) -> &MerkleHash {
        &self.db.node_id
    }

    /// Adds an entry to the `children` file for the current node. Alias for [`MerkleNodeDB::add_child`].
    fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), MerkleDbError> {
        MerkleNodeDB::add_child(&mut self.db, child)
    }

    /// Flushes the open `node` and `children` file handles, closes them, then calls `fsync` on them.
    /// Is idempotent and calls [`MerkleNodeDb::close`] internally when required.
    fn finish(mut self) -> Result<(), MerkleDbError> {
        (&mut self).idempotent_finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::merkle_tree::node::{CommitNode, DirNode};
    use crate::test;

    /// Dropping a `FileNodeSession` without calling `finish()` must still
    /// flush+sync the underlying files. This is to match the implicit-drop semantics
    /// that `MerkleNodeDB` has before these `MerkleStore` traits were introduced.
    #[test]
    fn drop_finishes_file_node_session() -> Result<(), crate::error::OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let commit = CommitNode::default();
            let dir = DirNode::default();
            let commit_hash = commit.hash();

            // Scope the session so Drop runs at its end.
            {
                let store = repo.merkle_store();
                let session = store.begin()?;
                let mut ns = session.create_node(&commit, None)?;
                ns.add_child(&dir)?;
                // Deliberately DO NOT call ns.finish() or session.finish().
            }

            let store = repo.merkle_store();
            assert!(store.exists(&commit_hash)?);
            let children = store.get_children(&commit_hash)?;
            assert_eq!(children.len(), 1, "expected the single dir child");
            Ok(())
        })
    }
}
