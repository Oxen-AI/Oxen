use std::path::{Path, PathBuf};

use crate::core::db::merkle_node::merkle_node_db::{MerkleDbError, MerkleNodeDB};
use crate::error::OxenError;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{LocalRepository, MerkleHash, TMerkleTreeNode};

/// File-based Merkle node store backend. Implements the [`MerkleReader`] and
/// [`MerkleWriter`] traits.
///
/// Borrows the path to a local repository (via the [`LocalRepository`]'s [`PathBuf`]) so it
/// can delegate straight to [`MerkleNodeDB`]'s existing repository-based methods without any
/// modification.
#[derive(Debug)]
pub struct FileBackend {
    /// Location to the repository's root in the filesystem. Must be an absolute path.
    pub(crate) repo_path: PathBuf,
}

impl FileBackend {
    pub fn new(repo: &LocalRepository) -> Self {
        Self {
            repo_path: repo.path.clone(),
        }
    }
}

/// Merkle reader implementation for the [`FileBackend`].
///
/// NOTE: Uses MerkleDbError internally, but is unfortunately forced to convert into
/// an [`OxenError`] at the trait boundary. It is a bug if any of these methods do
/// not return a wrapped `MerkleDbError`.
impl MerkleReader for FileBackend {
    /// Checks if a node with the given `hash` exists in the store.
    ///
    /// Alias for [`MerkleNodeDB::exists`].
    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        Ok(MerkleNodeDB::exists(&self.repo_path, hash))
    }

    /// Retrieves the node with the given `hash` from the store. `None` means no such node exists.
    ///
    /// Alias for [`MerkleNodeDB::open_read_only`].
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, OxenError> {
        if !MerkleNodeDB::exists(&self.repo_path, hash) {
            return Ok(None);
        }
        let db = MerkleNodeDB::open_read_only(&self.repo_path, hash)?;
        let node = db.node()?;
        Ok(Some(MerkleNodeRecord::new(
            db.node_id,
            db.dtype,
            db.parent_id,
            node,
        )))
    }

    /// Retrieves the children of the node with the given `hash` from the store.
    /// An empty vec means that either the node is a not a directory or virtual node or it is one
    /// but has no files.
    ///
    /// Alias for [`MerkleNodeDB::open_read_only`] & a `.map()` call on it.
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError> {
        if !MerkleNodeDB::exists(&self.repo_path, hash) {
            return Ok(Vec::with_capacity(0));
        }
        let mut db = MerkleNodeDB::open_read_only(&self.repo_path, hash)?;
        let children = db.map()?;
        Ok(children)
    }
}

/// Merkle writer implementation for the [`FileBackend`].
///
/// NOTE: Uses MerkleDbError internally, but is unfortunately forced to convert into
/// an [`OxenError`] at the trait boundary. It is a bug if any of these methods do
/// not return a wrapped `MerkleDbError`.
impl MerkleWriter for FileBackend {
    /// Returns a new [`FileWriteSession`] for writing Merkle tree nodes to the store.
    fn begin<'a>(&'a self) -> Result<Box<dyn MerkleWriteSession + 'a>, OxenError> {
        Ok(Box::new(FileWriteSession {
            repo_path: &self.repo_path,
        }))
    }
}

/// Write session for the file backend. Used to write multiple nodes & their children.
///
/// Writes happen eagerly through each [`FileNodeSession`]; this session's
/// [`finish`] is a no-op.
pub struct FileWriteSession<'repo> {
    repo_path: &'repo Path,
}

/// Merkle write session implementation that the [`FileBackend`] uses.
///
/// NOTE: Uses MerkleDbError internally, but is unfortunately forced to convert into
/// an [`OxenError`] at the trait boundary. It is a bug if any of these methods do
/// not return a wrapped `MerkleDbError`.
impl<'repo> MerkleWriteSession for FileWriteSession<'repo> {
    /// Creates a new session for writing a `node` and `children` file.
    /// Returns a [`FileNodeSession`] that calls [`MerkleNodeDB::open_read_write`] internally.
    fn create_node<'a>(
        &'a self,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Box<dyn NodeWriteSession + 'a>, OxenError> {
        let session = FileNodeSession::new(self.repo_path, node, parent_id)?;
        Ok(Box::new(session))
    }

    /// A no-op -- the node write session from [`create_node`] eagerly writes its files.
    /// The [`FileNodeSession::finish`] method flushes and closes open file handles.
    fn finish(self: Box<Self>) -> Result<(), OxenError> {
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
    /// Opens a new [`MerkleNodeDB`] in read-write mode.
    fn new(
        repo_path: &Path,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Self, MerkleDbError> {
        Ok(Self {
            db: MerkleNodeDB::open_read_write(repo_path, node, parent_id)?,
            finished: false,
        })
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
///
/// NOTE: Uses MerkleDbError internally, but is unfortunately forced to convert into
/// an [`OxenError`] at the trait boundary. It is a bug if any of these methods do
/// not return a wrapped `MerkleDbError`.
impl NodeWriteSession for FileNodeSession {
    /// The node currently being written.
    fn node_id(&self) -> &MerkleHash {
        &self.db.node_id
    }

    /// Adds an entry to the `children` file for the current node. Alias for [`MerkleNodeDB::add_child`].
    fn add_child(&mut self, child: &dyn TMerkleTreeNode) -> Result<(), OxenError> {
        MerkleNodeDB::add_child(&mut self.db, child)?;
        Ok(())
    }

    /// Flushes the open `node` and `children` file handles, closes them, then calls `fsync` on them.
    /// Consumes the boxed session; [`Drop`] becomes a no-op after this returns `Ok` because the
    /// `finished` sentinel guards `idempotent_finish`.
    fn finish(mut self: Box<Self>) -> Result<(), OxenError> {
        self.idempotent_finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::model::merkle_tree::node::{CommitNode, DirNode};
    use crate::test;

    /// Dropping a `FileNodeSession` without calling `finish()` must still
    /// flush+sync the underlying files. This is to match the implicit-drop semantics
    /// that `MerkleNodeDB` has before these `MerkleStore` traits were introduced.
    #[test]
    fn test_drop_finishes_file_node_session() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let commit = CommitNode::default();
            let dir = DirNode::default();
            let commit_hash = commit.hash();

            // Scope the session so Drop runs at its end.
            {
                let store = FileBackend::new(&repo);
                let session = store.begin().expect("Could not begin session");
                let mut ns = session
                    .create_node(&commit, None)
                    .expect("Could not begin node session");
                ns.add_child(&dir)
                    .expect("Could not add a child to the node session");
                // Deliberately DO NOT call ns.finish() or session.finish().
            }

            let store = FileBackend::new(&repo);
            assert!(
                store
                    .exists(commit_hash)
                    .expect("commit to exist after being written")
            );
            let children = store
                .get_children(commit_hash)
                .expect("children to exist after being written");
            assert_eq!(children.len(), 1, "expected the single dir child");
            Ok(())
        })
    }

    /// `exists` on a hash that was never written returns `Ok(false)`.
    #[test]
    fn test_exists_returns_false_for_missing_hash() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let store = FileBackend::new(&repo);
            let missing = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            assert!(
                !store.exists(&missing).expect("exists must not error"),
                "expected exists() to return false for an unwritten hash"
            );
            Ok(())
        })
    }

    /// `get_node` on a hash that was never written returns `Ok(None)`.
    #[test]
    fn test_get_node_returns_none_for_missing_hash() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let store = FileBackend::new(&repo);
            let missing = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            assert!(
                store
                    .get_node(&missing)
                    .expect("get_node must not error")
                    .is_none(),
                "expected get_node() to return None for an unwritten hash"
            );
            Ok(())
        })
    }

    /// `get_children` on a node that was written but never had children added returns
    /// `Ok(empty vec)`. Documents the leaf-children-are-empty contract from
    /// [`MerkleReader::get_children`].
    #[test]
    fn test_get_children_returns_empty_for_node_without_children() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let commit = CommitNode::default();
            let commit_hash = *commit.hash();
            {
                let store = FileBackend::new(&repo);
                let session = store.begin().expect("begin failed");
                let ns = session
                    .create_node(&commit, None)
                    .expect("create_node failed");
                ns.finish().expect("finish node session failed");
                session.finish().expect("finish session failed");
            }
            let store = FileBackend::new(&repo);
            let children = store
                .get_children(&commit_hash)
                .expect("get_children must not error");
            assert!(
                children.is_empty(),
                "expected an empty children list for a node with no add_child calls; got {} entries",
                children.len()
            );
            Ok(())
        })
    }

    /// A write session that begins and finishes without creating any node sessions
    /// should round-trip cleanly with no error.
    #[test]
    fn test_writer_session_with_no_nodes() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let store = FileBackend::new(&repo);
            let session = store.begin().expect("begin failed");
            session
                .finish()
                .expect("finish must not error on empty session");
            Ok(())
        })
    }
}
