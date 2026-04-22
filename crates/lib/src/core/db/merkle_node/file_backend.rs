use crate::core::db::merkle_node::merkle_node_db::{MerkleDbError, MerkleNodeDB};
use crate::model::LocalRepository;
use crate::model::merkle_tree::merkle_reader::{MerkleNodeRecord, MerkleReader};
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{MerkleHash, TMerkleTreeNode};

/// File-based Merkle node store backend.
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

/// Write session for the file backend.
///
/// Writes happen eagerly through each [`FileNodeSession`]; this session's
/// [`finish`] is a no-op. The `Drop` impl below exists for symmetry with
/// [`FileNodeSession`] — the compiler elides it in release builds.
pub struct FileWriteSession<'repo> {
    repo: &'repo LocalRepository,
}

impl<'repo> Drop for FileWriteSession<'repo> {
    fn drop(&mut self) {
        // Nothing to do: per-node writes are flushed eagerly through
        // `FileNodeSession::finish` / its Drop impl.
    }
}

/// Per-node write handle for the file backend.
///
/// A newtype around [`MerkleNodeDB`] with a `finished` sentinel that guards
/// [`Drop`] against double-closing the underlying file handles.
pub struct FileNodeSession {
    db: MerkleNodeDB,
    finished: bool,
}

impl FileNodeSession {
    fn new(db: MerkleNodeDB) -> Self {
        Self {
            db,
            finished: false,
        }
    }

    fn close_impl(&mut self) -> Result<(), MerkleDbError> {
        MerkleNodeDB::close(&mut self.db)
    }
}

impl Drop for FileNodeSession {
    fn drop(&mut self) {
        if !self.finished {
            self.close_impl()
                .expect("Did not explicitly call finish() and encountered an error.");
        }
    }
}

impl<'repo> MerkleReader for FileBackend<'repo> {
    type Error = MerkleDbError;

    fn exists(&self, hash: &MerkleHash) -> Result<bool, MerkleDbError> {
        Ok(MerkleNodeDB::exists(self.repo, hash))
    }

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

    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, MerkleDbError> {
        let mut db = MerkleNodeDB::open_read_only(self.repo, hash)?;
        db.map()
    }
}

impl<'repo> MerkleWriter for FileBackend<'repo> {
    type Error = MerkleDbError;
    type Session<'a>
        = FileWriteSession<'repo>
    where
        Self: 'a;

    fn begin(&self) -> Result<FileWriteSession<'repo>, MerkleDbError> {
        Ok(FileWriteSession { repo: self.repo })
    }
}

impl<'a, 'repo: 'a> MerkleWriteSession<'a> for FileWriteSession<'repo> {
    type Error = MerkleDbError;
    type NodeSession<'b>
        = FileNodeSession
    where
        Self: 'b;

    fn create_node<N: TMerkleTreeNode>(
        &self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<FileNodeSession, MerkleDbError> {
        Ok(FileNodeSession::new(MerkleNodeDB::open_read_write(
            self.repo, node, parent_id,
        )?))
    }

    fn finish(self) -> Result<(), MerkleDbError> {
        Ok(())
    }
}

impl NodeWriteSession for FileNodeSession {
    type Error = MerkleDbError;

    fn node_id(&self) -> &MerkleHash {
        &self.db.node_id
    }

    fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), MerkleDbError> {
        MerkleNodeDB::add_child(&mut self.db, child)
    }

    fn finish(mut self) -> Result<(), MerkleDbError> {
        self.finished = true;
        self.close_impl()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::merkle_tree::node::{CommitNode, DirNode};
    use crate::test;

    /// Dropping a `FileNodeSession` without calling `finish()` must still
    /// flush+sync the underlying files — matching the implicit-drop semantics
    /// that `MerkleNodeDB` has today.
    #[test]
    fn drop_finishes_file_node_session() -> Result<(), crate::error::OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let commit = CommitNode::default();
            let dir = DirNode::default();
            let commit_hash = commit.hash();

            // Scope the session so Drop runs at its end — no explicit finish.
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
