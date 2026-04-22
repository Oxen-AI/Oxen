use crate::core::db::merkle_node::merkle_node_db::{MerkleDbError, MerkleNodeDB};
use crate::error::OxenError;
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
///
/// All trait methods surface `OxenError` directly (the adapter layer is
/// inlined here). Internally we still rely on [`MerkleDbError`] and convert at
/// each method boundary via the `OxenError::MerkleDbError(#[from])` variant.
pub struct FileBackend<'repo> {
    repo: &'repo LocalRepository,
}

impl<'repo> FileBackend<'repo> {
    pub fn new(repo: &'repo LocalRepository) -> Self {
        Self { repo }
    }
}

impl<'repo> MerkleReader for FileBackend<'repo> {
    type Error = OxenError;

    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        Ok(MerkleNodeDB::exists(self.repo, hash))
    }

    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleNodeRecord>, OxenError> {
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
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError> {
        let mut db = MerkleNodeDB::open_read_only(self.repo, hash)?;
        Ok(db.map()?)
    }
}

impl<'repo> MerkleWriter for FileBackend<'repo> {
    type Error = OxenError;

    fn begin(&self) -> Result<impl MerkleWriteSession<Error = OxenError>, OxenError> {
        Ok(FileWriteSession { repo: self.repo })
    }
}

/// Write session for the file backend. Writes happen eagerly through each
/// [`FileNodeSession`]; this session's [`finish`] is a no-op.
pub struct FileWriteSession<'repo> {
    repo: &'repo LocalRepository,
}

impl<'repo> MerkleWriteSession for FileWriteSession<'repo> {
    type Error = OxenError;

    fn create_node<N: TMerkleTreeNode>(
        &self,
        node: &N,
        parent_id: Option<MerkleHash>,
    ) -> Result<impl NodeWriteSession<Error = OxenError>, OxenError> {
        Ok(FileNodeSession::new(MerkleNodeDB::open_read_write(
            self.repo, node, parent_id,
        )?))
    }

    fn finish(self) -> Result<(), OxenError> {
        Ok(())
    }
}

/// Per-node write handle for the file backend. Writes exactly 1 `node` and 1 `children` file.
///
/// Newtype around [`MerkleNodeDB`] with a `finished` sentinel so [`Drop`] cannot
/// double-close the underlying file handles. If a caller forgets to call
/// [`NodeWriteSession::finish`] explicitly, [`Drop`] still calls the same close
/// logic — preserving the drop-safe semantics `MerkleNodeDB` had before these
/// traits were introduced.
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

    fn idempotent_finish(&mut self) -> Result<(), MerkleDbError> {
        if self.finished {
            Ok(())
        } else {
            self.finished = true;
            MerkleNodeDB::close(&mut self.db)
        }
    }
}

impl Drop for FileNodeSession {
    fn drop(&mut self) {
        self.idempotent_finish()
            .expect("Did not explicitly call finish() and encountered an error.");
    }
}

impl NodeWriteSession for FileNodeSession {
    type Error = OxenError;

    fn node_id(&self) -> &MerkleHash {
        &self.db.node_id
    }

    fn add_child<N: TMerkleTreeNode>(&mut self, child: &N) -> Result<(), OxenError> {
        MerkleNodeDB::add_child(&mut self.db, child).map_err(OxenError::from)
    }

    fn finish(mut self) -> Result<(), OxenError> {
        self.idempotent_finish().map_err(OxenError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::merkle_tree::node::{CommitNode, DirNode};
    use crate::test;

    /// Dropping a `FileNodeSession` without calling `finish()` must still
    /// flush+sync the underlying files. This matches the implicit-drop
    /// semantics `MerkleNodeDB` had before these traits were introduced.
    #[test]
    fn drop_finishes_file_node_session() -> Result<(), OxenError> {
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
