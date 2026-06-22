//! On-disk [`MerkleNodeStore`]: the original layout — two files per node at
//! `.oxen/tree/nodes/<sha-3>/<sha-rest>/{node,children}` (see [`node_db_path`]). This is the
//! default backend and preserves the historical on-disk format byte-for-byte.

use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::model::MerkleHash;
use crate::util;
use crate::util::fs::AtomicFile;

use super::merkle_node_db::{CHILDREN_FILE, MerkleDbError, NODE_FILE, node_db_path};
use super::merkle_node_store::MerkleNodeStore;

/// Stores each node's two blobs as two files under the repo's `.oxen/tree/nodes` tree.
#[derive(Debug)]
pub(crate) struct FsMerkleNodeStore {
    repo_path: PathBuf,
}

// `new` is unconsumed until `MerkleNodeDB` delegates to a `MerkleNodeStore` (exercised by the tests
// below meanwhile); the trait impl methods carry the rest. Drop the allow when a consumer lands.
#[allow(dead_code)]
impl FsMerkleNodeStore {
    pub(crate) fn new(repo_path: impl Into<PathBuf>) -> Self {
        Self {
            repo_path: repo_path.into(),
        }
    }

    /// Read one of a node's files into owned bytes, mapping a missing file to
    /// [`MerkleDbError::MissingNodeDir`] so the absent-node contract matches across backends.
    fn read_blob(path: &Path, hash: &MerkleHash) -> Result<Bytes, MerkleDbError> {
        match std::fs::read(path) {
            Ok(data) => Ok(Bytes::from(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(MerkleDbError::MissingNodeDir(*hash))
            }
            Err(e) => Err(MerkleDbError::Io(e)),
        }
    }
}

impl MerkleNodeStore for FsMerkleNodeStore {
    fn exists(&self, hash: &MerkleHash) -> Result<bool, MerkleDbError> {
        let dir = node_db_path(&self.repo_path, hash);
        Ok(dir.join(NODE_FILE).exists() && dir.join(CHILDREN_FILE).exists())
    }

    fn read_node(&self, hash: &MerkleHash) -> Result<Bytes, MerkleDbError> {
        let path = node_db_path(&self.repo_path, hash).join(NODE_FILE);
        Self::read_blob(&path, hash)
    }

    fn read_children(&self, hash: &MerkleHash) -> Result<Bytes, MerkleDbError> {
        let path = node_db_path(&self.repo_path, hash).join(CHILDREN_FILE);
        Self::read_blob(&path, hash)
    }

    fn write_node(
        &self,
        hash: &MerkleHash,
        node: Bytes,
        children: Bytes,
    ) -> Result<(), MerkleDbError> {
        let dir = node_db_path(&self.repo_path, hash);
        if !dir.exists() {
            util::fs::create_dir_all(&dir).map_err(MerkleDbError::dir_create)?;
        }
        AtomicFile::new(dir.join(NODE_FILE))
            .write(node.as_ref())
            .map_err(MerkleDbError::fs_transport)?;
        AtomicFile::new(dir.join(CHILDREN_FILE))
            .write(children.as_ref())
            .map_err(MerkleDbError::fs_transport)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;

    use super::*;

    /// The file backend must report absence, persist both blobs, read them back unchanged, handle a
    /// childless (empty children) node, and report a missing node as `MissingNodeDir`.
    #[test]
    fn fs_store_satisfies_the_contract() -> Result<(), OxenError> {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = FsMerkleNodeStore::new(dir.path());

        let hash = MerkleHash::new(0x1234_5678_9abc_def0);
        let node = Bytes::from_static(b"node blob: header + lookup table");
        let children = Bytes::from_static(b"children blob: concatenated child nodes");

        assert!(
            !store.exists(&hash)?,
            "node should not exist before writing"
        );

        store.write_node(&hash, node.clone(), children.clone())?;

        assert!(store.exists(&hash)?, "node should exist after writing");
        assert_eq!(store.read_node(&hash)?, node);
        assert_eq!(store.read_children(&hash)?, children);

        // An empty children blob (childless node) round-trips too.
        let leaf = MerkleHash::new(0x42);
        store.write_node(&leaf, Bytes::from_static(b"leaf"), Bytes::new())?;
        assert!(store.read_children(&leaf)?.is_empty());

        // A never-written node is reported absent and reads as MissingNodeDir.
        let missing = MerkleHash::new(0xdead_beef);
        assert!(!store.exists(&missing)?);
        assert!(matches!(
            store.read_node(&missing),
            Err(MerkleDbError::MissingNodeDir(_))
        ));
        Ok(())
    }
}
