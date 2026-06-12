use crate::core::db::merkle_node::LmdbBackend;
use crate::core::db::merkle_node::lmdb::{
    LmdbError,
    value_structs::{LmdbLinkRef, LmdbNodeRef},
};
use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::model::merkle_tree::merkle_reader::{MerkleEntry, MerkleReader};
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode, MerkleTreeNodeType};

/// Implements the [`MerkleReader`] trait for the [`LmdbBackend`].
///
/// LMDB encourages short-lived read transactions, so each method opens its own
/// rtxn and uses it for the entire scope of the read. Inside that scope all
/// access is zero-copy: bytes returned by [`LmdbBackend::retrieve_bytes`] are
/// borrowed directly from the LMDB mmap and wrapped in [`LmdbNodeRef`] /
/// [`LmdbLinkRef`] without intermediate parsing or `Vec` allocation. The msgpack
/// decode of the node payload runs against the borrowed slice directly.
impl MerkleReader for LmdbBackend {
    /// Checks if a node with the given `hash` exists in the store.
    ///
    /// NOTE: [`MerkleReader`]'s methods are **NOT** defined for files.
    ///       This will return `Ok(false)` on a file node that exists.
    fn exists(&self, hash: &MerkleHash) -> Result<bool, OxenError> {
        let rtxn = self.read_txn()?;
        let Some(bytes) = Self::retrieve_bytes(&rtxn, &self.merkle_tree_nodes, hash)? else {
            return Ok(false);
        };
        let node_ref = LmdbNodeRef::from_bytes(bytes)?;
        use MerkleTreeNodeType::*;
        match node_ref.kind()? {
            File | FileChunk => Ok(false),
            Commit | Dir | VNode => Ok(true),
        }
    }

    /// Retrieves the node with the given `hash` from the store. `None` means no such node exists.
    ///
    /// NOTE: to comply with [`MerkleReader::get_node`]'s semantics, this method
    /// has to consider present file nodes as not existing.
    fn get_node(&self, hash: &MerkleHash) -> Result<Option<MerkleEntry>, OxenError> {
        let rtxn = self.read_txn()?;

        // ── Read the node entry (zero-copy view into mmap). ────────────────
        let Some(node_bytes) = Self::retrieve_bytes(&rtxn, &self.merkle_tree_nodes, hash)? else {
            return Ok(None);
        };
        let node_ref = LmdbNodeRef::from_bytes(node_bytes)?;
        let kind = node_ref.kind()?;
        use MerkleTreeNodeType::*;
        if matches!(kind, File | FileChunk) {
            // Trait contract: file-typed nodes are reported as absent.
            return Ok(None);
        }

        // ── Read the link entry to recover the parent_id. ──────────────────
        let Some(link_bytes) = Self::retrieve_bytes(&rtxn, &self.merkle_links, hash)? else {
            // Node exists but no link row — table-cross integrity violation.
            return Err(LmdbError::IntegrityNoLink(hash.to_hex_hash()).into());
        };
        let link_ref = LmdbLinkRef::from_bytes(link_bytes)?;
        let parent_id = link_ref.parent_id();

        // ── Decode the EMerkleTreeNode from the borrowed msgpack tail. ─────
        Ok(Some(MerkleEntry {
            node: EMerkleTreeNode::from_type_and_bytes(kind, node_ref.data)?,
            parent_id,
        }))
    }

    /// Retrieves the children of the node with the given `hash` from the store.
    /// An empty vec means that either the node is not a directory or virtual node,
    /// or it is one but has no children.
    fn get_children(
        &self,
        hash: &MerkleHash,
    ) -> Result<Vec<(MerkleHash, MerkleTreeNode)>, OxenError> {
        let rtxn = self.read_txn()?;

        let Some(link_bytes) = Self::retrieve_bytes(&rtxn, &self.merkle_links, hash)? else {
            // Existing semantics: missing link is treated as "no children".
            return Ok(Vec::new());
        };
        let link_ref = LmdbLinkRef::from_bytes(link_bytes)?;

        let mut loaded = Vec::with_capacity(link_ref.num_children());
        for child_hash in link_ref.children_iter() {
            let Some(child_bytes) =
                Self::retrieve_bytes(&rtxn, &self.merkle_tree_nodes, &child_hash)?
            else {
                return Err(LmdbError::IntegrityNoHash(child_hash.to_hex_hash()).into());
            };
            let child_ref = LmdbNodeRef::from_bytes(child_bytes)?;
            let child_kind = child_ref.kind()?;
            loaded.push((
                child_hash,
                MerkleTreeNode {
                    node: EMerkleTreeNode::from_type_and_bytes(child_kind, child_ref.data)?,
                    hash: child_hash,
                    parent_id: Some(*hash),
                    children: vec![],
                },
            ));
        }
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {

    // ────────────────────────────────────────────────────────────────────────────
    // Reader semantics: file/file-chunk vs everything else.
    // The trait `MerkleReader::{exists, get_node}` treats file-typed nodes as absent.
    // The inherent `LmdbBackend::{full_exists, full_get_node}` see them.
    // ────────────────────────────────────────────────────────────────────────────

    use crate::{
        core::db::merkle_node::lmdb::tests::{
            commit_with_hash, dir_with_hash, file_chunk_node_with_hash, file_node_with_hash, h,
            vnode_with_hash, with_test_backend, write_one,
        },
        error::OxenError,
        model::MerkleTreeNodeType,
        model::merkle_tree::merkle_reader::MerkleReader,
    };

    #[test]
    fn test_get_node_returns_none_for_file_node() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let f_h = h("11111111111111111111111111111111");
            let f = file_node_with_hash(repo, f_h);
            write_one(backend, &f, None)?;
            // The trait says `get_node` must treat file nodes as absent.
            assert!(backend.get_node(&f_h)?.is_none());
            Ok(())
        })
    }

    #[test]
    fn test_get_node_returns_none_for_file_chunk_node() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let c_h = h("22222222222222222222222222222222");
            let c = file_chunk_node_with_hash(c_h);
            write_one(backend, &c, None)?;
            assert!(backend.get_node(&c_h)?.is_none());
            Ok(())
        })
    }

    #[test]
    fn test_exists_returns_false_for_file_node() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let f_h = h("33333333333333333333333333333333");
            let f = file_node_with_hash(repo, f_h);
            write_one(backend, &f, None)?;
            // `exists` mirrors `get_node`'s file-as-absent semantics.
            assert!(!backend.exists(&f_h)?);
            Ok(())
        })
    }

    #[test]
    fn test_exists_returns_false_for_file_chunk_node() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let c_h = h("44444444444444444444444444444444");
            let c = file_chunk_node_with_hash(c_h);
            write_one(backend, &c, None)?;
            assert!(!backend.exists(&c_h)?);
            Ok(())
        })
    }

    #[test]
    fn test_full_get_node_returns_some_for_file_node() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let f_h = h("55555555555555555555555555555555");
            let f = file_node_with_hash(repo, f_h);
            write_one(backend, &f, None)?;
            let stored = backend
                .full_get_node(&f_h)?
                .expect("file node should be stored");
            assert_eq!(stored.kind, MerkleTreeNodeType::File);
            Ok(())
        })
    }

    #[test]
    fn test_full_get_node_returns_some_for_file_chunk_node() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let c_h = h("66666666666666666666666666666666");
            let c = file_chunk_node_with_hash(c_h);
            write_one(backend, &c, None)?;
            let stored = backend
                .full_get_node(&c_h)?
                .expect("file chunk node should be stored");
            assert_eq!(stored.kind, MerkleTreeNodeType::FileChunk);
            Ok(())
        })
    }

    #[test]
    fn test_full_exists_returns_true_for_file_and_chunk_nodes() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let f_h = h("77777777777777777777777777777777");
            let c_h = h("88888888888888888888888888888888");
            let f = file_node_with_hash(repo, f_h);
            let c = file_chunk_node_with_hash(c_h);
            write_one(backend, &f, None)?;
            write_one(backend, &c, None)?;
            assert!(backend.full_exists(&f_h)?);
            assert!(backend.full_exists(&c_h)?);
            Ok(())
        })
    }

    /// Across all five node kinds: the trait's `exists` matches `full_exists` for
    /// non-file kinds, and disagrees for file/file-chunk.
    #[test]
    fn test_exists_vs_full_exists_for_each_node_kind() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let commit_h = h("11111111111111111111111111111111");
            let dir_h = h("22222222222222222222222222222222");
            let vnode_h = h("33333333333333333333333333333333");
            let file_h = h("44444444444444444444444444444444");
            let chunk_h = h("55555555555555555555555555555555");

            write_one(backend, &commit_with_hash(repo, commit_h), None)?;
            write_one(backend, &dir_with_hash(repo, dir_h), None)?;
            write_one(backend, &vnode_with_hash(repo, vnode_h), None)?;
            write_one(backend, &file_node_with_hash(repo, file_h), None)?;
            write_one(backend, &file_chunk_node_with_hash(chunk_h), None)?;

            // full_exists sees everything.
            for hash in [&commit_h, &dir_h, &vnode_h, &file_h, &chunk_h] {
                assert!(
                    backend.full_exists(hash)?,
                    "full_exists should be true for {hash}"
                );
            }
            // exists agrees on commit/dir/vnode, hides file/file-chunk.
            assert!(backend.exists(&commit_h)?);
            assert!(backend.exists(&dir_h)?);
            assert!(backend.exists(&vnode_h)?);
            assert!(!backend.exists(&file_h)?);
            assert!(!backend.exists(&chunk_h)?);
            Ok(())
        })
    }
}
