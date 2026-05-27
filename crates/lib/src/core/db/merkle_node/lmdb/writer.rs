use std::cell::Cell;
use std::rc::Rc;

use heed::{Database, Env, WithTls};

use crate::core::db::merkle_node::lmdb::LmdbError;
use crate::core::db::merkle_node::lmdb::hash_content_name::{Filename, HashCN};
use crate::core::db::merkle_node::lmdb::lmdb_backend::{
    KeyLmdb, LmdbBackend, LmdbTables, ValueLmdb,
};
use crate::core::db::merkle_node::lmdb::value_structs::{LmdbDupes, LmdbLink, LmdbNode};
use crate::error::OxenError;
use crate::model::merkle_tree::merkle_writer::{
    MerkleWriteSession, MerkleWriter, NodeWriteSession,
};
use crate::model::{MerkleHash, MerkleTreeNodeType, TMerkleTreeNode};

/// Merkle writer implementation for the [`LmdbBackend`].
impl MerkleWriter for LmdbBackend {
    /// Returns a new [`LmdbWriteSession`] that can be used to write nodes to the store.
    ///
    /// No write transaction is opened here; the session buffers all writes via the shared
    /// [`Self::pending`] queue and opens exactly one [`heed::RwTxn`] in [`Self::finish`]
    /// to commit them atomically.
    fn begin<'a>(&'a self) -> Result<Box<dyn MerkleWriteSession + 'a>, OxenError> {
        Ok(Box::new(LmdbWriteSession {
            env: &self.lmdb_env,
            tables: &self.tables,
            pending: Rc::new(Cell::new(Vec::new())),
        }))
    }
}

/// One node's worth of buffered writes — the unit produced by a [`LmdbNodeWriteSession`]
/// and consumed by [`LmdbWriteSession::finish`].
struct PendingWrite {
    node_hash: MerkleHash,
    node_kind: MerkleTreeNodeType,
    node_data: Vec<u8>,
    filename: Option<Filename>,
    parent_id: Option<MerkleHash>,
    children: Vec<(MerkleHash, Option<Filename>, LmdbNode)>,
}

/// Implements [`MerkleWriteSession`] for the [`LmdbBackend`] with all-or-nothing semantics.
///
/// Each [`LmdbNodeWriteSession`] this hands out shares the [`Self::pending`] queue via
/// [`Rc<Cell<...>>`]. Node sessions buffer their state in memory and push a [`PendingWrite`]
/// onto the queue when their `finish` is called. This session's [`Self::finish`] opens
/// a single [`heed::RwTxn`], drains the queue, and commits — so either every queued write
/// is persisted or none is.
///
/// **Callers MUST call `finish()` on every node session and on the outer write session.**
/// There is intentionally no [`Drop`] guard: any session that goes out of scope without
/// an explicit `finish()` silently loses its buffered state.
///
/// Single-threaded by construction: [`Rc`] makes the session itself `!Send`, and the
/// [`MerkleWriteSession`] trait doesn't require `Send`, so the session can never move
/// across threads. The borrow on `pending` lives only for one [`Vec::push`] inside a
/// node session's `finish`, with no nested re-entry — so the [`Cell::take`] /
/// [`Cell::set`] dance can never observe a concurrent or aliased access. If async or
/// multi-threading is needed one day, then this can be migrated to an [`Arc<Mutuex<.>>`].
///
/// LMDB allows only one write transaction per environment, so other write transactions
/// against the same env block until this session's `finish` returns. Read transactions
/// taken before `finish` see pre-session data; reads taken after see the new state.
struct LmdbWriteSession<'env> {
    env: &'env Env<WithTls>,
    tables: &'env LmdbTables,
    pending: Rc<Cell<Vec<PendingWrite>>>,
}

impl<'env> MerkleWriteSession for LmdbWriteSession<'env> {
    /// Start writing a single node and allow callers to write its children.
    /// On success, returns a [`LmdbNodeWriteSession`].
    fn create_node<'node_trans>(
        &'node_trans self,
        node: &dyn TMerkleTreeNode,
        parent_id: Option<MerkleHash>,
    ) -> Result<Box<dyn NodeWriteSession + 'node_trans>, OxenError> {
        Ok(Box::new(LmdbNodeWriteSession {
            pending: Rc::clone(&self.pending),
            node_hash: node.hash(),
            node_kind: node.node_type(),
            node_data: node.to_msgpack_bytes()?,
            filename: node.name(),
            parent_id,
            children_buffer: Vec::new(),
        }))
    }

    /// Drain every queued [`PendingWrite`] into a single [`heed::RwTxn`] and commit.
    /// All-or-nothing: either every node and its children/links are persisted, or none is.
    ///
    /// Each write goes through [`LmdbNode::encode`] / [`LmdbLink::encode`] which
    /// produce the zero-copy on-disk byte format (`magic + version + header + tail`)
    /// readable via the corresponding [`super::value_structs::LmdbNodeRef`] /
    /// [`super::value_structs::LmdbLinkRef`].
    fn finish(self: Box<Self>) -> Result<(), OxenError> {
        let pending = self.pending.take();
        if pending.is_empty() {
            return Ok(());
        }
        let mut wtxn = LmdbBackend::write_txn(self.env)?;
        for pw in pending {
            //
            // Perform the following writes:
            //
            //  (1) write the node into the store:
            //          (MerkleHash, XXH3(name)) = HashNC -> LmdbNode
            //
            //  (2) append the node into the duplicates:
            //          (MerkleHash) -> Vec<XXH3(name)>
            //
            //  (3) write the tree links:
            //          (MerkleHash) -> Vec<(MerkleHash, XXH3(name))>
            //

            // (1) write into node store
            let hash_name_content = HashCN::new(&pw.node_hash, pw.filename.as_ref());
            LmdbBackend::put_serialized(
                &mut wtxn,
                &self.tables.merkle_node_store,
                hash_name_content.as_u128(),
                LmdbNode::encode,
                LmdbNode {
                    kind: pw.node_kind,
                    data: pw.node_data,
                },
            )?;

            // (2) append the node into the duplicates
            let duplicates: Vec<HashCN> = match LmdbBackend::retrieve_bytes(
                &wtxn,
                &self.tables.merkle_node_dupes,
                hash_name_content.as_u128(),
            )? {
                Some(existing) => {
                    let mut existing = LmdbDupes::decode(existing)?.hash_cns;
                    existing.push(hash_name_content);
                    existing
                }
                None => vec![hash_name_content],
            };
            LmdbBackend::put_serialized(
                &mut wtxn,
                &self.tables.merkle_node_dupes,
                &pw.node_hash.to_u128(),
                LmdbDupes::encode,
                LmdbDupes {
                    hash_cns: duplicates,
                },
            )?;

            // (3) write the tree links
            let children: Vec<HashCN> = {
                let mut children = Vec::with_capacity(pw.children.len());
                for (child_hash, maybe_child_filename, child_node) in pw.children {
                    let child_hash_cn = HashCN::new(&child_hash, maybe_child_filename.as_ref());
                    children.push(child_hash_cn);
                }
                children
            };
            LmdbBackend::put_serialized(
                &mut wtxn,
                &self.tables.merkle_links,
                &child_hash.to_u128(),
                LmdbLink::encode,
                LmdbLink {
                    parent_id: pw.parent_id,
                    children,
                },
            )?;
        }
        wtxn.commit().map_err(LmdbError::Write)?;
        Ok(())
    }
}

// impl<'a> MerkleWriteSession {
//     fn write_single_node(&self, wxtn: &mut heed::RwTxn<'_>, content: MerkleHash, maybe_filename: Option<Filename>, kind: MerkleTreeNodeType, data: Vec<u8>) -> Result<(), LmdbError> {
//         let (hash, table) = match maybe_filename {
//             Some(filename) => {
//                 let hash_of_content_and_name = HashCN::new(content, &filename);
//                 (*hash_of_content_and_name.as_u128(), self.merkle_tree_files)
//             },
//             None => {
//
//             },
//         };
//         LmdbBackend::put_serialized(
//             &mut wtxn,
//             self.merkle_tree_else,
//             content,
//             LmdbNode::encode,
//             LmdbNode { kind, data },
//         )
//     }
// }

/// Buffers one node's data + children in memory; on `finish`, hands the buffer off to the
/// parent [`LmdbWriteSession`]'s pending queue.
///
/// **Callers MUST call `finish()` before this goes out of scope.** No [`Drop`] guard
/// exists; an unfinished node session silently discards its buffered state.
struct LmdbNodeWriteSession {
    // **Always** points to the parent [`LmdbWriteSession::pending`]
    pending: Rc<Cell<Vec<PendingWrite>>>,
    node_hash: MerkleHash,
    node_kind: MerkleTreeNodeType,
    node_data: Vec<u8>,
    filename: Option<Filename>,
    parent_id: Option<MerkleHash>,
    children_buffer: Vec<(MerkleHash, Option<Filename>, LmdbNode)>,
}

impl NodeWriteSession for LmdbNodeWriteSession {
    /// Hash of the node currently being written.
    fn node_id(&self) -> &MerkleHash {
        &self.node_hash
    }

    /// Serialize this child and queue for writing.
    fn add_child(&mut self, child: &dyn TMerkleTreeNode) -> Result<(), OxenError> {
        let child_as_lmdb_node = LmdbNode {
            kind: child.node_type(),
            data: child.to_msgpack_bytes()?,
        };
        self.children_buffer.push((
            child.hash(),
            child.name().map(Filename::new_assume_invariants),
            child_as_lmdb_node,
        ));
        Ok(())
    }

    /// Hand the buffered node + children off to the parent session's queue.
    /// The actual LMDB writes happen in [`LmdbWriteSession::finish`].
    fn finish(self: Box<Self>) -> Result<(), OxenError> {
        let LmdbNodeWriteSession {
            pending,
            node_hash,
            node_kind,
            node_data,
            filename,
            parent_id,
            children_buffer,
        } = *self;
        // Since Rc is !Send, there's never any parallel access to `pending`.
        // Therefore, we will never accidentily loose a `PendingWrite` push.
        let mut queue = pending.take();
        queue.push(PendingWrite {
            node_hash,
            node_kind,
            node_data,
            filename,
            parent_id,
            children: children_buffer,
        });
        pending.set(queue);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::repository_config::MerkleStoreKind;
    use crate::core::db::merkle_node::LmdbBackend;
    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::model::merkle_tree::merkle_reader::MerkleReader;
    use crate::model::merkle_tree::merkle_writer::MerkleWriter;
    use crate::model::merkle_tree::node::CommitNode;
    use crate::test;

    use crate::core::db::merkle_node::lmdb::tests::{
        commit_with_hash, dir_with_hash, file_chunk_node_with_hash, file_node_with_hash, h,
        open_lmdb_at, vnode_with_hash, with_test_backend, write_one,
    };

    /// End-to-end smoke test for the queue-based design: write a parent commit with
    /// a child commit added, verify both land in the store after `finish` and the
    /// parent's link records the child.
    #[test]
    fn test_lmdb_writer_queue_roundtrip_node_with_child_redundant_write() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let parent_h = h("11111111111111111111111111111111");
            let child_h = h("22222222222222222222222222222222");
            let parent = commit_with_hash(repo, parent_h);
            let child = commit_with_hash(repo, child_h);

            let session = backend.begin()?;
            {
                let mut parent_ns = session.create_node(&parent, None)?;
                parent_ns.add_child(&child)?;
                parent_ns.finish()?;
            }
            {
                // redundant, but this is using the interface more explictly
                // it will write the same node twice: last write wins
                let child_ns = session.create_node(&child, Some(parent_h))?;
                child_ns.finish()?;
            }
            session.finish()?;

            rt_node_with_child_verify(backend, parent_h, child_h)
        })
    }

    #[test]
    fn test_lmdb_writer_queue_roundtrip_node_with_child() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let parent_h = h("11111111111111111111111111111111");
            let child_h = h("22222222222222222222222222222222");
            let parent = commit_with_hash(repo, parent_h);
            let child = commit_with_hash(repo, child_h);

            let session = backend.begin()?;
            {
                let mut parent_ns = session.create_node(&parent, None)?;
                parent_ns.add_child(&child)?;
                parent_ns.finish()?;
            }
            // relying on the create_node implementation to ensure that added
            // children nodes are written into the node storage as well
            session.finish()?;

            rt_node_with_child_verify(backend, parent_h, child_h)
        })
    }

    fn rt_node_with_child_verify(
        backend: &LmdbBackend,
        parent_h: MerkleHash,
        child_h: MerkleHash,
    ) -> Result<(), OxenError> {
        assert!(backend.exists(&parent_h)?);
        assert!(backend.exists(&child_h)?);
        let children = backend.get_children(&parent_h)?;
        assert_eq!(children.len(), 1, "parent should have one child");
        assert_eq!(children[0].0, child_h, "child hash should match");
        Ok(())
    }

    /// Nothing was written, nothing should be in the store, but `finish` must succeed.
    #[test]
    fn test_lmdb_writer_empty_session_is_noop() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let session = backend.begin()?;
            session.finish()?;
            let missing = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            assert!(!backend.exists(&missing)?);
            Ok(())
        })
    }

    /// Two `LmdbNodeWriteSession`s alive at the same time used to be a borrow-checker
    /// violation under the shared-`RwTxn` design. With the pending-queue design they
    /// can coexist freely; both writes must land after the parent's `finish`.
    #[test]
    fn test_lmdb_writer_concurrent_node_sessions() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let a_h = h("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            let b_h = h("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            let a = commit_with_hash(repo, a_h);
            let b = commit_with_hash(repo, b_h);

            let session = backend.begin()?;
            let a_ns = session.create_node(&a, None)?;
            let b_ns = session.create_node(&b, Some(a_h))?;
            // finish in reverse order on purpose
            b_ns.finish()?;
            a_ns.finish()?;
            session.finish()?;

            assert!(backend.exists(&a_h)?);
            assert!(backend.exists(&b_h)?);
            Ok(())
        })
    }

    // ────────────────────────────────────────────────────────────────────────────
    // get_links works on every node kind, including file/file-chunk.
    // ────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_get_links_returns_link_for_each_node_kind() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let parent = h("00000000000000000000000000000001");
            let commit_h = h("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            let dir_h = h("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            let vnode_h = h("cccccccccccccccccccccccccccccccc");
            let file_h = h("dddddddddddddddddddddddddddddddd");
            let chunk_h = h("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

            write_one(backend, &commit_with_hash(repo, commit_h), None)?;
            write_one(backend, &dir_with_hash(repo, dir_h), Some(parent))?;
            write_one(backend, &vnode_with_hash(repo, vnode_h), Some(parent))?;
            write_one(backend, &file_node_with_hash(repo, file_h), Some(parent))?;
            write_one(backend, &file_chunk_node_with_hash(chunk_h), Some(parent))?;

            let commit_link = backend.get_links(&commit_h)?.expect("commit link");
            assert_eq!(commit_link.parent_id, None);
            assert!(commit_link.children.is_empty());

            for hash in [&dir_h, &vnode_h, &file_h, &chunk_h] {
                let link = backend.get_links(hash)?.expect("link for non-commit kind");
                assert_eq!(
                    link.parent_id,
                    Some(parent),
                    "parent should round-trip for {hash}"
                );
                assert!(
                    link.children.is_empty(),
                    "no add_child was called for {hash}"
                );
            }
            Ok(())
        })
    }

    #[test]
    fn test_get_links_returns_none_for_unwritten_hash() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let missing = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            assert!(backend.get_links(&missing)?.is_none());
            Ok(())
        })
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Missing-hash behavior across all four read methods.
    // ────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_reads_against_missing_hash() -> Result<(), OxenError> {
        with_test_backend(|_repo, backend| {
            let missing = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            assert!(!backend.exists(&missing)?);
            assert!(backend.get_node(&missing)?.is_none());
            assert!(backend.get_children(&missing)?.is_empty());
            assert!(!backend.full_exists(&missing)?);
            assert!(backend.full_get_node(&missing)?.is_none());
            assert!(backend.get_links(&missing)?.is_none());
            Ok(())
        })
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Multi-child round-trip — exercises the `LmdbLink` serialization path with
    // N>1 children.
    // ────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_get_children_returns_all_children_in_order() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let parent_h = h("11111111111111111111111111111111");
            let parent = commit_with_hash(repo, parent_h);

            // 5 distinct children with deterministic, distinguishable hashes.
            let kid_hashes: Vec<MerkleHash> = (0..5)
                .map(|i| h(&format!("{:032x}", 0xC0DE_0000_u64 + i)))
                .collect();
            let kids: Vec<CommitNode> = kid_hashes
                .iter()
                .map(|kh| commit_with_hash(repo, *kh))
                .collect();

            let session = backend.begin()?;
            {
                let mut parent_ns = session.create_node(&parent, None)?;
                for k in &kids {
                    parent_ns.add_child(k)?;
                }
                parent_ns.finish()?;
            }
            for k in &kids {
                let ns = session.create_node(k, Some(parent_h))?;
                ns.finish()?;
            }
            session.finish()?;

            let children = backend.get_children(&parent_h)?;
            assert_eq!(children.len(), 5, "all 5 children should be present");
            let returned_hashes: Vec<MerkleHash> = children.iter().map(|(c, _)| *c).collect();
            assert_eq!(
                returned_hashes, kid_hashes,
                "children returned in add_child order"
            );
            Ok(())
        })
    }

    #[test]
    fn test_get_children_empty_for_node_with_no_add_child() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let commit_h = h("11111111111111111111111111111111");
            let commit = commit_with_hash(repo, commit_h);
            write_one(backend, &commit, None)?;
            assert!(backend.get_children(&commit_h)?.is_empty());
            Ok(())
        })
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Persistence across env open/close.
    // ────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_data_persists_across_env_reopen() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(MerkleStoreKind::File, |repo| {
            let commit_h = h("11111111111111111111111111111111");
            let commit = commit_with_hash(&repo, commit_h);

            // First open: write, then drop the backend (env close).
            {
                let backend = open_lmdb_at(&repo.path);
                write_one(&backend, &commit, None)?;
            }
            // Second open: reopen, read.
            {
                let backend = open_lmdb_at(&repo.path);
                assert!(backend.exists(&commit_h)?);
                assert!(backend.get_node(&commit_h)?.is_some());
            }
            Ok(())
        })
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Idempotent re-writes (last-write-wins).
    // ────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_repeated_writes_to_same_hash_do_not_error() -> Result<(), OxenError> {
        with_test_backend(|repo, backend| {
            let commit_h = h("11111111111111111111111111111111");
            let commit = commit_with_hash(repo, commit_h);

            // Write twice, in two separate sessions.
            write_one(backend, &commit, None)?;
            write_one(backend, &commit, None)?;

            assert!(backend.exists(&commit_h)?);
            Ok(())
        })
    }
}
