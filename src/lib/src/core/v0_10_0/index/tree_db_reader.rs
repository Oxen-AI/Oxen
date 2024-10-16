use crate::core::db::key_val::tree_db;
use crate::core::db::key_val::tree_db::{TreeObject, TreeObjectChild};
use crate::core::db::{self};
use crate::error::OxenError;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::PathBuf;

use super::TreeObjectReader;

// This is an interface to enable reading tree objects for merge, and must be capable of reading
// from either a TreeObjectReader (for local commits) or a single rocksdb (for pre-push validation of a single reduced tree db)
pub enum CommitTreeReader {
    TreeObjectReader(TreeObjectReader),
    DB(DBWithThreadMode<MultiThreaded>),
}

impl CommitTreeReader {
    pub fn get_entry_from_child(
        &self,
        child: &TreeObjectChild,
    ) -> Result<Option<TreeObject>, OxenError> {
        match self {
            CommitTreeReader::TreeObjectReader(reader) => reader.get_node_from_child(child),
            CommitTreeReader::DB(db) => tree_db::get_tree_object(db, child.hash()),
        }
    }

    pub fn get_root_entry(&self) -> Result<Option<TreeObject>, OxenError> {
        match self {
            CommitTreeReader::TreeObjectReader(reader) => reader.get_root_node(),
            CommitTreeReader::DB(db) => tree_db::get_tree_object(db, ""),
        }
    }
}

pub struct TreeDBMerger {
    pub client_reader: CommitTreeReader,
    pub server_reader: CommitTreeReader,
    pub lca_reader: CommitTreeReader,
}

impl TreeDBMerger {
    pub fn new(
        client_db_path: PathBuf,
        server_reader: TreeObjectReader,
        lca_reader: TreeObjectReader,
    ) -> TreeDBMerger {
        let opts = db::key_val::opts::default();
        let client_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open(&opts, client_db_path).unwrap();

        TreeDBMerger {
            client_reader: CommitTreeReader::DB(client_db),
            server_reader: CommitTreeReader::TreeObjectReader(server_reader),
            lca_reader: CommitTreeReader::TreeObjectReader(lca_reader),
        }
    }

    pub fn r_tree_has_conflict(
        &self,
        client_node: &Option<TreeObject>,
        server_node: &Option<TreeObject>,
        lca_node: &Option<TreeObject>,
    ) -> Result<bool, OxenError> {
        log::debug!(
            "calling tree on client {:?} server {:?} lca {:?}",
            client_node,
            server_node,
            lca_node
        );
        // All 3 are present
        match (client_node, server_node, lca_node) {
            (Some(client_node), Some(server_node), Some(lca_node)) => {
                self.handle_all_nodes_present(client_node, server_node, lca_node)
            }
            (Some(client_node), Some(server_node), None) => {
                self.handle_missing_lca(client_node, server_node)
            }
            (Some(head), None, Some(lca)) | (None, Some(head), Some(lca)) => {
                self.handle_missing_head(head, lca)
            }
            (Some(_head), None, None) | (None, Some(_head), None) => {
                // Node is new to one commit, doesn't exist in the other or LCA. No merge conflict.
                Ok(false)
            }
            _ => {
                log::debug!("flagging conflict due to undefined pattern");
                log::debug!("client node is {:?}", client_node);
                log::debug!("server node is {:?}", server_node);
                log::debug!("lca node is {:?}", lca_node);
                Ok(true)
            }
        }
    }

    pub fn handle_all_nodes_present(
        &self,
        client_node: &TreeObject,
        server_node: &TreeObject,
        lca_node: &TreeObject,
    ) -> Result<bool, OxenError> {
        if client_node.hash() == server_node.hash() {
            return Ok(false); // No changes in either head commit
        }

        if client_node.hash() == lca_node.hash() || server_node.hash() == lca_node.hash() {
            return Ok(false); // Changes in only one head commit
        }

        match (client_node, server_node, lca_node) {
            (
                TreeObject::Dir {
                    children: client_children,
                    ..
                }
                | TreeObject::VNode {
                    children: client_children,
                    ..
                },
                TreeObject::Dir {
                    children: server_children,
                    ..
                }
                | TreeObject::VNode {
                    children: server_children,
                    ..
                },
                TreeObject::Dir {
                    children: _lca_children,
                    ..
                }
                | TreeObject::VNode {
                    children: _lca_children,
                    ..
                },
            ) => {
                // Recurse down on children
                let mut visited_paths: HashSet<&PathBuf> = HashSet::new();
                for child in client_children {
                    visited_paths.insert(child.path());

                    // Client child can be obtained directly from the node. Server and lca must first be binary searched on all children of that node
                    // since it is PATH comparisons that matter here, not hash comparisons.

                    let client_child: Option<TreeObject> =
                        self.client_reader.get_entry_from_child(child)?;

                    let maybe_server_child = server_node.binary_search_on_path(child.path())?;
                    let maybe_lca_child = lca_node.binary_search_on_path(child.path())?;

                    let server_child: Option<TreeObject> = match maybe_server_child {
                        Some(child) => self.server_reader.get_entry_from_child(&child)?,
                        None => None,
                    };
                    let lca_child: Option<TreeObject> = match maybe_lca_child {
                        Some(child) => self.lca_reader.get_entry_from_child(&child)?,
                        None => None,
                    };

                    if self.r_tree_has_conflict(&client_child, &server_child, &lca_child)? {
                        return Ok(true);
                    }
                }

                // Deletion on client and modification on server IS a conflict
                // Anything below is already not in the client child
                for child in server_children {
                    if !visited_paths.contains(child.path()) {
                        // If it's not in the LCA OR client child, no conflict
                        let maybe_lca_child = lca_node.binary_search_on_path(child.path())?;
                        let maybe_lca_node = match maybe_lca_child {
                            Some(child) => self.lca_reader.get_entry_from_child(&child)?,
                            None => None,
                        };

                        if let Some(lca_node) = maybe_lca_node {
                            // If the hashes differ here, then we have a CHANGE in server and DELETION in client. conflict.
                            if lca_node.hash() != child.hash() {
                                log::debug!("flagging conflict due to deletion on client and modification on server");
                                return Ok(true);
                            }
                        }
                    }
                }
                // If neither loop runs, then server_children = client_children = None
                // (this case is covered by hash checks, but the compiler doesn't know that)
                Ok(false)
            }
            (_, _, _) => {
                log::debug!("flagging conflict due to different file type in all 3 nodes 1");
                Ok(true) // If at least one of these is a file or schema, this is a merge conflict.
            }
        }
    }

    fn handle_missing_lca(
        &self,
        client_node: &TreeObject,
        server_node: &TreeObject,
    ) -> Result<bool, OxenError> {
        // Missing LCA node == node is new to both commits, so hash must be exactly same
        if client_node.hash() == server_node.hash() {
            return Ok(false);
        }
        match (client_node, server_node) {
            (
                TreeObject::Dir {
                    children: client_children,
                    ..
                }
                | TreeObject::VNode {
                    children: client_children,
                    ..
                },
                TreeObject::Dir { .. } | TreeObject::VNode { .. },
            ) => {
                for child in client_children {
                    let client_child: Option<TreeObject> =
                        self.client_reader.get_entry_from_child(child)?;

                    let maybe_server_child = server_node.binary_search_on_path(child.path())?;
                    let server_child: Option<TreeObject> = match maybe_server_child {
                        Some(child) => self.server_reader.get_entry_from_child(&child)?,
                        None => None,
                    };
                    let lca_child: Option<TreeObject> = None;

                    if self.r_tree_has_conflict(&client_child, &server_child, &lca_child)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            // One of the new files is a file or schema, and its hash does not match the other. == Merge conflict
            (_, _) => {
                log::debug!("flagging conflict due to different file type in all 3 nodes 2");
                Ok(true)
            }
        }
    }

    fn handle_missing_head(&self, head: &TreeObject, lca: &TreeObject) -> Result<bool, OxenError> {
        // Missing one of the head nodes == node is deleted in one commit. Persist the deletion and merge
        if head.hash() == lca.hash() {
            return Ok(false);
        }

        match (head, lca) {
            (
                TreeObject::Dir {
                    children: head_children,
                    ..
                }
                | TreeObject::VNode {
                    children: head_children,
                    ..
                },
                TreeObject::Dir {
                    children: _lca_children,
                    ..
                }
                | TreeObject::VNode {
                    children: _lca_children,
                    ..
                },
            ) => {
                // Recurse down on children
                let mut visited_paths: HashSet<&PathBuf> = HashSet::new();

                for head_child in head_children {
                    visited_paths.insert(head_child.path());

                    // Client + server symmetric here
                    let head_node: Option<TreeObject> =
                        self.client_reader.get_entry_from_child(head_child)?;
                    let other_head_node: Option<TreeObject> = None;

                    let maybe_lca_child = lca.binary_search_on_path(head_child.path())?;
                    let lca_node = match maybe_lca_child {
                        Some(child) => self.lca_reader.get_entry_from_child(&child)?,
                        None => None,
                    };

                    if self.r_tree_has_conflict(&head_node, &other_head_node, &lca_node)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            // One of the new files is a file or schema, and its hash does not match the other. == Merge conflict
            (_, _) => {
                log::debug!("flagging conflict due to different file type in all 3 nodes 3");
                Ok(true)
            }
        }
    }
}
