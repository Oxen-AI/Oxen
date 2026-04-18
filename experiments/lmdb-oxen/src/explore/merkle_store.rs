use crate::explore::{
    hash::{ContentHash, HasContentHash, LocationHash},
    lazy_merkle::{LazyNode, MerkleTreeB, MerkleTreeL, NodeContent, Root, UncomittedRoot},
    merkle_reader::MerkleReader,
    merkle_writer::{MerkleWriter, WriteSession},
};

/// A complete Merkle tree store supports reading and writing with a shared error type.
pub trait MerkleStore: MerkleReader + MerkleWriter<Error = <Self as MerkleReader>::Error> {
    /// Commit a tree to the store. Produces the resulting [`Root`].
    ///
    /// Each tree node is written to the `locations` table keyed by its
    /// [`LocationHash`] (derived from its content, parent location, and
    /// name). Dir content lists are written to the `contents` table keyed
    /// by their [`ContentHash`], deduplicating across occurrences.
    fn commit_tree(&self, updates: UncomittedRoot) -> Result<Root, <Self as MerkleReader>::Error> {
        let UncomittedRoot {
            parent,
            repository,
            children,
        } = updates;

        let mut writer = self.write_session()?;

        // Collect root children's content hashes (for commit hash) and
        // their location hashes (for Root.children). Root-level nodes have
        // `parent = None` in both their `LocationHash::new(...)` inputs and
        // in their stored `MerkleTreeL.parent` — the `path()` walker stops
        // at `parent = None`.
        let child_content_hashes: Vec<ContentHash> =
            children.iter().map(|c| c.content_hash()).collect();

        let mut child_locations: Vec<LazyNode> = Vec::with_capacity(children.len());
        for node in children {
            let loc = write_tree(self, &mut writer, None, node)?;
            child_locations.push(LazyNode::new(loc));
        }

        let commit_root = Root::new(repository, &child_content_hashes, child_locations, parent);
        writer.queue_commit(&commit_root)?;
        writer.finish()?;

        Ok(commit_root)
    }
}

/// Recursively writes an entire Merkle tree from this node down to its
/// leaves. Returns the [`LocationHash`] of the written node.
///
/// `parent_location = None` means this node is a direct child of the
/// commit root (equivalently: its stored record has `parent = None`).
fn write_tree<M: MerkleStore>(
    store: &M,
    ws: &mut <M as MerkleWriter>::Session<'_>,
    parent_location: Option<LocationHash>,
    node: MerkleTreeB,
) -> Result<LocationHash, <M as MerkleReader>::Error> {
    let content_hash = node.content_hash();
    // Compute this node's location hash. Inputs: content, parent, name.
    let my_location = LocationHash::new(
        &content_hash,
        parent_location.as_ref(),
        // `node` isn't moved yet; borrow its name for the hash compute.
        match &node {
            MerkleTreeB::Dir { name, .. } => name,
            MerkleTreeB::File { name, .. } => name,
        },
    );

    // Same (content, parent, name) already stored ⇒ identical subtree;
    // the stored bits are already correct by construction.
    if store.location_exists(my_location)? {
        return Ok(my_location);
    }

    let stored_parent = parent_location.map(LazyNode::new);

    match node {
        MerkleTreeB::Dir {
            content_hash: ch,
            name,
            children,
        } => {
            // Dedup the intrinsic content record: write only if absent.
            if !store.content_exists(ch)? {
                let child_contents: Vec<ContentHash> =
                    children.iter().map(|c| c.content_hash()).collect();
                ws.queue_content(
                    ch,
                    &NodeContent::Dir {
                        children: child_contents,
                    },
                )?;
            }

            // Recurse: each child becomes a LazyNode(child_location).
            let mut child_locations: Vec<LazyNode> = Vec::with_capacity(children.len());
            for c in children {
                let loc = write_tree(store, ws, Some(my_location), c)?;
                child_locations.push(LazyNode::new(loc));
            }

            ws.queue_location(
                my_location,
                &MerkleTreeL::Dir {
                    content_hash: ch,
                    name,
                    parent: stored_parent,
                    children: child_locations,
                },
            )?;
        }
        MerkleTreeB::File {
            content_hash: ch,
            name,
        } => {
            if !store.content_exists(ch)? {
                // CURRENTLY JUST A DEMO -- we'd want actual file metadata here
                ws.queue_content(ch, &NodeContent::File { size: 0 })?;
            }

            ws.queue_location(
                my_location,
                &MerkleTreeL::File {
                    content_hash: ch,
                    name,
                    parent: stored_parent,
                },
            )?;
        }
    }
    Ok(my_location)
}

/// Any type that implements the Merkle reading and writing traits is automatically an instance
/// of a MerkleStore, provided that the error types in both the reader & writer align.
impl<T> MerkleStore for T where T: MerkleReader + MerkleWriter<Error = <T as MerkleReader>::Error> {}
