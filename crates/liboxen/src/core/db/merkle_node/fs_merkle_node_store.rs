//! On-disk [`MerkleNodeStore`]: the original layout — two files per node at
//! `.oxen/tree/nodes/<sha-3>/<sha-rest>/{node,children}` (see [`node_db_path`]). This is the
//! default backend and preserves the historical on-disk format byte-for-byte.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::constants;
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

/// Parse a hex node id and push it onto `hashes`, logging and skipping a non-hex name.
fn push_hash(hashes: &mut Vec<MerkleHash>, id: &str) {
    match u128::from_str_radix(id, 16) {
        Ok(value) => hashes.push(MerkleHash::new(value)),
        Err(_) => log::warn!("Skipping non-hex merkle node dir {id}"),
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

    fn node_byte_sizes(&self, hash: &MerkleHash) -> Result<(u64, u64), MerkleDbError> {
        let dir = node_db_path(&self.repo_path, hash);
        let node_len = match std::fs::metadata(dir.join(NODE_FILE)) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(MerkleDbError::MissingNodeDir(*hash));
            }
            Err(e) => return Err(MerkleDbError::Io(e)),
        };
        // `write_node` always writes both files, so a missing children file means a childless
        // node whose blob is empty; treat it as zero-length rather than an error.
        let children_len = match std::fs::metadata(dir.join(CHILDREN_FILE)) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == ErrorKind::NotFound => 0,
            Err(e) => return Err(MerkleDbError::Io(e)),
        };
        Ok((node_len, children_len))
    }

    fn list_hashes(&self) -> Result<Vec<MerkleHash>, MerkleDbError> {
        let nodes_dir = self
            .repo_path
            .join(constants::OXEN_HIDDEN_DIR)
            .join(constants::TREE_DIR)
            .join(constants::NODES_DIR);

        // A repo with no committed nodes (e.g. freshly init'd) has no nodes dir yet.
        let prefix_entries = match std::fs::read_dir(&nodes_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(MerkleDbError::Io(e)),
        };

        // `node_db_prefix` splits the hex hash into `{prefix}` (first 3 chars) and `{suffix}`
        // (the rest), so a node normally lives at `{prefix}/{suffix}/{node,children}`. When the
        // hex hash is 3 chars or shorter the suffix is empty and the `node`/`children` files sit
        // directly under the prefix dir, so a prefix dir may hold both: subdirs (each a suffix)
        // and the leaf files of the short-hash node sharing that prefix.
        //
        // `DirEntry::file_type` does not follow symlinks, so symlinked entries report as neither
        // file nor dir and are skipped — Oxen does not track symlinks.
        let mut hashes = Vec::new();
        for prefix_entry in prefix_entries {
            let prefix_entry = prefix_entry.map_err(MerkleDbError::Io)?;
            if !prefix_entry
                .file_type()
                .map_err(MerkleDbError::Io)?
                .is_dir()
            {
                continue;
            }
            let Some(prefix) = prefix_entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let mut prefix_is_node_dir = false;
            for inner_entry in std::fs::read_dir(prefix_entry.path()).map_err(MerkleDbError::Io)? {
                let inner_entry = inner_entry.map_err(MerkleDbError::Io)?;
                let file_type = inner_entry.file_type().map_err(MerkleDbError::Io)?;
                if file_type.is_dir() {
                    // A suffix dir: the hash is `{prefix}{suffix}`, but only if it actually holds a
                    // node blob — `write_node` always writes `NODE_FILE`, so a suffix dir without
                    // one is not a node (matching `exists`/`node_byte_sizes`).
                    let Some(suffix) = inner_entry.file_name().to_str().map(str::to_owned) else {
                        continue;
                    };
                    match std::fs::metadata(inner_entry.path().join(NODE_FILE)) {
                        Ok(_) => push_hash(&mut hashes, &format!("{prefix}{suffix}")),
                        Err(e) if e.kind() == ErrorKind::NotFound => {}
                        Err(e) => return Err(MerkleDbError::Io(e)),
                    }
                } else if file_type.is_file() && inner_entry.file_name().to_str() == Some(NODE_FILE)
                {
                    // The leaf files live directly under the prefix dir: the hash is `{prefix}`.
                    prefix_is_node_dir = true;
                }
            }
            if prefix_is_node_dir {
                push_hash(&mut hashes, &prefix);
            }
        }
        Ok(hashes)
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

    fn delete(&self, hash: &MerkleHash) -> Result<(), MerkleDbError> {
        // Remove only this node's two files. A short hash whose suffix is empty stores its
        // files directly in the `{prefix}` shard alongside sibling suffix subdirs, so removing
        // the whole dir would wipe those unrelated nodes — delete the files individually instead.
        // Deleting an absent node is idempotent.
        let dir = node_db_path(&self.repo_path, hash);
        for file in [NODE_FILE, CHILDREN_FILE] {
            match std::fs::remove_file(dir.join(file)) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(MerkleDbError::Io(e)),
            }
        }
        // Best-effort cleanup: prune the node dir only if it is now empty. `remove_dir` fails
        // (and is ignored) when the dir still holds sibling suffix subdirs. An empty `{prefix}`
        // shard dir may be left behind; `list_hashes` ignores it, matching the prior behavior.
        let _ = std::fs::remove_dir(&dir);
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

        // Byte sizes are reported without reading the blobs.
        assert_eq!(
            store.node_byte_sizes(&hash)?,
            (node.len() as u64, children.len() as u64)
        );

        // An empty children blob (childless node) round-trips too.
        let leaf = MerkleHash::new(0x42);
        store.write_node(&leaf, Bytes::from_static(b"leaf"), Bytes::new())?;
        assert!(store.read_children(&leaf)?.is_empty());
        assert_eq!(store.node_byte_sizes(&leaf)?, (4, 0));

        // Enumeration returns exactly the written hashes, regardless of order.
        let listed: std::collections::HashSet<MerkleHash> =
            store.list_hashes()?.into_iter().collect();
        assert_eq!(
            listed,
            std::collections::HashSet::from([hash, leaf]),
            "list_hashes should return exactly the written nodes"
        );

        // A never-written node is reported absent and reads as MissingNodeDir.
        let missing = MerkleHash::new(0xdead_beef);
        assert!(!store.exists(&missing)?);
        assert!(matches!(
            store.read_node(&missing),
            Err(MerkleDbError::MissingNodeDir(_))
        ));
        assert!(matches!(
            store.node_byte_sizes(&missing),
            Err(MerkleDbError::MissingNodeDir(_))
        ));

        // Delete removes a node; deleting an absent node is idempotent.
        store.delete(&hash)?;
        assert!(!store.exists(&hash)?, "node should be gone after delete");
        store.delete(&missing)?;
        assert_eq!(
            store.list_hashes()?,
            vec![leaf],
            "only the surviving node remains after delete"
        );
        Ok(())
    }

    /// Deleting a short-hash node (whose files share the `{prefix}` shard with longer-hash
    /// siblings) must remove only that node, leaving the siblings intact.
    #[test]
    fn fs_store_delete_preserves_prefix_shard_siblings() -> Result<(), OxenError> {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = FsMerkleNodeStore::new(dir.path());

        // `short` hashes to "abc" (3 hex chars), so its files live directly in the shard dir.
        // `sibling` shares that "abc" prefix but has a non-empty suffix subdir under it.
        let short = MerkleHash::new(0xabc);
        let sibling = MerkleHash::new(0xabc_def);
        let body = Bytes::from_static(b"blob");
        store.write_node(&short, body.clone(), Bytes::new())?;
        store.write_node(&sibling, body.clone(), Bytes::new())?;

        store.delete(&short)?;

        assert!(!store.exists(&short)?, "short-hash node should be deleted");
        assert!(
            store.exists(&sibling)?,
            "sibling sharing the prefix shard must survive"
        );
        assert_eq!(store.list_hashes()?, vec![sibling]);
        Ok(())
    }

    /// `list_hashes` on a repo path with no nodes dir yet returns empty rather than erroring.
    #[test]
    fn fs_store_list_hashes_empty_when_no_nodes_dir() -> Result<(), OxenError> {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = FsMerkleNodeStore::new(dir.path());
        assert!(store.list_hashes()?.is_empty());
        Ok(())
    }
}
