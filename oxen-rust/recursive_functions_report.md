# Recursive Functions in oxen-rust

All paths are relative to `oxen-rust/`.

---

## 1. Utility Functions

### 1.1 `get_repo_root`
- **File:** `src/lib/src/util/fs.rs:576`
- **Recursion:** Traverses up the filesystem looking for `.oxen` directory; calls itself on `parent`
- **Usages:**
  - `src/lib/src/util/fs.rs:582` (recursive self-call)
  - `src/lib/src/util/fs.rs:593` (`get_repo_root_from_current_dir`)
  - `src/cli/src/cmd/save.rs:45`

### 1.2 `r_search_merkle_tree`
- **File:** `src/lib/src/util/glob.rs:179`
- **Recursion:** Recursively searches the Merkle tree for entries matching a glob pattern across path components
- **Usages:**
  - `src/lib/src/util/glob.rs:165` (initial call from `search_merkle_tree`)
  - `src/lib/src/util/glob.rs:224` (recursive self-call)

### 1.3 `r_collect_removed_paths`
- **File:** `src/lib/src/util/glob.rs:409`
- **Recursion:** Recursively collects removed file paths by searching Merkle tree subdirectories
- **Usages:**
  - `src/lib/src/util/glob.rs:395` (initial call from `collect_removed_paths`)
  - `src/lib/src/util/glob.rs:426` (recursive self-call)

---

## 2. Core: Data Frame / Tabular

### 2.1 `any_val_to_json`
- **File:** `src/lib/src/core/df/tabular.rs:723`
- **Recursion:** Converts Polars `AnyValue` to JSON; recurses for nested `List`, `Struct`, and `StructOwned` types at lines 770, 789, 811
- **Usages:**
  - `src/lib/src/core/df/tabular.rs:770` (recursive: nested list)
  - `src/lib/src/core/df/tabular.rs:789` (recursive: struct fields)
  - `src/lib/src/core/df/tabular.rs:811` (recursive: owned struct fields)
  - `src/lib/src/core/df/tabular.rs:836`
  - `src/lib/src/core/df/tabular.rs:929`
  - `src/lib/src/core/df/tabular.rs:932`
  - `src/lib/src/core/df/tabular.rs:940`
  - `src/lib/src/core/df/tabular.rs:2070` (test)
  - `src/lib/src/core/df/tabular.rs:2074` (test)
  - `src/lib/src/core/df/tabular.rs:2078` (test)
  - `src/lib/src/core/df/tabular.rs:2082` (test)
  - `src/lib/src/core/df/tabular.rs:2086` (test)
  - `src/lib/src/core/df/tabular.rs:2098` (test)
  - `src/lib/src/core/df/tabular.rs:2102` (test)

---

## 3. Core: Status

### 3.1 `find_changes`
- **File:** `src/lib/src/core/v_latest/status.rs:446`
- **Recursion:** Recursively walks working directory to find untracked/modified/removed files vs Merkle tree
- **Usages:**
  - `src/lib/src/core/v_latest/status.rs:67` (initial call)
  - `src/lib/src/core/v_latest/status.rs:523` (recursive self-call)

### 3.2 `find_local_changes`
- **File:** `src/lib/src/core/v_latest/status.rs:647`
- **Recursion:** Like `find_changes` but also tracks unsynced files for local status reporting
- **Usages:**
  - `src/lib/src/core/v_latest/status.rs:141` (initial call)
  - `src/lib/src/core/v_latest/status.rs:733` (recursive self-call)

### 3.3 `count_removed_entries`
- **File:** `src/lib/src/core/v_latest/status.rs:868`
- **Recursion:** Recursively traverses Merkle tree counting file entries in removed directories
- **Usages:**
  - `src/lib/src/core/v_latest/status.rs:628`
  - `src/lib/src/core/v_latest/status.rs:848`
  - `src/lib/src/core/v_latest/status.rs:887` (recursive self-call)

---

## 4. Core: Entries

### 4.1 `p_dir_entries`
- **File:** `src/lib/src/core/v_latest/entries.rs:398`
- **Recursion:** Recursively walks Merkle tree building `MetadataEntry` list for directory listings (handles VNode, Directory, and File children)
- **Usages:**
  - `src/lib/src/core/v_latest/entries.rs:216`
  - `src/lib/src/core/v_latest/entries.rs:262`
  - `src/lib/src/core/v_latest/entries.rs:414` (recursive: VNode child)
  - `src/lib/src/core/v_latest/entries.rs:455` (recursive: depth > 0 children)
  - `src/lib/src/core/v_latest/entries.rs:482` (recursive: Directory child)

### 4.2 `traverse_and_update_sizes_and_counts` *(mutual recursion)*
- **File:** `src/lib/src/core/v_latest/entries.rs:557`
- **Recursion:** Mutually recursive with `process_children`. Traverses Merkle tree computing per-node byte totals and data-type counts.
- **Usages:**
  - `src/lib/src/core/v_latest/entries.rs:551` (initial call)
  - `src/lib/src/core/v_latest/entries.rs:640` (called by `process_children`)

### 4.3 `process_children` *(mutual recursion)*
- **File:** `src/lib/src/core/v_latest/entries.rs:631`
- **Recursion:** Iterates over child nodes calling `traverse_and_update_sizes_and_counts` on each
- **Usages:**
  - `src/lib/src/core/v_latest/entries.rs:570`
  - `src/lib/src/core/v_latest/entries.rs:582`
  - `src/lib/src/core/v_latest/entries.rs:594`

---

## 5. Core: Branches

### 5.1 `r_remove_if_not_in_target`
- **File:** `src/lib/src/core/v_latest/branches.rs:453`
- **Recursion:** Recursively walks the "from" commit tree during checkout to identify files not in the target branch
- **Usages:**
  - `src/lib/src/core/v_latest/branches.rs:413`
  - `src/lib/src/core/v_latest/branches.rs:512` (recursive: Directory child)
  - `src/lib/src/core/v_latest/branches.rs:536` (recursive: VNode child)

### 5.2 `r_restore_missing_or_modified_files`
- **File:** `src/lib/src/core/v_latest/branches.rs:552`
- **Recursion:** Recursively walks the target commit tree during checkout to find missing/modified files to restore
- **Usages:**
  - `src/lib/src/core/v_latest/branches.rs:226`
  - `src/lib/src/core/v_latest/branches.rs:361`
  - `src/lib/src/core/v_latest/branches.rs:697` (recursive: Directory child)
  - `src/lib/src/core/v_latest/branches.rs:712` (recursive: VNode child)

---

## 6. Core: Merge

### 6.1 `r_ff_merge_commit`
- **File:** `src/lib/src/core/v_latest/merge.rs:508`
- **Recursion:** Recursively processes the merge commit tree during fast-forward merge; identifies files to restore or flag as conflicts
- **Usages:**
  - `src/lib/src/core/v_latest/merge.rs:456`
  - `src/lib/src/core/v_latest/merge.rs:589` (recursive: Directory child)
  - `src/lib/src/core/v_latest/merge.rs:603` (recursive: VNode/Commit child)

### 6.2 `r_ff_base_dir`
- **File:** `src/lib/src/core/v_latest/merge.rs:623`
- **Recursion:** Recursively walks the base commit tree during FF merge to find files to remove
- **Usages:**
  - `src/lib/src/core/v_latest/merge.rs:474`
  - `src/lib/src/core/v_latest/merge.rs:675` (recursive: Directory child)
  - `src/lib/src/core/v_latest/merge.rs:681` (recursive: VNode/Commit child)

---

## 7. Core: Remove

### 7.1 `r_process_remove_dir`
- **File:** `src/lib/src/core/v_latest/rm.rs:670`
- **Recursion:** Recursively stages all files under a directory for removal
- **Usages:**
  - `src/lib/src/core/v_latest/rm.rs:629`
  - `src/lib/src/core/v_latest/rm.rs:689` (recursive: Directory child)
  - `src/lib/src/core/v_latest/rm.rs:694` (recursive: VNode child)

---

## 8. Core: Fetch & Download

### 8.1 `r_download_entries` (fetch)
- **File:** `src/lib/src/core/v_latest/fetch.rs:496`
- **Recursion:** Async recursive; walks Merkle tree downloading missing entries to versions directory. Uses `Box::pin` for async recursion.
- **Usages:**
  - `src/lib/src/core/v_latest/fetch.rs:484`
  - `src/lib/src/core/v_latest/fetch.rs:516` (recursive via `Box::pin`)

### 8.2 `r_download_entries` (download)
- **File:** `src/lib/src/core/v_latest/download.rs:94`
- **Recursion:** Async recursive; walks Merkle tree downloading entries to working directory. Uses `Box::pin`.
- **Usages:**
  - `src/lib/src/core/v_latest/download.rs:44`
  - `src/lib/src/core/v_latest/download.rs:80`
  - `src/lib/src/core/v_latest/download.rs:109` (recursive via `Box::pin`)

---

## 9. Core: Prune

### 9.1 `collect_node_hashes`
- **File:** `src/lib/src/core/v_latest/prune.rs:183`
- **Recursion:** Recursively collects all referenced Merkle hashes and version file hashes for pruning
- **Usages:**
  - `src/lib/src/core/v_latest/prune.rs:171`
  - `src/lib/src/core/v_latest/prune.rs:204` (recursive self-call)

---

## 10. Core: Schemas

### 10.1 `r_list_schemas` (v_latest)
- **File:** `src/lib/src/core/v_latest/data_frames/schemas.rs:45`
- **Recursion:** Recursively walks Merkle tree collecting tabular file schemas
- **Usages:**
  - `src/lib/src/core/v_latest/data_frames/schemas.rs:41`
  - `src/lib/src/core/v_latest/data_frames/schemas.rs:55` (recursive: Directory child)
  - `src/lib/src/core/v_latest/data_frames/schemas.rs:59` (recursive: VNode child)

### 10.2 `r_list_schemas` (objects_schema_reader)
- **File:** `src/lib/src/core/index/schema_reader/objects_schema_reader.rs:77`
- **Recursion:** Recursively walks older-format tree collecting schema entries (method on `ObjectsSchemaReader`)
- **Usages:**
  - `src/lib/src/core/index/schema_reader/objects_schema_reader.rs:72`
  - `src/lib/src/core/index/schema_reader/objects_schema_reader.rs:88` (recursive self-call)

### 10.3 `r_list_schema_entries`
- **File:** `src/lib/src/core/index/schema_reader/objects_schema_reader.rs:131`
- **Recursion:** Same tree traversal as above but collects `SchemaEntry` structs
- **Usages:**
  - `src/lib/src/core/index/schema_reader/objects_schema_reader.rs:126`
  - `src/lib/src/core/index/schema_reader/objects_schema_reader.rs:142` (recursive self-call)

---

## 11. Core: CommitMerkleTree (v_latest)

### 11.1 `CommitMerkleTree::dir_entries`
- **File:** `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:633`
- **Recursion:** Recursively collects all `FileNode` entries from Directory/VNode children via `Self::dir_entries(child)`
- **Usages:**
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:644` (recursive: `Self::dir_entries(child)`)

### 11.2 `CommitMerkleTree::load_children`
- **File:** `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:733`
- **Recursion:** Recursively loads Merkle tree children from DB up to requested depth
- **Usages:**
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:264`
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:286`
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:806` (recursive)

### 11.3 `CommitMerkleTree::load_children_and_collect_paths`
- **File:** `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:831`
- **Recursion:** Like `load_children` but also builds a hash-to-path map
- **Usages:**
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:316`
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:913` (recursive)

### 11.4 `CommitMerkleTree::load_children_and_collect_partial_nodes`
- **File:** `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:937`
- **Recursion:** Like `load_children` but also builds a `HashMap<PathBuf, PartialNode>`
- **Usages:**
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:348`
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:1004` (recursive)

### 11.5 `CommitMerkleTree::load_unique_children_list`
- **File:** `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:1048`
- **Recursion:** Recursively collects nodes with hashes not in `shared_hashes` (used during push)
- **Usages:**
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:1093` (recursive)

### 11.6 `CommitMerkleTree::r_print`
- **File:** `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:1132`
- **Recursion:** Recursively prints the Merkle tree with indentation for debugging
- **Usages:**
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:1124` (`print_tree`)
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:1129` (`print_node`)
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:1148` (recursive)

---

## 12. Core: CommitMerkleTree (v0_19_0)

### 12.1 `CommitMerkleTree::dir_entries` (v_old)
- **File:** `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:358`
- **Recursion:** Same as v_latest version
- **Usages:**
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:369` (recursive)

### 12.2 `CommitMerkleTree::read_children_until_depth` (v_old)
- **File:** `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:420`
- **Recursion:** Recursively reads tree from per-node RocksDB databases up to a depth
- **Usages:**
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:222`
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:475` (recursive)

### 12.3 `CommitMerkleTree::read_children_from_node` (v_old)
- **File:** `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:504`
- **Recursion:** Recursively reads all children when `recurse = true`
- **Usages:**
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:203`
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:543` (recursive)

### 12.4 `CommitMerkleTree::r_print` (v_old)
- **File:** `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:581`
- **Recursion:** Same as v_latest version
- **Usages:**
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:573`
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:578`
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:597` (recursive)

---

## 13. Model: MerkleTreeNode (tree traversal methods)

All in `src/lib/src/model/merkle_tree/node/merkle_tree_node.rs`. Each recurses via `child.method()` where `child: &MerkleTreeNode`.

### 13.1 `MerkleTreeNode::total_vnodes`
- **Line:** 112
- **Recursion:** `child.total_vnodes()` at line 118
- **Usages:**
  - `src/lib/src/core/v_latest/index/commit_merkle_tree.rs:630`
  - `src/lib/src/core/v_old/v0_19_0/index/commit_merkle_tree.rs:355`
  - `src/lib/src/repositories/commits/commit_writer.rs:1300` (test)
  - `src/lib/src/repositories/commits/commit_writer.rs:1501` (test)

### 13.2 `MerkleTreeNode::list_paths_helper`
- **Line:** 230
- **Recursion:** `child.list_paths_helper(...)` at lines 243, 245
- **Usages:** `merkle_tree_node.rs:226` (called by `list_paths`)

### 13.3 `MerkleTreeNode::list_dir_paths_helper`
- **Line:** 259
- **Recursion:** `child.list_dir_paths_helper(...)` at lines 270, 272
- **Usages:** `merkle_tree_node.rs:255` (called by `list_dir_paths`)

### 13.4 `MerkleTreeNode::list_file_hashes_helper`
- **Line:** 286
- **Recursion:** `child.list_file_hashes_helper(...)` at lines 297, 299
- **Usages:** `merkle_tree_node.rs:282` (called by `list_file_hashes`)

### 13.5 `MerkleTreeNode::list_file_paths_helper`
- **Line:** 313
- **Recursion:** `child.list_file_paths_helper(...)` at lines 325, 327
- **Usages:** `merkle_tree_node.rs:309` (called by `list_file_paths`)

### 13.6 `MerkleTreeNode::list_files_helper`
- **Line:** 341
- **Recursion:** `child.list_files_helper(...)` at lines 355, 357
- **Usages:** `merkle_tree_node.rs:337` (called by `list_files`)

### 13.7 `MerkleTreeNode::list_files_and_dirs_helper`
- **Line:** 376
- **Recursion:** `child.list_files_and_dirs_helper(...)` at lines 392, 394
- **Usages:** `merkle_tree_node.rs:372` (called by `list_files_and_dirs`)

### 13.8 `MerkleTreeNode::list_dir_and_vnode_hashes_helper`
- **Line:** 408
- **Recursion:** `child.list_dir_and_vnode_hashes_helper(...)` at lines 423, 425
- **Usages:** `merkle_tree_node.rs:404` (called by `list_dir_and_vnode_hashes`)

### 13.9 `MerkleTreeNode::list_shared_dir_and_vnode_hashes_helper`
- **Line:** 442
- **Recursion:** `child.list_shared_dir_and_vnode_hashes_helper(...)` at lines 462, 464
- **Usages:** `merkle_tree_node.rs:438` (called by `list_shared_dir_and_vnode_hashes`)

### 13.10 `MerkleTreeNode::get_all_children`
- **Line:** 498
- **Recursion:** `child.get_all_children()` at line 503
- **Usages:** Only recursive self-call (no external callers found)

### 13.11 `MerkleTreeNode::get_by_path_helper`
- **Line:** 543
- **Recursion:** `child.get_by_path_helper(...)` at lines 632, 642, 669, 673
- **Usages:** `merkle_tree_node.rs:540` (called by `get_by_path`)

### 13.12 `MerkleTreeNode::get_nodes_along_paths_helper`
- **Line:** 793
- **Recursion:** `child.get_nodes_along_paths_helper(...)` at lines 846, 857, 870, 880
- **Usages:** `merkle_tree_node.rs:790` (called by `get_nodes_along_paths`)

---

## 14. Model: DataType

### 14.1 `DataType::from_polars`
- **File:** `src/lib/src/model/data_frame/schema/data_type.rs:181`
- **Recursion:** `Self::from_polars(inner)` at line 201 for nested `List` types
- **Usages:** Called widely via `Schema::from_polars(...)` which delegates internally; direct recursive call at line 201

### 14.2 `DataType::to_polars`
- **File:** `src/lib/src/model/data_frame/schema/data_type.rs:144`
- **Recursion:** `val.to_polars()` at line 173 for nested `List` types; `f.to_polars()` at line 168 for `Struct` fields (calls `Field::to_polars` which calls `DataType::to_polars`)
- **Usages:**
  - `src/lib/src/core/df/tabular.rs:206`
  - `src/lib/src/core/df/tabular.rs:229`
  - `src/lib/src/model/data_frame/schema/field.rs:55`
  - `src/lib/src/model/data_frame/schema.rs:54`

---

## 15. Model: WorkspaceMetadataEntry

### 15.1 `WorkspaceMetadataEntry::from_metadata_entry`
- **File:** `src/lib/src/model/entry/metadata_entry.rs:106`
- **Recursion:** `Self::from_metadata_entry` at line 123 mapping over `children`
- **Usages:**
  - `src/lib/src/core/v_latest/entries.rs:100`
  - `src/lib/src/repositories/workspaces.rs:406`
  - `src/lib/src/repositories/workspaces.rs:447`
  - `src/lib/src/repositories/workspaces.rs:466`
  - `src/lib/src/repositories/workspaces.rs:511`

---

## 16. Repositories: Tree

### 16.1 `dir_entries_with_paths`
- **File:** `src/lib/src/repositories/tree.rs:593`
- **Recursion:** Recursively collects `(FileNode, PathBuf)` pairs from Merkle tree
- **Usages:**
  - `src/lib/src/core/v_latest/index/restore.rs:179`
  - `src/lib/src/repositories/tree.rs:609` (recursive: Directory child)
  - `src/lib/src/repositories/tree.rs:612` (recursive: VNode child)

### 16.2 `unique_dir_entries`
- **File:** `src/lib/src/repositories/tree.rs:631`
- **Recursion:** Recursively collects file entries not in `shared_hashes`
- **Usages:**
  - `src/lib/src/core/v_latest/merge.rs:964`
  - `src/lib/src/core/v_latest/merge.rs:966`
  - `src/lib/src/core/v_latest/merge.rs:968`
  - `src/lib/src/repositories/tree.rs:648` (recursive: Directory child)
  - `src/lib/src/repositories/tree.rs:655` (recursive: VNode child)

### 16.3 `r_list_all_files`
- **File:** `src/lib/src/repositories/tree.rs:713`
- **Recursion:** Recursively collects all `FileNode` entries with directory paths
- **Usages:**
  - `src/lib/src/repositories/tree.rs:709`
  - `src/lib/src/repositories/tree.rs:729` (recursive: Directory child)
  - `src/lib/src/repositories/tree.rs:732` (recursive: VNode child)

### 16.4 `r_list_all_dirs`
- **File:** `src/lib/src/repositories/tree.rs:747`
- **Recursion:** Recursively collects all `DirNode` entries with paths
- **Usages:**
  - `src/lib/src/repositories/tree.rs:743`
  - `src/lib/src/repositories/tree.rs:762` (recursive: Directory child)
  - `src/lib/src/repositories/tree.rs:765` (recursive: VNode child)

### 16.5 `r_list_files_and_dirs`
- **File:** `src/lib/src/repositories/tree.rs:783`
- **Recursion:** Recursively collects both file and directory nodes
- **Usages:**
  - `src/lib/src/repositories/tree.rs:779`
  - `src/lib/src/repositories/tree.rs:816` (recursive: Directory child)
  - `src/lib/src/repositories/tree.rs:819` (recursive: VNode child)

### 16.6 `r_list_files_by_type`
- **File:** `src/lib/src/repositories/tree.rs:849`
- **Recursion:** Recursively collects files filtered by `EntryDataType`
- **Usages:**
  - `src/lib/src/repositories/tree.rs:845`
  - `src/lib/src/repositories/tree.rs:868` (recursive: Directory child)
  - `src/lib/src/repositories/tree.rs:871` (recursive: VNode child)

### 16.7 `p_write_tree`
- **File:** `src/lib/src/repositories/tree.rs:1080`
- **Recursion:** Recursively persists `MerkleTreeNode` tree to RocksDB
- **Usages:**
  - `src/lib/src/repositories/tree.rs:1076`
  - `src/lib/src/repositories/tree.rs:1092` (recursive: VNode child)
  - `src/lib/src/repositories/tree.rs:1096` (recursive: Directory child)

---

## 17. Repositories: Commits

### 17.1 `r_create_dir_node`
- **File:** `src/lib/src/repositories/commits/commit_writer.rs:834`
- **Recursion:** Recursively creates directory nodes in the Merkle tree during commit
- **Usages:**
  - `src/lib/src/repositories/commits/commit_writer.rs:819`
  - `src/lib/src/repositories/commits/commit_writer.rs:916` (recursive self-call)

---

## 18. Repositories: Fork

### 18.1 `copy_dir_recursive`
- **File:** `src/lib/src/repositories/fork.rs:128`
- **Recursion:** Recursively copies a directory tree, skipping workspaces
- **Usages:**
  - `src/lib/src/repositories/fork.rs:78`
  - `src/lib/src/repositories/fork.rs:148` (recursive self-call)

### 18.2 `count_items`
- **File:** `src/lib/src/repositories/fork.rs:164`
- **Recursion:** Recursively counts non-directory items in a directory tree
- **Usages:**
  - `src/lib/src/repositories/fork.rs:67`
  - `src/lib/src/repositories/fork.rs:174` (recursive self-call)

---

## 19. Repositories: Size

### 19.1 `get_size`
- **File:** `src/lib/src/repositories/size.rs:96`
- **Recursion:** Reads repo size from cache file; if file doesn't exist, creates it then calls itself (single-level recursion)
- **Usages:**
  - `src/lib/src/repositories/size.rs:107` (recursive self-call on cache miss)
  - `src/lib/src/namespaces.rs:60`
  - `src/server/src/controllers/repositories.rs:228`

---

## 20. Command: Migrate

### 20.1 `rewrite_nodes`
- **File:** `src/lib/src/command/migrate/m20250111083535_add_child_counts_to_nodes.rs:161`
- **Recursion:** Recursively rewrites Merkle tree nodes during migration, redistributing children into new VNode buckets
- **Usages:**
  - `src/lib/src/command/migrate/m20250111083535_add_child_counts_to_nodes.rs:145`
  - `src/lib/src/command/migrate/m20250111083535_add_child_counts_to_nodes.rs:285` (recursive: Directory child)
  - `src/lib/src/command/migrate/m20250111083535_add_child_counts_to_nodes.rs:289` (recursive: VNode child)

---

## Summary

**Total: 57 recursive functions** (55 direct + 2 mutually recursive)

| Category | Count |
|---|---|
| MerkleTreeNode traversal helpers | 12 |
| CommitMerkleTree (v_latest) | 6 |
| CommitMerkleTree (v_old) | 4 |
| repositories/tree.rs | 7 |
| core/v_latest (branches, merge, rm, status) | 9 |
| core/v_latest (fetch, download, prune, entries, schemas) | 8 |
| Model types (DataType, WorkspaceMetadataEntry) | 3 |
| Utility (fs, glob) | 3 |
| Repositories (commits, fork, size) | 4 |
| Command (migrate) | 1 |
