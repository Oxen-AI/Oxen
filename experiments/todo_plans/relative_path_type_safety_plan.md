# Relative Path Type Safety Plan

## Context

The Oxen codebase uses raw `PathBuf` for paths that carry three fundamentally different semantics:
- **Absolute filesystem paths** — e.g., `/home/user/repo/1/2/3`
- **Paths relative to the repo root** — e.g., `1/2/3/file.txt`
- **Leaf names** — e.g., `3` or `file.txt`

There is no type-level distinction between these. Two bugs were caused by this confusion, both **silent** — no error is raised, the operation just doesn't work.

---

## Confirmed Bugs

### Bug 1: Double-prefix path join in `r_collect_removed_paths`

**File:** `crates/lib/src/util/glob.rs:411-434`

```rust
pub fn r_collect_removed_paths(
    repo: &LocalRepository,
    dir_path: &PathBuf,                          // ABSOLUTE: e.g., /repo/1
    removed_paths: &mut HashSet<PathBuf>,
) -> Result<(), OxenError> {
    let repo_path = repo.path.clone();
    if dir_path.is_dir() {
        let glob_path = util::fs::path_relative_to_dir(dir_path, &repo_path)?.join("*");
        search_merkle_tree(removed_paths, repo, &glob_path)?;
        //                 ^^^^^^^^^^^^^^ returns REPO-RELATIVE paths (e.g., "1/2")
        let paths = removed_paths.clone();

        for path in paths.iter() {
            if repo_path.join(path).is_dir() {   // ✓ correct check
                let dir_path = dir_path.join(path);  // ✗ BUG: /repo/1 + "1/2" = /repo/1/1/2
                r_collect_removed_paths(repo, &dir_path, removed_paths)?;
            }
        }
    }
    Ok(())
}
```

**How it breaks:** When `dir_path = /repo/1` and `search_merkle_tree` returns `"1/2"` (repo-relative), the join produces `/repo/1/1/2` — a nonexistent path. The recursive call sees `dir_path.is_dir()` is false and does nothing. Nested removed directories are never discovered.

**Fix:** Change line 427 from `dir_path.join(path)` to `repo_path.join(path)`.

### Bug 2: Leaf name vs full repo-relative path in `r_process_remove_dir`

**File:** `crates/lib/src/core/v_latest/rm.rs:666-765`

When staging a directory for removal, the old code cloned the merkle tree node as-is:

```rust
// BEFORE FIX (line 729-734):
EMerkleTreeNode::Directory(_) => {
    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node: node.clone(),   // node.name() = "3" (leaf only)
    };
    staged_db.put(path.to_str().unwrap(), &buf).unwrap();
    //           ^^^^^ key = "1/2/3" (full path) — but the NODE inside has name "3"
}
```

During commit, `split_into_vnodes` (`commit_writer.rs:649-661`) compares staged entries against existing children using `StagedMerkleTreeNode` equality, which is based on `maybe_path()`:

```rust
// staged_merkle_tree_node.rs:38-47
impl PartialEq for StagedMerkleTreeNode {
    fn eq(&self, other: &Self) -> bool {
        if let Ok(path) = self.node.maybe_path()
            && let Ok(other_path) = other.node.maybe_path()
        {
            return path == other_path;   // Path-based comparison
        }
        self.node.hash == other.node.hash
    }
}
```

Existing children are constructed by `node_data_to_staged_node` (`commit_writer.rs:555-581`) which sets names to full repo-relative paths:

```rust
fn node_data_to_staged_node(base_dir: impl AsRef<Path>, node: &MerkleTreeNode) -> ... {
    MerkleTreeNodeType::Dir => {
        let mut dir_node = node.dir()?;
        let path = base_dir.join(dir_node.name());     // "1/2" + "3" = "1/2/3"
        dir_node.set_name(path.to_str().unwrap());      // name = "1/2/3"
        // ...
    }
}
```

So the existing child has `maybe_path() = "1/2/3"` but the staged removal has `maybe_path() = "3"`. The `children.remove(child)` call at `commit_writer.rs:660` silently fails — the directory stays in the merkle tree.

**Fix (already applied):** Update the node name to the full path before staging:

```rust
// AFTER FIX (rm.rs:729-743):
EMerkleTreeNode::Directory(_) => {
    let mut updated_node = node.clone();
    if let EMerkleTreeNode::Directory(ref mut dir_node) = updated_node.node {
        dir_node.set_name(path.to_string_lossy().as_ref());   // "1/2/3" (full path)
    }
    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node: updated_node,
    };
    // ...
}
```

Compare with `process_remove_file` (`rm.rs:546-552`) which already did this correctly:

```rust
pub fn process_remove_file(path: &Path, file_node: &FileNode, ...) -> ... {
    let mut update_node = file_node.clone();
    update_node.set_name(&path.to_string_lossy());   // ✓ always set full path
    // ...
}
```

---

## Path Lifecycle Through the System

Understanding why consistency matters requires tracing a path through staging → reading → commit.

### Staging (add/rm → RocksDB)

All staged entries are written to a RocksDB database. The **key** is the full repo-relative path string. The **value** is a serialized `StagedMerkleTreeNode` whose inner node name must also be the full repo-relative path.

| Code site | File | What it stores |
|-----------|------|---------------|
| `process_add_file` | `add.rs:835` | `FileNode { name: relative_path_str }` — full path like `"1/2/3/file.txt"` |
| `process_remove_file` | `rm.rs:552` | `update_node.set_name(&path.to_string_lossy())` — full path |
| `process_remove_file_and_parents` | `rm.rs:408` | Same pattern |
| `r_process_remove_dir` (dirs) | `rm.rs:738` | `dir_node.set_name(path.to_string_lossy())` — full path (after fix) |
| `process_remove_dir` (parents) | `rm.rs:642-650` | `MerkleTreeNode::default_dir_from_path(&relative_path)` — full path |

### Reading staged entries (status.rs → HashMap)

`read_staged_entries` (`core/v_latest/status.rs:374-443`) iterates the staged DB and organizes into:

```
HashMap<PathBuf, Vec<StagedMerkleTreeNode>>
  key = parent directory (repo-relative)
  value = children of that directory
```

Each entry's `maybe_path()` (which returns the node name) determines identity for later comparison.

### Commit (commit_writer.rs → merkle tree)

1. **`commit_dir_entries_new`** (`commit_writer.rs:337-340`) loads existing merkle tree nodes for each directory.
2. **`get_node_dir_children`** (`commit_writer.rs:583-595`) converts existing children to `StagedMerkleTreeNode` via `node_data_to_staged_node`, setting names to `base_dir + leaf_name` (full repo-relative paths).
3. **`split_into_vnodes`** (`commit_writer.rs:649-661`) matches staged entries against existing children:
   ```rust
   StagedEntryStatus::Removed => {
       children.remove(child);           // Uses PartialEq → compares maybe_path()
       removed_children.insert(child);
   }
   _ => {
       children.replace(child.clone());  // Uses PartialEq → compares maybe_path()
   }
   ```
   **If `maybe_path()` doesn't match, the remove/replace silently does nothing.**

---

## High-Risk Areas (Same Pattern, Not Yet Broken)

These functions accept `path: &Path` that **must** be repo-relative for correctness, but the type doesn't enforce it:

| # | File | Function | Lines | Why it's risky |
|---|------|----------|-------|---------------|
| 1 | `core/v_latest/rm.rs` | `process_remove_file` | 546-572 | Sets `file_node.name` from `path` — wrong path format = silent commit failure |
| 2 | `core/v_latest/rm.rs` | `process_remove_file_and_parents` | 400-428 | Same — also uses `path` as DB key |
| 3 | `core/v_latest/rm.rs` | `r_process_remove_dir` | 666-765 | `path` builds up via `join(leaf)` — if initial call passes wrong format, all descendants wrong |
| 4 | `core/v_latest/rm.rs` | `process_remove_dir` | 605-655 | Calls `path_relative_to_dir(parent, repo_path)` on already-relative path — redundant, confusing |
| 5 | `core/v_latest/add.rs` | `process_add_file` | 758-850 | `relative_path` computed on line 762, used as node name (835) and DB key (849) |
| 6 | `commits/commit_writer.rs` | `node_data_to_staged_node` | 555-581 | `base_dir` must be repo-relative — nothing enforces this |
| 7 | `commits/commit_writer.rs` | `split_into_vnodes` | 600-787 | HashMap keys must be repo-relative — `PathBuf` doesn't distinguish |

---

## Proposal: `RepoRelativePath` Newtype

### Definition

New file: `crates/lib/src/util/repo_relative_path.rs`
Re-export from: `crates/lib/src/util.rs` (add `pub mod repo_relative_path;` and `pub use repo_relative_path::RepoRelativePath;`)

```rust
/// A path guaranteed to be relative to a repository root.
///
/// Invariants:
/// - Never absolute (no leading `/` or drive letter)
/// - Never contains `..` components
/// - Normalized (no `.` components)
///
/// This is the format used for staged DB keys and `MerkleTreeNode` names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepoRelativePath(PathBuf);
```

### Constructors

```rust
/// Create from an arbitrary path by stripping `repo_root` prefix.
/// Delegates to the existing `util::fs::path_relative_to_dir`.
pub fn new(repo_root: &Path, path: &Path) -> Result<Self, OxenError>

/// Create from a path already known to be relative. Validates invariants.
pub fn from_relative(path: impl Into<PathBuf>) -> Result<Self, OxenError>
```

**No `From<PathBuf>` impl** — construction must go through validation. This is the entire point.

### Methods

| Method | Signature | Purpose |
|--------|-----------|---------|
| `as_path` | `&self -> &Path` | Borrow inner path |
| `into_path_buf` | `self -> PathBuf` | Consume into PathBuf |
| `as_str` | `&self -> &str` | For DB keys, node names |
| `join` | `&self, leaf: &str -> Result<Self>` | Append a component, validated |
| `parent` | `&self -> Option<Self>` | Parent directory |
| `to_absolute` | `&self, repo_root: &Path -> PathBuf` | Reconstruct `repo_root.join(self)` |

### Trait Impls

- `AsRef<Path>` — works with `repo_path.join(relative)` and `impl AsRef<Path>` parameters
- `Display` — delegates to inner path display
- `From<RepoRelativePath> for PathBuf` — consuming conversion out
- `Serialize` / `Deserialize` — for staged DB (serialize as string)

### Relationship with `path_relative_to_dir`

`RepoRelativePath::new()` delegates to the existing `util::fs::path_relative_to_dir` (`crates/lib/src/util/fs.rs:1284`), then validates the result. This reuses the existing normalization logic (which handles Windows casing, `.`/`..` components, etc.).

### How it prevents the confirmed bugs

**Bug 1 (glob.rs):** If `removed_paths` were `HashSet<RepoRelativePath>`, you couldn't call `dir_path.join(path)` where `dir_path` is absolute — the types make you write `path.to_absolute(&repo_path)` or `repo_path.join(path.as_path())`.

**Bug 2 (rm.rs):** If `r_process_remove_dir` took `path: &RepoRelativePath`, the caller would be forced to construct a validated full repo-relative path at the call site. And `set_name(path.as_str())` would always set the full path.

---

## Should We Also Add `LeafName`?

**Not yet.** The leaf-vs-full-path confusion (bug #2) is correctly addressed by ensuring all `StagedMerkleTreeNode` names use full repo-relative paths. Leaf names are only transient values during tree traversal (e.g., `dir_node.name()` at `rm.rs:684` where it's immediately joined with the parent path). A `LeafName` type would require changes to `FileNode` and `DirNode` with a large blast radius and minimal additional safety beyond what `RepoRelativePath` provides.

---

## Phased Adoption Plan

### Phase 1: Foundation (1 PR)

- Create `crates/lib/src/util/repo_relative_path.rs` with struct, constructors, methods, traits
- Add `pub mod repo_relative_path;` + `pub use` to `crates/lib/src/util.rs`
- Unit tests: construction from absolute+root, validation rejects absolute/`..`, `join`, `parent`, `as_str`

### Phase 2: Fix bug #1 + adopt in rm.rs (1 PR, highest risk)

- Fix `r_collect_removed_paths` in `glob.rs:427`
- Update these function signatures to use `path: &RepoRelativePath`:
  - `r_process_remove_dir` (`rm.rs:666`)
  - `process_remove_dir` (`rm.rs:605`)
  - `process_remove_file` (`rm.rs:546`)
  - `process_remove_file_and_parents` (`rm.rs:400`)
- This covers both confirmed bugs and the four highest-risk functions

### Phase 3: Adopt in add.rs (1 PR)

- Update `process_add_file` (`add.rs:758`) and `process_add_file_with_staged_db_manager` (`add.rs:854`)
- Change `relative_path` local from `PathBuf` to `RepoRelativePath`
- Most heavily exercised code path — thorough testing required

### Phase 4: Adopt in commit_writer.rs (1 PR)

- Update `node_data_to_staged_node` (`commit_writer.rs:555`) — `base_dir: &RepoRelativePath`
- Update `get_node_dir_children` (`commit_writer.rs:583`) — same
- Update `split_into_vnodes` entry map to `HashMap<RepoRelativePath, ...>`
- Ensures the full staging → commit pipeline uses typed paths

### Phase 5: Broader rollout (lower priority, many small PRs)

- Remaining ~20 `path_relative_to_dir` call sites across `status.rs`, `workspaces/files.rs`, `api/client/`

---

## Verification

For each phase:

```bash
# Compile check
cargo check --workspace

# New removal tests
cargo test -p liboxen -- test_remove_

# Existing related tests
cargo test -p liboxen -- "test_rm_dir_doesnt_break_tree" \
  "test_command_commit_removed_dir" \
  "test_rm_multi_level_directory" \
  "test_rm_subdir" \
  "test_wildcard_add_remove_nested_nlp_dir" \
  "test_rm_r_dir_at_root"

# Full suite
bin/test-rust
```
