//! Migration: zero-pad hex-hash directory names to 32 characters.
//!
//! Commit `5ab062e66754f14daa4f9533e348f6b339fc1a22` changed `MerkleHash`'s `Display` impl to
//! always produce a 32-character zero-padded hex string. Before that fix, hashes whose leading
//! bits were zero produced shorter strings (e.g. `MerkleHash(0x123) → "123"`). Every on-disk
//! path derived from `MerkleHash::to_string()` inherited that variable length, so the 2-level
//! `{prefix}/{suffix}` directories ended up malformed for those hashes. This migration walks
//! the affected directories and renames them to match the new fixed-length form (`up`) or
//! reverts to the pre-fix variable-length form (`down`).
//!
//! Affected on-disk locations (relative to a repo's `.oxen/`):
//!   - `tree/nodes/{p}/{s}`                  (3-char prefix)
//!   - `tree/sync_status/nodes/{p}/{s}`      (3-char prefix)
//!   - `tree/sync_status/commits/{p}/{s}`    (3-char prefix)
//!   - `versions/files/{p}/{s}`              (2-char prefix)

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use super::Migrate;

use crate::constants;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::repositories;
use crate::util;
use crate::util::progress_bar::{ProgressBarType, oxen_progress_bar};

/// Controls whether the `up`/`down` pre-flight collision scan runs. The per-rename guard
/// always runs and cannot be disabled. Set via `set_skip_collision_check` from the CLI
/// dispatcher before invoking the migration.
static SKIP_COLLISION_CHECK: AtomicBool = AtomicBool::new(false);

pub fn set_skip_collision_check(skip: bool) {
    SKIP_COLLISION_CHECK.store(skip, Ordering::Relaxed);
}

fn skip_collision_check() -> bool {
    SKIP_COLLISION_CHECK.load(Ordering::Relaxed)
}

pub struct ZeroPadHashDirsMigration;

#[derive(Clone, Copy)]
enum Direction {
    Up,
    Down,
}

impl Migrate for ZeroPadHashDirsMigration {
    fn name(&self) -> &'static str {
        "zero_pad_hash_dirs"
    }

    fn description(&self) -> &'static str {
        "Rename hex-hash directories so their names match the zero-padded 32-character format"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        run(path, all, Direction::Up)
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        run(path, all, Direction::Down)
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        // Needed if any affected base dir has entries that don't match the 32-char zero-padded
        // layout. Cheap scan: first hit wins.
        for (base, split) in affected_base_dirs(repo) {
            if !base.exists() {
                continue;
            }
            if base_dir_needs_migration(&base, split)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn run(path: &Path, all: bool, direction: Direction) -> Result<(), OxenError> {
    if all {
        run_on_all_repos(path, direction)
    } else {
        let repo = LocalRepository::from_dir(path)?;
        run_on_one_repo(&repo, direction)
    }
}

fn run_on_all_repos(path: &Path, direction: Direction) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match run_on_one_repo(&repo, direction) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not run zero_pad_hash_dirs migration for repo {:?}\nErr: {}",
                        util::fs::canonicalize(&repo.path),
                        err
                    );
                }
            }
        }
        bar.inc(1);
    }
    Ok(())
}

fn run_on_one_repo(repo: &LocalRepository, direction: Direction) -> Result<(), OxenError> {
    log::info!(
        "Running zero_pad_hash_dirs ({}) on repo: {:?}",
        match direction {
            Direction::Up => "up",
            Direction::Down => "down",
        },
        repo.path
    );

    for (base, split) in affected_base_dirs(repo) {
        if !base.exists() {
            continue;
        }
        migrate_base_dir(&base, split, direction)?;
    }
    Ok(())
}

/// Enumerate (base_dir, split_pos) for every on-disk location whose directory names derive
/// from `MerkleHash::to_string()`.
fn affected_base_dirs(repo: &LocalRepository) -> Vec<(PathBuf, usize)> {
    let oxen = repo.path.join(constants::OXEN_HIDDEN_DIR);
    let tree = oxen.join(constants::TREE_DIR);
    let sync_status = tree.join(constants::SYNC_STATUS_DIR);
    vec![
        (tree.join(constants::NODES_DIR), 3),
        (sync_status.join(constants::NODES_DIR), 3),
        (sync_status.join(constants::COMMITS_DIR), 3),
        (
            oxen.join(constants::VERSIONS_DIR)
                .join(constants::FILES_DIR),
            2,
        ),
    ]
}

/// Cheap scan used by `is_needed`: returns true as soon as any entry doesn't match the
/// current fixed-length layout (32-char hex = split + (32 - split)).
fn base_dir_needs_migration(base: &Path, split: usize) -> Result<bool, OxenError> {
    let expected_suffix_len = 32 - split;
    for entry in std::fs::read_dir(base)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let prefix_name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };
        // A correctly-migrated prefix has exactly `split` all-hex chars.
        if prefix_name.len() != split || !is_all_hex(&prefix_name) {
            return Ok(true);
        }
        // Check each suffix child.
        let prefix_path = entry.path();
        for sub in std::fs::read_dir(&prefix_path)? {
            let sub = sub?;
            let name = match sub.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            if !sub.file_type()?.is_dir() {
                // Leaf files (node, children, is_synced, file, ...) directly under prefix
                // indicate the short-hash (old hex == prefix) malformed case.
                return Ok(true);
            }
            if !is_all_hex(&name) {
                // A non-hex subdir such as `chunks` here means the short-hash case too:
                // prefix dir *is* the leaf.
                return Ok(true);
            }
            if name.len() != expected_suffix_len {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn migrate_base_dir(base: &Path, split: usize, direction: Direction) -> Result<(), OxenError> {
    // Snapshot prefix dirs up front so freshly-created targets don't get re-walked.
    let prefix_dirs = snapshot_subdirs(base)?;

    // Phase A: collect intended renames.
    let mut renames: Vec<(PathBuf, PathBuf)> = Vec::new();
    for prefix_name in &prefix_dirs {
        let prefix_path = base.join(prefix_name);
        collect_renames_for_prefix(
            base,
            &prefix_path,
            prefix_name,
            split,
            direction,
            &mut renames,
        )?;
    }

    if renames.is_empty() {
        return Ok(());
    }

    // Phase B: pre-flight collision check (unless --skip-collision-check).
    if !skip_collision_check() {
        for (src, dst) in &renames {
            if dst.exists() {
                return Err(collision_error(src, dst));
            }
        }
    }

    // Phase C: execute renames (suffix dirs move first — every entry in `renames` is a
    // leaf-or-short-hash dir, all handled before any prefix-dir cleanup below).
    for (src, dst) in &renames {
        if let Some(parent) = dst.parent() {
            util::fs::create_dir_all(parent)?;
        }
        // Always-on rename-time guard: catches races and overwrite-on-rename platforms.
        if dst.exists() {
            return Err(collision_error(src, dst));
        }
        log::debug!("zero_pad_hash_dirs rename {src:?} -> {dst:?}");
        util::fs::rename(src, dst)?;
    }

    // Phase D: remove now-empty old prefix dirs.
    for prefix_name in &prefix_dirs {
        let prefix_path = base.join(prefix_name);
        if !prefix_path.exists() {
            continue; // whole-prefix short-hash rename already moved it
        }
        if dir_is_empty(&prefix_path)?
            && let Err(err) = std::fs::remove_dir(&prefix_path)
        {
            log::warn!(
                "could not remove empty prefix dir {:?}: {}",
                prefix_path,
                err
            );
        }
    }

    Ok(())
}

fn collect_renames_for_prefix(
    base: &Path,
    prefix_path: &Path,
    prefix_name: &str,
    split: usize,
    direction: Direction,
    out: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<(), OxenError> {
    let children = snapshot_children(prefix_path)?;
    let hex_subdirs: Vec<String> = children
        .iter()
        .filter(|c| c.is_dir && is_all_hex(&c.name))
        .map(|c| c.name.clone())
        .collect();
    let has_non_hex_children = children.iter().any(|c| !c.is_dir || !is_all_hex(&c.name));

    if has_non_hex_children {
        // Short-hash case: `prefix_path` itself is the leaf. old_hex == prefix_name, empty
        // suffix. Rename the whole prefix dir.
        if let Some((src, dst)) = planned_rename(base, prefix_name, "", split, direction, true)? {
            out.push((src, dst));
        }
        // In this case there should be no hex_subdirs; if there are, they're ambiguous
        // (mixed layout). Log and ignore — the whole-prefix rename takes precedence.
        if !hex_subdirs.is_empty() {
            log::warn!(
                "prefix {:?} has both leaf content and hex subdirs — treating as short-hash, ignoring subdirs",
                prefix_path
            );
        }
    } else {
        for suffix_name in hex_subdirs {
            if let Some((src, dst)) =
                planned_rename(base, prefix_name, &suffix_name, split, direction, false)?
            {
                out.push((src, dst));
            }
        }
    }
    Ok(())
}

/// Given an old (prefix, suffix) pair, compute the intended (src, dst) if a rename is
/// needed. Returns `None` if already in the target form.
fn planned_rename(
    base: &Path,
    old_prefix: &str,
    old_suffix: &str,
    split: usize,
    direction: Direction,
    short_hash: bool,
) -> Result<Option<(PathBuf, PathBuf)>, OxenError> {
    let old_hex: String = format!("{old_prefix}{old_suffix}");
    if !is_all_hex(&old_hex) || old_hex.is_empty() {
        log::warn!(
            "skipping non-hex entry under {:?}: old_prefix={old_prefix:?} old_suffix={old_suffix:?}",
            base
        );
        return Ok(None);
    }
    let value: u128 = u128::from_str_radix(&old_hex, 16).map_err(|e| {
        OxenError::basic_str(format!(
            "zero_pad_hash_dirs: failed to parse {old_hex:?} as u128 hex under {base:?}: {e}"
        ))
    })?;

    let new_hex = match direction {
        Direction::Up => format!("{value:032x}"),
        // Preserves the pre-fix `take(split).collect` / `skip(split).collect` semantics
        // from the old `node_db_prefix` (see `git show 78d08189d^`): short hexes pass
        // through unchanged.
        Direction::Down => format!("{value:x}"),
    };

    // Char-iterator split mirrors the pre-fix `node_db_prefix`. Safe for short strings
    // (produces shorter prefix + empty suffix) as well as the fixed-length form.
    let new_prefix: String = new_hex.chars().take(split).collect();
    let new_suffix: String = new_hex.chars().skip(split).collect();

    let src = if short_hash {
        base.join(old_prefix)
    } else {
        base.join(old_prefix).join(old_suffix)
    };
    let dst = if new_suffix.is_empty() {
        base.join(&new_prefix)
    } else {
        base.join(&new_prefix).join(&new_suffix)
    };

    if src == dst {
        return Ok(None);
    }
    Ok(Some((src, dst)))
}

fn collision_error(src: &Path, dst: &Path) -> OxenError {
    OxenError::basic_str(format!(
        "zero_pad_hash_dirs: rename target already exists. src={src:?} dst={dst:?}. \
        Re-run without --skip-collision-check for an earlier pre-flight failure, or \
        investigate whether the migration was partially applied."
    ))
}

fn is_all_hex(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit())
}

fn dir_is_empty(p: &Path) -> Result<bool, OxenError> {
    let mut it = std::fs::read_dir(p)?;
    Ok(it.next().is_none())
}

struct Child {
    name: String,
    is_dir: bool,
}

fn snapshot_children(dir: &Path) -> Result<Vec<Child>, OxenError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };
        let is_dir = entry.file_type()?.is_dir();
        out.push(Child { name, is_dir });
    }
    Ok(out)
}

fn snapshot_subdirs(dir: &Path) -> Result<Vec<String>, OxenError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Ok(n) = entry.file_name().into_string() {
            out.push(n);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use std::fs;

    /// Produce the legacy (pre-fix) on-disk split for a given hash and split position.
    /// Mirrors the original `node_db_prefix` char-iterator logic.
    fn legacy_split(value: u128, split: usize) -> (String, String) {
        let hex = format!("{value:x}");
        let prefix: String = hex.chars().take(split).collect();
        let suffix: String = hex.chars().skip(split).collect();
        (prefix, suffix)
    }

    /// Write a dummy leaf directory under the legacy layout for `(base, value, split)`.
    /// Mimics whatever content might live inside each kind of leaf (e.g. a `node` file).
    fn seed_legacy(base: &Path, value: u128, split: usize, leaf_file: &str, contents: &[u8]) {
        let (prefix, suffix) = legacy_split(value, split);
        let leaf_dir = if suffix.is_empty() {
            base.join(&prefix)
        } else {
            base.join(&prefix).join(&suffix)
        };
        fs::create_dir_all(&leaf_dir).unwrap();
        fs::write(leaf_dir.join(leaf_file), contents).unwrap();
    }

    fn padded_path(base: &Path, value: u128, split: usize) -> PathBuf {
        let hex = format!("{value:032x}");
        let prefix: String = hex.chars().take(split).collect();
        let suffix: String = hex.chars().skip(split).collect();
        base.join(prefix).join(suffix)
    }

    #[test]
    fn test_up_renames_short_hashes_for_all_base_dirs() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let bases = affected_base_dirs(&repo);
            // Seed one malformed (value=1) and one well-formed-random (value=0xdeadbeef...) entry in each base.
            let values: Vec<u128> = vec![
                1u128,
                0x123u128,
                0xdead_beef_1234_5678_9abc_def0_1234_5678u128,
            ];
            for (base, split) in &bases {
                util::fs::create_dir_all(base)?;
                for v in &values {
                    seed_legacy(base, *v, *split, "node", b"dummy");
                }
            }

            // Run the migration.
            run_on_one_repo(&repo, Direction::Up)?;

            // Every value's content should now be reachable at the padded path.
            for (base, split) in &bases {
                for v in &values {
                    let p = padded_path(base, *v, *split).join("node");
                    assert!(
                        p.exists(),
                        "missing padded path after up: {p:?} (value={v:#x}, split={split})"
                    );
                    let contents = fs::read(&p).unwrap();
                    assert_eq!(contents, b"dummy");
                }
            }
            // is_needed should now report false.
            assert!(!ZeroPadHashDirsMigration.is_needed(&repo)?);
            Ok(())
        })
    }

    #[test]
    fn test_up_is_idempotent() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let bases = affected_base_dirs(&repo);
            for (base, split) in &bases {
                util::fs::create_dir_all(base)?;
                seed_legacy(base, 0x42u128, *split, "node", b"x");
            }
            run_on_one_repo(&repo, Direction::Up)?;
            // Second run is a no-op.
            run_on_one_repo(&repo, Direction::Up)?;
            for (base, split) in &bases {
                let p = padded_path(base, 0x42u128, *split).join("node");
                assert!(p.exists());
            }
            Ok(())
        })
    }

    #[test]
    fn test_round_trip_random_values() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let (base, split) = affected_base_dirs(&repo).into_iter().next().unwrap();
            util::fs::create_dir_all(&base)?;

            // Use a deterministic pseudo-random sequence so failures reproduce.
            let mut state: u128 = 0xdead_beef_cafe_babe_1234_5678_9abc_def0;
            let mut values: Vec<u128> = Vec::with_capacity(64);
            for _ in 0..64 {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                values.push(state);
                seed_legacy(&base, state, split, "node", &state.to_le_bytes());
            }

            run_on_one_repo(&repo, Direction::Up)?;
            for v in &values {
                let p = padded_path(&base, *v, split).join("node");
                assert!(p.exists(), "missing up path for {v:#x}");
                assert_eq!(fs::read(&p).unwrap(), v.to_le_bytes().to_vec());
            }

            run_on_one_repo(&repo, Direction::Down)?;
            for v in &values {
                let (prefix, suffix) = legacy_split(*v, split);
                let leaf_dir = if suffix.is_empty() {
                    base.join(&prefix)
                } else {
                    base.join(&prefix).join(&suffix)
                };
                let p = leaf_dir.join("node");
                assert!(p.exists(), "missing down path for {v:#x} at {p:?}");
                assert_eq!(fs::read(&p).unwrap(), v.to_le_bytes().to_vec());
            }
            Ok(())
        })
    }

    #[test]
    fn test_collision_hard_fails_with_descriptive_error() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            set_skip_collision_check(false);
            let (base, split) = affected_base_dirs(&repo).into_iter().next().unwrap();
            util::fs::create_dir_all(&base)?;
            // Seed the malformed src.
            seed_legacy(&base, 0x1u128, split, "node", b"src");
            // Pre-place the padded dst with different contents to force a collision.
            let dst = padded_path(&base, 0x1u128, split);
            util::fs::create_dir_all(&dst)?;
            fs::write(dst.join("node"), b"dst").unwrap();

            let err = run_on_one_repo(&repo, Direction::Up).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("already exists"), "unexpected error: {msg}");
            assert!(msg.contains("dst="), "unexpected error: {msg}");
            Ok(())
        })
    }

    #[test]
    fn test_collision_with_skip_preflight_still_caught_at_rename_time() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            set_skip_collision_check(true);
            let (base, split) = affected_base_dirs(&repo).into_iter().next().unwrap();
            util::fs::create_dir_all(&base)?;
            seed_legacy(&base, 0x2u128, split, "node", b"src");
            let dst = padded_path(&base, 0x2u128, split);
            util::fs::create_dir_all(&dst)?;
            fs::write(dst.join("node"), b"dst").unwrap();

            let err = run_on_one_repo(&repo, Direction::Up).unwrap_err();
            // Reset for other tests.
            set_skip_collision_check(false);
            let msg = err.to_string();
            assert!(msg.contains("already exists"), "unexpected error: {msg}");
            Ok(())
        })
    }

    #[test]
    fn test_short_hash_edge_case_3_char_prefix() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Use the 3-char-split `tree/nodes` base; seed a value whose legacy hex is
            // exactly 3 chars ("abc" = 0xabc).
            let base = repo
                .path
                .join(constants::OXEN_HIDDEN_DIR)
                .join(constants::TREE_DIR)
                .join(constants::NODES_DIR);
            util::fs::create_dir_all(&base)?;
            let short_prefix = base.join("abc");
            util::fs::create_dir_all(&short_prefix)?;
            fs::write(short_prefix.join("node"), b"payload").unwrap();
            fs::write(short_prefix.join("children"), b"kids").unwrap();

            run_on_one_repo(&repo, Direction::Up)?;

            // After up: the `abc` short-hash prefix should be renamed to
            // `000/<26 zeros>abc/` (3-char prefix + 29-char suffix = 32-char hex).
            let padded = base.join("000").join("00000000000000000000000000abc");
            assert!(padded.join("node").exists());
            assert_eq!(fs::read(padded.join("node")).unwrap(), b"payload");
            assert_eq!(fs::read(padded.join("children")).unwrap(), b"kids");
            assert!(!base.join("abc").exists());

            // And down restores the short-hash layout.
            run_on_one_repo(&repo, Direction::Down)?;
            let short = base.join("abc");
            assert!(short.join("node").exists());
            assert_eq!(fs::read(short.join("node")).unwrap(), b"payload");
            Ok(())
        })
    }

    #[test]
    fn test_is_needed_false_on_correctly_padded_layout() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let bases = affected_base_dirs(&repo);
            for (base, split) in &bases {
                util::fs::create_dir_all(base)?;
                let hex = format!("{:032x}", 0xfeed_face_1234_5678_9abc_def0_0000_0001u128);
                let prefix: String = hex.chars().take(*split).collect();
                let suffix: String = hex.chars().skip(*split).collect();
                let leaf = base.join(&prefix).join(&suffix);
                fs::create_dir_all(&leaf).unwrap();
                fs::write(leaf.join("node"), b"x").unwrap();
            }
            assert!(!ZeroPadHashDirsMigration.is_needed(&repo)?);
            Ok(())
        })
    }

    #[test]
    fn test_is_needed_true_on_short_hash_layout() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let base = repo
                .path
                .join(constants::OXEN_HIDDEN_DIR)
                .join(constants::TREE_DIR)
                .join(constants::NODES_DIR);
            util::fs::create_dir_all(&base)?;
            let short = base.join("1"); // 1-char prefix — never written by current code, but
            // simulates the malformed layout `is_needed` targets.
            fs::create_dir_all(&short).unwrap();
            fs::write(short.join("node"), b"x").unwrap();
            assert!(ZeroPadHashDirsMigration.is_needed(&repo)?);
            Ok(())
        })
    }
}
