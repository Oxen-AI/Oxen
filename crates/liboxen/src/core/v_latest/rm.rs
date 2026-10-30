use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::MerkleHash;
use crate::model::staged_data::StagedDataOpts;
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;

use crate::core::v_latest::index::CommitMerkleTree;
use crate::model::merkle_tree::node::FileNode;
use crate::view::ErrorFileInfo;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use rocksdb::{IteratorMode, WriteBatch};
use tokio::time::Duration;

use crate::core::staged::get_staged_db_manager;
use crate::core::staged::staged_db_manager::staged_entries_under;
use crate::core::v_latest::add::CumulativeStats;
use crate::core::v_latest::add::add_file_node_and_parent_dir;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;

use crate::constants::STAGED_DIR;
use crate::model::Commit;
use crate::model::StagedEntryStatus;

use parking_lot::Mutex;
use rmp_serde::Serializer;
use serde::Serialize;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str;

use rocksdb::DB;

use std::sync::Arc;

/// Removes the `paths` given outright and every match in `glob_matches`, which pairs each glob given
/// with its matches. A match whose removal is already staged is skipped rather than refused unless it
/// is also given outright, and a glob whose every match is skipped fails.
pub async fn rm(
    paths: &HashSet<PathBuf>,
    glob_matches: &[(PathBuf, HashSet<PathBuf>)],
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<(), OxenError> {
    let db_opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DB = DB::open(&db_opts, dunce::simplified(&db_path))?;

    let mut targets = paths.clone();
    targets.extend(
        glob_matches
            .iter()
            .flat_map(|(_, matches)| matches)
            .cloned(),
    );

    let start = std::time::Instant::now();
    let removals = look_up_removals(&targets, paths, glob_matches, repo, opts, &staged_db)?;
    refuse_lost_changes(repo, &removals.paths(), opts, &staged_db).await?;
    let total = apply_removals(repo, removals, opts, &staged_db)?;

    // Stop the timer, and round the duration to the nearest second
    let duration = Duration::from_millis(start.elapsed().as_millis() as u64);
    log::debug!("---END--- oxen rm: {targets:?} duration: {duration:?}");

    // TODO: Add function to CumulativeStats to output that print statement
    println!(
        "🐂 oxen removed {} files ({}) in {}",
        total.total_files,
        bytesize::ByteSize::b(total.total_bytes),
        humantime::format_duration(duration)
    );
    Ok(())
}

/// Stages the removal of the committed `paths`, which are gone from the working tree.
pub(crate) fn stage_removal_of_deleted_paths(
    paths: &HashSet<PathBuf>,
    repo: &LocalRepository,
    staged_db: &DB,
) -> Result<CumulativeStats, OxenError> {
    let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? else {
        return Err(OxenError::basic_str("Error: head commit not found"));
    };
    let dir_hashes = CommitMerkleTree::dir_hashes(repo, &head_commit)?;
    let mut parent_nodes = HashMap::new();
    let mut committed = Vec::with_capacity(paths.len());
    for path in paths {
        let relative_path = util::fs::path_relative_to_dir(path, &repo.path)?;
        let parent_path = relative_path.parent().unwrap_or(Path::new(""));
        let name = util::fs::path_relative_to_dir(&relative_path, parent_path)?;
        let parent = match parent_nodes.entry(parent_path.to_path_buf()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(CommitMerkleTree::dir_with_children(
                repo,
                &head_commit,
                parent_path,
                Some(&dir_hashes),
            )?),
        };
        let Some(node) = parent
            .as_ref()
            .map(|parent| parent.get_by_path(name))
            .transpose()?
            .flatten()
        else {
            let error = format!("{relative_path:?} is neither on disk nor committed");
            return Err(OxenError::basic_str(error));
        };
        committed.push((relative_path, node));
    }
    stage_committed_removals(repo, &head_commit, &dir_hashes, committed, true, staged_db)
}

// We have the inner function here so we can open the staged db once
fn remove_staged_recursively_inner(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
    staged_db: &DB,
) -> Result<(), OxenError> {
    // Iterate over staged_db and check if the path starts with the given path
    let iter = staged_db.iterator(IteratorMode::Start);
    for item in iter {
        match item {
            Ok((key, _)) => match str::from_utf8(&key) {
                Ok(key) => {
                    log::debug!("considering key: {key:?}");
                    for path in paths {
                        let path = util::fs::path_relative_to_dir(path, &repo.path)?;
                        let db_path = PathBuf::from(key);
                        log::debug!("considering rm db_path: {db_path:?} for path: {path:?}");
                        if db_path.starts_with(&path) && path != Path::new("") {
                            let mut parent = db_path.parent().unwrap_or(Path::new(""));
                            remove_staged_entry(&db_path, staged_db)?;
                            while parent != Path::new("") {
                                log::debug!("maybe cleaning up empty dir: {parent:?}");
                                cleanup_empty_dirs(parent, staged_db)?;
                                parent = parent.parent().unwrap_or(Path::new(""));
                                if parent == Path::new("") {
                                    cleanup_empty_dirs(parent, staged_db)?;
                                }
                            }
                        }
                    }
                }
                _ => {
                    return Err(OxenError::basic_str("Could not read utf8 val..."));
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(())
}

async fn first_modified_file(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
) -> Result<Option<PathBuf>, OxenError> {
    let paths_vec: Vec<PathBuf> = paths.iter().map(|p| repo.path.join(p)).collect();
    let opts = StagedDataOpts::from_paths(&paths_vec);
    let status = repositories::status::status_from_opts(repo, &opts).await?;
    log::debug!("status modified_files: {:?}", status.modified_files);
    log::debug!("paths: {paths:?}");
    // Keep a modified entry if any requested path is an ancestor (or exact match). A file whose
    // HEAD content is missing locally is modified too, and status reports it apart.
    Ok(status
        .modified_files
        .into_iter()
        .chain(status.unrestorable_files)
        .find(|path| paths.iter().any(|p| path.starts_with(p))))
}

// Removes an empty directory from the staged db
fn cleanup_empty_dirs(path: &Path, staged_db: &DB) -> Result<(), OxenError> {
    let iter = staged_db.iterator(IteratorMode::Start);
    let mut total = 0;
    for item in iter {
        match item {
            Ok((key, _)) => match str::from_utf8(&key) {
                Ok(key) => {
                    log::debug!("considering key: {key:?}");
                    let db_path = PathBuf::from(key);
                    if db_path.starts_with(path) && path != db_path {
                        total += 1;
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not read iterate over db values",
                    ));
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    log::debug!("total sub paths for dir {path:?}: {total}");
    if total == 0 {
        log::debug!("removing empty dir: {path:?}");
        staged_db.delete(path.to_str().unwrap())?;
    }
    Ok(())
}

fn remove_staged_inner(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
    rm_opts: &RmOpts,
    staged_db: &DB,
) -> Result<(), OxenError> {
    log::debug!("remove_staged paths {paths:?}");
    for path in paths {
        let relative_path = util::fs::path_relative_to_dir(path, &repo.path)?;
        let Some(entry) = get_staged_entry(&relative_path, staged_db)? else {
            continue;
        };
        if entry.node.is_dir() && !rm_opts.recursive {
            let error = format!("`oxen rm` on directory {path:?} requires -r");
            return Err(OxenError::basic_str(error));
        }
        remove_staged_entry(&relative_path, staged_db)?;
    }

    Ok(())
}

pub fn remove_staged(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
    rm_opts: &RmOpts,
) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DB = DB::open(&opts, dunce::simplified(&db_path))?;
    remove_staged_inner(repo, paths, rm_opts, &staged_db)
}

/// Whether the removal of the committed `node` at `path` is staged: for a directory, the removal of
/// every file under it.
fn is_removal_staged(
    path: &Path,
    node: &MerkleTreeNode,
    staged_db: &DB,
) -> Result<bool, OxenError> {
    match &node.node {
        EMerkleTreeNode::File(_) => Ok(get_staged_entry(path, staged_db)?
            .is_some_and(|entry| entry.status == StagedEntryStatus::Removed)),
        EMerkleTreeNode::Directory(dir_node) => {
            let removed_files = staged_entries_under(path, staged_db)?
                .into_iter()
                .filter(|(_, entry)| {
                    entry.status == StagedEntryStatus::Removed && entry.node.is_file()
                })
                .count();
            Ok(dir_node.num_files() > 0 && removed_files as u64 == dir_node.num_files())
        }
        _ => Ok(false),
    }
}

/// Refuses, unless `opts.force`, to remove `paths` when that would lose content HEAD lacks: without
/// `--staged`, a working copy that differs from HEAD or a file staged as added or edited at or
/// under a path, and with it, a staged file whose content the working copy lacks too.
async fn refuse_lost_changes(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
    opts: &RmOpts,
    staged_db: &DB,
) -> Result<(), OxenError> {
    if opts.force {
        return Ok(());
    }
    // `--staged` keeps the working copy, so a modification there is never lost
    if !opts.staged
        && let Some(modified) = first_modified_file(repo, paths).await?
    {
        return Err(OxenError::RmWouldLoseChanges(modified.into()));
    }
    for path in paths {
        let relative_path = util::fs::path_relative_to_dir(path, &repo.path)?;
        for (entry_path, entry) in staged_entries_under(&relative_path, staged_db)? {
            let added_or_edited = matches!(
                entry.status,
                StagedEntryStatus::Added | StagedEntryStatus::Modified
            );
            if !added_or_edited || !entry.node.is_file() {
                continue;
            }
            if !opts.staged {
                return Err(OxenError::RmWouldLoseChanges(entry_path.into()));
            }
            let full_path = repo.path.join(&entry_path);
            let in_working_copy = full_path.is_file()
                && !repo
                    .is_modified_from_node(&full_path, &entry.node.file()?)
                    .await?;
            if !in_working_copy {
                return Err(OxenError::RmWouldLoseStagedContent(entry_path.into()));
            }
        }
    }
    Ok(())
}

fn get_staged_entry(
    path: &Path,
    staged_db: &DB,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let path_str = path.to_str().unwrap();
    let Some(value) = staged_db.get(path_str)? else {
        return Ok(None);
    };
    Ok(Some(rmp_serde::from_slice(&value)?))
}

fn remove_staged_entry(path: &Path, staged_db: &DB) -> Result<(), OxenError> {
    log::debug!("remove_staged path: {path:?} from staged db {staged_db:?}");
    staged_db.delete(path.to_str().unwrap())?;
    Ok(())
}

fn remove_file_inner(
    repo: &LocalRepository,
    path: &Path,
    file_node: &FileNode,
    staged_db: &DB,
) -> Result<CumulativeStats, OxenError> {
    let path = util::fs::path_relative_to_dir(path, &repo.path)?;
    log::debug!("remove_file path is {path:?}");
    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    match process_remove_file_and_parents(repo, &path, staged_db, file_node) {
        Ok(Some(node)) => {
            if let EMerkleTreeNode::File(file_node) = &node.node.node {
                total.total_bytes += file_node.num_bytes();
                total.total_files += 1;
                total
                    .data_type_counts
                    .entry(file_node.data_type().clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
            Ok(total)
        }
        Err(e) => {
            let error = format!("Error adding file {path:?}: {e:?}");
            Err(OxenError::basic_str(error))
        }
        _ => {
            let error = format!("Error adding file {path:?}");
            Err(OxenError::basic_str(error))
        }
    }
}

pub fn remove_file(
    repo: &LocalRepository,
    path: &Path,
    file_node: &FileNode,
) -> Result<CumulativeStats, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DB = DB::open(&opts, dunce::simplified(&db_path))?;

    remove_file_inner(repo, path, file_node, &staged_db)
}

pub fn remove_file_with_db_manager(
    repo: &LocalRepository,
    path: &Path,
    file_node: &FileNode,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let mut err_files: Vec<ErrorFileInfo> = vec![];

    let staged_db_manager = get_staged_db_manager(repo)?;
    let status = StagedEntryStatus::Removed;
    match add_file_node_and_parent_dir(file_node, status, path, &staged_db_manager, seen_dirs) {
        Ok(_) => {}
        Err(e) => {
            err_files.push(ErrorFileInfo {
                hash: file_node.hash().to_string(),
                path: Some(path.to_path_buf()),
                error: format!("Failed to add file to staged db: {e}"),
            });
        }
    }

    Ok(err_files)
}

// TODO: Refactor to capture err files
pub fn remove_dir_with_db_manager(
    repo: &LocalRepository,
    root_dir: &MerkleTreeNode,
    root_path: &Path,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<(), OxenError> {
    let empty_path = PathBuf::new();
    let mut staged_nodes: HashMap<PathBuf, StagedMerkleTreeNode> = HashMap::new();
    // let err_files: Vec<ErrorFileInfo> = vec![];

    let staged_db_manager = get_staged_db_manager(repo)?;

    // Walk the tree, collecting every node under the dir
    let nodes = root_dir.list_files_and_dirs()?;
    let parent_path = root_path.parent().unwrap_or(&empty_path);

    for (path, node) in nodes {
        let path = parent_path.join(path);
        let corrected_node = match &node.node {
            EMerkleTreeNode::File(file_node) => {
                let mut file_node = file_node.clone();
                file_node.set_name(&path.to_string_lossy());
                MerkleTreeNode {
                    hash: node.hash,
                    node: EMerkleTreeNode::File(file_node.clone()),
                    parent_id: node.parent_id,
                    children: node.children.clone(),
                }
            }

            EMerkleTreeNode::Directory(dir_node) => {
                let mut dir_node = dir_node.clone();
                dir_node.set_name(path.to_string_lossy());
                MerkleTreeNode {
                    hash: node.hash,
                    node: EMerkleTreeNode::Directory(dir_node.clone()),
                    parent_id: node.parent_id,
                    children: node.children.clone(),
                }
            }
            _ => {
                return Err(OxenError::basic_str("Error: Unexpected node type"));
            }
        };

        let staged_node = StagedMerkleTreeNode {
            status: StagedEntryStatus::Removed,
            node: corrected_node,
        };

        staged_nodes.insert(path, staged_node);
    }

    log::debug!("staged_nodes: {}", staged_nodes.len());

    // Stage the root dir's parents
    let mut parent_path = root_path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        parent_path = parent.to_path_buf();

        match staged_db_manager.add_directory(&parent_path, seen_dirs) {
            Ok(_) => {}
            Err(e) => {
                log::debug!("Error adding parent dirs: {e:?}");
                return Err(e);
            }
        }

        if parent_path == Path::new("") {
            break;
        }
    }

    // Write all files to staged db
    match staged_db_manager.upsert_staged_nodes(&staged_nodes) {
        Ok(_) => {
            log::debug!("Successfully upserted staged nodes");
            Ok(())
        }
        Err(e) => {
            log::error!("Failed to upsert staged nodes due to error: {e:?}");
            Err(e)
        }
    }
}

// Stages the file_node as removed, and all its parents in the repo as modified
fn process_remove_file_and_parents(
    repo: &LocalRepository,
    path: &Path,
    staged_db: &DB,
    file_node: &FileNode,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let repo_path = repo.path.clone();
    let mut update_node = file_node.clone();
    update_node.set_name(path.to_string_lossy().to_string().as_str());
    log::debug!("Update node is: {update_node:?}");
    let node = MerkleTreeNode::from_file(update_node);

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node,
    };

    log::debug!("Staged entry is: {staged_entry}");

    // Write removed node to staged db
    log::debug!("writing removed file to staged db: {staged_entry}");
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    let node_path = path.to_str().unwrap();
    staged_db.put(node_path, &buf).unwrap();

    // Add all the parent dirs to the staged db
    let mut parent_path = path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path.clone())?;
        parent_path = parent.to_path_buf();

        let relative_path_str = relative_path.to_str().unwrap();

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Modified,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing dir to staged db: {dir_entry}");
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    Ok(Some(staged_entry))
}

/// The paths an `oxen rm` removes: those HEAD has, and under `--staged` those only staged.
struct Removals {
    head_commit: Option<Commit>,
    dir_hashes: HashMap<PathBuf, MerkleHash>,
    committed: Vec<(PathBuf, MerkleTreeNode)>,
    uncommitted: HashSet<PathBuf>,
}

impl Removals {
    fn paths(&self) -> HashSet<PathBuf> {
        let committed = self.committed.iter().map(|(path, _)| path.clone());
        committed.chain(self.uncommitted.iter().cloned()).collect()
    }
}

/// Looks every path up before anything changes, so a refused path leaves the others untouched.
fn look_up_removals(
    targets: &HashSet<PathBuf>,
    given: &HashSet<PathBuf>,
    glob_matches: &[(PathBuf, HashSet<PathBuf>)],
    repo: &LocalRepository,
    opts: &RmOpts,
    staged_db: &DB,
) -> Result<Removals, OxenError> {
    let (head_commit, dir_hashes) = match repositories::commits::head_commit_maybe(repo)? {
        Some(head_commit) => {
            let dir_hashes = CommitMerkleTree::dir_hashes(repo, &head_commit)?;
            (Some(head_commit), dir_hashes)
        }
        None if opts.staged => (None, HashMap::new()),
        None => return Err(OxenError::basic_str("Error: head commit not found")),
    };

    let mut skipped = HashSet::new();
    let mut parent_nodes = HashMap::new();
    let mut committed = Vec::with_capacity(targets.len());
    let mut uncommitted = HashSet::new();
    for path in targets {
        let relative_path = util::fs::path_relative_to_dir(path, &repo.path)?;

        let parent_path = relative_path.parent().unwrap_or(Path::new(""));
        let parent_node = match parent_nodes.entry(parent_path.to_path_buf()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(match &head_commit {
                Some(head_commit) => CommitMerkleTree::dir_with_children(
                    repo,
                    head_commit,
                    parent_path,
                    Some(&dir_hashes),
                )?,
                None => None,
            }),
        };

        // Get file name without parent paths for lookup in Merkle Tree
        let name = util::fs::path_relative_to_dir(relative_path.clone(), parent_path)?;

        // Lookup node in Merkle Tree
        let node = match parent_node {
            Some(parent_node) => parent_node.get_by_path(name)?,
            None => None,
        };

        match node {
            Some(node) => {
                if node.is_dir() && !opts.recursive {
                    let error = format!("`oxen rm` on directory {relative_path:?} requires -r");
                    return Err(OxenError::basic_str(error));
                }
                // A path whose removal is staged is untracked
                if is_removal_staged(&relative_path, &node, staged_db)? {
                    // A glob passes over its matches whose removal is staged and removes the rest
                    if !given.contains(path) {
                        skipped.insert(path.clone());
                        continue;
                    }
                    return Err(OxenError::RemovalAlreadyStaged(relative_path.into()));
                }
                committed.push((relative_path, node));
            }
            // `--staged` on a path HEAD lacks drops whatever is staged for it
            None if opts.staged => {
                uncommitted.insert(path.clone());
            }
            None if parent_node.is_none() => {
                let error = format!("Error: parent dir not found in tree for {relative_path:?}");
                return Err(OxenError::basic_str(error));
            }
            None => {
                let error =
                    format!("Error: {relative_path:?} must be committed in order to use `oxen rm`");
                return Err(OxenError::basic_str(error));
            }
        }
    }
    // A glob with nothing left to remove fails, as git fails a pathspec that matches no file
    for (glob, matches) in glob_matches {
        if !matches.is_empty() && matches.is_subset(&skipped) {
            return Err(OxenError::RemovalAlreadyStaged(glob.clone().into()));
        }
    }

    Ok(Removals {
        head_commit,
        dir_hashes,
        committed,
        uncommitted,
    })
}

fn apply_removals(
    repo: &LocalRepository,
    removals: Removals,
    opts: &RmOpts,
    staged_db: &DB,
) -> Result<CumulativeStats, OxenError> {
    let mut total = CumulativeStats::default();
    let uncommitted = &removals.uncommitted;
    if !uncommitted.is_empty() && opts.recursive {
        remove_staged_recursively_inner(repo, uncommitted, staged_db)?;
    } else if !uncommitted.is_empty() {
        remove_staged_inner(repo, uncommitted, opts, staged_db)?;
    }

    // Every committed path has a HEAD to have been found in
    if let Some(head_commit) = &removals.head_commit {
        total += stage_committed_removals(
            repo,
            head_commit,
            &removals.dir_hashes,
            removals.committed,
            opts.staged,
            staged_db,
        )?;
    }
    Ok(total)
}

/// Stages the removal of each committed path, given with its HEAD node, and deletes its working
/// copy unless `keep_working_copy`.
fn stage_committed_removals(
    repo: &LocalRepository,
    head_commit: &Commit,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    committed: Vec<(PathBuf, MerkleTreeNode)>,
    keep_working_copy: bool,
    staged_db: &DB,
) -> Result<CumulativeStats, OxenError> {
    let mut total = CumulativeStats::default();
    for (path, node) in committed {
        match &node.node {
            EMerkleTreeNode::Directory(_) => {
                let removal =
                    remove_dir_inner(repo, head_commit, &path, Some(dir_hashes), staged_db)?;
                if !keep_working_copy {
                    removal.delete_from_working_copy(repo)?;
                }
                total += removal.stats;
                // TODO: Currently, there's no way to avoid re-staging the parent dirs with glob
                // paths. Potentially, we can could a mutex global to all paths?
            }
            EMerkleTreeNode::File(file_node) => {
                total += remove_file_inner(repo, &path, file_node, staged_db)?;
                let full_path = repo.path.join(&path);
                // user might have removed file manually before using `oxen rm`
                if !keep_working_copy && full_path.exists() {
                    log::debug!("Removing file: {full_path:?}");
                    util::fs::remove_file(&full_path)?;
                }
            }
            _ => return Err(OxenError::basic_str("Error: Unexpected file type")),
        }
    }
    Ok(total)
}

/// A directory whose removal is staged, with what HEAD has under it and what its removal unstaged.
struct DirRemoval {
    path: PathBuf,
    stats: CumulativeStats,
    head: MerkleTreeNode,
    unstaged: Vec<(PathBuf, StagedMerkleTreeNode)>,
}

impl DirRemoval {
    /// Deletes the directory's tracked files from the working copy, then each tracked directory
    /// that leaves empty. Untracked files, and the directories holding them, stay.
    fn delete_from_working_copy(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        // What was staged is tracked, except a path whose removal was staged, which is untracked
        let mut untracked = HashSet::new();
        let mut files = Vec::new();
        let mut dirs = Vec::new();
        for (path, entry) in &self.unstaged {
            if entry.status == StagedEntryStatus::Removed {
                untracked.insert(path.clone());
            } else if entry.node.is_dir() {
                dirs.push(path.clone());
            } else {
                files.push(path.clone());
            }
        }
        let head_files = self.head.list_file_paths()?.into_iter();
        let head_files = head_files.map(|file| self.path.join(file));
        files.extend(head_files.filter(|file| !untracked.contains(file)));
        let head_dirs = self.head.list_dir_paths()?.into_iter();
        let head_dirs = head_dirs.map(|dir| self.path.join(dir));
        dirs.extend(head_dirs.filter(|dir| !untracked.contains(dir)));

        // Oxen does not track symlinks, so nothing at or behind a symlinked directory is deleted
        let is_symlink = |path: &Path| {
            fs::symlink_metadata(repo.path.join(path)).is_ok_and(|meta| meta.is_symlink())
        };
        let candidates = self
            .path
            .ancestors()
            .chain(dirs.iter().map(PathBuf::as_path));
        let links: Vec<PathBuf> = candidates
            .filter(|path| !path.as_os_str().is_empty() && is_symlink(path))
            .map(Path::to_path_buf)
            .collect();
        let behind_link = |path: &PathBuf| links.iter().any(|link| path.starts_with(link));
        files.retain(|file| !behind_link(file));
        dirs.retain(|dir| !behind_link(dir));

        for file in files {
            let full_path = repo.path.join(file);
            // A file deleted by hand before `oxen rm` is already gone
            if let Err(err) = fs::remove_file(&full_path)
                && err.kind() != ErrorKind::NotFound
            {
                return Err(OxenError::file_error(&full_path, err));
            }
        }
        // A directory sorts after its parent, so the reverse order empties children first
        dirs.sort_unstable();
        for dir in dirs.iter().rev().map(|dir| repo.path.join(dir)) {
            if dir == repo.path {
                continue;
            }
            if let Err(err) = fs::remove_dir(&dir)
                && !matches!(
                    err.kind(),
                    ErrorKind::DirectoryNotEmpty | ErrorKind::NotFound
                )
            {
                return Err(OxenError::file_error(&dir, err));
            }
        }
        Ok(())
    }
}

pub fn process_remove_file(
    path: &Path,
    file_node: &FileNode,
    staged_db: &DB,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let mut update_node = file_node.clone();
    update_node.set_name(&path.to_string_lossy());

    let node = MerkleTreeNode::from_file(update_node);

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node,
    };

    // Write removed node to staged db
    log::debug!("writing removed file to staged db: {staged_entry}");
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    let relative_path_str = path.to_str().unwrap();
    staged_db.put(relative_path_str, &buf).unwrap();

    Ok(Some(staged_entry))
}

fn remove_dir_inner(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
    dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
    staged_db: &DB,
) -> Result<DirRemoval, OxenError> {
    let dir_node =
        match CommitMerkleTree::dir_with_children_recursive(repo, commit, path, dir_hashes)? {
            Some(node) => node,
            None => {
                let error = format!("Error: {path:?} must be committed in order to use `oxen rm`");
                return Err(OxenError::basic_str(error));
            }
        };

    // Drop everything staged under the directory, so files added since HEAD unstage
    let unstaged = staged_entries_under(path, staged_db)?;
    let mut batch = WriteBatch::default();
    for (entry_path, _) in &unstaged {
        batch.delete(entry_path.to_string_lossy().as_bytes());
    }
    staged_db.write(batch)?;

    Ok(DirRemoval {
        path: path.to_path_buf(),
        stats: process_remove_dir(repo, path, &dir_node, staged_db)?,
        head: dir_node,
        unstaged,
    })
}

pub fn remove_dir(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<CumulativeStats, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DB = DB::open(&opts, dunce::simplified(&db_path))?;

    Ok(remove_dir_inner(repo, commit, path, None, &staged_db)?.stats)
}

// Stage dir and all its children for removal
fn process_remove_dir(
    repo: &LocalRepository,
    path: &Path,
    dir_node: &MerkleTreeNode,
    staged_db: &DB,
) -> Result<CumulativeStats, OxenError> {
    log::debug!("Process Remove Dir");

    let progress_1 = Arc::new(ProgressBar::new_spinner());
    progress_1.set_style(ProgressStyle::default_spinner());
    progress_1.enable_steady_tick(Duration::from_millis(100));

    // root_path is the path of the directory rm was called on
    let repo = repo.clone();
    let repo_path = repo.path.clone();

    let progress_1_clone = Arc::clone(&progress_1);

    // recursive helper function
    log::debug!("Begin r_process_remove_dir");
    let cumulative_stats = r_process_remove_dir(&repo, path, dir_node, staged_db);

    // Add all the parent dirs to the staged db
    let mut parent_path = path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path.clone())?;
        parent_path = parent.to_path_buf();

        let Some(relative_path_str) = relative_path.to_str() else {
            let error = format!("Error: {relative_path:?} is not a valid string");
            return Err(OxenError::basic_str(error));
        };

        // Ensures that removed entries don't have their parents re-added by oxen rm
        // RocksDB's DBWithThreadMode only has this function to check if a key exists in the DB,
        // so I added the else condition to make this reliable

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Modified,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing dir to staged db: {dir_entry}");
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    progress_1_clone.finish_and_clear();

    cumulative_stats
}

// Recursively remove all files and directories starting from a particular directory

// TODO: Refactor to singular match statement/loop
// TODO: Currently, this function is only called sequentially. Consider using Arc/AtomicU64 to parallelize
fn r_process_remove_dir(
    _repo: &LocalRepository,
    path: &Path,
    node: &MerkleTreeNode,
    staged_db: &DB,
) -> Result<CumulativeStats, OxenError> {
    let mut total = CumulativeStats::default();

    // Iterate through children, removing files
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::Directory(dir_node) => {
                log::debug!("Recursive process_remove_dir found dir: {dir_node}");
                // Update path, and move to the next level of recurstion
                let new_path = path.join(dir_node.name());
                total += r_process_remove_dir(_repo, &new_path, child, staged_db)?;
            }
            EMerkleTreeNode::VNode(_) => {
                log::debug!("Recursive process_remove_dir found vnode");
                // Move to the next level of recursion
                total += r_process_remove_dir(_repo, path, child, staged_db)?;
            }
            EMerkleTreeNode::File(file_node) => {
                log::debug!("Recursive process_remove_dir found file: {file_node}");
                // Add the relative path of the dir to the path
                let new_path = path.join(file_node.name());

                // Remove the file node and add its stats to the totals
                match process_remove_file(&new_path, file_node, staged_db) {
                    Ok(Some(node)) => {
                        if let EMerkleTreeNode::File(file_node) = &node.node.node {
                            total.total_bytes += file_node.num_bytes();
                            total.total_files += 1;
                            total
                                .data_type_counts
                                .entry(file_node.data_type().clone())
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                    }
                    Err(e) => {
                        let error = format!("Error adding file {new_path:?}: {e:?}");
                        return Err(OxenError::basic_str(error));
                    }
                    _ => {
                        let error = format!("Error adding file {new_path:?}");
                        return Err(OxenError::basic_str(error));
                    }
                }
            }
            _ => {
                let error = "Error: Unexpected node type".to_string();
                return Err(OxenError::basic_str(error));
            }
        }
    }

    match &node.node {
        // if node is a Directory, stage it for removal
        EMerkleTreeNode::Directory(_) => {
            // Update the dir node name to the full relative path so that
            // split_into_vnodes and cleanup_rm_dirs can match it against
            // existing children (which use full relative paths).
            let updated_node = {
                let mut updated_node = node.clone();
                if let EMerkleTreeNode::Directory(ref mut dir_node) = updated_node.node {
                    dir_node.set_name(path.to_str().unwrap());
                }
                updated_node
            };
            let staged_entry = StagedMerkleTreeNode {
                status: StagedEntryStatus::Removed,
                node: updated_node,
            };

            // Write removed node to staged db
            log::debug!("writing removed dir to staged db: {staged_entry}");
            let mut buf = Vec::new();
            staged_entry
                .serialize(&mut Serializer::new(&mut buf))
                .unwrap();

            let relative_path_str = path.to_str().unwrap();
            staged_db.put(relative_path_str, &buf).unwrap();
        }

        // if node is a VNode, do nothing
        EMerkleTreeNode::VNode(_) => {}

        // node should always be a directory or vnode, so any other types result in an error
        _ => {
            return Err(OxenError::basic_str(format!(
                "Unexpected node type: {:?}",
                node.node.node_type()
            )));
        }
    }

    Ok(total)
}
