use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;

use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, SingleThreaded};
use std::path::PathBuf;
use std::str;
use std::time::Duration;
use std::time::Instant;
use time::OffsetDateTime;

use crate::config::UserConfig;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::constants::MERGE_HEAD_FILE;
use crate::constants::ORIG_HEAD_FILE;
use crate::constants::{HEAD_FILE, STAGED_DIR};
use crate::core::db;
use crate::core::db::key_val::str_val_db;
use crate::core::refs::with_ref_manager;
use crate::core::v_latest::index::CommitMerkleTree;
use crate::core::v_latest::status;
use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::model::MerkleTreeNodeType;
use crate::model::NewCommit;
use crate::model::NewCommitBody;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;
use crate::model::merkle_tree::node::VNode;
use crate::model::merkle_tree::node::commit_node::CommitNodeOpts;
use crate::model::merkle_tree::node::dir_node::DirNodeOpts;
use crate::model::merkle_tree::node::vnode::VNodeOpts;
use crate::model::{Commit, LocalRepository, StagedEntryStatus};
use crate::repositories::merkle_tree::merkle_writer::{MerkleWriteSession, NodeWriteSession};

use crate::util::hasher;
use crate::util::progress_bar::FinishOnDropProgressBar;
use crate::{repositories, util};

use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::merkle_tree::node::{CommitNode, DirNode};

#[derive(Clone)]
pub struct EntryVNode {
    pub id: MerkleHash,
    pub entries: Vec<StagedMerkleTreeNode>,
    pub removed_entries: Vec<StagedMerkleTreeNode>,
}

impl std::fmt::Debug for EntryVNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EntryVNode {{ id: {:?}, entries: {} }}",
            self.id,
            self.entries
                .iter()
                .map(|e| e.node.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl EntryVNode {
    pub fn new(id: MerkleHash) -> Self {
        EntryVNode {
            id,
            entries: vec![],
            removed_entries: vec![],
        }
    }
}

pub fn commit(repo: &LocalRepository, message: impl AsRef<str>) -> Result<Commit, OxenError> {
    let cfg = UserConfig::get()?;
    commit_with_cfg(repo, message, &cfg, None, &default_commit_progress_bar())
}

/// A default style spinner with a 100ms tick rate. Callers must set the message.
/// NOTE: ProgressBar uses interior mutibility.
pub(crate) fn default_commit_progress_bar() -> FinishOnDropProgressBar {
    let commit_progress_bar = ProgressBar::new_spinner();
    commit_progress_bar.set_style(ProgressStyle::default_spinner());
    commit_progress_bar.enable_steady_tick(Duration::from_millis(100));
    FinishOnDropProgressBar(commit_progress_bar)
}

pub(crate) fn commit_with_cfg(
    repo: &LocalRepository,
    message: impl AsRef<str>,
    cfg: &UserConfig,
    parent_ids: Option<Vec<String>>,
    commit_progress_bar: &ProgressBar,
) -> Result<Commit, OxenError> {
    // time the commit
    let start_time = Instant::now();
    let message = message.as_ref();

    // Read the staged files from the staged db
    let opts = db::key_val::opts::default();
    let staged_db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    log::debug!("commit_with_cfg staged db path: {staged_db_path:?}");
    let staged_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&staged_db_path))?;

    // Read all the staged entries
    let (dir_entries, total_changes) =
        status::read_staged_entries(repo, &staged_db, commit_progress_bar)?;
    commit_progress_bar.set_message(format!("Committing {total_changes} changes"));

    log::debug!("got dir entries: {:?}", dir_entries.len());

    if dir_entries.is_empty() {
        return Err(OxenError::NoChanges);
    }

    // let mut dir_tree = entries_to_dir_tree(&dir_entries)?;
    // dir_tree.print();

    // println!("🫧======================🫧");

    let new_commit = NewCommitBody {
        message: message.to_string(),
        author: cfg.name.clone(),
        email: cfg.email.clone(),
    };
    let branch = repositories::branches::current_branch(repo)?;
    let maybe_branch_name = branch.map(|b| b.name);
    let commit = if let Some(parent_ids) = parent_ids {
        log::debug!("parent ids: {parent_ids:?}");
        commit_dir_entries_with_parents(
            repo,
            parent_ids,
            dir_entries,
            &new_commit,
            maybe_branch_name
                .clone()
                .unwrap_or(DEFAULT_BRANCH_NAME.to_string()),
        )?
    } else {
        log::debug!("no parent ids, committing new");
        commit_dir_entries_new(repo, dir_entries, &new_commit)?
    };

    // Clear the staged db
    let staged_db_path = staged_db.path().to_owned();
    drop(staged_db);
    util::fs::remove_dir_all(staged_db_path)?;

    // Write HEAD file and update branch
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);
    log::debug!("Looking for HEAD file at {head_path:?}");

    let commit_id = commit.id.to_owned();
    let branch_name = maybe_branch_name.unwrap_or(DEFAULT_BRANCH_NAME.to_string());
    let head_path_exists = head_path.exists();

    with_ref_manager(repo, |manager| {
        if !head_path_exists {
            log::debug!("HEAD file does not exist, creating new branch");
            manager.set_head(&branch_name)?;
            manager.set_branch_commit_id(&branch_name, &commit_id)?;
        }
        manager.set_head_commit_id(&commit_id)
    })?;

    // Print that we finished
    println!(
        "🐂 commit {} in {}",
        commit,
        humantime::format_duration(Duration::from_millis(
            start_time.elapsed().as_millis() as u64
        ))
    );

    Ok(commit)
}

pub(crate) fn commit_dir_entries_with_parents(
    repo: &LocalRepository,
    parent_commits: Vec<String>,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    new_commit: &NewCommitBody,
    target_branch: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let message = &new_commit.message;
    let target_branch = target_branch.as_ref();

    // if the HEAD file exists, we have parents
    // otherwise this is the first commit
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);

    let maybe_head_commit = if head_path.exists() {
        repositories::revisions::get(repo, target_branch)?
    } else {
        None
    };

    let directories = dir_entries
        .keys()
        .map(|path| path.to_path_buf())
        .collect::<Vec<_>>();

    log::debug!("collecting existing nodes for directories: {directories:?}");

    let mut existing_nodes: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
    if let Some(commit) = &maybe_head_commit {
        existing_nodes = repositories::tree::list_nodes_from_paths(repo, commit, &directories)?;
    }

    log::debug!(
        "existing nodes (count: {}) {:?}",
        existing_nodes.len(),
        existing_nodes.keys()
    );

    // Sort children and split into VNodes
    let vnode_entries = split_into_vnodes(repo, &dir_entries, &existing_nodes, new_commit)?;

    let timestamp = OffsetDateTime::now_utc();

    let new_commit = create_commit_data(repo, message, timestamp, parent_commits, new_commit)?;

    // Compute the commit hash
    let commit_id = compute_commit_id(&new_commit)?;

    let mut parent_hashes = Vec::new();
    for parent_id in &new_commit.parent_ids {
        if let Some(parent_commit) = repositories::commits::get_by_id(repo, parent_id)? {
            let node = CommitMerkleTree::from_commit(repo, &parent_commit)?;
            parent_hashes.push(node.root.hash);
        }
    }

    let node = CommitNode::new(
        repo,
        CommitNodeOpts {
            hash: commit_id,
            parent_ids: parent_hashes,
            email: new_commit.email.clone(),
            author: new_commit.author.clone(),
            message: message.to_string(),
            timestamp,
        },
    )?;

    let opts = db::key_val::opts::default();
    let commit_id_string = format!("{commit_id}").to_string();
    let dir_hash_db_path =
        CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, &commit_id_string);
    let dir_hash_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;

    let (dir_hashes, parent_id) = match &maybe_head_commit {
        Some(commit) => (
            CommitMerkleTree::dir_hashes(repo, commit)?,
            Some(commit.hash()?),
        ),
        None => (HashMap::new(), None),
    };

    for (path, hash) in &dir_hashes {
        if let Some(path_str) = path.to_str() {
            str_val_db::put(&dir_hash_db, path_str, &hash.to_string())?;
        } else {
            log::error!("Failed to convert path to string: {path:?}");
        }
    }

    let store = repo.merkle_store()?;
    let session = store.begin()?;
    let mut commit_ns = session.create_node(&node, parent_id)?;
    write_commit_entries(
        repo,
        commit_id,
        &*session,
        &mut *commit_ns,
        &dir_hash_db,
        &dir_hashes,
        &vnode_entries,
    )?;
    commit_ns.finish()?;
    session.finish()?;

    Ok(node.to_commit())
}

pub fn commit_dir_entries_new(
    repo: &LocalRepository,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    new_commit: &NewCommitBody,
) -> Result<Commit, OxenError> {
    let message = &new_commit.message;
    // if the HEAD commit exists, we have parents
    // otherwise this is the first commit
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    let mut parent_ids = vec![];
    if let Some(parent) = &maybe_head_commit {
        parent_ids.push(parent.hash()?);
    }

    let directories = dir_entries
        .keys()
        .map(|path| path.to_path_buf())
        .collect::<Vec<_>>();
    log::debug!("new commit directories: {directories:?}");

    let mut existing_nodes: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
    if let Some(commit) = &maybe_head_commit {
        existing_nodes = repositories::tree::list_nodes_from_paths(repo, commit, &directories)?;
    }

    // Sort children and split into VNodes
    let vnode_entries = split_into_vnodes(repo, &dir_entries, &existing_nodes, new_commit)?;

    // Compute the commit hash
    let timestamp = OffsetDateTime::now_utc();
    let new_commit = create_commit_data(
        repo,
        message,
        timestamp,
        parent_ids.iter().map(|id| id.to_string()).collect(),
        new_commit,
    )?;

    let commit_id = compute_commit_id(&new_commit)?;

    let node = CommitNode::new(
        repo,
        CommitNodeOpts {
            hash: commit_id,
            parent_ids: new_commit
                .parent_ids
                .iter()
                .map(|id: &String| id.parse().unwrap())
                .collect(),
            email: new_commit.email.clone(),
            author: new_commit.author.clone(),
            message: message.to_string(),
            timestamp,
        },
    )?;

    let opts = db::key_val::opts::default();
    let commit_id_string = format!("{commit_id}").to_string();
    let dir_hash_db_path =
        CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, &commit_id_string);
    let dir_hash_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;

    let (dir_hashes, parent_id) = match &maybe_head_commit {
        Some(commit) => (
            CommitMerkleTree::dir_hashes(repo, commit)?,
            Some(commit.hash()?),
        ),
        None => (HashMap::new(), None),
    };

    for (path, hash) in &dir_hashes {
        if let Some(path_str) = path.to_str() {
            str_val_db::put(&dir_hash_db, path_str, &hash.to_string())?;
        } else {
            log::error!("Failed to convert path to string: {path:?}");
        }
    }

    let store = repo.merkle_store()?;
    let session = store.begin()?;
    let mut commit_ns = session.create_node(&node, parent_id)?;

    write_commit_entries(
        repo,
        commit_id,
        &*session,
        &mut *commit_ns,
        &dir_hash_db,
        &dir_hashes,
        &vnode_entries,
    )?;
    commit_ns.finish()?;
    session.finish()?;

    // Remove all the directories that are staged for removal
    cache_invalidate_dir_hash_db(&dir_hash_db, dir_entries.values())?;

    Ok(node.to_commit())
}

pub fn commit_dir_entries(
    repo: &LocalRepository,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    new_commit: &NewCommitBody,
    target_branch: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    log::debug!("commit_dir_entries got {} entries", dir_entries.len());
    if log::max_level() == log::Level::Debug {
        for (path, entries) in &dir_entries {
            log::debug!(
                "commit_dir_entries entry {:?} with {} nodes",
                path,
                entries.len()
            );
        }
    }

    if dir_entries.is_empty() {
        return Err(OxenError::basic_str("No changes to commit"));
    }

    let message = &new_commit.message;
    // if the HEAD file exists, we have parents
    // otherwise this is the first commit
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);

    let maybe_head_commit = if head_path.exists() {
        repositories::revisions::get(repo, target_branch)?
    } else {
        None
    };

    let mut parent_ids = vec![];
    if let Some(parent) = &maybe_head_commit {
        parent_ids.push(parent.hash()?);
    }

    let directories = dir_entries
        .keys()
        .map(|path| path.to_path_buf())
        .collect::<Vec<_>>();
    log::debug!("commit_dir_entries directories: {directories:?}");

    let mut existing_nodes: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
    if let Some(commit) = &maybe_head_commit {
        existing_nodes = repositories::tree::list_nodes_from_paths(repo, commit, &directories)?;
    }

    // Sort children and split into VNodes
    let vnode_entries = split_into_vnodes(repo, &dir_entries, &existing_nodes, new_commit)?;

    // Compute the commit hash
    let timestamp = OffsetDateTime::now_utc();
    let new_commit = NewCommit {
        parent_ids: parent_ids.iter().map(|id| id.to_string()).collect(),
        message: message.to_string(),
        author: new_commit.author.clone(),
        email: new_commit.email.clone(),
        timestamp,
    };
    let commit_id = compute_commit_id(&new_commit)?;

    let node = CommitNode::new(
        repo,
        CommitNodeOpts {
            hash: commit_id,
            parent_ids,
            email: new_commit.email.clone(),
            author: new_commit.author.clone(),
            message: message.to_string(),
            timestamp,
        },
    )?;

    let opts = db::key_val::opts::default();
    let commit_id_string = format!("{commit_id}").to_string();
    let dir_hash_db_path =
        CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, &commit_id_string);
    let dir_hash_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;

    let dir_hashes = match &maybe_head_commit {
        Some(commit) => CommitMerkleTree::dir_hashes(repo, commit)?,
        None => HashMap::new(),
    };

    for (path, hash) in &dir_hashes {
        if let Some(path_str) = path.to_str() {
            str_val_db::put(&dir_hash_db, path_str, &hash.to_owned().to_string())?;
        } else {
            log::error!("Failed to convert path to string: {path:?}");
        }
    }

    let store = repo.merkle_store()?;
    let session = store.begin()?;
    let mut commit_ns = session.create_node(&node, None)?;
    write_commit_entries(
        repo,
        commit_id,
        &*session,
        &mut *commit_ns,
        &dir_hash_db,
        &dir_hashes,
        &vnode_entries,
    )?;
    commit_ns.finish()?;
    session.finish()?;

    // Remove all the directories that are staged for removal
    cache_invalidate_dir_hash_db(&dir_hash_db, dir_entries.values())?;

    Ok(node.to_commit())
}

fn node_data_to_staged_node(
    base_dir: impl AsRef<Path>,
    node: &MerkleTreeNode,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let base_dir = base_dir.as_ref();
    match node.node.node_type() {
        MerkleTreeNodeType::Dir => {
            let mut dir_node = node.dir()?;
            let path = base_dir.join(dir_node.name());
            dir_node.set_name(path.to_str().unwrap());
            Ok(Some(StagedMerkleTreeNode {
                status: StagedEntryStatus::Unmodified,
                node: MerkleTreeNode::from_dir(dir_node),
            }))
        }
        MerkleTreeNodeType::File => {
            let mut file_node = node.file()?;
            let path = base_dir.join(file_node.name());
            file_node.set_name(path.to_str().unwrap());
            Ok(Some(StagedMerkleTreeNode {
                status: StagedEntryStatus::Unmodified,
                node: MerkleTreeNode::from_file(file_node),
            }))
        }
        _ => Ok(None),
    }
}

fn get_node_dir_children(
    base_dir: impl AsRef<Path>,
    node: &MerkleTreeNode,
) -> Result<HashSet<StagedMerkleTreeNode>, OxenError> {
    let dir_children = repositories::tree::list_files_and_folders(node)?;
    let children = dir_children
        .into_iter()
        .flat_map(|child| node_data_to_staged_node(&base_dir, &child))
        .flatten()
        .collect();

    Ok(children)
}

// This should return the directory to vnode mapping that we need to update
// It also maps directories to removed files to update their metadata
#[allow(clippy::type_complexity)]
fn split_into_vnodes(
    repo: &LocalRepository,
    entries: &HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    existing_nodes: &HashMap<PathBuf, MerkleTreeNode>,
    new_commit: &NewCommitBody,
) -> Result<HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)>, OxenError> {
    let mut results: HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)> =
        HashMap::new();

    if log::max_level() == log::Level::Debug {
        log::debug!("split_into_vnodes new_commit: {:?}", new_commit.message);
        log::debug!("split_into_vnodes entries keys: {:?}", entries.keys());
        log::debug!(
            "split_into_vnodes existing_nodes keys: {:?}",
            existing_nodes.keys()
        );
    }

    // Create the VNode buckets per directory
    for (directory, new_children) in entries {
        let mut children = HashSet::new();
        let mut removed_children = HashSet::new();

        // Lookup children in the existing merkle tree
        if let Some(existing_node) = existing_nodes.get(directory) {
            log::debug!("got existing node for {directory:?}");
            children = get_node_dir_children(directory, existing_node)?;
            log::debug!(
                "got {} existing children for dir {:?}",
                children.len(),
                directory
            );
        } else {
            log::debug!("no existing node for {directory:?}");
        };

        log::debug!("new_children {}", new_children.len());

        // Update the children with the new entries from status
        for child in new_children.iter() {
            log::debug!(
                "new_child {:?} {:?}",
                child.node.node.node_type(),
                child.node.maybe_path()
            );

            // Overwrite the existing child
            // if add or modify, replace the child
            // if remove, remove the child
            if let Ok(child_path) = child.node.maybe_path()
                && child_path != Path::new("")
            {
                // Defensive normalization: staged entries should already carry full
                // repo-relative paths (set during rm staging), but if a leaf-only
                // name slips through, reconstruct the full path so that HashSet
                // lookups (which compare via maybe_path) match existing children
                // from `node_data_to_staged_node`.
                let needs_prefix =
                    !directory.as_os_str().is_empty() && !child_path.starts_with(directory);
                let child = if needs_prefix {
                    let full_path = directory.join(&child_path);
                    let mut prefixed = child.clone();
                    match &mut prefixed.node.node {
                        EMerkleTreeNode::Directory(dn) => {
                            dn.set_name(full_path.to_str().unwrap());
                        }
                        EMerkleTreeNode::File(fn_) => {
                            fn_.set_name(full_path.to_str().unwrap());
                        }
                        _ => {}
                    }
                    prefixed
                } else {
                    child.clone()
                };

                match child.status {
                    StagedEntryStatus::Removed => {
                        log::debug!(
                            "removing child {:?} {:?} (was {:?})",
                            child.node.node.node_type(),
                            child.node.maybe_path().unwrap(),
                            child_path
                        );
                        children.remove(&child);
                        removed_children.insert(child);
                    }
                    _ => {
                        log::debug!(
                            "replacing child {:?} {:?} (was {:?})",
                            child.node.node.node_type(),
                            child.node.maybe_path().unwrap(),
                            child_path
                        );
                        log::debug!("replaced child {}", child.node);
                        children.replace(child);
                    }
                }
            }
        }

        // Log the children
        if log::max_level() == log::Level::Debug {
            for child in children.iter() {
                log::debug!(
                    "child populated {:?} {:?} status {:?}",
                    child.node.node.node_type(),
                    child.node.maybe_path().unwrap(),
                    child.status
                );
            }
        }

        // Compute number of vnodes based on the repo's vnode size and number of children
        let total_children = children.len();
        let vnode_size = repo.vnode_size();
        let num_vnodes = (total_children as f32 / vnode_size as f32).ceil() as u128;
        // Antoher way to do it would be log2(N / 10000) if we wanted it to scale more logarithmically
        // let num_vnodes = (total_children as f32 / 10000_f32).log2();
        // let num_vnodes = 2u128.pow(num_vnodes.ceil() as u32);
        log::debug!(
            "{num_vnodes} VNodes for {total_children} children in {directory:?} with vnode size {vnode_size}"
        );
        let mut vnode_children: Vec<EntryVNode> =
            vec![EntryVNode::new(MerkleHash::new(0)); num_vnodes as usize];

        // Split entries into vnodes
        for child in children.into_iter() {
            // let bucket = child.node.hash.to_u128() % num_vnodes;
            let bucket = hasher::hash_buffer_128bit(
                child
                    .node
                    .maybe_path()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .as_bytes(),
            ) % num_vnodes;
            vnode_children[bucket as usize].entries.push(child.clone());
        }

        // Compute hashes and sort entries
        for vnode in vnode_children.iter_mut() {
            // Sort the entries in the vnode by path
            // to make searching for entries faster
            vnode.entries.sort_by(|a, b| {
                a.node
                    .maybe_path()
                    .unwrap()
                    .cmp(&b.node.maybe_path().unwrap())
            });

            // Compute hash for the vnode
            let mut vnode_hasher = xxhash_rust::xxh3::Xxh3::new();
            vnode_hasher.update(b"vnode");
            // add the dir name to the vnode hash
            vnode_hasher.update(directory.to_str().unwrap().as_bytes());

            let mut has_new_entries = false;
            for entry in vnode.entries.iter() {
                if let EMerkleTreeNode::File(file_node) = &entry.node.node {
                    vnode_hasher.update(&file_node.combined_hash().to_le_bytes());
                } else {
                    vnode_hasher.update(&entry.node.hash.to_le_bytes());
                }
                if entry.status != StagedEntryStatus::Unmodified {
                    has_new_entries = true;
                }
            }

            // If the vnode has new entries, we need to update the uuid to make a new vnode
            if existing_nodes.contains_key(directory) && has_new_entries {
                let uuid = uuid::Uuid::new_v4();
                vnode_hasher.update(uuid.as_bytes());
            }

            vnode.id = MerkleHash::new(vnode_hasher.digest128());
        }

        // Sort before we hash
        let removed_children = removed_children.iter().cloned().collect();
        results.insert(directory.to_owned(), (vnode_children, removed_children));
    }

    // Make sure to update all the vnode ids based on all their children

    // TODO: We have to start from the bottom vnodes in the tree and update all the vnode ids above it
    log::debug!(
        "split_into_vnodes results: {:?} for commit {}",
        results.len(),
        new_commit.message
    );
    if log::max_level() == log::Level::Debug {
        for (dir, (vnodes, _)) in results.iter_mut() {
            log::debug!("dir {:?} has {} vnodes", dir, vnodes.len());
            for vnode in vnodes.iter_mut() {
                log::debug!("  vnode {} has {} entries", vnode.id, vnode.entries.len());
                for entry in vnode.entries.iter() {
                    log::debug!(
                        "    entry {:?} [{}] `{:?}` with status {:?}",
                        entry.node.node.node_type(),
                        entry.node.node.hash(),
                        entry.node.maybe_path(),
                        entry.status
                    );
                }
            }
        }
    }

    Ok(results)
}

pub fn compute_commit_id(new_commit: &NewCommit) -> Result<MerkleHash, OxenError> {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"commit");
    hasher.update(format!("{:?}", new_commit.parent_ids).as_bytes());
    hasher.update(new_commit.message.as_bytes());
    hasher.update(new_commit.author.as_bytes());
    hasher.update(new_commit.email.as_bytes());
    hasher.update(&new_commit.timestamp.unix_timestamp().to_le_bytes());
    Ok(MerkleHash::new(hasher.digest128()))
}

#[allow(clippy::too_many_arguments)]
fn write_commit_entries(
    repo: &LocalRepository,
    commit_id: MerkleHash,
    session: &dyn MerkleWriteSession,
    commit_ns: &mut dyn NodeWriteSession,
    dir_hash_db: &DBWithThreadMode<SingleThreaded>,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    entries: &HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)>,
) -> Result<(), OxenError> {
    // Write the root dir, then recurse into the vnodes and subdirectories
    let mut total_written = 0;
    let root_path = PathBuf::from("");
    let dir_node = compute_dir_node(repo, commit_id, entries, dir_hashes, &root_path)?;
    commit_ns.add_child(&dir_node)?;
    total_written += 1;

    str_val_db::put(
        dir_hash_db,
        root_path.to_str().unwrap(),
        &dir_node.hash().to_string(),
    )?;

    r_create_dir_node(
        repo,
        session,
        commit_id,
        commit_id,
        &dir_node,
        &root_path,
        dir_hash_db,
        dir_hashes,
        entries,
        &mut total_written,
    )?;

    // The dir_hash_db was pre-populated from the previous commit, so
    // removed directories still have stale entries that must be deleted;
    // otherwise tree lookups will find the old subtree.
    cache_invalidate_dir_hash_db(dir_hash_db, entries.iter().map(|(_, (_, removed))| removed))
}

/// Perform cache-invalidation: remove dir_hash entries for directories that were removed.
/// The `entries` are staged files. This removes every directory that is staged for removal
/// from the supplied `dir_hash_db`.
fn cache_invalidate_dir_hash_db<'a>(
    dir_hash_db: &DBWithThreadMode<SingleThreaded>,
    entries: impl Iterator<Item = &'a Vec<StagedMerkleTreeNode>>,
) -> Result<(), OxenError> {
    for removed_children in entries {
        for child in removed_children {
            if child.status == StagedEntryStatus::Removed
                && let EMerkleTreeNode::Directory(dir_node) = &child.node.node
            {
                let child_path = PathBuf::from(dir_node.name());
                // `child_path` already contains the full relative path
                // (e.g., "annotations/train"), so use it directly as the key
                let path_str = child_path.to_string_lossy();
                log::debug!("deleting removed dir hash: {path_str}");
                str_val_db::delete(dir_hash_db, path_str)?;
            }
        }
    }
    Ok(())
}

/// Write the merkle subtree rooted at `dir_node` into `session`.
///
/// Each `NodeWriteSession` opened here follows the strict `create_node → add_child* → finish`
/// pattern: no other `create_node` is called on `session` between a node session's open and
/// its finish. At any moment at most one `NodeWriteSession` from this call frame is alive,
/// so the open file-handle count is constant in `dir_depth` rather than `O(dir_depth)`.
#[allow(clippy::too_many_arguments)]
fn r_create_dir_node(
    repo: &LocalRepository,
    session: &dyn MerkleWriteSession,
    commit_id: MerkleHash,
    parent_id: MerkleHash,
    dir_node: &DirNode,
    dir_path: &Path,
    dir_hash_db: &DBWithThreadMode<SingleThreaded>,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    entries: &HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)>,
    total_written: &mut u64,
) -> Result<(), OxenError> {
    log::debug!("r_create_dir_node path {dir_path:?}");

    let Some((vnodes, _)) = entries.get(dir_path) else {
        // Mirrors the prior early-return: the recursive call site below is gated on
        // `entries.contains_key(&sub_path)`, so this branch is only reachable from
        // the top-level call in `write_commit_entries`. Open and immediately close
        // the dir's session so the dir node is still written to the store.
        log::debug!("r_create_dir_node No entries found for directory {dir_path:?}");
        let dir_ns = session.create_node(dir_node, Some(parent_id))?;
        dir_ns.finish()?;
        return Ok(());
    };

    log::debug!("Processing dir {:?} with {} vnodes", dir_path, vnodes.len());

    // ── Phase 1: open dir_ns, add this dir's vnodes as its immediate children.
    let mut dir_ns = session.create_node(dir_node, Some(parent_id))?;
    let dir_id = *dir_ns.node_id();

    let vnode_objs: Vec<VNode> = vnodes
        .iter()
        .map(|v| {
            VNode::new(
                repo,
                VNodeOpts {
                    hash: v.id,
                    num_entries: v.entries.len() as u64,
                },
            )
        })
        .collect::<Result<_, _>>()?;

    for v_obj in &vnode_objs {
        dir_ns.add_child(v_obj)?;
        *total_written += 1;
    }

    // ── Phase 2: finish dir_ns. No other create_node has been called on `session`
    // since dir_ns was opened, so the strict invariant holds.
    dir_ns.finish()?;

    // ── Phase 3: write each vnode's children. Defer recursion into staged subdirs
    // until after every vnode_ns from this frame is finished. Each subdir's merkle
    // parent in its `node` file header is its referencing vnode's id.
    let mut subdirs_to_recurse: Vec<(DirNode, PathBuf, MerkleHash)> = Vec::new();

    for (v, v_obj) in vnodes.iter().zip(vnode_objs.iter()) {
        log::debug!("Processing vnode {} with {} entries", v.id, v.entries.len());

        let mut vnode_ns = session.create_node(v_obj, Some(dir_id))?;
        for entry in v.entries.iter() {
            log::trace!("Processing entry {} in vnode {}", entry.node, v.id);
            match &entry.node.node {
                EMerkleTreeNode::Directory(node) => {
                    // If the dir has updates, we need a new dir db
                    let sub_path = entry.node.maybe_path()?;
                    let sub_dir_node_hash = if entries.contains_key(&sub_path) {
                        let n = compute_dir_node(repo, commit_id, entries, dir_hashes, &sub_path)?;

                        vnode_ns.add_child(&n)?;
                        *total_written += 1;

                        let n_hash = *n.hash();
                        subdirs_to_recurse.push((n, sub_path.clone(), v.id));
                        n_hash
                    } else {
                        // Look up the old dir node and reference it
                        let Some(old_dir_node) =
                            CommitMerkleTree::read_node(repo, node.hash(), false)?
                        else {
                            log::trace!(
                                "r_create_dir_node could not read old dir node {}",
                                node.hash().to_hex_hash(),
                            );
                            continue;
                        };
                        let n = old_dir_node.dir()?;
                        vnode_ns.add_child(&n)?;
                        *total_written += 1;
                        *n.hash()
                    };

                    str_val_db::put(
                        dir_hash_db,
                        sub_path.to_str().unwrap(),
                        &sub_dir_node_hash.to_string(),
                    )?;
                }
                EMerkleTreeNode::File(file_node) => {
                    let mut file_node = file_node.clone();
                    let file_path = PathBuf::from(&file_node.name());
                    let file_name = file_path.file_name().unwrap().to_str().unwrap();

                    log::trace!(
                        "Processing file {:?} in vnode {} in commit {}",
                        dir_path,
                        v.id,
                        commit_id.to_hex_hash(),
                    );

                    // Just single file chunk for now
                    let chunks = vec![file_node.hash().to_u128()];
                    file_node.set_chunk_hashes(chunks);
                    let last_commit_id = if entry.status == StagedEntryStatus::Unmodified {
                        *file_node.last_commit_id()
                    } else {
                        commit_id
                    };
                    file_node.set_last_commit_id(&last_commit_id);
                    file_node.set_name(file_name);

                    vnode_ns.add_child(&file_node)?;
                    *total_written += 1;
                }
                _ => {
                    return Err(OxenError::basic_str(format!(
                        "r_create_dir_node found unexpected node type: {:?}",
                        entry.node
                    )));
                }
            }
        }
        vnode_ns.finish()?;
    }

    // ── Phase 4: recurse into each staged subdir. By construction, no
    // NodeWriteSession owned by this call frame is alive at this point.
    for (sub_dir_node, sub_path, vnode_id) in subdirs_to_recurse {
        r_create_dir_node(
            repo,
            session,
            commit_id,
            vnode_id,
            &sub_dir_node,
            &sub_path,
            dir_hash_db,
            dir_hashes,
            entries,
            total_written,
        )?;
    }

    log::debug!("Finished processing dir {dir_path:?} total written {total_written} entries");

    Ok(())
}

fn get_children(
    entries: &HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)>,
    dir_path: impl AsRef<Path>,
) -> Result<Vec<PathBuf>, OxenError> {
    let dir_path = dir_path.as_ref().to_path_buf();
    let mut children = vec![];

    for (path, _) in entries.iter() {
        if path.starts_with(&dir_path) {
            children.push(path.clone());
        }
    }

    Ok(children)
}

fn compute_dir_node(
    repo: &LocalRepository,
    commit_id: MerkleHash,
    entries: &HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)>,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    path: impl AsRef<Path>,
) -> Result<DirNode, OxenError> {
    let path = path.as_ref().to_path_buf();
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"dir");
    hasher.update(path.to_str().unwrap().as_bytes());

    let mut num_bytes = 0;
    let mut num_entries = 0;
    let mut data_type_counts: HashMap<String, u64> = HashMap::new();
    let mut data_type_sizes: HashMap<String, u64> = HashMap::new();

    let children = get_children(entries, &path)?;
    log::debug!(
        "Aggregating dir {path:?} for [{commit_id}] with {children:?} children num_bytes {num_bytes:?} data_type_counts {data_type_counts:?}"
    );
    let head_commit_maybe = repositories::commits::head_commit_maybe(repo)?;
    if let Some(ref head_commit) = head_commit_maybe
        && let Ok(Some(old_dir_node)) =
            repositories::tree::get_dir_without_children(repo, head_commit, &path, Some(dir_hashes))
    {
        let old_dir_node = old_dir_node.dir().unwrap();
        num_entries = old_dir_node.num_entries();
        num_bytes = old_dir_node.num_bytes();
        data_type_counts = old_dir_node.data_type_counts().clone();
        data_type_sizes = old_dir_node.data_type_sizes().clone();
    }

    for child in children.iter() {
        let Some((vnodes, removed_entries)) = entries.get(child) else {
            let err_msg = format!("compute_dir_node No entries found for directory {path:?}");
            return Err(OxenError::basic_str(err_msg));
        };
        for vnode in vnodes.iter() {
            // Include VNode hashes in the directory hash so that when a VNode
            // gets a new UUID-based hash (because it has modified entries), the
            // parent directory hash changes too. Without this, two commits can
            // produce the same directory hash even though their VNode children
            // differ, causing stale node data to be read from shared storage.
            hasher.update(&vnode.id.to_le_bytes());
            for entry in vnode.entries.iter() {
                // log::debug!("Aggregating entry {}", entry.node);
                match &entry.node.node {
                    EMerkleTreeNode::Directory(dir_node) => {
                        if path == *child {
                            num_entries += 1;
                        }
                        // log::debug!(
                        //     "Updating hash for dir {} -> hash {} status {:?}",
                        //     dir_node.name(),
                        //     dir_node.hash(),
                        //     entry.status
                        // );
                        hasher.update(dir_node.name().as_bytes());
                        hasher.update(&dir_node.hash().to_le_bytes());
                    }
                    EMerkleTreeNode::File(file_node) => {
                        // log::debug!(
                        //     "Updating hash for file {} -> hash {} status {:?}",
                        //     file_node.name(),
                        //     file_node.hash(),
                        //     entry.status
                        // );
                        hasher.update(file_node.name().as_bytes());
                        hasher.update(&file_node.combined_hash().to_le_bytes());

                        match entry.status {
                            StagedEntryStatus::Added => {
                                num_bytes += file_node.num_bytes();
                                if path == *child {
                                    num_entries += 1;
                                }
                                *data_type_counts
                                    .entry(file_node.data_type().to_string())
                                    .or_insert(0) += 1;
                                *data_type_sizes
                                    .entry(file_node.data_type().to_string())
                                    .or_insert(0) += file_node.num_bytes();
                            }
                            StagedEntryStatus::Modified => {
                                // The old size is already included in
                                // num_bytes/data_type_sizes from the parent commit.
                                // Look up the old file to compute the delta.
                                if let Some(head) = &head_commit_maybe
                                    && let Some(old_file) = repositories::tree::get_file_by_path(
                                        repo,
                                        head,
                                        path.join(file_node.name()),
                                    )?
                                {
                                    let delta =
                                        file_node.num_bytes() as i64 - old_file.num_bytes() as i64;
                                    num_bytes = (num_bytes as i64 + delta) as u64;
                                    let size_entry = data_type_sizes
                                        .entry(file_node.data_type().to_string())
                                        .or_insert(0);
                                    *size_entry = (*size_entry as i64 + delta) as u64;
                                }
                            }
                            _ => {
                                // Do nothing
                            }
                        }
                    }
                    _ => {
                        return Err(OxenError::basic_str(format!(
                            "compute_dir_node found unexpected node type: {:?}",
                            entry.node
                        )));
                    }
                }
            }
        }
        // Adjust dir node metadata for removed entries
        for entry in removed_entries.iter() {
            match &entry.node.node {
                EMerkleTreeNode::Directory(_) => {
                    // Do nothing
                }
                EMerkleTreeNode::File(file_node) => {
                    if entry.status == StagedEntryStatus::Removed {
                        if path == *child {
                            num_entries -= 1;
                        }
                        num_bytes = num_bytes.saturating_sub(file_node.num_bytes());
                        if let Some(count) =
                            data_type_counts.get_mut(&file_node.data_type().to_string())
                        {
                            *count = count.saturating_sub(1);
                        }
                        if let Some(size) =
                            data_type_sizes.get_mut(&file_node.data_type().to_string())
                        {
                            *size = size.saturating_sub(file_node.num_bytes());
                        }
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(format!(
                        "compute_dir_node found unexpected node type: {:?}",
                        entry.node
                    )));
                }
            }
        }
    }

    let hash = MerkleHash::new(hasher.digest128());
    let file_name = path.file_name().unwrap_or_default().to_str().unwrap();
    log::debug!(
        "Aggregated dir {path:?} [{hash}] num_bytes {num_bytes:?} num_entries {num_entries:?} data_type_counts {data_type_counts:?}"
    );

    let node = DirNode::new(
        repo,
        DirNodeOpts {
            name: file_name.to_owned(),
            hash,
            num_bytes,
            num_entries,
            last_commit_id: commit_id,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts,
            data_type_sizes,
        },
    )?;
    Ok(node)
}

fn create_merge_commit(
    repo: &LocalRepository,
    message: &str,
    timestamp: OffsetDateTime,
    new_commit: &NewCommitBody,
) -> Result<NewCommit, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
    let orig_head_path = hidden_dir.join(ORIG_HEAD_FILE);

    // Read parent commit ids
    let merge_commit_id = util::fs::read_from_path(&merge_head_path)?;
    let head_commit_id = util::fs::read_from_path(&orig_head_path)?;

    // Cleanup
    util::fs::remove_file(merge_head_path)?;
    util::fs::remove_file(orig_head_path)?;

    Ok(NewCommit {
        parent_ids: vec![merge_commit_id, head_commit_id],
        message: String::from(message),
        author: new_commit.author.clone(),
        email: new_commit.email.clone(),
        timestamp,
    })
}

fn is_merge_commit(repo: &LocalRepository) -> bool {
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
    merge_head_path.exists()
}

fn create_commit_data(
    repo: &LocalRepository,
    message: &str,
    timestamp: OffsetDateTime,
    parent_commits: Vec<String>,
    new_commit: &NewCommitBody,
) -> Result<NewCommit, OxenError> {
    if is_merge_commit(repo) {
        create_merge_commit(repo, message, timestamp, new_commit)
    } else {
        Ok(NewCommit {
            parent_ids: parent_commits,
            message: message.to_string(),
            author: new_commit.author.clone(),
            email: new_commit.email.clone(),
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::model::merkle_tree::merkle_reader::MerkleEntry;
    use crate::test;
    use std::collections::HashSet;
    use std::path::Path;

    use crate::core::v_latest::index::CommitMerkleTree;
    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::opts::RmOpts;
    use crate::repositories;

    use crate::util;
    use test::add_n_files_m_dirs;

    #[tokio::test]
    async fn test_first_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init(dir)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 2).await?;
            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            /*
            [Commit] 861d5cd233eff0940060bd76ce24f10a
              [Dir] ""
                [VNode]
                  [File] README.md
                  [File] files.csv
                  [Dir] files
                    [VNode]
                      [Dir] dir_0
                        [VNode]
                          [File] file4.txt
                          [File] file0.txt
                          [File] file2.txt
                          [File] file6.txt
                          [File] file8.txt
                      [Dir] dir_1
                        [VNode]
                          [File] file7.txt
                          [File] file3.txt
                          [File] file5.txt
                          [File] file1.txt
                          [File] file9.txt
            */

            // Make sure we have 4 vnodes
            let vnodes = tree.total_vnodes();
            assert_eq!(vnodes, 4);

            // Make sure the root is a commit node
            let root = &tree.root;
            let commit = root.commit();
            assert!(commit.is_ok());

            // Make sure the root commit has 1 child, the root dir node
            let root_commit_children = &root.children;
            assert_eq!(root_commit_children.len(), 1);

            let dir_node_data = root_commit_children.iter().next().unwrap();
            let dir_node = dir_node_data.dir();
            assert!(dir_node.is_ok());
            assert_eq!(dir_node.unwrap().name(), "");

            // Make sure dir node has one child, the VNode
            let vnode_data = dir_node_data.children.first().unwrap();
            let vnode = vnode_data.vnode();
            assert!(vnode.is_ok());

            // Make sure the vnode has 3 children, the 2 files and the dir
            let vnode_children = &vnode_data.children;
            assert_eq!(vnode_children.len(), 3);

            // Check that files.csv is in the merkle tree
            let has_paths_csv = tree.has_path(Path::new("files.csv"))?;
            assert!(has_paths_csv);

            // Check that README.md is in the merkle tree
            let has_readme = tree.has_path(Path::new("README.md"))?;
            assert!(has_readme);

            // Check that files/dir_0/file0.txt is in the merkle tree
            let has_path0 = tree.has_path(Path::new("files/dir_0/file0.txt"))?;
            assert!(has_path0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_only_dirs_at_top_level() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(async |dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init(dir)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("all_files/dir_0/new_file.txt");
            util::fs::create_dir_all(new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            repositories::add(&repo, &repo.path).await?;

            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            let has_path0 = tree.has_path(Path::new("all_files/dir_0/new_file.txt"))?;
            assert!(has_path0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_single_file_deep_in_dir() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init(dir)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("files/dir_0/new_file.txt");
            util::fs::create_dir_all(new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            repositories::add(&repo, &new_file).await?;

            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            let has_path0 = tree.has_path(Path::new("files/dir_0/new_file.txt"))?;
            assert!(has_path0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_2nd_commit_keeps_num_bytes_and_data_type_counts() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init(dir)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3).await?;
            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let first_commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let first_tree = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            first_tree.print();

            // Get the original root dir file count
            let original_root_node = first_tree.get_by_path(Path::new(""))?.unwrap();
            let original_root_dir = original_root_node.dir()?;
            let original_root_dir_file_count = original_root_dir.num_files();

            // Ten image files + README.md + files.csv
            assert_eq!(original_root_dir_file_count, 12);

            // Add a new file to files/dir_1/
            let new_file = repo.path.join("README.md");
            util::fs::write_to_path(&new_file, "Update that README.md")?;
            repositories::add(&repo, &new_file).await?;

            // Commit the data
            let second_commit = super::commit(&repo, "Second commit")?;

            // Make sure commit hashes are different
            assert!(first_commit.id != second_commit.id);

            // Make sure the head commit is updated
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, second_commit.id);

            // Read the merkle tree
            let second_tree = CommitMerkleTree::from_commit(&repo, &second_commit)?;
            second_tree.print();

            // Make sure the root dir file count is the same
            let updated_root_dir = second_tree.get_by_path(Path::new(""))?;
            let updated_root_dir = updated_root_dir.unwrap().dir()?;
            let updated_root_dir_file_count = updated_root_dir.num_files();
            assert_eq!(updated_root_dir_file_count, original_root_dir_file_count);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_second_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init(dir)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3).await?;
            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let first_commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let first_tree = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            first_tree.print();

            // Count the number of files in the files/dir_1 dir
            let original_dir_1_node = first_tree.get_by_path(Path::new("files/dir_1"))?;
            let original_dir_1_node = original_dir_1_node.unwrap().dir()?;
            let original_dir_1_file_count = original_dir_1_node.num_files();

            // Add a new file to files/dir_1/
            let new_file = repo.path.join("files/dir_1/new_file.txt");
            util::fs::write_to_path(&new_file, "New file")?;
            repositories::add(&repo, &new_file).await?;

            // Commit the data
            let second_commit = super::commit(&repo, "Second commit")?;

            // Make sure commit hashes are different
            assert!(first_commit.id != second_commit.id);

            // Make sure the head commit is updated
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, second_commit.id);

            // Read the merkle tree
            let second_tree = CommitMerkleTree::from_commit(&repo, &second_commit)?;
            second_tree.print();

            assert_eq!(second_tree.total_vnodes(), 5);

            assert!(!first_tree.has_path(Path::new("files/dir_1/new_file.txt"))?);
            assert!(second_tree.has_path(Path::new("files/dir_1/new_file.txt"))?);

            // Make sure the last commit id is updated on new_file.txt
            let updated_node = second_tree.get_by_path(Path::new("files/dir_1/new_file.txt"))?;
            assert!(updated_node.is_some());
            let updated_file_node = updated_node.unwrap().file()?;
            let updated_commit_id = updated_file_node.last_commit_id().to_string();
            assert_eq!(updated_commit_id, second_commit.id);

            // Make sure that last commit id is not updated on other files in the dir
            let other_file_node = second_tree.get_by_path(Path::new("files/dir_1/file7.txt"))?;
            assert!(other_file_node.is_some());
            let other_file_node = other_file_node.unwrap().file()?;
            let other_commit_id = other_file_node.last_commit_id().to_string();
            assert_eq!(other_commit_id, first_commit.id);

            // Make sure last commit is updated on the dir
            let dir_node = second_tree.get_by_path(Path::new("files/dir_1"))?;
            assert!(dir_node.is_some());
            let dir_node = dir_node.unwrap().dir()?;
            let dir_commit_id = dir_node.last_commit_id().to_string();
            assert_eq!(dir_commit_id, second_commit.id);

            // Make sure the hashes of the directories are valid
            // We should update the hashes of dir_1 and all it's parents, but none of the siblings
            let first_tree_dir_1 = first_tree.get_by_path(Path::new("files/dir_1"))?;
            let second_tree_dir_1 = second_tree.get_by_path(Path::new("files/dir_1"))?;
            assert!(first_tree_dir_1.is_some());
            assert!(second_tree_dir_1.is_some());
            assert!(first_tree_dir_1.unwrap().hash != second_tree_dir_1.unwrap().hash);

            // Make sure there is one vnode in each dir
            let first_tree_vnodes = first_tree.get_vnodes_for_dir(Path::new("files/dir_1"))?;
            let second_tree_vnodes = second_tree.get_vnodes_for_dir(Path::new("files/dir_1"))?;
            assert_eq!(first_tree_vnodes.len(), 1);
            assert_eq!(second_tree_vnodes.len(), 1);

            // And that the vnode hashes are different
            assert!(first_tree_vnodes[0].hash != second_tree_vnodes[0].hash);

            // Siblings should be the same
            let first_tree_dir_0 = first_tree.get_by_path(Path::new("files/dir_0"))?;
            let second_tree_dir_0 = second_tree.get_by_path(Path::new("files/dir_0"))?;
            assert!(first_tree_dir_0.is_some());
            assert!(second_tree_dir_0.is_some());
            assert_eq!(
                first_tree_dir_0.unwrap().hash,
                second_tree_dir_0.unwrap().hash
            );

            // Parent should be updated
            let first_tree_files = first_tree.get_by_path(Path::new("files"))?;
            let second_tree_files = second_tree.get_by_path(Path::new("files"))?;
            assert!(first_tree_files.is_some());
            assert!(second_tree_files.is_some());
            assert!(first_tree_files.unwrap().hash != second_tree_files.unwrap().hash);

            // Root should be updated
            let first_tree_root = first_tree.get_by_path(Path::new(""))?;
            let second_tree_root = second_tree.get_by_path(Path::new(""))?;
            assert!(first_tree_root.is_some());
            assert!(second_tree_root.is_some());
            assert!(first_tree_root.unwrap().hash != second_tree_root.unwrap().hash);

            // Read the first tree again, and make sure the file count of the files/dir_1 is the same as the first time we read it
            let first_tree_again = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            first_tree_again.print();
            let dir_1_node_again = first_tree_again.get_by_path(Path::new("files/dir_1"))?;
            let dir_1_node_again = dir_1_node_again.unwrap().dir()?;
            let dir_1_file_count_again = dir_1_node_again.num_files();
            assert_eq!(original_dir_1_file_count, dir_1_file_count_again);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_configurable_vnode_size() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let mut repo = repositories::init::init(dir)?;
            // Set the vnode size to 5
            repo.set_vnode_size(5);

            // Write data to the repo, 23 files in 2 dirs
            add_n_files_m_dirs(&repo, 23, 2).await?;
            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let first_commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let first_tree = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            first_tree.print();

            // Make sure we have the correct number of vnodes
            let root_node = first_tree.get_by_path(Path::new(""))?.unwrap();
            // The root dir should have one vnode because there are only 3 files/dirs (README.md, files.csv, files)
            assert_eq!(root_node.num_vnodes(), 1);

            // Both dir_0 and dir_1 should have 3 vnodes each (vnode size is 5 and there will be 12 and 13 files respectively)
            // 12 / 5 = 2.4 -> 3 vnodes
            // 13 / 5 = 2.6 -> 3 vnodes
            let dir_0_node = first_tree.get_by_path(Path::new("files/dir_0"))?.unwrap();
            assert_eq!(dir_0_node.num_vnodes(), 3);
            let dir_1_node = first_tree.get_by_path(Path::new("files/dir_1"))?.unwrap();
            assert_eq!(dir_1_node.num_vnodes(), 3);

            // Add a new files
            for i in 0..10 {
                let dir_num = i % 2;
                let new_file = repo
                    .path
                    .join("files")
                    .join(format!("dir_{dir_num}"))
                    .join(format!("new_file_{i}.txt"));
                util::fs::write_to_path(&new_file, format!("New fileeeee {i}"))?;
                repositories::add(&repo, &new_file).await?;
            }

            // Commit the data
            let second_commit = super::commit(&repo, "Second commit")?;

            // Make sure commit hashes are different
            assert!(first_commit.id != second_commit.id);

            // Make sure the head commit is updated
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, second_commit.id);

            // Read the second merkle tree
            let second_tree = CommitMerkleTree::from_commit(&repo, &second_commit)?;
            second_tree.print();

            // Make sure we can read the new files
            for i in 0..10 {
                let dir_num = i % 2;
                let file_path = Path::new("files")
                    .join(format!("dir_{dir_num}"))
                    .join(format!("new_file_{i}.txt"));

                let file_node = second_tree.get_by_path(&file_path)?;
                assert!(file_node.is_some());

                let file_node =
                    repositories::tree::get_node_by_path(&repo, &second_commit, &file_path)?;
                assert!(file_node.is_some());
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_20_files_6_vnode_size() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let mut repo = repositories::init::init(dir)?;
            // Set the vnode size to 6
            repo.set_vnode_size(6);

            // Write data to the repo, 20 files in 1 dir
            add_n_files_m_dirs(&repo, 20, 1).await?;
            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let first_commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let first_tree = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            first_tree.print();

            // Make sure we have the correct number of vnodes
            let root_node = first_tree.get_by_path(Path::new(""))?.unwrap();
            // The root dir should have one vnode because there are only 3 files/dirs (README.md, files.csv, files)
            assert_eq!(root_node.num_vnodes(), 1);

            // There should only be 3 vnodes in the dir
            // 20 / 6 = 3.333 -> 4 vnodes
            let dir_0_node = first_tree.get_by_path(Path::new("files/dir_0"))?.unwrap();
            assert_eq!(dir_0_node.num_vnodes(), 4);

            // Add a news file
            let new_file = repo.path.join("files/dir_0/new_file.txt");
            util::fs::write_to_path(&new_file, "New file")?;
            repositories::add(&repo, &new_file).await?;

            // Commit the data
            let second_commit = super::commit(&repo, "Second commit")?;

            // Make sure commit hashes are different
            assert!(first_commit.id != second_commit.id);

            // Make sure the head commit is updated
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, second_commit.id);

            // Read the second merkle tree
            let second_tree = CommitMerkleTree::from_commit(&repo, &second_commit)?;
            second_tree.print();

            let second_dir_0_node = second_tree.get_by_path(Path::new("files/dir_0"))?.unwrap();
            assert_eq!(second_dir_0_node.num_vnodes(), 4);

            // Make sure 3 of the vnodes have the same hash as the first vnode
            let first_children_hashes: HashSet<MerkleHash> =
                dir_0_node.children.iter().map(|vnode| vnode.hash).collect();
            let second_children_hashes: HashSet<MerkleHash> = second_dir_0_node
                .children
                .iter()
                .map(|vnode| vnode.hash)
                .collect();
            let intersection: HashSet<&MerkleHash> = second_children_hashes
                .intersection(&first_children_hashes)
                .collect();
            assert_eq!(intersection.len(), 3);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_third_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init(dir)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3).await?;
            let status = repositories::status(&repo).await?;
            status.print();

            // Commit the data
            let first_commit = super::commit(&repo, "First commit")?;

            // Read the merkle tree
            let first_tree = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            first_tree.print();

            let original_readme_node = first_tree.get_by_path(Path::new("README.md"))?;
            assert!(original_readme_node.is_some());
            let original_readme_node = original_readme_node.unwrap();
            let original_readme_hash = original_readme_node.hash;

            // Update README.md
            let new_file = repo.path.join("README.md");
            util::fs::write_to_path(&new_file, "Update README.md in second commit")?;
            repositories::add(&repo, &new_file).await?;

            // Commit the data
            let second_commit = super::commit(&repo, "Second commit")?;

            // Make sure commit hashes are different
            assert!(first_commit.id != second_commit.id);

            // Make sure the head commit is updated
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, second_commit.id);

            // Read the merkle tree
            let second_tree = CommitMerkleTree::from_commit(&repo, &second_commit)?;
            second_tree.print();

            // Make sure the README.md hash is different
            let updated_readme_node = second_tree.get_by_path(Path::new("README.md"))?;
            assert!(updated_readme_node.is_some());
            let updated_readme_node = updated_readme_node.unwrap();
            let updated_readme_hash = updated_readme_node.hash;
            assert!(original_readme_hash != updated_readme_hash);

            // Write a new file to files/dir_1/
            let new_file = repo.path.join("files/dir_1/new_file.txt");
            util::fs::write_to_path(&new_file, "New file")?;
            repositories::add(&repo, &new_file).await?;

            // Commit the data
            let third_commit = super::commit(&repo, "Third commit")?;

            // Read the merkle tree
            let third_tree = CommitMerkleTree::from_commit(&repo, &third_commit)?;
            third_tree.print();

            // Make sure the head commit is updated
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, third_commit.id);
            assert!(third_commit.id != second_commit.id);
            assert!(third_commit.id != first_commit.id);

            // List the dir hashes
            let dir_hashes = CommitMerkleTree::dir_hashes(&repo, &third_commit)?;

            for (path, hash) in dir_hashes {
                println!("dir_hash: {path:?} {hash}");
                let node = third_tree.get_by_path(&path)?.unwrap();
                assert_eq!(node.hash, hash);
            }

            Ok(())
        })
        .await
    }

    /*
    This tests a bug we found where removing a directory breaks the tree by
    updating the root dir hash of the _initial_ commit to the root dir hash from
    the commit where the directory was removed.
     */
    #[tokio::test]
    async fn test_rm_dir_doesnt_break_tree() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(async |repo| {
            // create initial commit
            let readme = repo.path.join("README.md");
            repositories::add(&repo, &readme).await?;
            let first_commit = super::commit(&repo, "Initial commit")?;

            let first_tree = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            let first_root_dir_node = first_tree.get_by_path(Path::new(""))?.unwrap();

            // add the data
            repositories::add(&repo, repo.path.join("train")).await?;
            repositories::add(&repo, repo.path.join("test")).await?;
            let second_commit = super::commit(&repo, "Adding the data")?;

            let second_tree = CommitMerkleTree::from_commit(&repo, &second_commit)?;
            let second_root_dir_node = second_tree.get_by_path(Path::new(""))?.unwrap();
            assert_ne!(first_root_dir_node.hash, second_root_dir_node.hash);

            // remove a directory
            let rm_opts = RmOpts::from_path_recursive(Path::new("train"));
            repositories::rm(&repo, &rm_opts).await?;
            let third_commit = super::commit(&repo, "Removing train dir")?;

            let third_tree = CommitMerkleTree::from_commit(&repo, &third_commit)?;
            let third_root_dir_node = third_tree.get_by_path(Path::new(""))?.unwrap();
            assert_ne!(first_root_dir_node.hash, third_root_dir_node.hash);

            // now read the first commit again and make sure the root dir hash is the same
            let first_tree_2 = CommitMerkleTree::from_commit(&repo, &first_commit)?;
            let first_root_dir_node_2 = first_tree_2.get_by_path(Path::new(""))?.unwrap();
            assert_eq!(first_root_dir_node.hash, first_root_dir_node_2.hash);
            assert_ne!(first_root_dir_node_2.hash, third_root_dir_node.hash);

            Ok(())
        })
        .await
    }

    // ════════════════════════════════════════════════════════════════════════
    // Regression test for the refactor of `r_create_dir_node` and
    // `write_commit_entries` that reordered merkle-tree node writes.
    //
    // Two implementations are exercised side-by-side:
    //
    //   * "previous node writing" — `r_create_dir_node_previous` and
    //     `write_commit_entries_previous`, preserved verbatim from before
    //     the refactor. Holds the parent's `NodeWriteSession` open across
    //     recursion into its children's subtrees, so nested write sessions
    //     are live simultaneously on the same `MerkleWriteSession`.
    //
    //   * "updated node writing" — the production `r_create_dir_node` and
    //     `write_commit_entries` after the refactor. Each `NodeWriteSession`
    //     follows a strict `create_node → add_child* → finish` shape with
    //     no other `create_node` interleaved on the same write session.
    //
    // ── What this test verifies ─────────────────────────────────────────────
    //
    // The merkle store written by the updated node writing is the *same
    // merkle store* as the one written by the previous node writing.
    // "Same" here means: same set of node hashes (== same set of per-node
    // directories under `tree/nodes/<prefix>/<suffix>/`) and, for each
    // hash, the same decoded node value, the same decoded list of
    // children, and the same decoded `parent_id` link back up the tree.
    //
    // Concretely, the test:
    //
    //   1. Stages a non-trivial nested tree (README.md, files.csv, plus
    //      `files/dir_<n>/file<i>.txt`) into a fresh repo so that the
    //      recursion is exercised: multiple directories, multiple vnodes per
    //      directory, vnodes containing both file entries and directory
    //      references.
    //   2. Builds the merkle-write inputs ONCE (`vnode_entries`,
    //      `dir_hashes`, `commit_id`, the root `CommitNode`, etc.) so both
    //      runs see byte-identical inputs. The timestamp is pinned to a
    //      fixed Unix epoch so `commit_id` is deterministic across runs.
    //   3. Run #1 — previous node writing: opens a fresh write session and
    //      invokes the preserved-verbatim `write_commit_entries_previous`
    //      against the inputs. Snapshots `tree/nodes/`.
    //   4. Wipes `tree/nodes/` so run #2 starts from an empty merkle store.
    //   5. Run #2 — updated node writing: same fresh-session/finish dance,
    //      but invokes the production `write_commit_entries`. Snapshots
    //      `tree/nodes/` again.
    //   6. Compares the two snapshots and asserts they're equal.
    //
    // ── Why structural comparison, not byte-by-byte ─────────────────────────
    //
    // The natural form of this test would be: snapshot every byte under
    // `tree/nodes/` after each run and assert the two byte streams are
    // identical. We deliberately do NOT do that because it would flake.
    //
    // `DirNode` carries `data_type_counts: HashMap<String, u64>` and
    // `data_type_sizes: HashMap<String, u64>` (see
    // `crates/liboxen/src/model/merkle_tree/node/dir_node.rs:48-49`). When the
    // node is msgpack-encoded for the per-node `children` and `node` files,
    // the serializer iterates each `HashMap` to emit its key/value pairs.
    // `std::collections::HashMap` uses `RandomState`, which is randomized
    // per-instance via a thread-local PRNG, so two `HashMap`s containing
    // the exact same keys/values can iterate in different orders.
    //
    // `compute_dir_node` allocates a fresh `HashMap` on every call. The
    // previous and updated implementations both call `compute_dir_node`
    // (the updated implementation calls it from its caller for the root
    // and inside Phase 3 for staged subdirs; the previous implementation
    // calls it inline in the recursion), and each call gets its own
    // `RandomState`. The result: the same logical `DirNode` produced by
    // each run can msgpack-encode to byte sequences with the *same length*
    // but *different inner-map key orders*. The bytes are functionally
    // equivalent — they decode back to identical `DirNode` values — but
    // `assert_eq!` on the raw bytes would fail nondeterministically.
    //
    // This non-determinism is observable even comparing the previous
    // implementation to itself (running the same code twice produces
    // non-identical bytes), so it isn't a property of the refactor; it's a
    // property of how `DirNode` is serialized today. The merkle node
    // *hash* is computed from the raw field bytes (not from msgpack), so
    // node hashes are stable and the set of `tree/nodes/<prefix>/<suffix>/`
    // paths is stable; only the per-node msgpack body bytes are not.
    //
    // ── What the structural comparison actually tests ───────────────────────
    //
    // For each node directory, `snapshot_tree_nodes` opens the per-node
    // `MerkleNodeDB` read-only (via `MerkleStore::get_node` /
    // `get_children`) and **deserializes** the stored bytes back into:
    //
    //   - `parent_id: Option<MerkleHash>`  (read from the node-file header)
    //   - `node: EMerkleTreeNode`          (msgpack-deserialized body of
    //                                       the node-file's data section)
    //   - `children: Vec<(MerkleHash, EMerkleTreeNode)>`
    //                                      (each child decoded from the
    //                                       children-file via the lookup
    //                                       table in the node-file)
    //
    // The test then compares these triples between the two runs with
    // `assert_eq!`. Equality on `EMerkleTreeNode` is derived
    // (`#[derive(PartialEq, Eq)]` on the enum and on every node variant
    // including `DirNode`), which means equality on the inner
    // `HashMap<String, u64>` fields is by content (key/value membership)
    // rather than by iteration order.
    //
    // Therefore: the test verifies that **what serialize-then-deserialize
    // round-trips to** is identical between the two write paths, even
    // though the on-disk msgpack bytes themselves can differ in
    // HashMap-key ordering. That is the property that actually matters for
    // correctness — the merkle store must be readable to the same logical
    // contents — and it is the strongest equivalence that's observable
    // without changing `DirNode`'s map-typed fields to a deterministic
    // container (e.g. `BTreeMap`) or to a fixed-seeded `BuildHasher`.
    // ════════════════════════════════════════════════════════════════════════

    use super::{
        EntryVNode, NewCommitBody, compute_commit_id, compute_dir_node, create_commit_data,
        split_into_vnodes, write_commit_entries,
    };
    use crate::constants::STAGED_DIR;
    use crate::core::db;
    use crate::core::db::key_val::str_val_db;
    use crate::core::v_latest::status;
    use crate::model::LocalRepository;
    use crate::model::StagedEntryStatus;
    use crate::model::merkle_tree::merkle_writer::{MerkleWriteSession, NodeWriteSession};
    use crate::model::merkle_tree::node::commit_node::CommitNodeOpts;
    use crate::model::merkle_tree::node::vnode::VNodeOpts;
    use crate::model::merkle_tree::node::{
        CommitNode, DirNode, EMerkleTreeNode, MerkleTreeNode, StagedMerkleTreeNode, VNode,
    };
    use indicatif::ProgressBar;
    use rocksdb::{DBWithThreadMode, SingleThreaded};
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;
    use time::OffsetDateTime;

    /// PATTERN A LEGACY — preserved verbatim from the pre-refactor implementation.
    /// Holds `maybe_parent_ns` (the dir's `NodeWriteSession`) open across the
    /// recursion: nested `child_ns` and `vnode_ns` are opened on the same
    /// `MerkleWriteSession` while the parent's session is still alive.
    #[allow(clippy::too_many_arguments)]
    fn r_create_dir_node_previous(
        repo: &LocalRepository,
        session: &dyn MerkleWriteSession,
        commit_id: MerkleHash,
        mut maybe_parent_ns: Option<&mut dyn NodeWriteSession>,
        dir_hash_db: &DBWithThreadMode<SingleThreaded>,
        dir_hashes: &HashMap<PathBuf, MerkleHash>,
        entries: &HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)>,
        path: impl AsRef<Path>,
        total_written: &mut u64,
    ) -> Result<(), OxenError> {
        let path = path.as_ref().to_path_buf();

        let Some((vnodes, _)) = entries.get(&path) else {
            return Ok(());
        };

        for vnode in vnodes.iter() {
            let opts = VNodeOpts {
                hash: vnode.id,
                num_entries: vnode.entries.len() as u64,
            };
            let vnode_obj = VNode::new(repo, opts)?;
            let parent_id_for_vnode = maybe_parent_ns.as_deref().map(|ns| *ns.node_id());
            if let Some(parent_ns) = maybe_parent_ns.as_deref_mut() {
                parent_ns.add_child(&vnode_obj)?;
                *total_written += 1;
            }

            let mut vnode_ns = session.create_node(&vnode_obj, parent_id_for_vnode)?;
            for entry in vnode.entries.iter() {
                match &entry.node.node {
                    EMerkleTreeNode::Directory(node) => {
                        let dir_path = entry.node.maybe_path()?;
                        let dir_node = if entries.contains_key(&dir_path) {
                            let dir_node =
                                compute_dir_node(repo, commit_id, entries, dir_hashes, &dir_path)?;

                            vnode_ns.add_child(&dir_node)?;
                            *total_written += 1;

                            let mut child_ns = session.create_node(&dir_node, Some(vnode.id))?;
                            r_create_dir_node_previous(
                                repo,
                                session,
                                commit_id,
                                Some(&mut *child_ns),
                                dir_hash_db,
                                dir_hashes,
                                entries,
                                &dir_path,
                                total_written,
                            )?;
                            child_ns.finish()?;

                            dir_node
                        } else {
                            let Some(old_dir_node) =
                                CommitMerkleTree::read_node(repo, node.hash(), false)?
                            else {
                                continue;
                            };
                            let dir_node = old_dir_node.dir()?;
                            vnode_ns.add_child(&dir_node)?;
                            *total_written += 1;
                            dir_node
                        };

                        str_val_db::put(
                            dir_hash_db,
                            dir_path.to_str().unwrap(),
                            &dir_node.hash().to_string(),
                        )?;
                    }
                    EMerkleTreeNode::File(file_node) => {
                        let mut file_node = file_node.clone();
                        let file_path = PathBuf::from(&file_node.name());
                        let file_name = file_path.file_name().unwrap().to_str().unwrap();

                        let chunks = vec![file_node.hash().to_u128()];
                        file_node.set_chunk_hashes(chunks);
                        let last_commit_id = if entry.status == StagedEntryStatus::Unmodified {
                            *file_node.last_commit_id()
                        } else {
                            commit_id
                        };
                        file_node.set_last_commit_id(&last_commit_id);
                        file_node.set_name(file_name);

                        vnode_ns.add_child(&file_node)?;
                        *total_written += 1;
                    }
                    _ => {
                        return Err(OxenError::basic_str(format!(
                            "[legacy A] r_create_dir_node found unexpected node type: {:?}",
                            entry.node
                        )));
                    }
                }
            }
            vnode_ns.finish()?;
        }

        Ok(())
    }

    /// PATTERN A LEGACY caller — preserved verbatim. Opens `dir_ns` for the root
    /// dir up-front and holds it across the recursion via `maybe_parent_ns`.
    #[allow(clippy::too_many_arguments)]
    fn write_commit_entries_previous(
        repo: &LocalRepository,
        commit_id: MerkleHash,
        session: &dyn MerkleWriteSession,
        commit_ns: &mut dyn NodeWriteSession,
        dir_hash_db: &DBWithThreadMode<SingleThreaded>,
        dir_hashes: &HashMap<PathBuf, MerkleHash>,
        entries: &HashMap<PathBuf, (Vec<EntryVNode>, Vec<StagedMerkleTreeNode>)>,
    ) -> Result<(), OxenError> {
        let mut total_written: u64 = 0;
        let root_path = PathBuf::from("");
        let dir_node: DirNode = compute_dir_node(repo, commit_id, entries, dir_hashes, &root_path)?;
        commit_ns.add_child(&dir_node)?;
        total_written += 1;

        str_val_db::put(
            dir_hash_db,
            root_path.to_str().unwrap(),
            &dir_node.hash().to_string(),
        )?;
        let mut dir_ns = session.create_node(&dir_node, Some(commit_id))?;
        r_create_dir_node_previous(
            repo,
            session,
            commit_id,
            Some(&mut *dir_ns),
            dir_hash_db,
            dir_hashes,
            entries,
            root_path,
            &mut total_written,
        )?;
        dir_ns.finish()?;
        Ok(())
    }

    /// Snapshot of one node in the merkle store: its parent_id, its decoded
    /// node value, and its decoded children list (each child as `(hash, EMerkleTreeNode)`).
    /// Stored in a path-keyed map indexed by node hash.
    type NodeSnapshot = (
        Option<MerkleHash>,
        EMerkleTreeNode,
        Vec<(MerkleHash, EMerkleTreeNode)>,
    );

    /// Walk every per-node directory under `<repo>/.oxen/tree/nodes/<prefix>/<suffix>/`,
    /// open each per-node DB read-only via the `MerkleStore` API, and capture
    /// the **deserialized** structured contents:
    ///
    /// - `parent_id: Option<MerkleHash>`    — from the per-node `node` file's
    ///   fixed-size header (16 raw bytes; deterministic).
    /// - `node: EMerkleTreeNode`            — msgpack-decoded from the data
    ///   section of the per-node `node` file. Decoding undoes the HashMap
    ///   key-order non-determinism described in the module-level comment.
    /// - `children: Vec<(MerkleHash, EMerkleTreeNode)>` — each child msgpack-
    ///   decoded from the per-node `children` file via the lookup table in `node`.
    ///
    /// Returned as a `BTreeMap` keyed by node hash so the iteration order is
    /// deterministic and comparable. The result is the deserialize side of a
    /// serialize → write → read → deserialize round-trip; it represents the
    /// merkle-store content as the rest of oxen would observe it through
    /// `MerkleStore::get_node` / `get_children`.
    ///
    /// **Why this is structural rather than byte-for-byte.** `DirNode`'s
    /// `data_type_counts` / `data_type_sizes` are `HashMap<String, u64>`
    /// (`crates/liboxen/src/model/merkle_tree/node/dir_node.rs:48-49`). The
    /// msgpack serializer iterates each map to emit key/value pairs, and
    /// `std::collections::HashMap` uses `RandomState` (per-instance,
    /// thread-local PRNG seed) — so two HashMaps with the same key/value
    /// content can iterate in different orders, producing same-length but
    /// byte-different msgpack outputs. `compute_dir_node` allocates a fresh
    /// `HashMap` on every call, so even running the previous-node-writing
    /// code twice can produce non-identical on-disk bytes for `DirNode`
    /// bodies.
    /// The merkle hash is computed from raw fields (not msgpack), so node
    /// hashes are stable, but the byte image of the msgpack body is not.
    ///
    /// Decoding back into `EMerkleTreeNode` makes the comparison agnostic to
    /// that map-key ordering: equality on `EMerkleTreeNode` (derived
    /// `PartialEq`/`Eq`, including for `DirNode` and its inner `HashMap`s)
    /// is by membership/content, not iteration order. This is the strongest
    /// equivalence observable without changing the production map-typed
    /// fields to a deterministic container (e.g. `BTreeMap`) or to a
    /// fixed-seeded `BuildHasher`.
    fn snapshot_tree_nodes(
        repo: &LocalRepository,
    ) -> Result<BTreeMap<MerkleHash, NodeSnapshot>, OxenError> {
        let nodes_dir = util::fs::oxen_hidden_dir(&repo.path)
            .join("tree")
            .join("nodes");
        let mut out: BTreeMap<MerkleHash, NodeSnapshot> = BTreeMap::new();
        if !nodes_dir.exists() {
            return Ok(out);
        }
        let store = repo.merkle_store()?;
        for entry in walkdir::WalkDir::new(&nodes_dir)
            .follow_links(false)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            let Ok(meta) = path.metadata() else { continue };
            if !meta.is_dir() {
                continue;
            }
            let rel = match path.strip_prefix(&nodes_dir) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let components: Vec<&str> = rel
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => s.to_str(),
                    _ => None,
                })
                .collect();
            if components.len() != 2 {
                continue;
            }
            let hex = format!("{}{}", components[0], components[1]);
            let Ok(hash_value) = u128::from_str_radix(&hex, 16) else {
                continue;
            };
            let hash = MerkleHash::new(hash_value);

            let Some(MerkleEntry { node, parent_id }) = store.get_node(&hash)? else {
                continue;
            };
            let children: Vec<(MerkleHash, EMerkleTreeNode)> = store
                .get_children(&hash)?
                .into_iter()
                .map(|(h, n)| (h, n.node))
                .collect();
            out.insert(hash, (parent_id, node, children));
        }
        Ok(out)
    }

    /// Remove `<repo>/.oxen/tree/nodes/` so the second run starts from a clean
    /// merkle store. The dir_hash_db is left in place; its writes are
    /// idempotent (same key/value), so leaving stale entries doesn't perturb
    /// the second run.
    fn wipe_tree_nodes(repo: &LocalRepository) -> Result<(), OxenError> {
        let nodes_dir = util::fs::oxen_hidden_dir(&repo.path)
            .join("tree")
            .join("nodes");
        if nodes_dir.exists() {
            std::fs::remove_dir_all(&nodes_dir).map_err(|e| OxenError::basic_str(e.to_string()))?;
        }
        Ok(())
    }

    /// Regression test: the updated-node-writing production code
    /// (`r_create_dir_node` / `write_commit_entries`) produces a merkle
    /// store that round-trips to the **same deserialized content** as the
    /// preserved-verbatim previous-node-writing implementation
    /// (`r_create_dir_node_previous` / `write_commit_entries_previous`),
    /// given identical merkle-write inputs.
    ///
    /// Test shape (also documented in the module-level comment above):
    ///
    /// 1. Init a fresh repo. Stage a non-trivial nested tree
    ///    (README.md, files.csv, `files/dir_<n>/file<i>.txt`) — exercises
    ///    multi-vnode dirs, multi-level recursion, and mixed file/dir-ref
    ///    entries inside vnodes.
    /// 2. Build the merkle-write inputs ONCE, with a pinned timestamp so
    ///    `commit_id` is identical between runs.
    /// 3. Run the previous node writing → snapshot deserialized contents
    ///    of `tree/nodes/`.
    /// 4. Wipe `tree/nodes/`.
    /// 5. Run the updated node writing → snapshot deserialized contents.
    /// 6. Assert both snapshots have identical hash sets and identical
    ///    per-node `(parent_id, EMerkleTreeNode, children)` triples.
    ///
    /// This is **structural** equivalence, not byte equivalence — see the
    /// module-level comment above and `snapshot_tree_nodes`'s doc for why
    /// byte-by-byte comparison is intentionally not done. The merkle store
    /// content as observed via `MerkleStore::get_node` / `get_children` is
    /// what callers actually consume, and this test pins that.
    #[tokio::test]
    async fn updated_node_writing_structurally_equivalent_to_previous() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let repo = repositories::init::init(dir)?;
            // Stage a non-trivial nested tree (README.md, files.csv, files/dir_*/file*.txt)
            // so the test exercises the recursion: multiple dirs, multiple vnodes
            // per dir, mixed file/dir entries inside vnodes.
            test::add_n_files_m_dirs(&repo, 12, 3).await?;

            // Read the staged dir_entries map (mirrors the start of `commit_with_cfg`).
            let opts = db::key_val::opts::default();
            let staged_db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
            let staged_db: DBWithThreadMode<SingleThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&staged_db_path))?;
            let progress = ProgressBar::hidden();
            let (dir_entries, _total) = status::read_staged_entries(&repo, &staged_db, &progress)?;
            drop(staged_db);

            // Build the merkle-write inputs once — both runs must see the
            // SAME inputs so any difference observed below is attributable to
            // the previous vs updated write logic, not to a difference in
            // what was asked of either implementation. Mirrors the input-prep
            // portion of `commit_dir_entries_new`. The timestamp is pinned to
            // a fixed Unix epoch so `commit_id` is identical across runs.
            let new_commit = NewCommitBody {
                message: "regression test".to_string(),
                author: "test".to_string(),
                email: "test@test.com".to_string(),
            };
            let existing_nodes: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
            let vnode_entries =
                split_into_vnodes(&repo, &dir_entries, &existing_nodes, &new_commit)?;
            let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
            let new_commit_data =
                create_commit_data(&repo, &new_commit.message, timestamp, vec![], &new_commit)?;
            let commit_id = compute_commit_id(&new_commit_data)?;
            let node = CommitNode::new(
                &repo,
                CommitNodeOpts {
                    hash: commit_id,
                    parent_ids: vec![],
                    email: new_commit_data.email.clone(),
                    author: new_commit_data.author.clone(),
                    message: new_commit.message.clone(),
                    timestamp,
                },
            )?;
            let commit_id_string = format!("{commit_id}");
            let dir_hash_db_path =
                CommitMerkleTree::dir_hash_db_path_from_commit_id(&repo, &commit_id_string);
            let dir_hashes: HashMap<PathBuf, MerkleHash> = HashMap::new();

            // ── Run 1: previous node writing ───────────────────────────────────
            {
                let dir_hash_db: DBWithThreadMode<SingleThreaded> =
                    DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;
                let store = repo.merkle_store()?;
                let session = store.begin()?;
                let mut commit_ns = session.create_node(&node, None)?;
                write_commit_entries_previous(
                    &repo,
                    commit_id,
                    &*session,
                    &mut *commit_ns,
                    &dir_hash_db,
                    &dir_hashes,
                    &vnode_entries,
                )?;
                commit_ns.finish()?;
                session.finish()?;
            }
            let snap_previous = snapshot_tree_nodes(&repo)?;
            wipe_tree_nodes(&repo)?;

            // ── Run 2: updated node writing (production) ──────────────────────
            {
                let dir_hash_db: DBWithThreadMode<SingleThreaded> =
                    DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;
                let store = repo.merkle_store()?;
                let session = store.begin()?;
                let mut commit_ns = session.create_node(&node, None)?;
                write_commit_entries(
                    &repo,
                    commit_id,
                    &*session,
                    &mut *commit_ns,
                    &dir_hash_db,
                    &dir_hashes,
                    &vnode_entries,
                )?;
                commit_ns.finish()?;
                session.finish()?;
            }
            let snap_updated = snapshot_tree_nodes(&repo)?;

            // ── Compare ────────────────────────────────────────────────────────
            //
            // Both `snap_previous` and `snap_updated` are the deserialized merkle store
            // content (see `snapshot_tree_nodes` doc) — i.e. the result of
            // a full serialize → write → read → deserialize round-trip via
            // `MerkleStore`. Equality below is therefore equality of what
            // any consumer of the merkle store would observe, not equality
            // of the on-disk msgpack bytes (which the module-level comment
            // explains can legitimately differ in HashMap key ordering).
            assert!(
                !snap_previous.is_empty(),
                "expected non-empty previous-node-writing snapshot — \
                 if this fails, the test is no longer exercising the write path",
            );

            // 1. Same set of node hashes ⟹ same set of per-node directories
            //    under `tree/nodes/<prefix>/<suffix>/`. Each node hash is
            //    content-addressed (computed from raw fields, not msgpack),
            //    so this assertion catches any divergence in the field
            //    values that go into hashing — even before we look at the
            //    deserialized bodies.
            let keys_previous: Vec<&MerkleHash> = snap_previous.keys().collect();
            let keys_updated: Vec<&MerkleHash> = snap_updated.keys().collect();
            assert_eq!(
                keys_previous, keys_updated,
                "tree/nodes/ hash sets differ between previous and updated node writing",
            );

            // 2. Per-node round-tripped equivalence: for every node, the
            //    `parent_id` recorded in its on-disk header, the
            //    msgpack-decoded `EMerkleTreeNode`, and the
            //    msgpack-decoded children list must all match.
            //
            //    `EMerkleTreeNode` (and every variant: `DirNode`, `VNode`,
            //    `FileNode`, ...) derives `PartialEq` / `Eq`. Equality on
            //    `HashMap` fields inside (e.g. `DirNode::data_type_counts`,
            //    `data_type_sizes`) is by membership, not iteration order —
            //    so this assertion catches every real divergence in the
            //    merkle store contents while remaining robust to the
            //    msgpack-key-order non-determinism of the write side.
            for (hash, snap_previous_entry) in &snap_previous {
                let snap_updated_entry = snap_updated.get(hash).expect(
                    "hash present in previous-node-writing snapshot but missing in updated",
                );
                assert_eq!(
                    snap_previous_entry,
                    snap_updated_entry,
                    "previous vs updated node writing diverged for node {} \
                     after serialize → write → read → deserialize round-trip",
                    hash.to_hex_hash(),
                );
            }
            Ok(())
        })
        .await
    }
}
