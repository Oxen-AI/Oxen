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
use crate::core::refs::RefWriter;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::core::v0_19_0::index::MerkleNodeDB;
use crate::core::v0_19_0::status;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::VNode;
use crate::model::MerkleHash;
use crate::model::MerkleTreeNodeType;
use crate::model::NewCommit;
use crate::model::NewCommitBody;
use crate::model::User;
use crate::model::{Commit, LocalRepository, StagedEntryStatus};

use crate::{repositories, util};
use std::str::FromStr;

use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::merkle_tree::node::{CommitNode, DirNode};

#[derive(Clone)]
pub struct EntryVNode {
    pub id: MerkleHash,
    pub entries: Vec<StagedMerkleTreeNode>,
}

impl EntryVNode {
    pub fn new(id: MerkleHash) -> Self {
        EntryVNode {
            id,
            entries: vec![],
        }
    }
}

pub fn commit(repo: &LocalRepository, message: impl AsRef<str>) -> Result<Commit, OxenError> {
    let cfg = UserConfig::get()?;
    commit_with_cfg(repo, message, &cfg, None)
}

pub fn commit_with_parent_ids(
    repo: &LocalRepository,
    message: impl AsRef<str>,
    parent_ids: Vec<String>,
) -> Result<Commit, OxenError> {
    let cfg = UserConfig::get()?;
    commit_with_cfg(repo, message, &cfg, Some(parent_ids))
}

pub fn commit_with_user(
    repo: &LocalRepository,
    message: impl AsRef<str>,
    user: &User,
) -> Result<Commit, OxenError> {
    let cfg = UserConfig {
        name: user.name.clone(),
        email: user.email.clone(),
    };
    commit_with_cfg(repo, message, &cfg, None)
}

pub fn commit_with_cfg(
    repo: &LocalRepository,
    message: impl AsRef<str>,
    cfg: &UserConfig,
    parent_ids: Option<Vec<String>>,
) -> Result<Commit, OxenError> {
    // time the commit
    let start_time = Instant::now();
    let message = message.as_ref();

    // Read the staged files from the staged db
    let opts = db::key_val::opts::default();
    let staged_db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    log::debug!("commit_with_cfg staged db path: {:?}", staged_db_path);
    let staged_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&staged_db_path))?;

    let commit_progress_bar = ProgressBar::new_spinner();
    commit_progress_bar.set_style(ProgressStyle::default_spinner());
    commit_progress_bar.enable_steady_tick(Duration::from_millis(100));

    // Read all the staged entries
    let (dir_entries, total_changes) =
        status::read_staged_entries(repo, &staged_db, &commit_progress_bar)?;
    commit_progress_bar.set_message(format!("Committing {} changes", total_changes));

    log::debug!("got dir entries: {:?}", dir_entries.len());

    if dir_entries.is_empty() {
        return Err(OxenError::basic_str("No changes to commit"));
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
        commit_dir_entries_with_parents(
            repo,
            parent_ids,
            dir_entries,
            &new_commit,
            staged_db,
            &commit_progress_bar,
        )?
    } else {
        commit_dir_entries_new(
            repo,
            dir_entries,
            &new_commit,
            staged_db,
            &commit_progress_bar,
        )?
    };

    // Write HEAD file and update branch
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);
    log::debug!("Looking for HEAD file at {:?}", head_path);
    let ref_writer = RefWriter::new(repo)?;
    let commit_id = commit.id.to_owned();
    let branch_name = maybe_branch_name.unwrap_or(DEFAULT_BRANCH_NAME.to_string());
    if !head_path.exists() {
        log::debug!("HEAD file does not exist, creating new branch");
        ref_writer.set_head(&branch_name);
        ref_writer.set_branch_commit_id(&branch_name, &commit_id)?;
    }
    ref_writer.set_head_commit_id(&commit_id)?;

    // Print that we finished

    println!(
        "🐂 commit {} in {}",
        commit.id,
        humantime::format_duration(Duration::from_millis(
            start_time.elapsed().as_millis() as u64
        ))
    );

    
    Ok(commit)
}

pub fn commit_dir_entries_with_parents(
    repo: &LocalRepository,
    parent_commits: Vec<String>,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    new_commit: &NewCommitBody,
    staged_db: DBWithThreadMode<SingleThreaded>,
    commit_progress_bar: &ProgressBar,
) -> Result<Commit, OxenError> {
    let message = &new_commit.message;
    // if the HEAD file exists, we have parents
    // otherwise this is the first commit
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);

    let maybe_head_commit = if head_path.exists() {
        let commit = repositories::commits::head_commit(repo)?;
        Some(commit)
    } else {
        None
    };

    let directories = dir_entries
        .keys()
        .map(|path| path.to_path_buf())
        .collect::<Vec<_>>();

    let mut existing_nodes: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
    if let Some(commit) = &maybe_head_commit {
        existing_nodes = CommitMerkleTree::load_nodes(repo, commit, &directories)?;
    }

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

    let node = CommitNode {
        hash: commit_id,
        parent_ids: parent_hashes,
        message: message.to_string(),
        author: new_commit.author.clone(),
        email: new_commit.email.clone(),
        timestamp,
        ..Default::default()
    };

    let opts = db::key_val::opts::default();
    let dir_hash_db_path = CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, commit_id);
    let dir_hash_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;

    // Copy over the dir hashes from the previous commit
    let mut parent_id: Option<MerkleHash> = None;
    if let Some(commit) = &maybe_head_commit {
        parent_id = Some(commit.hash()?);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        for (path, hash) in dir_hashes {
            if let Some(path_str) = path.to_str() {
                str_val_db::put(&dir_hash_db, path_str, &hash.to_string())?;
            } else {
                log::error!("Failed to convert path to string: {:?}", path);
            }
        }
    }

    let mut commit_db = MerkleNodeDB::open_read_write(repo, &node, parent_id)?;
    write_commit_entries(
        repo,
        &maybe_head_commit,
        commit_id,
        &mut commit_db,
        &dir_hash_db,
        &vnode_entries,
    )?;
    commit_progress_bar.finish_and_clear();

    // Clear the staged db
    // Close the connection before removing the staged db
    let staged_db_path = staged_db.path().to_owned();
    drop(staged_db);

    // Clear the staged db
    util::fs::remove_dir_all(staged_db_path)?;
    Ok(node.to_commit())
}

pub fn commit_dir_entries_new(
    repo: &LocalRepository,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    new_commit: &NewCommitBody,
    staged_db: DBWithThreadMode<SingleThreaded>,
    commit_progress_bar: &ProgressBar,
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

    let mut existing_nodes: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
    if let Some(commit) = &maybe_head_commit {
        existing_nodes = CommitMerkleTree::load_nodes(repo, commit, &directories)?;
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

    let node = CommitNode {
        hash: commit_id,
        parent_ids: new_commit
            .parent_ids
            .iter()
            .map(|id: &String| MerkleHash::from_str(id).unwrap())
            .collect(),
        message: message.to_string(),
        author: new_commit.author.clone(),
        email: new_commit.email.clone(),
        timestamp,
        ..Default::default()
    };

    let opts = db::key_val::opts::default();
    let dir_hash_db_path = CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, commit_id);
    let dir_hash_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;

    // Copy over the dir hashes from the previous commit
    let mut parent_id: Option<MerkleHash> = None;
    if let Some(commit) = &maybe_head_commit {
        parent_id = Some(commit.hash()?);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        for (path, hash) in dir_hashes {
            if let Some(path_str) = path.to_str() {
                str_val_db::put(&dir_hash_db, path_str, &hash.to_string())?;
            } else {
                log::error!("Failed to convert path to string: {:?}", path);
            }
        }
    }

    let mut commit_db = MerkleNodeDB::open_read_write(repo, &node, parent_id)?;

    write_commit_entries(
        repo,
        &maybe_head_commit,
        commit_id,
        &mut commit_db,
        &dir_hash_db,
        &vnode_entries,
    )?;

    commit_progress_bar.finish_and_clear();

    // Remove all the directories that are staged for removal
    cleanup_rm_dirs(&dir_hash_db, &dir_entries)?;

    // Close the connection before removing the staged db
    let staged_db_path = staged_db.path().to_owned();
    drop(staged_db);

    // Clear the staged db
    util::fs::remove_dir_all(staged_db_path)?;

    Ok(node.to_commit())
}

pub fn commit_dir_entries(
    repo: &LocalRepository,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    new_commit: &NewCommitBody,
    commit_progress_bar: &ProgressBar,
) -> Result<Commit, OxenError> {

    log::debug!("commit_dir_entries got {} entries", dir_entries.len());
    for (path, entries) in &dir_entries {
        log::debug!(
            "commit_dir_entries entry {:?} with {} nodes",
            path,
            entries.len()
        );
    }

    let message = &new_commit.message;
    // if the HEAD file exists, we have parents
    // otherwise this is the first commit
    let head_path = util::fs::oxen_hidden_dir(&repo.path).join(HEAD_FILE);

    let maybe_head_commit = if head_path.exists() {
        let commit = repositories::commits::head_commit(repo)?;
        Some(commit)
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

    let mut existing_nodes: HashMap<PathBuf, MerkleTreeNode> = HashMap::new();
    if let Some(commit) = &maybe_head_commit {
        existing_nodes = CommitMerkleTree::load_nodes(repo, commit, &directories)?;
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

    let node = CommitNode {
        hash: commit_id,
        parent_ids,
        message: message.to_string(),
        author: new_commit.author.clone(),
        email: new_commit.email.clone(),
        timestamp,
        ..Default::default()
    };

    let opts = db::key_val::opts::default();
    let dir_hash_db_path = CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, commit_id);
    let dir_hash_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&dir_hash_db_path))?;

    // Copy over the dir hashes from the previous commit
    let mut parent_id: Option<MerkleHash> = None;
    if let Some(commit) = &maybe_head_commit {
        parent_id = Some(commit.hash()?);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        log::debug!(
            "Copy over {} dir hashes from previous commit {}",
            dir_hashes.len(),
            commit
        );
        for (path, hash) in dir_hashes {
            if let Some(path_str) = path.to_str() {
                str_val_db::put(&dir_hash_db, path_str, &hash.to_string())?;
            } else {
                log::error!("Failed to convert path to string: {:?}", path);
            }
        }
    }

    let mut commit_db = MerkleNodeDB::open_read_write(repo, &node, parent_id)?;
    write_commit_entries(
        repo,
        &maybe_head_commit,
        commit_id,
        &mut commit_db,
        &dir_hash_db,
        &vnode_entries,
    )?;
    commit_progress_bar.finish_and_clear();

    // Remove all the directories that are staged for removal
    cleanup_rm_dirs(&dir_hash_db, &dir_entries)?;

    Ok(node.to_commit())
}

fn cleanup_rm_dirs(
    dir_hash_db: &DBWithThreadMode<SingleThreaded>,
    dir_entries: &HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
) -> Result<(), OxenError> {
    for (path, entries) in dir_entries.iter() {
        for entry in entries.iter() {
            if let EMerkleTreeNode::Directory(dir_node) = &entry.node.node {
                if entry.status == StagedEntryStatus::Removed {
                    let dir_path = path.join(&dir_node.name);
                    log::debug!("dir path for cleanup: {dir_path:?}");
                    let key = dir_path.to_str().unwrap();
                    dir_hash_db.delete(key)?;
                }
            }
        }
    }
    Ok(())
}

fn node_data_to_staged_node(
    base_dir: impl AsRef<Path>,
    node: &MerkleTreeNode,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let base_dir = base_dir.as_ref();
    match node.node.dtype() {
        MerkleTreeNodeType::Dir => {
            let mut dir_node = node.dir()?;
            let path = base_dir.join(dir_node.name);
            dir_node.name = path.to_str().unwrap().to_string();
            Ok(Some(StagedMerkleTreeNode {
                status: StagedEntryStatus::Unmodified,
                node: MerkleTreeNode::from_dir(dir_node),
            }))
        }
        MerkleTreeNodeType::File => {
            let mut file_node = node.file()?;
            let path = base_dir.join(file_node.name);
            file_node.name = path.to_str().unwrap().to_string();
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
    let dir_children = CommitMerkleTree::node_files_and_folders(node)?;
    let children = dir_children
        .into_iter()
        .flat_map(|child| node_data_to_staged_node(&base_dir, &child))
        .flatten()
        .collect();

    Ok(children)
}

// This should return the directory to vnode mapping that we need to update
fn split_into_vnodes(
    repo: &LocalRepository,
    entries: &HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    existing_nodes: &HashMap<PathBuf, MerkleTreeNode>,
    new_commit: &NewCommitBody,
) -> Result<HashMap<PathBuf, Vec<EntryVNode>>, OxenError> {
    let mut results: HashMap<PathBuf, Vec<EntryVNode>> = HashMap::new();

    log::debug!("split_into_vnodes new_commit: {:?}", new_commit.message);
    log::debug!("split_into_vnodes entries keys: {:?}", entries.keys());
    log::debug!(
        "split_into_vnodes existing_nodes keys: {:?}",
        existing_nodes.keys()
    );

    // Create the VNode buckets per directory
    for (directory, new_children) in entries {
        let mut children = HashSet::new();

        // Lookup children in the existing merkle tree
        if let Some(existing_node) = existing_nodes.get(directory) {
            log::debug!("got existing node for {:?}", directory);
            children = get_node_dir_children(directory, existing_node)?;
            log::debug!(
                "got {} existing children for dir {:?}",
                children.len(),
                directory
            );
        } else {
            log::debug!("no existing node for {:?}", directory);
        };

        log::debug!("new_children {}", new_children.len());

        // Update the children with the new entries from status
        for child in new_children.iter() {
            log::debug!(
                "new_child {:?} {:?}",
                child.node.node.dtype(),
                child.node.maybe_path()
            );

            // Overwrite the existing child
            // if add or modify, replace the child
            // if remove, remove the child
            if let Ok(path) = child.node.maybe_path() {
                if path != PathBuf::from("") {
                    match child.status {
                        StagedEntryStatus::Removed => {
                            log::debug!(
                                "removing child {:?} {:?} with {:?} {:?}",
                                child.node.node.dtype(),
                                path,
                                child.node.node.dtype(),
                                child.node.maybe_path().unwrap()
                            );
                            children.remove(child);
                        }
                        _ => {
                            log::debug!(
                                "replacing child {:?} {:?} with {:?} {:?}",
                                child.node.node.dtype(),
                                path,
                                child.node.node.dtype(),
                                child.node.maybe_path().unwrap()
                            );
                            children.replace(child.clone());
                        }
                    }
                }
            }
        }

        // Log the children
        for child in children.iter() {
            log::debug!(
                "child populated {:?} {:?} status {:?}",
                child.node.node.dtype(),
                child.node.maybe_path().unwrap(),
                child.status
            );
        }

        // Compute number of vnodes based on the repo's vnode size and number of children
        let total_children = children.len();
        let vnode_size = repo.vnode_size();
        let num_vnodes = (total_children as f32 / vnode_size as f32).ceil() as u128;

        // Antoher way to do it would be log2(N / 10000) if we wanted it to scale more logarithmically
        // let num_vnodes = (total_children as f32 / 10000_f32).log2();
        // let num_vnodes = 2u128.pow(num_vnodes.ceil() as u32);
        log::debug!(
            "{} VNodes for {} children in {:?} with vnode size {}",
            num_vnodes,
            total_children,
            directory,
            vnode_size
        );
        let mut vnode_children: Vec<EntryVNode> =
            vec![EntryVNode::new(MerkleHash::new(0)); num_vnodes as usize];

        // Split entries into vnodes
        for child in children.into_iter() {
            let bucket = child.node.hash.to_u128() % num_vnodes;
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
            // generate a uuid for the vnode
            vnode_hasher.update(directory.to_str().unwrap().as_bytes());

            let mut has_new_entries = false;
            for entry in vnode.entries.iter() {
                if let EMerkleTreeNode::File(file_node) = &entry.node.node {
                    vnode_hasher.update(&file_node.combined_hash.to_le_bytes());
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
        results.insert(directory.to_owned(), vnode_children);
    }

    // Make sure to update all the vnode ids based on all their children

    // TODO: We have to start from the bottom vnodes in the tree and update all the vnode ids above it
    log::debug!(
        "split_into_vnodes results: {:?} for commit {}",
        results.len(),
        new_commit.message
    );
    for (dir, vnodes) in results.iter_mut() {
        log::debug!("dir {:?} has {} vnodes", dir, vnodes.len());
        for vnode in vnodes.iter_mut() {
            log::debug!("  vnode {} has {} entries", vnode.id, vnode.entries.len());
            for entry in vnode.entries.iter() {
                log::debug!(
                    "    entry {:?} [{}] `{:?}` with status {:?}",
                    entry.node.node.dtype(),
                    entry.node.node.hash(),
                    entry.node.maybe_path(),
                    entry.status
                );
            }
        }
    }

    Ok(results)
}

fn compute_commit_id(new_commit: &NewCommit) -> Result<MerkleHash, OxenError> {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"commit");
    hasher.update(format!("{:?}", new_commit.parent_ids).as_bytes());
    hasher.update(new_commit.message.as_bytes());
    hasher.update(new_commit.author.as_bytes());
    hasher.update(new_commit.email.as_bytes());
    hasher.update(&new_commit.timestamp.unix_timestamp().to_le_bytes());
    Ok(MerkleHash::new(hasher.digest128()))
}

fn write_commit_entries(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    commit_id: MerkleHash,
    commit_db: &mut MerkleNodeDB,
    dir_hash_db: &DBWithThreadMode<SingleThreaded>,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
) -> Result<(), OxenError> {
    // Write the root dir, then recurse into the vnodes and subdirectories
    let mut total_written = 0;
    let root_path = PathBuf::from("");
    let dir_node = compute_dir_node(repo, maybe_head_commit, commit_id, entries, &root_path)?;
    commit_db.add_child(&dir_node)?;
    total_written += 1;

    str_val_db::put(
        dir_hash_db,
        root_path.to_str().unwrap(),
        &dir_node.hash.to_string(),
    )?;
    let dir_db = MerkleNodeDB::open_read_write(repo, &dir_node, Some(commit_id))?;
    r_create_dir_node(
        repo,
        maybe_head_commit,
        commit_id,
        &mut Some(dir_db),
        dir_hash_db,
        entries,
        root_path,
        &mut total_written,
    )?;

    log::debug!("R CREATE OVER LOL");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn r_create_dir_node(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    commit_id: MerkleHash,
    maybe_dir_db: &mut Option<MerkleNodeDB>,
    dir_hash_db: &DBWithThreadMode<SingleThreaded>,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    path: impl AsRef<Path>,
    total_written: &mut u64,
) -> Result<(), OxenError> {
    let path = path.as_ref().to_path_buf();

    let keys = entries.keys();
    log::debug!("r_create_dir_node path {:?} keys: {:?}", path, keys);

    let Some(vnodes) = entries.get(&path) else {
        log::debug!(
            "r_create_dir_node No entries found for directory {:?}",
            path
        );
        return Ok(());
    };

    log::debug!("Processing dir {:?} with {} vnodes", path, vnodes.len());
    for vnode in vnodes.iter() {
        let vnode_obj = VNode {
            hash: vnode.id,
            ..Default::default()
        };
        if let Some(dir_db) = maybe_dir_db {
            dir_db.add_child(&vnode_obj)?;
            *total_written += 1;
        }

        // log::debug!(
        //     "Processing vnode {} with {} entries",
        //     vnode.id,
        //     vnode.entries.len()
        // );

        // Maybe because we don't need to overwrite vnode dbs that already exist,
        // but still need to recurse and create the children
        // let mut maybe_vnode_db = MerkleNodeDB::open_read_write_if_not_exists(
        //     repo,
        //     &vnode_obj,
        //     maybe_dir_db.as_ref().map(|db| db.node_id),
        // )?;
        let mut vnode_db = MerkleNodeDB::open_read_write(
            repo,
            &vnode_obj,
            maybe_dir_db.as_ref().map(|db| db.node_id),
        )?;
        for entry in vnode.entries.iter() {
            // log::debug!("Processing entry {} in vnode {}", entry.node, vnode.id);
            match &entry.node.node {
                EMerkleTreeNode::Directory(node) => {
                    // If the dir has updates, we need a new dir db
                    let dir_path = entry.node.maybe_path().unwrap();
                    // log::debug!("Processing dir node {:?}", dir_path);
                    let dir_node = if entries.contains_key(&dir_path) {
                        let dir_node = compute_dir_node(
                            repo,
                            maybe_head_commit,
                            commit_id,
                            entries,
                            &dir_path,
                        )?;

                        // if let Some(vnode_db) = &mut maybe_vnode_db {
                        vnode_db.add_child(&dir_node)?;
                        *total_written += 1;
                        // }

                        // if the vnode is new, we need a new dir db
                        // let mut child_db = if maybe_vnode_db.is_some() {
                        let mut child_db = Some(MerkleNodeDB::open_read_write(
                            repo,
                            &dir_node,
                            Some(vnode.id),
                        )?);
                        // } else {
                        //     // Otherwise, check if the dir is new before opening a new db
                        //     MerkleNodeDB::open_read_write_if_not_exists(
                        //         repo,
                        //         &dir_node,
                        //         Some(vnode.id),
                        //     )?
                        // };

                        r_create_dir_node(
                            repo,
                            maybe_head_commit,
                            commit_id,
                            &mut child_db,
                            dir_hash_db,
                            entries,
                            &dir_path,
                            total_written,
                        )?;
                        dir_node
                    } else {
                        // log::debug!("r_create_dir_node skipping {:?}", dir_path);
                        // Look up the old dir node and reference it
                        let Some(old_dir_node) =
                            CommitMerkleTree::read_node(repo, &node.hash, false)?
                        else {
                            // log::debug!(
                            //     "r_create_dir_node could not read old dir node {:?}",
                            //     node.hash
                            // );
                            continue;
                        };
                        let dir_node = old_dir_node.dir()?;

                        // if let Some(vnode_db) = &mut maybe_vnode_db {
                        vnode_db.add_child(&dir_node)?;
                        *total_written += 1;
                        // }
                        dir_node
                    };

                    // Write the dir hash to the dir_hashes db or delete it if it has children
                    // log::debug!(
                    //     "dir entry has {} children {:?} ",
                    //     entry.node.children.len(),
                    //     dir_path
                    // );
                    // if entry.node.children.is_empty() {
                    //     str_val_db::delete(dir_hash_db, dir_path.to_str().unwrap())?;
                    // } else {
                    str_val_db::put(
                        dir_hash_db,
                        dir_path.to_str().unwrap(),
                        &dir_node.hash.to_string(),
                    )?;
                    // }
                }
                EMerkleTreeNode::File(file_node) => {
                    let mut file_node = file_node.clone();
                    let file_path = PathBuf::from(&file_node.name);
                    let file_name = file_path.file_name().unwrap().to_str().unwrap();

                    // log::debug!(
                    //     "Processing file {:?} in vnode {} in commit {}",
                    //     path,
                    //     vnode.id,
                    //     commit_id
                    // );

                    // Just single file chunk for now
                    let chunks = vec![file_node.hash.to_u128()];
                    file_node.chunk_hashes = chunks;
                    file_node.last_commit_id = if entry.status == StagedEntryStatus::Unmodified {
                        file_node.last_commit_id
                    } else {
                        commit_id
                    };
                    file_node.name = file_name.to_string();

                    // if let Some(vnode_db) = &mut maybe_vnode_db {
                    // log::debug!(
                    //     "Adding file {} to vnode {} in commit {}",
                    //     file_name,
                    //     vnode.id,
                    //     commit_id
                    // );
                    vnode_db.add_child(&file_node)?;
                    *total_written += 1;
                    // }
                }
                _ => {
                    return Err(OxenError::basic_str(format!(
                        "r_create_dir_node found unexpected node type: {:?}",
                        entry.node
                    )));
                }
            }
        }
    }

    log::debug!(
        "Finished processing dir {:?} total written {} entries",
        path,
        total_written
    );

    Ok(())
}

fn get_children(
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
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
    maybe_head_commit: &Option<Commit>,
    commit_id: MerkleHash,
    entries: &HashMap<PathBuf, Vec<EntryVNode>>,
    path: impl AsRef<Path>,
) -> Result<DirNode, OxenError> {
    let path = path.as_ref().to_path_buf();
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.update(b"dir");
    hasher.update(path.to_str().unwrap().as_bytes());

    let mut num_bytes = 0;
    let mut data_type_counts: HashMap<String, u64> = HashMap::new();
    let mut data_type_sizes: HashMap<String, u64> = HashMap::new();

    // Collect the previous commit counts
    if let Some(head_commit) = maybe_head_commit {
        if let Ok(Some(old_dir_node)) =
            CommitMerkleTree::dir_without_children(repo, head_commit, &path)
        {
            let old_dir_node = old_dir_node.dir().unwrap();
            num_bytes = old_dir_node.num_bytes;
            data_type_counts = old_dir_node.data_type_counts;
            data_type_sizes = old_dir_node.data_type_sizes;
        };
    }

    let children = get_children(entries, &path)?;
    log::debug!(
        "Aggregating dir {:?} for [{}] with {:?} children num_bytes {:?} data_type_counts {:?}",
        path,
        commit_id,
        children,
        num_bytes,
        data_type_counts
    );
    for child in children.iter() {
        let Some(vnodes) = entries.get(child) else {
            let err_msg = format!("compute_dir_node No entries found for directory {:?}", path);
            return Err(OxenError::basic_str(err_msg));
        };

        for vnode in vnodes.iter() {
            for entry in vnode.entries.iter() {
                log::debug!("Aggregating entry {}", entry);
                match &entry.node.node {
                    EMerkleTreeNode::Directory(node) => {
                        log::debug!("No need to aggregate dir {}", node.name);
                    }
                    EMerkleTreeNode::File(file_node) => {
                        log::debug!(
                            "Updating hash for file {} -> hash {}",
                            file_node.name,
                            file_node.hash
                        );
                        hasher.update(file_node.name.as_bytes());
                        hasher.update(&file_node.combined_hash.to_le_bytes());

                        match entry.status {
                            StagedEntryStatus::Added => {
                                num_bytes += file_node.num_bytes;
                                *data_type_counts
                                    .entry(file_node.data_type.to_string())
                                    .or_insert(0) += 1;
                                *data_type_sizes
                                    .entry(file_node.data_type.to_string())
                                    .or_insert(0) += file_node.num_bytes;
                            }
                            StagedEntryStatus::Removed => {
                                num_bytes -= file_node.num_bytes;
                                *data_type_counts
                                    .entry(file_node.data_type.to_string())
                                    .or_insert(1) -= 1;
                                *data_type_sizes
                                    .entry(file_node.data_type.to_string())
                                    .or_insert(0) -= file_node.num_bytes;
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
    }

    let hash = MerkleHash::new(hasher.digest128());
    let file_name = path.file_name().unwrap_or_default().to_str().unwrap();
    log::debug!(
        "Aggregated dir {:?} [{}] num_bytes {:?} data_type_counts {:?}",
        path,
        hash,
        num_bytes,
        data_type_counts
    );

    let node = DirNode {
        dtype: MerkleTreeNodeType::Dir,
        name: file_name.to_owned(),
        hash,
        num_bytes,
        last_commit_id: commit_id,
        last_modified_seconds: 0,
        last_modified_nanoseconds: 0,
        data_type_counts,
        data_type_sizes,
    };
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
    use std::collections::HashSet;
    use std::path::Path;

    use crate::core::v0_19_0::index::CommitMerkleTree;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::repositories;
    use crate::test;
    use crate::test::add_n_files_m_dirs;
    use crate::util;

    #[test]
    fn test_first_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 2)?;
            let status = repositories::status(&repo)?;
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
            assert_eq!(dir_node.unwrap().name, "");

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
    }

    #[test]
    fn test_commit_only_dirs_at_top_level() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("all_files/dir_0/new_file.txt");
            util::fs::create_dir_all(new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            repositories::add(&repo, &repo.path)?;

            let status = repositories::status(&repo)?;
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
    }

    #[test]
    fn test_commit_single_file_deep_in_dir() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Add a new file to files/dir_0/
            let new_file = repo.path.join("files/dir_0/new_file.txt");
            util::fs::create_dir_all(new_file.parent().unwrap())?;
            util::fs::write_to_path(&new_file, "New file")?;
            repositories::add(&repo, &new_file)?;

            let status = repositories::status(&repo)?;
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
    }

    #[test]
    fn test_2nd_commit_keeps_num_bytes_and_data_type_counts() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
            let status = repositories::status(&repo)?;
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
            repositories::add(&repo, &new_file)?;

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
    }

    #[test]
    fn test_second_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
            let status = repositories::status(&repo)?;
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
            repositories::add(&repo, &new_file)?;

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
            let updated_commit_id = updated_file_node.last_commit_id.to_string();
            assert_eq!(updated_commit_id, second_commit.id);

            // Make sure that last commit id is not updated on other files in the dir
            let other_file_node = second_tree.get_by_path(Path::new("files/dir_1/file7.txt"))?;
            assert!(other_file_node.is_some());
            let other_file_node = other_file_node.unwrap().file()?;
            let other_commit_id = other_file_node.last_commit_id.to_string();
            assert_eq!(other_commit_id, first_commit.id);

            // Make sure last commit is updated on the dir
            let dir_node = second_tree.get_by_path(Path::new("files/dir_1"))?;
            assert!(dir_node.is_some());
            let dir_node = dir_node.unwrap().dir()?;
            let dir_commit_id = dir_node.last_commit_id.to_string();
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
    }

    #[test]
    fn test_commit_configurable_vnode_size() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let mut repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;
            // Set the vnode size to 5
            repo.set_vnode_size(5);

            // Write data to the repo, 23 files in 2 dirs
            add_n_files_m_dirs(&repo, 23, 2)?;
            let status = repositories::status(&repo)?;
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

            // Add a news files
            for i in 0..10 {
                let dir_num = i % 2;
                let new_file = repo
                    .path
                    .join(format!("files/dir_{}/new_file_{}.txt", dir_num, i));
                util::fs::write_to_path(&new_file, format!("New fileeeee {}", i))?;
                repositories::add(&repo, &new_file)?;
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

            Ok(())
        })
    }

    #[test]
    fn test_commit_20_files_6_vnode_size() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let mut repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;
            // Set the vnode size to 6
            repo.set_vnode_size(6);

            // Write data to the repo, 20 files in 1 dir
            add_n_files_m_dirs(&repo, 20, 1)?;
            let status = repositories::status(&repo)?;
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
            repositories::add(&repo, &new_file)?;

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
    }

    #[test]
    fn test_third_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
            let status = repositories::status(&repo)?;
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
            repositories::add(&repo, &new_file)?;

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
            repositories::add(&repo, &new_file)?;

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
                println!("dir_hash: {:?} {}", path, hash);
                let node = third_tree.get_by_path(&path)?.unwrap();
                assert_eq!(node.hash, hash);
            }

            Ok(())
        })
    }
}
