use crate::core;
use crate::core::db::merkle_node::MerkleNodeDB;
use crate::error::OxenError;
use crate::model::entry::metadata_entry::WorkspaceMetadataEntry;
use crate::model::merkle_tree::node::{DirNode, EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::metadata::MetadataDir;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{
    Commit, CommitEntry, EntryDataType, LocalRepository, MerkleHash, MetadataEntry, ParsedResource,
};
use crate::opts::{PaginateOpts, SortBy, SortOpts};

use crate::repositories;
use crate::util;
use crate::view::PaginatedDirEntries;
use crate::view::entries::{EMetadataEntry, ResourceVersion};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;

use super::index::CommitMerkleTree;

pub fn get_directory(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<DirNode>, OxenError> {
    // Read just the directory node: its num_bytes and data_type_counts are stored on the node
    // itself, so its children (which the DirNode conversion drops) are never loaded.
    let node = match CommitMerkleTree::dir_node_only(repo, commit, path) {
        Ok(node) => node,
        Err(e) => {
            log::warn!("Error getting node by path: {e:?}");
            return Ok(None);
        }
    };
    let Some(node) = node else {
        return Ok(None);
    };
    Ok(Some(node.dir()?))
}

pub fn get_file(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    let file_node = repositories::tree::get_file_by_path(repo, commit, path)?;
    Ok(file_node)
}

pub fn list_directory(
    repo: &LocalRepository,
    directory: &Path,
    parsed_resource: &ParsedResource,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    list_directory_with_depth(
        repo,
        directory,
        parsed_resource,
        paginate_opts,
        &SortOpts::default(),
        0,
    )
}

pub fn list_directory_with_depth(
    repo: &LocalRepository,
    directory: &Path,
    parsed_resource: &ParsedResource,
    paginate_opts: &PaginateOpts,
    sort_opts: &SortOpts,
    depth: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let _perf = crate::perf_guard!("core::entries::list_directory");

    let revision = parsed_resource.version.to_str().unwrap_or("").to_string();
    let page = paginate_opts.page_num;
    let page_size = paginate_opts.page_size;

    let resource = Some(ResourceVersion {
        path: directory.to_string_lossy().into_owned(),
        version: revision.clone(),
    });

    log::debug!("list_directory directory {directory:?} revision {revision:?} depth {depth}");

    let commit = parsed_resource
        .commit
        .clone()
        .ok_or_else(|| OxenError::RevisionNotFound(revision.into()))?;

    log::debug!("list_directory commit {commit}");

    let _perf_get_dir = crate::perf_guard!("core::entries::get_dir_with_children");
    let dir = repositories::tree::get_dir_with_children(repo, &commit, directory, None)?
        .ok_or_else(|| OxenError::resource_not_found(directory.to_string_lossy()))?;
    drop(_perf_get_dir);

    log::debug!("list_directory dir {dir}");

    let EMerkleTreeNode::Directory(dir_node) = &dir.node else {
        return Err(OxenError::resource_not_found(directory.to_string_lossy()));
    };

    log::debug!("list_directory dir_node {dir_node}");

    // Found commits is used to cache the commits so that we don't have
    // to read them from disk again while looping over entries
    let mut found_commits: HashMap<MerkleHash, Option<Commit>> = HashMap::new();

    let _perf_dir_entry = crate::perf_guard!("core::entries::dir_node_to_metadata");
    let dir_entry =
        dir_node_to_metadata_entry(repo, dir_node, parsed_resource, &mut found_commits, false)?;
    let dir_entry = Some(EMetadataEntry::WorkspaceMetadataEntry(
        WorkspaceMetadataEntry::from_metadata_entry(dir_entry),
    ));
    drop(_perf_dir_entry);

    log::debug!("list_directory dir_entry {dir_entry:?}");

    let _perf_sort = crate::perf_guard!("core::entries::sort_children");
    let children = sorted_children(repo, &dir, sort_opts, &mut found_commits)?;
    log::debug!("list_directory got {} children", children.len());
    drop(_perf_sort);

    let _perf_paginate = crate::perf_guard!("core::entries::paginate_entries");
    let (children, pagination) = util::paginate(children, page, page_size);
    let metadata = Some(MetadataDir::new(dir_node.data_types()));
    drop(_perf_paginate);

    let _perf_entries = crate::perf_guard!("core::entries::collect_dir_entries");
    let entries = child_entries(
        repo,
        &children,
        directory,
        parsed_resource,
        &mut found_commits,
        sort_opts,
        depth,
    )?;
    drop(_perf_entries);

    let _perf_workspace = crate::perf_guard!("core::entries::populate_workspace_data");
    let entries: Vec<EMetadataEntry> = if let Some(workspace) = parsed_resource.workspace.as_ref() {
        repositories::workspaces::populate_entries_with_workspace_data(
            repo, directory, workspace, &entries,
        )?
    } else {
        entries
            .into_iter()
            .map(EMetadataEntry::MetadataEntry)
            .collect()
    };
    drop(_perf_workspace);

    Ok(PaginatedDirEntries {
        dir: dir_entry,
        entries,
        resource,
        metadata,
        page_size,
        page_number: page,
        total_pages: pagination.total_pages,
        total_entries: pagination.total_entries,
    })
}

pub fn get_meta_entry(
    repo: &LocalRepository,
    parsed_resource: &ParsedResource,
    path: &Path,
) -> Result<MetadataEntry, OxenError> {
    let commit = parsed_resource
        .commit
        .clone()
        .ok_or_else(|| OxenError::parsed_resource_not_found(parsed_resource.clone()))?;
    log::debug!("get_meta_entry path: {path:?} commit: {commit}");
    let node = repositories::tree::get_dir_without_children(repo, &commit, path, None)?;
    log::debug!("get_meta_entry node: {node:?}");

    if let Some(node) = node {
        log::debug!("get_meta_entry dir path found: {path:?}");
        dir_node_to_metadata_entry(
            repo,
            &node.dir()?,
            parsed_resource,
            &mut HashMap::new(),
            false,
        )
    } else {
        log::debug!("get_meta_entry file path: {path:?}");
        let file_node = get_file(repo, &commit, path)?;
        if let Some(file_node) = file_node {
            file_node_to_metadata_entry(repo, &file_node, parsed_resource, &mut HashMap::new())
        } else {
            log::debug!("get_meta_entry path not found: {path:?}");
            Err(OxenError::resource_not_found(path.to_string_lossy()))
        }
    }
}

#[derive(Clone, Copy)]
enum Child<'a> {
    Dir(&'a DirNode),
    File(&'a FileNode),
}

impl Child<'_> {
    fn is_dir(&self) -> bool {
        matches!(self, Child::Dir(_))
    }

    fn name(&self) -> &str {
        match self {
            Child::Dir(dir) => dir.name(),
            Child::File(file) => file.name(),
        }
    }

    fn last_commit_id(&self) -> MerkleHash {
        match self {
            Child::Dir(dir) => *dir.last_commit_id(),
            Child::File(file) => *file.last_commit_id(),
        }
    }
}

/// Files and named directories directly inside `dir`, which must be loaded with its vnodes.
/// Vnodes are buckets of a directory's own children, so their children count as its own.
fn child_nodes(dir: &MerkleTreeNode) -> Vec<Child<'_>> {
    let mut children = Vec::new();
    let mut pending = vec![dir.children.iter()];
    while let Some(nodes) = pending.last_mut() {
        let Some(node) = nodes.next() else {
            pending.pop();
            continue;
        };
        match &node.node {
            EMerkleTreeNode::VNode(_) => pending.push(node.children.iter()),
            EMerkleTreeNode::Directory(child) if !child.name().is_empty() => {
                children.push(Child::Dir(child))
            }
            EMerkleTreeNode::File(file) => children.push(Child::File(file)),
            _ => {}
        }
    }
    children
}

/// The files and directories directly inside `dir`, in the order a listing shows them.
/// Sorting reads only each child's kind and name, plus the commit it last changed in when
/// sorting by date, so a caller can cut a page before building any entry.
fn sorted_children<'a>(
    repo: &LocalRepository,
    dir: &'a MerkleTreeNode,
    sort_opts: &SortOpts,
    found_commits: &mut HashMap<MerkleHash, Option<Commit>>,
) -> Result<Vec<Child<'a>>, OxenError> {
    let mut keyed = child_nodes(dir)
        .into_iter()
        .map(|child| {
            let timestamp = match sort_opts.sort_by {
                SortBy::Name => None,
                SortBy::Date => {
                    latest_commit(repo, child.last_commit_id(), child.name(), found_commits)?
                        .map(|commit| commit.timestamp)
                }
            };
            Ok((child, timestamp))
        })
        .collect::<Result<Vec<_>, OxenError>>()?;

    keyed.sort_by(|(a, a_timestamp), (b, b_timestamp)| {
        let field = match sort_opts.sort_by {
            SortBy::Name => a.name().cmp(b.name()),
            SortBy::Date => a_timestamp.cmp(b_timestamp),
        };
        let field = if sort_opts.reverse {
            field.reverse()
        } else {
            field
        };
        // Directories always come first
        b.is_dir().cmp(&a.is_dir()).then(field)
    });
    Ok(keyed.into_iter().map(|(child, _)| child).collect())
}

/// Each directory's entry holds its own sorted children, `depth` levels down.
fn child_entries(
    repo: &LocalRepository,
    children: &[Child],
    directory: &Path,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Option<Commit>>,
    sort_opts: &SortOpts,
    depth: usize,
) -> Result<Vec<MetadataEntry>, OxenError> {
    children
        .iter()
        .map(|child| match child {
            Child::File(file) => {
                file_node_to_metadata_entry(repo, file, parsed_resource, found_commits)
            }
            Child::Dir(dir) => {
                let mut entry =
                    dir_node_to_metadata_entry(repo, dir, parsed_resource, found_commits, true)?;
                if depth > 0 {
                    entry.children = nested_entries(
                        repo,
                        &directory.join(dir.name()),
                        parsed_resource,
                        found_commits,
                        sort_opts,
                        depth - 1,
                    )?;
                }
                Ok(entry)
            }
        })
        .collect()
}

/// The sorted entries at `path`, `depth` levels down, or None when it's missing or empty.
fn nested_entries(
    repo: &LocalRepository,
    path: &Path,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Option<Commit>>,
    sort_opts: &SortOpts,
    depth: usize,
) -> Result<Option<Vec<MetadataEntry>>, OxenError> {
    let Some(commit) = parsed_resource.commit.as_ref() else {
        let revision = parsed_resource.version.to_string_lossy().into_owned();
        return Err(OxenError::RevisionNotFound(revision.into()));
    };
    let Some(dir) = repositories::tree::get_dir_with_children(repo, commit, path, None)? else {
        return Ok(None);
    };

    let mut resource = parsed_resource.clone();
    resource.path = path.to_path_buf();
    resource.resource = parsed_resource.version.join(path);

    let children = sorted_children(repo, &dir, sort_opts, found_commits)?;
    let entries = child_entries(
        repo,
        &children,
        path,
        &resource,
        found_commits,
        sort_opts,
        depth,
    )?;
    Ok((!entries.is_empty()).then_some(entries))
}

/// The commit an entry last changed in, read once per commit across a listing.
/// A commit the repository doesn't have is logged and comes back as None.
fn latest_commit<'a>(
    repo: &LocalRepository,
    commit_id: MerkleHash,
    entry_name: &str,
    found_commits: &'a mut HashMap<MerkleHash, Option<Commit>>,
) -> Result<Option<&'a Commit>, OxenError> {
    match found_commits.entry(commit_id) {
        Entry::Occupied(found) => {
            let found: &'a Option<Commit> = found.into_mut();
            Ok(found.as_ref())
        }
        Entry::Vacant(slot) => {
            let _perf = crate::perf_guard!("core::entries::get_commit_by_hash");
            let commit = repositories::commits::get_by_hash(repo, &commit_id)?;
            if commit.is_none() {
                tracing::error!(
                    oxen.commit_id = %commit_id,
                    oxen.entry_name = %entry_name,
                    "Entry names a commit the repository does not have"
                );
            }
            let inserted: &'a Option<Commit> = slot.insert(commit);
            Ok(inserted.as_ref())
        }
    }
}

fn dir_node_to_metadata_entry(
    repo: &LocalRepository,
    dir_node: &DirNode,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Option<Commit>>,
    // Should append resource is because at the top level we don't want to append the resource
    // but when we recurse we do
    should_append_resource: bool,
) -> Result<MetadataEntry, OxenError> {
    let _perf = crate::perf_guard!("core::entries::dir_node_to_metadata_entry");

    let latest_commit = latest_commit(
        repo,
        *dir_node.last_commit_id(),
        dir_node.name(),
        found_commits,
    )?
    .cloned();
    let mut parsed_resource = parsed_resource.clone();
    if should_append_resource {
        parsed_resource.resource = parsed_resource.resource.join(dir_node.name());
        parsed_resource.path = parsed_resource.path.join(dir_node.name());
    }

    Ok(MetadataEntry {
        filename: dir_node.name().to_string(),
        hash: dir_node.hash().to_string(),
        is_dir: true,
        latest_commit,
        resource: Some(parsed_resource.into()),
        size: dir_node.num_bytes(),
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: "".to_string(),
        metadata: Some(GenericMetadata::MetadataDir(MetadataDir::new(
            dir_node.data_types(),
        ))),
        is_queryable: None,
        children: None,
    })
}

fn file_node_to_metadata_entry(
    repo: &LocalRepository,
    file_node: &FileNode,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Option<Commit>>,
) -> Result<MetadataEntry, OxenError> {
    let _perf = crate::perf_guard!("core::entries::file_node_to_metadata_entry");

    let latest_commit = latest_commit(
        repo,
        *file_node.last_commit_id(),
        file_node.name(),
        found_commits,
    )?
    .cloned();
    let data_type = file_node.data_type();

    let mut parsed_resource = parsed_resource.clone();
    // HACK for not knowing if we have the full path or just the dir path
    // so we just add the file name to the end of the path if it's not already there
    let mut file_path = parsed_resource.path.clone();
    if !file_path.ends_with(file_node.name()) {
        file_path = file_path.join(file_node.name());
        parsed_resource.resource = parsed_resource.resource.join(file_node.name());
        parsed_resource.path = parsed_resource.path.join(file_node.name());
    }

    let _perf_indexed = crate::perf_guard!("core::entries::check_if_indexed");
    let is_indexed = if *data_type == EntryDataType::Tabular {
        Some(
            core::v_latest::workspaces::data_frames::is_queryable_data_frame_indexed_from_file_node(
                repo, file_node, &file_path,
            )?,
        )
    } else {
        None
    };
    drop(_perf_indexed);

    Ok(MetadataEntry {
        filename: file_node.name().to_string(),
        hash: file_node.hash().to_string(),
        is_dir: false,
        latest_commit,
        resource: Some(parsed_resource.into()),
        size: file_node.num_bytes(),
        data_type: file_node.data_type().clone(),
        mime_type: file_node.mime_type().to_string(),
        extension: file_node.extension().to_string(),
        metadata: file_node.metadata(),
        is_queryable: is_indexed,
        children: None,
    })
}

pub fn list_tabular_files_in_repo(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<MetadataEntry>, OxenError> {
    let entries = repositories::tree::list_tabular_files_in_repo(repo, commit)?;
    let entries: Vec<FileNode> = entries.into_iter().collect();
    let entries: Vec<MetadataEntry> = entries
        .into_iter()
        .map(|node| MetadataEntry::from_file_node(repo, Some(node), commit).unwrap())
        .collect();
    Ok(entries)
}

pub fn count_for_commit(repo: &LocalRepository, commit: &Commit) -> Result<usize, OxenError> {
    let tree = repositories::tree::get_root_with_children(repo, commit)?
        .ok_or_else(|| OxenError::commit_id_does_not_exist(&commit.id))?;
    let (entries, _) = repositories::tree::list_files_and_dirs(&tree)?;
    Ok(entries.len())
}

pub fn list_for_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    let tree = repositories::tree::get_root_with_children(repo, commit)?
        .ok_or_else(|| OxenError::commit_id_does_not_exist(&commit.id))?;
    let (entries, _) = repositories::tree::list_files_and_dirs(&tree)?;
    Ok(entries
        .into_iter()
        .map(|entry| CommitEntry::from_file_node(&entry.file_node))
        .collect())
}

pub fn update_metadata(repo: &LocalRepository, revision: impl AsRef<str>) -> Result<(), OxenError> {
    let commit = repositories::revisions::get(repo, revision.as_ref())?
        .ok_or_else(|| OxenError::RevisionNotFound(revision.as_ref().to_string().into()))?;
    let tree: CommitMerkleTree = CommitMerkleTree::from_commit(repo, &commit)?;
    let mut node = tree.root;

    // Initialize data structures for aggregation
    let mut num_bytes = 0;

    // Start the recursive traversal
    traverse_and_update_sizes_and_counts(repo, &mut node, &mut num_bytes)?;

    Ok(())
}

#[allow(clippy::type_complexity)]
fn traverse_and_update_sizes_and_counts(
    repo: &LocalRepository,
    node: &mut MerkleTreeNode,
    num_bytes: &mut u64,
) -> Result<(HashMap<String, u64>, HashMap<String, u64>), OxenError> {
    let mut local_counts: HashMap<String, u64> = HashMap::new();
    let mut local_sizes: HashMap<String, u64> = HashMap::new();

    let children: &mut Vec<MerkleTreeNode> = &mut node.children;

    match &mut node.node {
        EMerkleTreeNode::Commit(commit_node) => {
            log::debug!("Traversing node {commit_node:?}");
            process_children(
                repo,
                children,
                &mut local_counts,
                &mut local_sizes,
                num_bytes,
            )?;
            let mut dir_db = MerkleNodeDB::open_read_write(
                repo.merkle_node_store(),
                commit_node,
                node.parent_id,
            )?;
            add_children_to_db(&mut dir_db, &node.children)?;
            dir_db.close()?;
        }
        EMerkleTreeNode::VNode(vnode) => {
            log::debug!("Traversing vnode {vnode:?}");
            process_children(
                repo,
                children,
                &mut local_counts,
                &mut local_sizes,
                num_bytes,
            )?;
            let mut dir_db =
                MerkleNodeDB::open_read_write(repo.merkle_node_store(), vnode, node.parent_id)?;
            add_children_to_db(&mut dir_db, &node.children)?;
            dir_db.close()?;
        }
        EMerkleTreeNode::Directory(dir_node) => {
            log::debug!("No need to aggregate dir {}", dir_node.name());
            process_children(
                repo,
                children,
                &mut local_counts,
                &mut local_sizes,
                num_bytes,
            )?;
            dir_node.set_data_type_counts(local_counts.clone());
            dir_node.set_data_type_sizes(local_sizes.clone());
            let mut dir_db =
                MerkleNodeDB::open_read_write(repo.merkle_node_store(), dir_node, node.parent_id)?;
            add_children_to_db(&mut dir_db, &node.children)?;
            dir_db.close()?;
        }
        EMerkleTreeNode::File(file_node) => {
            log::debug!(
                "Updating hash for file {} -> hash {}",
                file_node.name(),
                file_node.hash()
            );
            *num_bytes += file_node.num_bytes();
            *local_counts
                .entry(file_node.data_type().to_string())
                .or_insert(0) += 1;
            *local_sizes
                .entry(file_node.data_type().to_string())
                .or_insert(0) += file_node.num_bytes();
        }
        _ => {
            return Err(OxenError::basic_str(format!(
                "compute_dir_node found unexpected node type: {:?}",
                node.node
            )));
        }
    }

    Ok((local_counts, local_sizes))
}

fn process_children(
    repo: &LocalRepository,
    children: &mut [MerkleTreeNode],
    local_counts: &mut HashMap<String, u64>,
    local_sizes: &mut HashMap<String, u64>,
    num_bytes: &mut u64,
) -> Result<(), OxenError> {
    for child in children.iter_mut() {
        let (child_counts, child_sizes) =
            traverse_and_update_sizes_and_counts(repo, child, num_bytes)?;
        for (key, count) in child_counts {
            *local_counts.entry(key).or_insert(0) += count;
        }
        for (key, size) in child_sizes {
            *local_sizes.entry(key).or_insert(0) += size;
        }
    }
    Ok(())
}

fn add_children_to_db(
    dir_db: &mut MerkleNodeDB,
    children: &[MerkleTreeNode],
) -> Result<(), OxenError> {
    for child in children {
        match &child.node {
            EMerkleTreeNode::Commit(commit_node) => {
                dir_db.add_child(commit_node)?;
            }
            EMerkleTreeNode::Directory(dir_node) => {
                dir_db.add_child(dir_node)?;
            }
            EMerkleTreeNode::File(file_node) => {
                dir_db.add_child(file_node)?;
            }
            EMerkleTreeNode::VNode(vnode) => {
                dir_db.add_child(vnode)?;
            }
            _ => {
                return Err(OxenError::basic_str("Unsupported node type"));
            }
        }
    }
    Ok(())
}
