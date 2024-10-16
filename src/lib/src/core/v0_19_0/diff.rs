use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::diff::diff_entries_counts::DiffEntriesCounts;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::diff::diff_file_node::DiffFileNode;
use crate::model::diff::AddRemoveModifyCounts;
use crate::model::merkle_tree::node::{DirNodeWithPath, FileNode, FileNodeWithDir};
use crate::model::{Commit, DiffEntry, LocalRepository};
use crate::opts::DFOpts;
use crate::repositories;
use crate::util;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;

// Filters out the entries that are not direct children of the provided dir, but
// still provides accurate recursive counts -
// TODO: can de-dup this with list_diff_entries somewhat
//       don't love that this is pretty specific to our UI...but will leave for now
pub fn list_diff_entries_in_dir_top_level(
    repo: &LocalRepository,
    dir: PathBuf,
    base_commit: &Commit,
    head_commit: &Commit,
    page: usize,
    page_size: usize,
) -> Result<DiffEntriesCounts, OxenError> {
    log::debug!(
        "list_top_level_diff_entries base_commit: '{}', head_commit: '{}'",
        base_commit,
        head_commit
    );

    // Load the trees into memory starting at the given dir
    let load_recursive = true;
    let base_tree = CommitMerkleTree::from_path(repo, base_commit, &dir, load_recursive)?;
    let head_tree = CommitMerkleTree::from_path(repo, head_commit, &dir, load_recursive)?;

    let (head_files, head_dirs) = repositories::tree::list_files_and_dirs(&head_tree)?;
    let (base_files, base_dirs) = repositories::tree::list_files_and_dirs(&base_tree)?;

    log::debug!("Collected {} head_files", head_files.len());
    log::debug!("Collected {} head_dirs", head_dirs.len());
    log::debug!("Collected {} base_files", base_files.len());
    log::debug!("Collected {} base_dirs", base_dirs.len());

    // TODO TBD: If the logic is an exact match, this can be deduped with list_diff_entries
    let mut dir_entries: Vec<DiffEntry> = vec![];
    collect_added_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
        &dir,
    )?;

    collect_removed_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
        &dir,
    )?;

    collect_modified_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
        &dir,
    )?;

    log::debug!("Collected {} dir_entries", dir_entries.len());
    dir_entries = subset_dir_diffs_to_direct_children(dir_entries, dir.clone())?;
    log::debug!("Filtered to {} dir_entries", dir_entries.len());

    dir_entries.sort_by(|a, b| a.filename.cmp(&b.filename));

    let mut added_commit_entries: Vec<DiffFileNode> = vec![];
    collect_added_entries(&base_files, &head_files, &mut added_commit_entries, &dir)?;

    let mut removed_commit_entries: Vec<DiffFileNode> = vec![];
    collect_removed_entries(&base_files, &head_files, &mut removed_commit_entries, &dir)?;

    let mut modified_commit_entries: Vec<DiffFileNode> = vec![];
    collect_modified_entries(&base_files, &head_files, &mut modified_commit_entries, &dir)?;

    let counts = AddRemoveModifyCounts {
        added: added_commit_entries.len(),
        removed: removed_commit_entries.len(),
        modified: modified_commit_entries.len(),
    };

    let mut combined: Vec<_> = added_commit_entries
        .into_iter()
        .chain(removed_commit_entries)
        .chain(modified_commit_entries)
        .collect();

    // Filter out the entries that are not direct children of the provided dir
    log::debug!("Combined {} combined", combined.len());
    combined = subset_file_diffs_to_direct_children(combined, dir)?;
    log::debug!("Filtered to {} combined", combined.len());

    combined.sort_by(|a, b| a.path.cmp(&b.path));

    let (files, pagination) =
        util::paginate::paginate_files_assuming_dirs(&combined, dir_entries.len(), page, page_size);

    let diff_entries: Vec<DiffEntry> = files
        .into_iter()
        .map(|entry| {
            DiffEntry::from_file_nodes(
                repo,
                entry.path,
                entry.base_entry,
                base_commit,
                entry.head_entry,
                head_commit,
                entry.status,
                false,
                None,
            )
        })
        .collect::<Result<Vec<DiffEntry>, OxenError>>()?;

    let (dirs, _) =
        util::paginate::paginate_dirs_assuming_files(&dir_entries, combined.len(), page, page_size);

    let all = dirs.into_iter().chain(diff_entries).collect();

    Ok(DiffEntriesCounts {
        entries: all,
        counts,
        pagination,
    })
}

pub fn list_diff_entries(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
    dir: PathBuf,
    page: usize,
    page_size: usize,
) -> Result<DiffEntriesCounts, OxenError> {
    log::debug!(
        "list_diff_entries dir: '{:?}', base_commit: '{}', head_commit: '{}'",
        dir,
        base_commit,
        head_commit
    );
    // Load the trees into memory starting at the given dir
    let load_recursive = true;
    let base_tree = CommitMerkleTree::from_path(repo, base_commit, &dir, load_recursive)?;
    let head_tree = CommitMerkleTree::from_path(repo, head_commit, &dir, load_recursive)?;

    let (head_files, head_dirs) = repositories::tree::list_files_and_dirs(&head_tree)?;
    let (base_files, base_dirs) = repositories::tree::list_files_and_dirs(&base_tree)?;

    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} head_files",
        dir,
        head_files.len()
    );
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} head_dirs",
        dir,
        head_dirs.len()
    );
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} base_files",
        dir,
        base_files.len()
    );
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} base_dirs",
        dir,
        base_dirs.len()
    );

    let mut dir_entries: Vec<DiffEntry> = vec![];
    collect_added_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
        &dir,
    )?;
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} added_dirs dir_entries",
        dir,
        dir_entries.len()
    );
    collect_removed_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
        &dir,
    )?;
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} removed_dirs dir_entries",
        dir,
        dir_entries.len()
    );
    collect_modified_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
        &dir,
    )?;
    dir_entries.sort_by(|a, b| a.filename.cmp(&b.filename));
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} modified_dirs dir_entries",
        dir,
        dir_entries.len()
    );

    // the DiffEntry takes a little bit of time to compute, so want to just find the commit entries
    // then filter them down to the ones we need
    let mut added_commit_entries: Vec<DiffFileNode> = vec![];
    collect_added_entries(&base_files, &head_files, &mut added_commit_entries, &dir)?;
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} collect_added_entries",
        dir,
        added_commit_entries.len()
    );

    let mut removed_commit_entries: Vec<DiffFileNode> = vec![];
    collect_removed_entries(&base_files, &head_files, &mut removed_commit_entries, &dir)?;
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} collect_removed_entries",
        dir,
        removed_commit_entries.len()
    );

    let mut modified_commit_entries: Vec<DiffFileNode> = vec![];
    collect_modified_entries(&base_files, &head_files, &mut modified_commit_entries, &dir)?;
    log::debug!(
        "list_diff_entries dir: '{:?}' collected {} collect_modified_entries",
        dir,
        modified_commit_entries.len()
    );
    let counts = AddRemoveModifyCounts {
        added: added_commit_entries.len(),
        removed: removed_commit_entries.len(),
        modified: modified_commit_entries.len(),
    };
    let mut combined: Vec<_> = added_commit_entries
        .into_iter()
        .chain(removed_commit_entries)
        .chain(modified_commit_entries)
        .collect();
    combined.sort_by(|a, b| a.path.cmp(&b.path));

    log::debug!(
        "list_diff_entries dir: '{:?}' got {} combined files",
        dir,
        combined.len()
    );

    let (files, pagination) =
        util::paginate::paginate_files_assuming_dirs(&combined, dir_entries.len(), page, page_size);
    log::debug!(
        "list_diff_entries dir: '{:?}' got {} initial dirs",
        dir,
        dir_entries.len()
    );
    log::debug!(
        "list_diff_entries dir: '{:?}' got {} files",
        dir,
        files.len()
    );
    let file_entries: Vec<DiffEntry> = files
        .into_iter()
        .map(|entry| {
            DiffEntry::from_file_nodes(
                repo,
                entry.path,
                entry.base_entry,
                base_commit,
                entry.head_entry,
                head_commit,
                entry.status,
                false,
                None,
            )
        })
        .collect::<Result<Vec<DiffEntry>, OxenError>>()?;

    let (dirs, _) =
        util::paginate::paginate_dirs_assuming_files(&dir_entries, combined.len(), page, page_size);
    log::debug!(
        "list_diff_entries dir: '{:?}' got {} filtered dirs",
        dir,
        dirs.len()
    );
    log::debug!(
        "list_diff_entries dir: '{:?}' Page num {} Page size {}",
        dir,
        page,
        page_size
    );
    let all = dirs.into_iter().chain(file_entries).collect();

    Ok(DiffEntriesCounts {
        entries: all,
        counts,
        pagination,
    })
}

pub fn list_changed_dirs(
    _repo: &LocalRepository,
    _base_commit: &Commit,
    _head_commit: &Commit,
) -> Result<Vec<(PathBuf, DiffEntryStatus)>, OxenError> {
    todo!()
}

pub fn diff_entries(
    repo: &LocalRepository,
    file_path: impl AsRef<Path>,
    base_entry: Option<FileNode>,
    base_commit: &Commit,
    head_entry: Option<FileNode>,
    head_commit: &Commit,
    df_opts: DFOpts,
) -> Result<DiffEntry, OxenError> {
    // Assume both entries exist
    let mut status = DiffEntryStatus::Modified;

    // If base entry is none, then it was added
    if base_entry.is_none() && head_entry.is_some() {
        status = DiffEntryStatus::Added;
    }

    // If head entry is none, then it was removed
    if head_entry.is_none() && base_entry.is_some() {
        status = DiffEntryStatus::Removed;
    }

    let should_do_full_diff = true;

    let entry = DiffEntry::from_file_nodes(
        repo,
        file_path,
        base_entry,
        base_commit,
        head_entry,
        head_commit,
        status,
        should_do_full_diff,
        Some(df_opts),
    )?;

    Ok(entry)
}

// Find the directories that are in HEAD but not in BASE
fn collect_added_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<DirNodeWithPath>,
    base_commit: &Commit,
    head_dirs: &HashSet<DirNodeWithPath>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
    base_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // DEBUG
    // for base_dir in base_dirs.iter() {
    //     log::debug!("collect_added_directories BASE dir {:?}", base_dir);
    // }

    // for head_dir in head_dirs.iter() {
    //     log::debug!("collect_added_directories HEAD dir {:?}", head_dir);
    // }
    let base_path = base_path.as_ref();
    for head_dir in head_dirs {
        // HEAD entry is *not* in BASE
        if !base_dirs.contains(head_dir) {
            log::debug!("collect_added_directories adding dir {:?}", head_dir);
            diff_entries.push(DiffEntry::from_dir_nodes(
                repo,
                base_path.join(&head_dir.path),
                None,
                base_commit,
                Some(head_dir.dir_node.clone()),
                head_commit,
                DiffEntryStatus::Added,
            )?);
        }
    }
    Ok(())
}

// Find the directories that are in BASE but not in HEAD
fn collect_removed_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<DirNodeWithPath>,
    base_commit: &Commit,
    head_dirs: &HashSet<DirNodeWithPath>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
    base_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // DEBUG
    // for base_dir in base_dirs.iter() {
    //     log::debug!(
    //         "collect_removed_directories BASE dir {}",
    //         base_dir.display()
    //     );
    // }

    // for head_dir in head_dirs.iter() {
    //     log::debug!(
    //         "collect_removed_directories HEAD dir {}",
    //         head_dir.display()
    //     );
    // }
    let base_path = base_path.as_ref();
    for base_dir in base_dirs {
        // HEAD entry is *not* in BASE
        if !head_dirs.contains(base_dir) {
            log::debug!("collect_removed_directories adding dir {:?}", base_dir);
            diff_entries.push(DiffEntry::from_dir_nodes(
                repo,
                base_path.join(&base_dir.path),
                Some(base_dir.dir_node.clone()),
                base_commit,
                None,
                head_commit,
                DiffEntryStatus::Removed,
            )?);
        }
    }
    Ok(())
}

// Find the directories that are in HEAD and are in BASE
fn collect_modified_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<DirNodeWithPath>,
    base_commit: &Commit,
    head_dirs: &HashSet<DirNodeWithPath>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
    base_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let base_path = base_path.as_ref();
    for head_dir in head_dirs {
        // HEAD entry is in BASE
        if let Some(base_dir) = base_dirs.get(head_dir) {
            log::debug!("collect_modified_directories adding dir {:?}", head_dir);
            let diff_entry = DiffEntry::from_dir_nodes(
                repo,
                base_path.join(&head_dir.path),
                Some(base_dir.dir_node.clone()),
                base_commit,
                Some(head_dir.dir_node.clone()),
                head_commit,
                DiffEntryStatus::Modified,
            )?;

            if diff_entry.has_changes() {
                diff_entries.push(diff_entry);
            }
        }
    }
    Ok(())
}

// Find the entries that are in HEAD but not in BASE
fn collect_added_entries(
    base_entries: &HashSet<FileNodeWithDir>,
    head_entries: &HashSet<FileNodeWithDir>,
    diff_entries: &mut Vec<DiffFileNode>,
    base_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // log::debug!(
    //     "collect_added_entries Computing difference for add entries head {} base {}",
    //     head_entries.len(),
    //     base_entries.len()
    // );

    // for base in base_entries.iter() {
    //     log::debug!("collect_added_entries BASE {:?}", base);
    // }

    // for head in head_entries.iter() {
    //     log::debug!("collect_added_entries HEAD {:?}", head);
    // }
    let base_path = base_path.as_ref();
    let diff = head_entries.difference(base_entries);
    for head_entry in diff {
        // HEAD entry is *not* in BASE
        diff_entries.push(DiffFileNode {
            path: base_path.join(head_entry.dir.join(&head_entry.file_node.name)),
            base_entry: None,
            head_entry: Some(head_entry.file_node.to_owned()),
            status: DiffEntryStatus::Added,
        });
    }
    Ok(())
}

// Find the entries that are in BASE but not in HEAD
fn collect_removed_entries(
    base_entries: &HashSet<FileNodeWithDir>,
    head_entries: &HashSet<FileNodeWithDir>,
    diff_entries: &mut Vec<DiffFileNode>,
    base_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let base_path = base_path.as_ref();
    for base_entry in base_entries {
        // BASE entry is *not* in HEAD
        if !head_entries.contains(base_entry) {
            diff_entries.push(DiffFileNode {
                path: base_path.join(base_entry.dir.join(&base_entry.file_node.name)),
                base_entry: Some(base_entry.file_node.to_owned()),
                head_entry: None,
                status: DiffEntryStatus::Removed,
            });
        }
    }
    Ok(())
}

// Find the entries that are in both base and head, but have different hashes
fn collect_modified_entries(
    base_entries: &HashSet<FileNodeWithDir>,
    head_entries: &HashSet<FileNodeWithDir>,
    diff_entries: &mut Vec<DiffFileNode>,
    base_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let base_path = base_path.as_ref();
    log::debug!(
        "collect_modified_entries modified entries base.len() {} head.len() {}",
        base_entries.len(),
        head_entries.len()
    );
    for head_entry in head_entries {
        // HEAD entry *is* in BASE
        if let Some(base_entry) = base_entries.get(head_entry) {
            // log::debug!(
            //     "collect_modified_entries found in base! {:?} != {:?}",
            //     head_entry.file_node,
            //     base_entry.file_node
            // );
            // HEAD entry has a different hash than BASE entry
            if head_entry.file_node.hash != base_entry.file_node.hash {
                diff_entries.push(DiffFileNode {
                    path: base_path.join(base_entry.dir.join(&base_entry.file_node.name)),
                    base_entry: Some(base_entry.file_node.to_owned()),
                    head_entry: Some(head_entry.file_node.to_owned()),
                    status: DiffEntryStatus::Modified,
                });
            }
        }
    }
    Ok(())
}

fn subset_dir_diffs_to_direct_children(
    entries: Vec<DiffEntry>,
    dir: PathBuf,
) -> Result<Vec<DiffEntry>, OxenError> {
    let mut filtered_entries: Vec<DiffEntry> = vec![];

    for entry in entries {
        log::debug!(
            "subset_dir_diffs_to_direct_children entry.filename {:?} dir {:?}",
            entry.filename,
            dir
        );

        let status = DiffEntryStatus::from_str(&entry.status)?;
        let relevant_entry = match status {
            DiffEntryStatus::Added | DiffEntryStatus::Modified => entry.head_entry.as_ref(),
            DiffEntryStatus::Removed => entry.base_entry.as_ref(),
        };

        if let Some(meta_entry) = relevant_entry {
            if let Some(resource) = &meta_entry.resource {
                let path = PathBuf::from(&resource.path);
                log::debug!(
                    "subset_dir_diffs_to_direct_children path {:?} dir {:?}",
                    path,
                    dir
                );
                if path.parent() == Some(dir.as_path()) {
                    filtered_entries.push(entry);
                }
            }
        }
    }

    Ok(filtered_entries)
}

fn subset_file_diffs_to_direct_children(
    entries: Vec<DiffFileNode>,
    dir: PathBuf,
) -> Result<Vec<DiffFileNode>, OxenError> {
    let mut filtered_entries: Vec<DiffFileNode> = vec![];

    for entry in entries {
        let relevant_entry = match entry.status {
            DiffEntryStatus::Added | DiffEntryStatus::Modified => entry.head_entry.as_ref(),
            DiffEntryStatus::Removed => entry.base_entry.as_ref(),
        };

        log::debug!(
            "subset_file_diffs_to_direct_children entry.path {:?} dir {:?}",
            entry.path,
            dir
        );

        if relevant_entry.is_some() && entry.path.parent() == Some(dir.as_path()) {
            filtered_entries.push(entry);
        }
    }

    Ok(filtered_entries)
}
