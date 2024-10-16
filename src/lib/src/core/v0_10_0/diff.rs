use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::error::OxenError;
use crate::model::diff::diff_commit_entry::DiffCommitEntry;
use crate::model::diff::diff_entries_counts::DiffEntriesCounts;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::diff::AddRemoveModifyCounts;
use crate::model::merkle_tree::node::FileNode;
use crate::model::{Commit, CommitEntry, DiffEntry, LocalRepository};
use crate::opts::DFOpts;
use crate::util;

use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;

pub fn diff_entries(
    repo: &LocalRepository,
    base_entry: Option<FileNode>,
    base_commit: &Commit,
    head_entry: Option<FileNode>,
    head_commit: &Commit,
    df_opts: DFOpts,
) -> Result<DiffEntry, OxenError> {
    let commit_entry = base_entry.map(|base_entry| CommitEntry::from_file_node(&base_entry));
    let head_commit_entry = head_entry.map(|head_entry| CommitEntry::from_file_node(&head_entry));
    p_diff_entries(
        repo,
        commit_entry,
        base_commit,
        head_commit_entry,
        head_commit,
        df_opts,
    )
}

pub fn p_diff_entries(
    repo: &LocalRepository,
    base_entry: Option<CommitEntry>,
    base_commit: &Commit,
    head_entry: Option<CommitEntry>,
    head_commit: &Commit,
    df_opts: DFOpts,
) -> Result<DiffEntry, OxenError> {
    if base_entry.is_none() && head_entry.is_none() {
        return Err(OxenError::basic_str(
            "diff_entries called with no base or head entry",
        ));
    }

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

    let entry = DiffEntry::from_commit_entry(
        repo,
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

pub fn list_changed_dirs(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<(PathBuf, DiffEntryStatus)>, OxenError> {
    let mut changed_dirs: Vec<(PathBuf, DiffEntryStatus)> = vec![];

    let base_entry_reader = CommitEntryReader::new_from_commit_id(
        repo,
        &base_commit.id,
        get_object_reader(repo, &base_commit.id)?,
    )?;
    let head_entry_reader = CommitEntryReader::new_from_commit_id(
        repo,
        &head_commit.id,
        get_object_reader(repo, &head_commit.id)?,
    )?;

    let base_dirs = base_entry_reader.list_dirs_set()?;
    let head_dirs = head_entry_reader.list_dirs_set()?;

    let added_dirs = head_dirs.difference(&base_dirs).collect::<HashSet<_>>();
    let removed_dirs = base_dirs.difference(&head_dirs).collect::<HashSet<_>>();
    let modified_or_unchanged_dirs = head_dirs.intersection(&base_dirs).collect::<HashSet<_>>();

    for dir in added_dirs.iter() {
        changed_dirs.push((dir.to_path_buf(), DiffEntryStatus::Added));
    }

    for dir in removed_dirs.iter() {
        changed_dirs.push((dir.to_path_buf(), DiffEntryStatus::Removed));
    }

    for dir in modified_or_unchanged_dirs.iter() {
        let base_dir_hash: Option<String> = base_entry_reader.get_dir_hash(dir)?;
        let head_dir_hash: Option<String> = head_entry_reader.get_dir_hash(dir)?;

        let base_dir_hash = match base_dir_hash {
            Some(base_dir_hash) => base_dir_hash,
            None => {
                return Err(OxenError::basic_str(
                    format!("Could not calculate dir diff tree: base_dir_hash not found for dir {:?} in commit {}",
                    dir, base_commit.id)
                ))
            }
        };

        let head_dir_hash = match head_dir_hash {
            Some(head_dir_hash) => head_dir_hash,
            None => {
                return Err(OxenError::basic_str(
                    format!("Could not calculate dir diff tree: head_dir_hash not found for dir {:?} in commit {}",
                    dir, head_commit.id)
                ))
            }
        };

        let base_dir_hash = base_dir_hash.to_string();
        let head_dir_hash = head_dir_hash.to_string();

        if base_dir_hash != head_dir_hash {
            changed_dirs.push((dir.to_path_buf(), DiffEntryStatus::Modified));
        }
    }

    // Sort by path for consistency
    changed_dirs.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(changed_dirs)
}

// Filters out the entries that are not direct children of the provided dir, but
// still provides accurate recursive counts -
// TODO: can de-dup this with list_diff_entries somewhat
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

    let base_reader = CommitEntryReader::new_from_commit_id(
        repo,
        &base_commit.id,
        get_object_reader(repo, &base_commit.id)?,
    )?;
    let head_reader = CommitEntryReader::new_from_commit_id(
        repo,
        &head_commit.id,
        get_object_reader(repo, &head_commit.id)?,
    )?;

    let head_entries = head_reader.list_directory_set(&dir)?;
    let base_entries = base_reader.list_directory_set(&dir)?;

    let head_dirs = head_reader.list_dir_children_set(&dir)?;
    let base_dirs = base_reader.list_dir_children_set(&dir)?;

    // TODO TBD: If the logic is an exact match, this can be deduped with list_diff_entries
    let mut dir_entries: Vec<DiffEntry> = vec![];
    collect_added_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;

    collect_removed_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;

    collect_modified_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;

    dir_entries = subset_dir_diffs_to_direct_children(dir_entries, dir.clone())?;

    dir_entries.sort_by(|a, b| a.filename.cmp(&b.filename));

    let mut added_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_added_entries(&base_entries, &head_entries, &mut added_commit_entries)?;

    let mut removed_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_removed_entries(&base_entries, &head_entries, &mut removed_commit_entries)?;

    let mut modified_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_modified_entries(&base_entries, &head_entries, &mut modified_commit_entries)?;

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
    combined = subset_file_diffs_to_direct_children(combined, dir)?;

    combined.sort_by(|a, b| a.path.cmp(&b.path));

    let (files, pagination) =
        util::paginate::paginate_files_assuming_dirs(&combined, dir_entries.len(), page, page_size);

    let diff_entries: Vec<DiffEntry> = files
        .into_iter()
        .map(|entry| {
            DiffEntry::from_commit_entry(
                repo,
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
        "list_diff_entries base_commit: '{}', head_commit: '{}'",
        base_commit,
        head_commit
    );
    // BASE is what we are merging into, HEAD is where it is coming from
    // We want to find all the entries that are added, modified, removed HEAD but not in BASE

    // Read the entries from the base commit and the head commit
    log::debug!(
        "Reading entries from head commit {} -> {}",
        head_commit.id,
        head_commit.message
    );

    let base_reader = CommitEntryReader::new_from_commit_id(
        repo,
        &base_commit.id,
        get_object_reader(repo, &base_commit.id)?,
    )?;
    let head_reader = CommitEntryReader::new_from_commit_id(
        repo,
        &head_commit.id,
        get_object_reader(repo, &head_commit.id)?,
    )?;

    let head_entries = head_reader.list_directory_set(&dir)?;
    let base_entries = base_reader.list_directory_set(&dir)?;

    let head_dirs = head_reader.list_dir_children_set(&dir)?;
    let base_dirs = base_reader.list_dir_children_set(&dir)?;

    log::debug!("Got {} head entries", head_entries.len());
    log::debug!(
        "Reading entries from base commit {} -> {}",
        base_commit.id,
        base_commit.message
    );

    log::debug!("Got {} base entries", base_entries.len());

    log::debug!("Got {} head_dirs", head_dirs.len());

    log::debug!("Got {} base_dirs", base_dirs.len());

    let mut dir_entries: Vec<DiffEntry> = vec![];
    collect_added_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;
    log::debug!("Collected {} added_dirs dir_entries", dir_entries.len());
    collect_removed_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;
    log::debug!("Collected {} removed_dirs dir_entries", dir_entries.len());
    collect_modified_directories(
        repo,
        &base_dirs,
        base_commit,
        &head_dirs,
        head_commit,
        &mut dir_entries,
    )?;
    dir_entries.sort_by(|a, b| a.filename.cmp(&b.filename));
    log::debug!("Collected {} modified_dirs dir_entries", dir_entries.len());

    // the DiffEntry takes a little bit of time to compute, so want to just find the commit entries
    // then filter them down to the ones we need
    let mut added_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_added_entries(&base_entries, &head_entries, &mut added_commit_entries)?;
    log::debug!(
        "Collected {} collect_added_entries",
        added_commit_entries.len()
    );

    let mut removed_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_removed_entries(&base_entries, &head_entries, &mut removed_commit_entries)?;
    log::debug!(
        "Collected {} collect_removed_entries",
        removed_commit_entries.len()
    );

    let mut modified_commit_entries: Vec<DiffCommitEntry> = vec![];
    collect_modified_entries(&base_entries, &head_entries, &mut modified_commit_entries)?;
    log::debug!(
        "Collected {} collect_modified_entries",
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

    log::debug!("Got {} combined files", combined.len());

    let (files, pagination) =
        util::paginate::paginate_files_assuming_dirs(&combined, dir_entries.len(), page, page_size);
    log::debug!("Got {} initial dirs", dir_entries.len());
    log::debug!("Got {} files", files.len());

    let diff_entries: Vec<DiffEntry> = files
        .into_iter()
        .map(|entry| {
            DiffEntry::from_commit_entry(
                repo,
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
    log::debug!("Got {} filtered dirs", dirs.len());
    log::debug!("Page num {} Page size {}", page, page_size);

    let all = dirs.into_iter().chain(diff_entries).collect();

    Ok(DiffEntriesCounts {
        entries: all,
        counts,
        pagination,
    })
}

// Find the directories that are in HEAD but not in BASE
fn collect_added_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<PathBuf>,
    base_commit: &Commit,
    head_dirs: &HashSet<PathBuf>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
) -> Result<(), OxenError> {
    for head_dir in head_dirs {
        // HEAD entry is *not* in BASE
        if !base_dirs.contains(head_dir) {
            diff_entries.push(DiffEntry::from_dir(
                repo,
                None,
                base_commit,
                Some(head_dir),
                head_commit,
                DiffEntryStatus::Added,
            )?);
        }
    }
    Ok(())
}

// Find the directories that are in HEAD and are in BASE
fn collect_modified_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<PathBuf>,
    base_commit: &Commit,
    head_dirs: &HashSet<PathBuf>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
) -> Result<(), OxenError> {
    for head_dir in head_dirs {
        // HEAD entry is in BASE
        if base_dirs.contains(head_dir) {
            let diff_entry = DiffEntry::from_dir(
                repo,
                Some(head_dir),
                base_commit,
                Some(head_dir),
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

// Find the directories that are in BASE but not in HEAD
fn collect_removed_directories(
    repo: &LocalRepository,
    base_dirs: &HashSet<PathBuf>,
    base_commit: &Commit,
    head_dirs: &HashSet<PathBuf>,
    head_commit: &Commit,
    diff_entries: &mut Vec<DiffEntry>,
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

    for base_dir in base_dirs {
        // HEAD entry is *not* in BASE
        if !head_dirs.contains(base_dir) {
            diff_entries.push(DiffEntry::from_dir(
                repo,
                Some(base_dir),
                base_commit,
                None,
                head_commit,
                DiffEntryStatus::Removed,
            )?);
        }
    }
    Ok(())
}

// Find the entries that are in HEAD but not in BASE
fn collect_added_entries(
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffCommitEntry>,
) -> Result<(), OxenError> {
    log::debug!(
        "Computing difference for add entries head {} base {}",
        head_entries.len(),
        base_entries.len()
    );
    let diff = head_entries.difference(base_entries);
    for head_entry in diff {
        // HEAD entry is *not* in BASE
        diff_entries.push(DiffCommitEntry {
            path: head_entry.path.to_owned(),
            base_entry: None,
            head_entry: Some(head_entry.to_owned()),
            status: DiffEntryStatus::Added,
        });
    }
    Ok(())
}

// Find the entries that are in BASE but not in HEAD
fn collect_removed_entries(
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffCommitEntry>,
) -> Result<(), OxenError> {
    for base_entry in base_entries {
        // BASE entry is *not* in HEAD
        if !head_entries.contains(base_entry) {
            diff_entries.push(DiffCommitEntry {
                path: base_entry.path.to_owned(),
                base_entry: Some(base_entry.to_owned()),
                head_entry: None,
                status: DiffEntryStatus::Removed,
            });
        }
    }
    Ok(())
}

// Find the entries that are in both base and head, but have different hashes
fn collect_modified_entries(
    base_entries: &HashSet<CommitEntry>,
    head_entries: &HashSet<CommitEntry>,
    diff_entries: &mut Vec<DiffCommitEntry>,
) -> Result<(), OxenError> {
    log::debug!(
        "collect_modified_entries modified entries base.len() {} head.len() {}",
        base_entries.len(),
        head_entries.len()
    );
    for head_entry in head_entries {
        // HEAD entry *is* in BASE
        if let Some(base_entry) = base_entries.get(head_entry) {
            // log::debug!(
            //     "collect_modified_entries found in base! {} != {}",
            //     head_entry.hash,
            //     base_entry.hash
            // );
            // HEAD entry has a different hash than BASE entry
            if head_entry.hash != base_entry.hash {
                diff_entries.push(DiffCommitEntry {
                    path: base_entry.path.to_owned(),
                    base_entry: Some(base_entry.to_owned()),
                    head_entry: Some(head_entry.to_owned()),
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
        let status = DiffEntryStatus::from_str(&entry.status)?;
        let relevant_entry = match status {
            DiffEntryStatus::Added | DiffEntryStatus::Modified => entry.head_entry.as_ref(),
            DiffEntryStatus::Removed => entry.base_entry.as_ref(),
        };

        if let Some(meta_entry) = relevant_entry {
            if let Some(resource) = &meta_entry.resource {
                let path = PathBuf::from(&resource.path);
                if path.parent() == Some(dir.as_path()) {
                    filtered_entries.push(entry);
                }
            }
        }
    }

    Ok(filtered_entries)
}

fn subset_file_diffs_to_direct_children(
    entries: Vec<DiffCommitEntry>,
    dir: PathBuf,
) -> Result<Vec<DiffCommitEntry>, OxenError> {
    let mut filtered_entries: Vec<DiffCommitEntry> = vec![];

    for entry in entries {
        let relevant_entry = match entry.status {
            DiffEntryStatus::Added | DiffEntryStatus::Modified => entry.head_entry.as_ref(),
            DiffEntryStatus::Removed => entry.base_entry.as_ref(),
        };

        if let Some(commit_entry) = relevant_entry {
            if commit_entry.path.parent() == Some(dir.as_path()) {
                filtered_entries.push(entry);
            }
        }
    }

    Ok(filtered_entries)
}
