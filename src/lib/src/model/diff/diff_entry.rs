use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::{CommitDirEntryReader, CommitEntryReader, ObjectDBReader};
use crate::error::OxenError;
use crate::model::diff::dir_diff_summary::DirDiffSummaryImpl;
use crate::model::diff::AddRemoveModifyCounts;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::{Commit, EntryDataType, MetadataEntry, ParsedResource};
use crate::opts::DFOpts;
use crate::view::TabularDiffView;
use crate::{
    model::{CommitEntry, LocalRepository},
    repositories, util,
};

use super::diff_entry_status::DiffEntryStatus;
use super::dir_diff_summary::DirDiffSummary;
use super::generic_diff::GenericDiff;
use super::generic_diff_summary::GenericDiffSummary;
use super::tabular_diff_summary::TabularDiffWrapper;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiffEntry {
    pub status: String,
    pub data_type: EntryDataType,
    pub filename: String,
    pub is_dir: bool,
    pub size: u64,

    // Resource
    pub head_resource: Option<ParsedResource>,
    pub base_resource: Option<ParsedResource>,

    // Entry
    pub head_entry: Option<MetadataEntry>,
    pub base_entry: Option<MetadataEntry>,

    // Diff summary
    pub diff_summary: Option<GenericDiffSummary>,

    // Full Diff (only exposed sometimes for performance reasons)
    pub diff: Option<GenericDiff>,
}

impl DiffEntry {
    pub fn has_changes(&self) -> bool {
        // TODO: size is an old check, because we didn't have hashes on dirs before
        match (&self.head_entry, &self.base_entry) {
            (Some(head), Some(base)) => {
                log::debug!("got metadata entries for diff {:?} and {:?}", head, base);
                head.hash != base.hash || head.size != base.size
            }
            _ => {
                log::debug!("did not get metadata entries for diff");
                false
            }
        }
    }

    pub fn from_dir(
        repo: &LocalRepository,
        base_dir: Option<&PathBuf>,
        base_commit: &Commit,
        head_dir: Option<&PathBuf>,
        head_commit: &Commit,
        status: DiffEntryStatus,
    ) -> Result<DiffEntry, OxenError> {
        // Get the metadata entries
        let mut base_entry = DiffEntry::metadata_from_dir(repo, base_dir, base_commit);
        let mut head_entry = DiffEntry::metadata_from_dir(repo, head_dir, head_commit);

        // Need to check whether we have the head or base entry to check data about the file
        let (current_dir, current_entry) = if let Some(dir) = head_dir {
            (dir, head_entry.to_owned().unwrap())
        } else {
            (base_dir.unwrap(), base_entry.to_owned().unwrap())
        };

        let diff_summary = DiffEntry::diff_summary_from_dir(
            repo,
            &base_entry,
            &head_entry,
            base_commit,
            head_commit,
        )?;
        let head_resource = DiffEntry::resource_from_dir(head_dir, head_commit);
        let base_resource = DiffEntry::resource_from_dir(base_dir, base_commit);

        if let Some(base_entry) = &mut base_entry {
            base_entry.resource.clone_from(&base_resource);
        }

        if let Some(head_entry) = &mut head_entry {
            head_entry.resource.clone_from(&head_resource);
        }

        Ok(DiffEntry {
            status: status.to_string(),
            data_type: EntryDataType::Dir,
            filename: current_dir.as_os_str().to_str().unwrap().to_string(),
            is_dir: true,
            size: current_entry.size,
            head_resource,
            base_resource,
            head_entry,
            base_entry,
            diff_summary,
            diff: None, // TODO: Come back to what we want a full directory diff to look like
        })
    }

    // If the summary for a dir diff is already calculated (such as when wanting a self diff for a directory)
    // this prevents re-traversing through the directory structure.
    pub fn from_dir_with_summary(
        repo: &LocalRepository,
        base_dir: Option<&PathBuf>,
        base_commit: &Commit,
        head_dir: Option<&PathBuf>,
        head_commit: &Commit,
        summary: GenericDiffSummary,
        status: DiffEntryStatus,
    ) -> Result<DiffEntry, OxenError> {
        let mut base_entry = DiffEntry::metadata_from_dir(repo, base_dir, base_commit);
        let mut head_entry = DiffEntry::metadata_from_dir(repo, head_dir, head_commit);

        log::debug!("from_dir base_entry: {:?}", base_entry);
        log::debug!("from_dir head_entry: {:?}", head_entry);

        log::debug!("from_dir base_dir: {:?}", base_dir);
        log::debug!("from_dir head_dir: {:?}", head_dir);
        // Need to check whether we have the head or base entry to check data about the file
        let (current_dir, current_entry) = if let Some(dir) = head_dir {
            (dir, head_entry.to_owned().unwrap())
        } else {
            (base_dir.unwrap(), base_entry.to_owned().unwrap())
        };

        let head_resource = DiffEntry::resource_from_dir(head_dir, head_commit);
        let base_resource = DiffEntry::resource_from_dir(base_dir, base_commit);

        if let Some(base_entry) = &mut base_entry {
            base_entry.resource.clone_from(&base_resource);
        }

        if let Some(head_entry) = &mut head_entry {
            head_entry.resource.clone_from(&head_resource);
        }

        Ok(DiffEntry {
            status: status.to_string(),
            data_type: EntryDataType::Dir,
            filename: current_dir.as_os_str().to_str().unwrap().to_string(),
            is_dir: true,
            size: current_entry.size,
            head_resource,
            base_resource,
            head_entry,
            base_entry,
            diff_summary: Some(summary),
            diff: None,
        })
    }

    pub fn from_dir_nodes(
        repo: &LocalRepository,
        dir_path: impl AsRef<Path>,
        base_dir: Option<DirNode>,
        base_commit: &Commit,
        head_dir: Option<DirNode>,
        head_commit: &Commit,
        status: DiffEntryStatus,
    ) -> Result<DiffEntry, OxenError> {
        let dir_path = dir_path.as_ref().to_path_buf();
        // Need to check whether we have the head or base entry to check data about the file
        let current_dir = if let Some(dir) = &head_dir {
            dir.clone()
        } else {
            base_dir.clone().unwrap()
        };
        let base_resource = DiffEntry::resource_from_dir_node(base_dir.clone(), &dir_path);
        let head_resource = DiffEntry::resource_from_dir_node(head_dir.clone(), &dir_path);

        let mut base_meta_entry = MetadataEntry::from_dir_node(repo, base_dir.clone(), base_commit);
        let mut head_meta_entry = MetadataEntry::from_dir_node(repo, head_dir.clone(), head_commit);

        if base_dir.is_some() {
            base_meta_entry
                .as_mut()
                .unwrap()
                .resource
                .clone_from(&base_resource);
        }

        if head_dir.is_some() {
            head_meta_entry
                .as_mut()
                .unwrap()
                .resource
                .clone_from(&head_resource);
        }

        Ok(DiffEntry {
            status: status.to_string(),
            data_type: EntryDataType::Dir,
            filename: dir_path.as_os_str().to_str().unwrap().to_string(),
            is_dir: true,
            size: current_dir.num_bytes,
            head_resource,
            base_resource,
            head_entry: head_meta_entry,
            base_entry: base_meta_entry,
            diff_summary: DiffEntry::diff_summary_from_dir_nodes(&base_dir, &head_dir)?,
            diff: None, // TODO: other full diffs...
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_file_nodes(
        repo: &LocalRepository,
        file_path: impl AsRef<Path>,
        base_entry: Option<FileNode>,
        base_commit: &Commit, // pass in commit objects for speed so we don't have to lookup later
        head_entry: Option<FileNode>,
        head_commit: &Commit,
        status: DiffEntryStatus,
        should_do_full_diff: bool,
        df_opts: Option<DFOpts>, // only for tabular
    ) -> Result<DiffEntry, OxenError> {
        let file_path = file_path.as_ref().to_path_buf();
        // Need to check whether we have the head or base entry to check data about the file
        let (current_entry, data_type) = if let Some(entry) = &head_entry {
            (entry.clone(), entry.data_type.clone())
        } else {
            let base_entry = base_entry.clone().unwrap();
            (base_entry.clone(), base_entry.data_type.clone())
        };
        let base_resource = DiffEntry::resource_from_file_node(base_entry.clone(), &file_path);
        let head_resource = DiffEntry::resource_from_file_node(head_entry.clone(), &file_path);

        let mut base_meta_entry =
            MetadataEntry::from_file_node(repo, base_entry.clone(), base_commit);
        let mut head_meta_entry =
            MetadataEntry::from_file_node(repo, head_entry.clone(), head_commit);

        if base_entry.is_some() {
            base_meta_entry
                .as_mut()
                .unwrap()
                .resource
                .clone_from(&base_resource);
        }

        if head_entry.is_some() {
            head_meta_entry
                .as_mut()
                .unwrap()
                .resource
                .clone_from(&head_resource);
        }

        if let Some(df_opts) = df_opts {
            if data_type == EntryDataType::Tabular && should_do_full_diff {
                log::debug!("doing full diff for tabular");
                let diff =
                    TabularDiffView::from_file_nodes(repo, &base_entry, &head_entry, df_opts);
                return Ok(DiffEntry {
                    status: status.to_string(),
                    data_type: data_type.clone(),
                    filename: file_path.as_os_str().to_str().unwrap().to_string(),
                    is_dir: false,
                    size: current_entry.num_bytes,
                    head_resource,
                    base_resource,
                    head_entry: head_meta_entry,
                    base_entry: base_meta_entry,
                    diff_summary: Some(GenericDiffSummary::TabularDiffWrapper(
                        diff.clone().tabular.summary.to_wrapper(),
                    )),
                    diff: Some(GenericDiff::TabularDiff(diff)),
                });
            }
        }

        log::debug!("fall through .... not doing full diff for tabular");
        Ok(DiffEntry {
            status: status.to_string(),
            data_type: data_type.clone(),
            filename: file_path.as_os_str().to_str().unwrap().to_string(),
            is_dir: false,
            size: current_entry.num_bytes,
            head_resource,
            base_resource,
            head_entry: head_meta_entry,
            base_entry: base_meta_entry,
            diff_summary: DiffEntry::diff_summary_from_file_nodes(
                data_type,
                &base_entry,
                &head_entry,
            )?,
            diff: None, // TODO: other full diffs...
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_commit_entry(
        repo: &LocalRepository,
        base_entry: Option<CommitEntry>,
        base_commit: &Commit, // pass in commit objects for speed so we don't have to lookup later
        head_entry: Option<CommitEntry>,
        head_commit: &Commit,
        status: DiffEntryStatus,
        should_do_full_diff: bool,
        df_opts: Option<DFOpts>, // only for tabular
    ) -> Result<DiffEntry, OxenError> {
        // Need to check whether we have the head or base entry to check data about the file
        let (current_entry, version_path) = if let Some(entry) = &head_entry {
            (entry.clone(), util::fs::version_path(repo, entry))
        } else {
            (
                base_entry.clone().unwrap(),
                util::fs::version_path(repo, &base_entry.clone().unwrap()),
            )
        };

        let data_type = util::fs::file_data_type(&version_path);

        let base_resource = DiffEntry::resource_from_entry(base_entry.clone());
        let head_resource = DiffEntry::resource_from_entry(head_entry.clone());

        let mut base_meta_entry =
            MetadataEntry::from_commit_entry(repo, base_entry.clone(), base_commit);
        let mut head_meta_entry =
            MetadataEntry::from_commit_entry(repo, head_entry.clone(), head_commit);

        if base_entry.is_some() {
            base_meta_entry
                .as_mut()
                .unwrap()
                .resource
                .clone_from(&base_resource);
        }

        if head_entry.is_some() {
            head_meta_entry
                .as_mut()
                .unwrap()
                .resource
                .clone_from(&head_resource);
        }

        // TODO: Clean this up, but want to get a prototype to work first
        // if tabular, and should_do_full_diff
        //     do full diff
        // log::debug!(
        //     "checking if should do full diff for tabular {},{},{}",
        //     data_type,
        //     should_do_full_diff,
        //     pagination.is_some()
        // );

        if let Some(df_opts) = df_opts {
            if data_type == EntryDataType::Tabular && should_do_full_diff {
                let diff =
                    TabularDiffView::from_commit_entries(repo, &base_entry, &head_entry, df_opts);
                return Ok(DiffEntry {
                    status: status.to_string(),
                    data_type: data_type.clone(),
                    filename: current_entry.path.as_os_str().to_str().unwrap().to_string(),
                    is_dir: false,
                    size: current_entry.num_bytes,
                    head_resource,
                    base_resource,
                    head_entry: head_meta_entry,
                    base_entry: base_meta_entry,
                    diff_summary: Some(GenericDiffSummary::TabularDiffWrapper(
                        diff.clone().tabular.summary.to_wrapper(),
                    )),
                    diff: Some(GenericDiff::TabularDiff(diff)),
                });
            }
        }

        Ok(DiffEntry {
            status: status.to_string(),
            data_type: data_type.clone(),
            filename: current_entry.path.as_os_str().to_str().unwrap().to_string(),
            is_dir: false,
            size: current_entry.num_bytes,
            head_resource,
            base_resource,
            head_entry: head_meta_entry,
            base_entry: base_meta_entry,
            diff_summary: DiffEntry::diff_summary_from_commit_entries(
                repo,
                data_type,
                &base_entry,
                &head_entry,
            )?,
            diff: None, // TODO: other full diffs...
        })
    }

    fn resource_from_entry(entry: Option<CommitEntry>) -> Option<ParsedResource> {
        entry.map(|entry| ParsedResource {
            commit: None,
            branch: None,
            version: PathBuf::from(entry.commit_id.to_string()),
            path: entry.path.clone(),
            resource: PathBuf::from(entry.commit_id.to_string()).join(entry.path.clone()),
        })
    }

    fn resource_from_file_node(
        node: Option<FileNode>,
        file_path: impl AsRef<Path>,
    ) -> Option<ParsedResource> {
        let path = file_path.as_ref().to_path_buf();
        node.map(|node| ParsedResource {
            commit: None,
            branch: None,
            version: PathBuf::from(node.last_commit_id.to_string()),
            path: path.clone(),
            resource: PathBuf::from(node.last_commit_id.to_string()).join(path),
        })
    }

    fn resource_from_dir_node(
        node: Option<DirNode>,
        dir_path: impl AsRef<Path>,
    ) -> Option<ParsedResource> {
        let path = dir_path.as_ref().to_path_buf();
        node.map(|node| ParsedResource {
            commit: None,
            branch: None,
            version: PathBuf::from(node.last_commit_id.to_string()),
            path: path.clone(),
            resource: PathBuf::from(node.last_commit_id.to_string()).join(path),
        })
    }

    fn resource_from_dir(dir: Option<&PathBuf>, commit: &Commit) -> Option<ParsedResource> {
        dir.map(|dir| ParsedResource {
            commit: Some(commit.to_owned()),
            branch: None,
            version: PathBuf::from(commit.id.to_string()),
            path: dir.clone(),
            resource: PathBuf::from(commit.id.to_string()).join(dir),
        })
    }

    fn metadata_from_dir(
        repo: &LocalRepository,
        dir: Option<&PathBuf>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        if let Some(dir) = dir {
            match repositories::entries::get_meta_entry(repo, commit, dir) {
                Ok(entry) => Some(entry),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn diff_summary_from_dir(
        repo: &LocalRepository,
        base_dir: &Option<MetadataEntry>,
        head_dir: &Option<MetadataEntry>,
        base_commit: &Commit,
        head_commit: &Commit,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        log::debug!("diff_summary_from_dir base_dir: {:?}", base_dir);
        log::debug!("diff_summary_from_dir head_dir: {:?}", head_dir);

        // if both base_dir and head_dir are none, then there is no diff summary
        if base_dir.is_none() && head_dir.is_none() {
            return Ok(None);
        }

        // if base_dir is some and head_dir is none, then we deleted all the files
        if base_dir.is_some() && head_dir.is_none() {
            let dir_entry = base_dir.as_ref().unwrap();
            let commit_id = &dir_entry.latest_commit.as_ref().unwrap().id;
            return DiffEntry::r_compute_removed_files(
                repo,
                dir_entry,
                get_object_reader(repo, commit_id)?,
            );
        }

        // if head_dir is some and base_dir is none, then we added all the files
        if head_dir.is_some() && base_dir.is_none() {
            let dir_entry = head_dir.as_ref().unwrap();
            let commit_id = &dir_entry.latest_commit.as_ref().unwrap().id;
            return DiffEntry::r_compute_added_files(
                repo,
                dir_entry,
                get_object_reader(repo, commit_id)?,
            );
        }

        // if both base_dir and head_dir are some, then we need to compare the two
        let base_dir = base_dir.as_ref().unwrap();

        DiffEntry::r_compute_diff_all_files(repo, base_dir, base_commit, head_commit)
    }

    fn r_compute_diff_all_files(
        repo: &LocalRepository,
        base_dir: &MetadataEntry,
        base_commit: &Commit,
        head_commit: &Commit,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        let base_commit_id = &base_commit.id;
        let head_commit_id = &head_commit.id;

        // base and head path will be the same so just choose base
        let path = PathBuf::from(&base_dir.resource.clone().unwrap().path);

        let mut num_removed = 0;

        let mut num_added = 0;
        let mut num_modified = 0;

        let base_object_reader = get_object_reader(repo, base_commit_id)?;
        let head_object_reader = get_object_reader(repo, head_commit_id)?;

        // Find all the children of the dir and sum up their counts
        let commit_entry_reader = CommitEntryReader::new_from_commit_id(
            repo,
            base_commit_id,
            base_object_reader.clone(),
        )?;
        let mut dirs = commit_entry_reader.list_dir_children(&path)?;

        let commit_entry_reader = CommitEntryReader::new_from_commit_id(
            repo,
            head_commit_id,
            head_object_reader.clone(),
        )?;
        let mut other = commit_entry_reader.list_dir_children(&path)?;
        dirs.append(&mut other);
        dirs.push(path.clone());

        // Uniq them
        let dirs: HashSet<PathBuf> = HashSet::from_iter(dirs);

        for dir in dirs {
            let base_dir_reader =
                CommitDirEntryReader::new(repo, base_commit_id, &dir, base_object_reader.clone())?;
            let head_dir_reader =
                CommitDirEntryReader::new(repo, head_commit_id, &dir, head_object_reader.clone())?;

            // List the entries in hash sets
            let head_entries = head_dir_reader.list_entries_set()?;
            let base_entries = base_dir_reader.list_entries_set()?;

            log::debug!(
                "diff_summary_from_dir head_entries: {:?}",
                head_entries.len()
            );
            log::debug!(
                "diff_summary_from_dir base_entries: {:?}",
                base_entries.len()
            );

            // Find the added entries
            let added_entries = head_entries
                .difference(&base_entries)
                .collect::<HashSet<_>>();

            num_added += added_entries.len();

            // Find the removed entries
            let removed_entries = base_entries
                .difference(&head_entries)
                .collect::<HashSet<_>>();
            num_removed += removed_entries.len();

            // Find the modified entries
            for base_entry in base_entries {
                if let Some(head_entry) = head_entries.get(&base_entry) {
                    if head_entry.hash != base_entry.hash {
                        num_modified += 1;
                    }
                }
            }
        }

        Ok(Some(GenericDiffSummary::DirDiffSummary(DirDiffSummary {
            dir: DirDiffSummaryImpl {
                file_counts: AddRemoveModifyCounts {
                    added: num_added,
                    removed: num_removed,
                    modified: num_modified,
                },
            },
        })))
    }

    fn r_compute_removed_files(
        repo: &LocalRepository,
        base_dir: &MetadataEntry,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        let commit_id = &base_dir.latest_commit.as_ref().unwrap().id;
        let path = PathBuf::from(&base_dir.resource.clone().unwrap().path);

        // Count all removals in the directory and its children
        let commit_entry_reader =
            CommitEntryReader::new_from_commit_id(repo, commit_id, object_reader.clone())?;
        let mut dirs = commit_entry_reader.list_dir_children(&path)?;
        dirs.push(path);

        let mut num_removed = 0;
        for dir in dirs {
            let dir_reader =
                CommitDirEntryReader::new(repo, commit_id, &dir, object_reader.clone())?;
            let count = dir_reader.num_entries();
            log::debug!("r_compute_removed_files dir: {:?} count: {}", dir, count);

            num_removed += count;
        }

        Ok(Some(GenericDiffSummary::DirDiffSummary(DirDiffSummary {
            dir: DirDiffSummaryImpl {
                file_counts: AddRemoveModifyCounts {
                    added: 0,
                    removed: num_removed,
                    modified: 0,
                },
            },
        })))
    }

    fn r_compute_added_files(
        repo: &LocalRepository,
        head_dir: &MetadataEntry,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        let commit_id = &head_dir.latest_commit.as_ref().unwrap().id;
        let path = PathBuf::from(&head_dir.resource.clone().unwrap().path);
        log::debug!("r_compute_added_files head_dir: {:?}", path);

        // Count all removals in the directory and its children
        let commit_entry_reader =
            CommitEntryReader::new_from_commit_id(repo, commit_id, object_reader.clone())?;
        let mut dirs = commit_entry_reader.list_dir_children(&path)?;
        dirs.push(path);

        log::debug!("r_compute_added_files got dirs: {:?}", dirs.len());

        let mut num_added = 0;
        for dir in dirs {
            let dir_reader =
                CommitDirEntryReader::new(repo, commit_id, &dir, object_reader.clone())?;
            let count = dir_reader.num_entries();
            log::debug!("r_compute_added_files dir: {:?} count: {}", dir, count);

            num_added += count;
        }

        Ok(Some(GenericDiffSummary::DirDiffSummary(DirDiffSummary {
            dir: DirDiffSummaryImpl {
                file_counts: AddRemoveModifyCounts {
                    added: num_added,
                    removed: 0,
                    modified: 0,
                },
            },
        })))
    }

    fn diff_summary_from_commit_entries(
        repo: &LocalRepository,
        data_type: EntryDataType,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        // TODO match on type, and create the appropriate summary
        match data_type {
            EntryDataType::Tabular => Ok(Some(GenericDiffSummary::TabularDiffWrapper(
                TabularDiffWrapper::from_commit_entries(repo, base_entry, head_entry)?,
            ))),
            _ => Ok(None),
        }
    }

    fn diff_summary_from_file_nodes(
        data_type: EntryDataType,
        base_entry: &Option<FileNode>,
        head_entry: &Option<FileNode>,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        // TODO match on type, and create the appropriate summary
        match data_type {
            EntryDataType::Tabular => Ok(Some(GenericDiffSummary::TabularDiffWrapper(
                TabularDiffWrapper::from_file_nodes(base_entry, head_entry)?,
            ))),
            _ => Ok(None),
        }
    }

    fn diff_summary_from_dir_nodes(
        base_entry: &Option<DirNode>,
        head_entry: &Option<DirNode>,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        Ok(Some(GenericDiffSummary::DirDiffSummary(
            DirDiffSummary::from_dir_nodes(base_entry, head_entry)?,
        )))
    }
}
