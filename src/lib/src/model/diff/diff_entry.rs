use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::OxenError;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::{Commit, EntryDataType, MetadataEntry, ParsedResource};
use crate::opts::DFOpts;
use crate::view::TabularDiffView;
use crate::{model::LocalRepository, repositories};

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
            size: current_dir.num_bytes(),
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
            (entry.clone(), entry.data_type().clone())
        } else {
            let base_entry = base_entry.clone().unwrap();
            (base_entry.clone(), base_entry.data_type().clone())
        };
        let base_version = base_commit.id.to_string();
        let head_version = head_commit.id.to_string();
        let base_resource =
            DiffEntry::resource_from_file_node(base_entry.clone(), &file_path, &base_version);
        let head_resource =
            DiffEntry::resource_from_file_node(head_entry.clone(), &file_path, &head_version);

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
                    size: current_entry.num_bytes(),
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

        // log::debug!("fall through .... not doing full diff for tabular");
        Ok(DiffEntry {
            status: status.to_string(),
            data_type: data_type.clone(),
            filename: file_path.as_os_str().to_str().unwrap().to_string(),
            is_dir: false,
            size: current_entry.num_bytes(),
            head_resource,
            base_resource,
            head_entry: head_meta_entry,
            base_entry: base_meta_entry,
            diff_summary: DiffEntry::diff_summary_from_file_nodes(
                data_type.clone(),
                &base_entry,
                &head_entry,
            )?,
            diff: None, // TODO: other full diffs...
        })
    }

    fn resource_from_file_node(
        node: Option<FileNode>,
        file_path: impl AsRef<Path>,
        version: impl AsRef<str>,
    ) -> Option<ParsedResource> {
        let path = file_path.as_ref().to_path_buf();
        node.map(|_| ParsedResource {
            commit: None,
            branch: None,
            workspace: None,
            version: PathBuf::from(version.as_ref()),
            path: path.clone(),
            resource: PathBuf::from(version.as_ref()).join(path),
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
            workspace: None,
            version: PathBuf::from(node.last_commit_id().to_string()),
            path: path.clone(),
            resource: PathBuf::from(node.last_commit_id().to_string()).join(path),
        })
    }

    fn resource_from_dir(dir: Option<&PathBuf>, commit: &Commit) -> Option<ParsedResource> {
        dir.map(|dir| ParsedResource {
            commit: Some(commit.to_owned()),
            branch: None,
            workspace: None,
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
