use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    model::{
        LocalRepository, MetadataEntry, StagedData, StagedEntry, StagedEntryStatus,
        SummarizedStagedDirStats,
    },
    util,
};

use super::{PaginatedDirEntries, StatusMessage, entries::EMetadataEntry};

// TODO: Removed dirs
#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteStagedStatus {
    pub added_dirs: SummarizedStagedDirStats,
    pub added_files: PaginatedDirEntries,
    pub modified_files: PaginatedDirEntries,
    pub removed_files: PaginatedDirEntries,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RemoteStagedStatusResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub staged: RemoteStagedStatus,
}

impl RemoteStagedStatus {
    pub fn from_staged(
        repo: &LocalRepository,
        staged: &StagedData,
        page_num: usize,
        page_size: usize,
    ) -> RemoteStagedStatus {
        let added_entries = RemoteStagedStatus::filter_to_meta_entry(
            repo,
            &staged.staged_files,
            StagedEntryStatus::Added,
        );
        let modified_entries = RemoteStagedStatus::filter_to_meta_entry(
            repo,
            &staged.staged_files,
            StagedEntryStatus::Modified,
        );
        let removed_entries = RemoteStagedStatus::filter_to_meta_entry(
            repo,
            &staged.staged_files,
            StagedEntryStatus::Removed,
        );

        let added_paginated =
            RemoteStagedStatus::paginate_entries(added_entries, page_num, page_size);
        let modified_paginated =
            RemoteStagedStatus::paginate_entries(modified_entries, page_num, page_size);
        let removed_paginated =
            RemoteStagedStatus::paginate_entries(removed_entries, page_num, page_size);

        RemoteStagedStatus {
            added_dirs: staged.staged_dirs.to_owned(),
            added_files: added_paginated,
            modified_files: modified_paginated,
            removed_files: removed_paginated,
        }
    }

    fn filter_to_meta_entry(
        repo: &LocalRepository,
        entries: &HashMap<PathBuf, StagedEntry>,
        status: StagedEntryStatus,
    ) -> Vec<MetadataEntry> {
        let paths = entries
            .iter()
            .filter(|(_, entry)| entry.status == status)
            .map(|(path, _)| path);
        RemoteStagedStatus::iter_to_meta_entry(repo, paths)
    }

    fn iter_to_meta_entry<'a, I: Iterator<Item = &'a PathBuf>>(
        repo: &LocalRepository,
        entries: I,
    ) -> Vec<MetadataEntry> {
        entries
            .map(|path| {
                let full_path = repo.path.join(path);
                let len = match util::fs::metadata(&full_path) {
                    Ok(m) => m.len(),
                    Err(_) => 0,
                };
                let path_str = path.to_string_lossy().to_string();

                MetadataEntry {
                    filename: path_str,
                    hash: "".to_string(),
                    is_dir: false,
                    size: len,
                    latest_commit: None,
                    data_type: util::fs::file_data_type(&full_path),
                    mime_type: util::fs::file_mime_type(&full_path),
                    extension: util::fs::file_extension(&full_path),
                    // not committed so does not have a resource or meta data computed
                    resource: None,
                    metadata: None,
                    is_queryable: None,
                    children: None,
                }
            })
            .collect()
    }

    fn paginate_entries(
        entries: Vec<MetadataEntry>,
        page_number: usize,
        page_size: usize,
    ) -> PaginatedDirEntries {
        let (paginated, pagination) = util::paginate(entries, page_number, page_size);

        PaginatedDirEntries {
            dir: None,
            entries: paginated
                .into_iter()
                .map(EMetadataEntry::MetadataEntry)
                .collect(),
            page_number: pagination.page_number,
            page_size: pagination.page_size,
            total_pages: pagination.total_pages,
            total_entries: pagination.total_entries,
            metadata: None,
            resource: None,
        }
    }
}
