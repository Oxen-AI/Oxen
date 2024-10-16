use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::core::df::tabular;
use crate::core::v0_10_0::cache::cachers;
use crate::core::v0_10_0::index::CommitReader;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{CommitEntry, DataFrameSize, LocalRepository};
use crate::opts::DFOpts;
use crate::util;

// THE DIFFERENCE BETWEEN WRAPPER AND SUMMARY IS JUST THE KEY NAME IN THE JSON RESPONSE
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffWrapper {
    pub tabular: TabularDiffSummaryImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffSummary {
    pub summary: TabularDiffSummaryImpl,
}

// Impl is so that we can wrap the json response in the "tabular" field to make summaries easier to distinguish
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffSummaryImpl {
    pub num_added_rows: usize,
    pub num_added_cols: usize,
    pub num_removed_rows: usize,
    pub num_removed_cols: usize,
    pub schema_has_changed: bool,
}

impl TabularDiffSummary {
    pub fn to_wrapper(&self) -> TabularDiffWrapper {
        TabularDiffWrapper {
            tabular: self.summary.clone(),
        }
    }
}

impl TabularDiffWrapper {
    pub fn from_file_nodes(
        base_entry: &Option<FileNode>,
        head_entry: &Option<FileNode>,
    ) -> Result<TabularDiffWrapper, OxenError> {
        match (base_entry, head_entry) {
            (Some(base_entry), Some(head_entry)) => {
                let base_size = match &base_entry.metadata {
                    Some(GenericMetadata::MetadataTabular(df_meta)) => DataFrameSize {
                        height: df_meta.tabular.height,
                        width: df_meta.tabular.width,
                    },
                    _ => return Err(OxenError::basic_str("Invalid metadata type")),
                };

                let head_size = match &head_entry.metadata {
                    Some(GenericMetadata::MetadataTabular(df_meta)) => DataFrameSize {
                        height: df_meta.tabular.height,
                        width: df_meta.tabular.width,
                    },
                    _ => return Err(OxenError::basic_str("Invalid metadata type")),
                };

                // TODO - this can be made less naive
                let schema_has_changed = base_size.width != head_size.width;

                let num_added_rows = if base_size.height < head_size.height {
                    head_size.height - base_size.height
                } else {
                    0
                };

                let num_removed_rows = if base_size.height > head_size.height {
                    base_size.height - head_size.height
                } else {
                    0
                };

                let num_added_cols = if base_size.width < head_size.width {
                    head_size.width - base_size.width
                } else {
                    0
                };

                let num_removed_cols = if base_size.width > head_size.width {
                    base_size.width - head_size.width
                } else {
                    0
                };

                Ok(TabularDiffWrapper {
                    tabular: TabularDiffSummaryImpl {
                        num_added_rows,
                        num_added_cols,
                        num_removed_rows,
                        num_removed_cols,
                        schema_has_changed,
                    },
                })
            }
            (Some(base_entry), None) => {
                let base_size = match &base_entry.metadata {
                    Some(GenericMetadata::MetadataTabular(df_meta)) => DataFrameSize {
                        height: df_meta.tabular.height,
                        width: df_meta.tabular.width,
                    },
                    _ => return Err(OxenError::basic_str("Invalid metadata type")),
                };

                Ok(TabularDiffWrapper {
                    tabular: TabularDiffSummaryImpl {
                        num_added_rows: 0,
                        num_added_cols: 0,
                        num_removed_rows: base_size.height,
                        num_removed_cols: base_size.width,
                        schema_has_changed: false,
                    },
                })
            }

            (None, Some(head_entry)) => {
                let head_size = match &head_entry.metadata {
                    Some(GenericMetadata::MetadataTabular(df_meta)) => DataFrameSize {
                        height: df_meta.tabular.height,
                        width: df_meta.tabular.width,
                    },
                    _ => return Err(OxenError::basic_str("Invalid metadata type")),
                };

                Ok(TabularDiffWrapper {
                    tabular: TabularDiffSummaryImpl {
                        num_added_rows: head_size.height,
                        num_added_cols: head_size.width,
                        num_removed_rows: 0,
                        num_removed_cols: 0,
                        schema_has_changed: false,
                    },
                })
            }

            (None, None) => Ok(TabularDiffWrapper {
                tabular: TabularDiffSummaryImpl {
                    num_added_rows: 0,
                    num_added_cols: 0,
                    num_removed_rows: 0,
                    num_removed_cols: 0,
                    schema_has_changed: false,
                },
            }),
        }
    }

    pub fn from_commit_entries(
        repo: &LocalRepository,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
    ) -> Result<TabularDiffWrapper, OxenError> {
        match (base_entry, head_entry) {
            (Some(base_entry), Some(head_entry)) => {
                let commit_reader = CommitReader::new(repo)?;

                let base_version_file = util::fs::version_path(repo, base_entry);
                let head_version_file = util::fs::version_path(repo, head_entry);

                let base_commit = commit_reader.get_commit_by_id(&base_entry.commit_id)?;
                let head_commit = commit_reader.get_commit_by_id(&head_entry.commit_id)?;

                let base_commit = if let Some(commit) = base_commit {
                    commit
                } else {
                    return Err(OxenError::resource_not_found(base_entry.commit_id.clone()));
                };

                let head_commit = if let Some(commit) = head_commit {
                    commit
                } else {
                    return Err(OxenError::resource_not_found(head_entry.commit_id.clone()));
                };

                let base_size = cachers::df_size::get_cache_for_version(
                    repo,
                    &base_commit,
                    &base_version_file,
                )?;

                let head_size = cachers::df_size::get_cache_for_version(
                    repo,
                    &head_commit,
                    &head_version_file,
                )?;

                // TODO - this can be made less naive
                let schema_has_changed = base_size.width != head_size.width;

                let num_added_rows = if base_size.height < head_size.height {
                    head_size.height - base_size.height
                } else {
                    0
                };

                let num_removed_rows = if base_size.height > head_size.height {
                    base_size.height - head_size.height
                } else {
                    0
                };

                let num_added_cols = if base_size.width < head_size.width {
                    head_size.width - base_size.width
                } else {
                    0
                };

                let num_removed_cols = if base_size.width > head_size.width {
                    base_size.width - head_size.width
                } else {
                    0
                };

                Ok(TabularDiffWrapper {
                    tabular: TabularDiffSummaryImpl {
                        num_added_rows,
                        num_added_cols,
                        num_removed_rows,
                        num_removed_cols,
                        schema_has_changed,
                    },
                })
            }
            (Some(base_entry), None) => {
                let base_version_file = util::fs::version_path(repo, base_entry);
                let base_commit =
                    CommitReader::new(repo)?.get_commit_by_id(&base_entry.commit_id)?;

                let base_commit = if let Some(commit) = base_commit {
                    commit
                } else {
                    return Err(OxenError::resource_not_found(base_entry.commit_id.clone()));
                };

                let base_size = cachers::df_size::get_cache_for_version(
                    repo,
                    &base_commit,
                    &base_version_file,
                )?;

                Ok(TabularDiffWrapper {
                    tabular: TabularDiffSummaryImpl {
                        num_added_rows: 0,
                        num_added_cols: 0,
                        num_removed_rows: base_size.height,
                        num_removed_cols: base_size.width,
                        schema_has_changed: false,
                    },
                })
            }

            (None, Some(head_entry)) => {
                let head_version_file = util::fs::version_path(repo, head_entry);

                let head_commit =
                    CommitReader::new(repo)?.get_commit_by_id(&head_entry.commit_id)?;

                let head_commit = if let Some(commit) = head_commit {
                    commit
                } else {
                    return Err(OxenError::resource_not_found(head_entry.commit_id.clone()));
                };

                let head_size = cachers::df_size::get_cache_for_version(
                    repo,
                    &head_commit,
                    &head_version_file,
                )?;

                Ok(TabularDiffWrapper {
                    tabular: TabularDiffSummaryImpl {
                        num_added_rows: head_size.height,
                        num_added_cols: head_size.width,
                        num_removed_rows: 0,
                        num_removed_cols: 0,
                        schema_has_changed: false,
                    },
                })
            }

            (None, None) => Ok(TabularDiffWrapper {
                tabular: TabularDiffSummaryImpl {
                    num_added_rows: 0,
                    num_added_cols: 0,
                    num_removed_rows: 0,
                    num_removed_cols: 0,
                    schema_has_changed: false,
                },
            }),
        }
    }

    pub fn maybe_get_df_from_file_node(
        repo: &LocalRepository,
        node: &Option<FileNode>,
    ) -> Option<DataFrame> {
        match node {
            Some(node) => {
                let version_path = util::fs::version_path_from_hash(repo, node.hash.to_string());
                match tabular::read_df_with_extension(
                    version_path,
                    node.extension.clone(),
                    &DFOpts::empty(),
                ) {
                    Ok(df) => Some(df),
                    Err(_) => None,
                }
            }
            None => None,
        }
    }

    pub fn maybe_get_df_from_commit_entry(
        repo: &LocalRepository,
        entry: &Option<CommitEntry>,
    ) -> Option<DataFrame> {
        match entry {
            Some(entry) => {
                let version_path = util::fs::version_path(repo, entry);
                match tabular::read_df(version_path, DFOpts::empty()) {
                    Ok(df) => Some(df),
                    Err(_) => None,
                }
            }
            None => None,
        }
    }

    pub fn maybe_get_size(df: &Option<DataFrame>) -> Option<DataFrameSize> {
        df.as_ref().map(|df| DataFrameSize {
            height: df.height(),
            width: df.width(),
        })
    }

    pub fn schema_has_changed(base_df: &Option<DataFrame>, head_df: &Option<DataFrame>) -> bool {
        if base_df.is_none() && head_df.is_none() {
            return false;
        }

        if let Some(base_df) = base_df {
            if let Some(head_df) = head_df {
                return TabularDiffWrapper::schema_has_changed_df(base_df, head_df);
            }
        }

        // if we get here, one of the dataframes is None and the other is not
        true
    }

    fn schema_has_changed_df(base_df: &DataFrame, head_df: &DataFrame) -> bool {
        let base_schema = base_df.schema();
        let head_schema = head_df.schema();

        // compare the schemas
        base_schema != head_schema
    }
}
