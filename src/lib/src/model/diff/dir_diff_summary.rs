use serde::{Deserialize, Serialize};

use crate::{
    error::OxenError,
    model::{diff::AddRemoveModifyCounts, merkle_tree::node::DirNode},
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiffSummary {
    pub dir: DirDiffSummaryImpl,
}

// Impl is so that we can wrap the json response in the "dir" field to make summaries easier to distinguish
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DirDiffSummaryImpl {
    pub file_counts: AddRemoveModifyCounts,
}

impl DirDiffSummary {
    pub fn from_dir_nodes(
        base_entry: &Option<DirNode>,
        head_entry: &Option<DirNode>,
    ) -> Result<DirDiffSummary, OxenError> {
        match (base_entry, head_entry) {
            (Some(base_entry), Some(head_entry)) => {
                let base_num_files = base_entry.num_files();
                let head_num_files = head_entry.num_files();

                let num_added_files = if base_num_files < head_num_files {
                    head_num_files - base_num_files
                } else {
                    0
                };

                let num_removed_files = if base_num_files > head_num_files {
                    base_num_files - head_num_files
                } else {
                    0
                };

                Ok(DirDiffSummary {
                    dir: DirDiffSummaryImpl {
                        file_counts: AddRemoveModifyCounts {
                            added: num_added_files as usize,
                            removed: num_removed_files as usize,
                            modified: 0,
                        },
                    },
                })
            }
            (Some(base_entry), None) => {
                let num_files = base_entry.num_files();

                Ok(DirDiffSummary {
                    dir: DirDiffSummaryImpl {
                        file_counts: AddRemoveModifyCounts {
                            added: 0,
                            removed: num_files as usize,
                            modified: 0,
                        },
                    },
                })
            }

            (None, Some(head_entry)) => {
                let num_files = head_entry.num_files();

                Ok(DirDiffSummary {
                    dir: DirDiffSummaryImpl {
                        file_counts: AddRemoveModifyCounts {
                            added: num_files as usize,
                            removed: 0,
                            modified: 0,
                        },
                    },
                })
            }

            (None, None) => Ok(DirDiffSummary {
                dir: DirDiffSummaryImpl {
                    file_counts: AddRemoveModifyCounts {
                        added: 0,
                        removed: 0,
                        modified: 0,
                    },
                },
            }),
        }
    }
}
