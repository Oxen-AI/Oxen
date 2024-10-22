use serde::{Deserialize, Serialize};

use crate::model::StagedDirStats;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// This is a representation of directories that are staged and we want a top level summary of counts
/// For example:
///   annotations/
///     train/
///       annotations.csv
///       one_shot.csv
///       unstaged.csv
///     test/
///       annotations.csv
///
/// Would have:
///     annotations/train/ -> num_staged: 2, total: 3
///     annotations/test/ -> num_staged: 1, total: 1
///
/// Rolled up to:
///     annotations/ -> num_staged: 3, total: 4

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SummarizedStagedDirStats {
    pub num_files_staged: usize,
    pub total_files: usize,
    pub paths: HashMap<PathBuf, Vec<StagedDirStats>>,
}

impl Default for SummarizedStagedDirStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SummarizedStagedDirStats {
    pub fn new() -> SummarizedStagedDirStats {
        SummarizedStagedDirStats {
            num_files_staged: 0,
            total_files: 0,
            paths: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub fn len(&self) -> usize {
        let mut total = 0;
        for stats in self.paths.values() {
            total += stats.len();
        }
        total
    }

    pub fn contains_key(&self, path: &Path) -> bool {
        self.paths.contains_key(path)
    }

    fn rollup_stats(&self, path: &Path, stats: &Vec<StagedDirStats>) -> StagedDirStats {
        let mut num_staged = 0;
        let mut total = 0;
        for stat in stats {
            num_staged += stat.num_files_staged;
            total += stat.total_files;
        }
        StagedDirStats {
            path: path.to_path_buf(),
            num_files_staged: num_staged,
            total_files: total,
            status: super::StagedEntryStatus::Added, // don't use this for anything right now
        }
    }

    pub fn get(&self, path: &Path) -> Option<StagedDirStats> {
        self.paths
            .get(path)
            .map(|stats| self.rollup_stats(path, stats))
    }

    pub fn add_stats(&mut self, stats: &StagedDirStats) {
        log::debug!("Adding stats: {:?}", stats);
        if let Some(first_component) = stats.path.components().next() {
            let path: &Path = first_component.as_ref();
            let path = path.to_path_buf();

            self.num_files_staged += stats.num_files_staged;
            self.total_files += stats.total_files;

            self.paths.entry(path).or_default().push(stats.clone());
        } else {
            let path = PathBuf::from("");

            self.num_files_staged += stats.num_files_staged;
            self.total_files += stats.total_files;

            self.paths.entry(path).or_default().push(stats.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{StagedDirStats, StagedEntryStatus, SummarizedStagedDirStats};

    use std::path::PathBuf;

    #[test]
    fn test_summarized_stats_add_stats() {
        let mut summarized = SummarizedStagedDirStats::new();
        let stats_annotations = StagedDirStats {
            path: PathBuf::from("annotations"),
            total_files: 0,
            num_files_staged: 0,
            status: StagedEntryStatus::Added,
        };

        let stats_train = StagedDirStats {
            path: PathBuf::from("annotations").join("train"),
            total_files: 3,
            num_files_staged: 2,
            status: StagedEntryStatus::Added,
        };

        let stats_test = StagedDirStats {
            path: PathBuf::from("annotations").join("test"),
            total_files: 1,
            num_files_staged: 1,
            status: StagedEntryStatus::Added,
        };

        summarized.add_stats(&stats_annotations);
        summarized.add_stats(&stats_train);
        summarized.add_stats(&stats_test);

        assert_eq!(summarized.len(), 3);
        assert_eq!(summarized.num_files_staged, 3);
        assert_eq!(summarized.total_files, 4);
    }
}
