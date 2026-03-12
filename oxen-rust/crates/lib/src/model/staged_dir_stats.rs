use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::StagedEntryStatus;

// Used for a quick summary of directory
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StagedDirStats {
    pub path: PathBuf,
    pub num_files_staged: usize,
    pub total_files: usize,
    pub status: StagedEntryStatus,
}

impl StagedDirStats {
    pub fn from<T: AsRef<Path>>(path: T, status: StagedEntryStatus) -> StagedDirStats {
        StagedDirStats {
            path: path.as_ref().to_path_buf(),
            num_files_staged: 0,
            total_files: 0,
            status,
        }
    }
}

// Hash on the path field so we can quickly look up
impl PartialEq for StagedDirStats {
    fn eq(&self, other: &StagedDirStats) -> bool {
        self.path == other.path
    }
}
impl Eq for StagedDirStats {}
impl Hash for StagedDirStats {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}
