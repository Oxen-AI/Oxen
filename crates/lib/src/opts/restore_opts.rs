use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RestoreOpts {
    pub paths: HashSet<PathBuf>,
    pub staged: bool,
    pub is_remote: bool,
    pub source_ref: Option<String>, // commit id or branch name
}

impl RestoreOpts {
    pub fn from_path(path: &Path) -> RestoreOpts {
        let mut paths = HashSet::new();
        paths.insert(path.to_owned());

        RestoreOpts {
            paths,
            staged: false,
            is_remote: false,
            source_ref: None,
        }
    }

    pub fn from_staged_path(path: &Path) -> RestoreOpts {
        let mut paths = HashSet::new();
        paths.insert(path.to_owned());

        RestoreOpts {
            paths,
            staged: true,
            is_remote: false,
            source_ref: None,
        }
    }

    pub fn from_path_ref(path: &Path, source_ref: &str) -> RestoreOpts {
        let mut paths = HashSet::new();
        paths.insert(path.to_owned());

        RestoreOpts {
            paths,
            staged: false,
            is_remote: false,
            source_ref: Some(source_ref.to_owned()),
        }
    }
}
