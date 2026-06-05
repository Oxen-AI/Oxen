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
    pub fn from_path<P: AsRef<Path>>(path: P) -> RestoreOpts {
        let mut paths = HashSet::new();
        paths.insert(path.as_ref().to_owned());

        RestoreOpts {
            paths,
            staged: false,
            is_remote: false,
            source_ref: None,
        }
    }

    pub fn from_staged_path<P: AsRef<Path>>(path: P) -> RestoreOpts {
        let mut paths = HashSet::new();
        paths.insert(path.as_ref().to_owned());

        RestoreOpts {
            paths,
            staged: true,
            is_remote: false,
            source_ref: None,
        }
    }

    pub fn from_path_ref<P: AsRef<Path>, S: AsRef<str>>(path: P, source_ref: S) -> RestoreOpts {
        let mut paths = HashSet::new();
        paths.insert(path.as_ref().to_owned());

        RestoreOpts {
            paths,
            staged: false,
            is_remote: false,
            source_ref: Some(source_ref.as_ref().to_owned()),
        }
    }
}
