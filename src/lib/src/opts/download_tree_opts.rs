use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DownloadTreeOpts {
    pub subtree_paths: PathBuf,
    pub depth: i32,
}

impl Default for DownloadTreeOpts {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadTreeOpts {
    pub fn new() -> DownloadTreeOpts {
        DownloadTreeOpts {
            subtree_paths: PathBuf::new(),
            depth: 0,
        }
    }
}