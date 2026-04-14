use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DownloadTreeOpts {
    pub subtree_paths: PathBuf,
    pub depth: i32,
    pub is_download: bool,
}
