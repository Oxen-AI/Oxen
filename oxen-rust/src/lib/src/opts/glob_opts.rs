use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct GlobOpts {
    pub paths: Vec<PathBuf>,
    pub staged_db: bool,
    pub merkle_tree: bool,
    pub working_dir: bool,
    pub walk_dirs: bool,
}

impl Default for GlobOpts {
    fn default() -> Self {
        GlobOpts {
            paths: Vec::new(),
            staged_db: false,
            merkle_tree: true,
            working_dir: false,
            walk_dirs: false,
        }
    }
}
