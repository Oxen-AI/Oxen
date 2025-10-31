use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct GlobOpts {
    pub paths: Vec<PathBuf>,
    pub staged_db: bool,
    pub merkle_tree: bool,
    pub working_dir: bool,
    pub walk_dirs: bool,
}
