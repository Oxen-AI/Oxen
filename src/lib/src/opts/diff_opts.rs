use std::path::PathBuf;

#[derive(Default, Clone, Debug)]
pub struct DiffOpts {
    pub repo_dir: Option<PathBuf>,
    pub path_1: PathBuf,
    pub path_2: Option<PathBuf>,
    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub revision_1: Option<String>,
    pub revision_2: Option<String>,
    pub output: Option<PathBuf>,
}
