use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DiffOpts {
    pub repo_dir: Option<PathBuf>,
    pub path_1: PathBuf,
    pub path_2: Option<PathBuf>,
    pub keys: Vec<String>,
    pub targets: Vec<String>,
    pub revision_1: Option<String>,
    pub revision_2: Option<String>,
    pub output: Option<PathBuf>,
    pub page: usize,
    pub page_size: usize,
}

impl Default for DiffOpts {
    fn default() -> Self {
        Self {
            repo_dir: None,
            path_1: PathBuf::new(),
            path_2: None,
            keys: Vec::new(),
            targets: Vec::new(),
            revision_1: None,
            revision_2: None,
            output: None,
            page: 1,
            page_size: 100,
        }
    }
}
