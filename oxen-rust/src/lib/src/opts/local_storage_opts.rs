use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct LocalStorageOpts {
    pub path: Option<PathBuf>,
}
