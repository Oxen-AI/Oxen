use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct AddOpts {
    pub paths: Vec<PathBuf>,
    pub directory: Option<PathBuf>,
    pub is_remote: bool,
}
