use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct InfoOpts {
    pub path: PathBuf,
    pub revision: Option<String>, // commit id or branch
    pub verbose: bool,
    pub output_as_json: bool,
}
