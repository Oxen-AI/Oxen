use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};

#[derive(Clone, Debug)]
pub struct PushOpts {
    pub remote: String,
    pub branch: String,
    pub delete: bool,
    pub force: bool,
    pub missing_files: bool,
}


impl Default for PushOpts {
    fn default() -> Self {
        Self {
            remote: DEFAULT_REMOTE_NAME.to_string(),
            branch: DEFAULT_BRANCH_NAME.to_string(),
            delete: false,
            force: false,
            missing_files: false,
        }
    }
}
