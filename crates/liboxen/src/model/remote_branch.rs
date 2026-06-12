use crate::constants;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteBranch {
    pub remote: String,
    pub branch: String,
}
impl Default for RemoteBranch {
    fn default() -> RemoteBranch {
        RemoteBranch {
            remote: String::from(constants::DEFAULT_REMOTE_NAME),
            branch: String::from(constants::DEFAULT_BRANCH_NAME),
        }
    }
}
