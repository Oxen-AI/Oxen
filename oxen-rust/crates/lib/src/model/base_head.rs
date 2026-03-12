use serde::{Deserialize, Serialize};

/// For creating a remote repo we need the repo name
/// and we need the root commit so that we do not generate a new one on creation on the server
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BaseHead {
    pub base: String,
    pub head: String,
}

impl BaseHead {
    pub fn new(base: String, head: String) -> BaseHead {
        BaseHead { base, head }
    }
}

impl std::fmt::Display for BaseHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.base, self.head)
    }
}

impl std::error::Error for BaseHead {}
