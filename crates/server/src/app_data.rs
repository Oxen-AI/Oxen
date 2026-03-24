use std::path::PathBuf;

pub struct OxenAppData {
    pub path: PathBuf,
}

impl OxenAppData {
    pub fn new(path: PathBuf) -> OxenAppData {
        OxenAppData { path }
    }
}

impl Clone for OxenAppData {
    fn clone(&self) -> Self {
        OxenAppData {
            path: self.path.clone(),
        }
    }
}
