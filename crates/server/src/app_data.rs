use std::path::PathBuf;

use liboxen::config::repository_config::MerkleStoreKind;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct OxenAppData {
    pub path: PathBuf,
    pub config: Config,
    pub merkle_store_kind: MerkleStoreKind,
}

impl OxenAppData {
    #[cfg(test)]
    pub fn new(path: PathBuf) -> OxenAppData {
        OxenAppData {
            path,
            config: Config::default(),
            merkle_store_kind: MerkleStoreKind::default(),
        }
    }
}
