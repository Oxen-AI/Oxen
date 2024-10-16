use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::model::MerkleHash;
use crate::view::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct MerkleHashes {
    pub hashes: HashSet<MerkleHash>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct MerkleHashesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub hashes: HashSet<MerkleHash>,
}
