use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::model::MerkleHash;
use crate::model::merkle_tree::merkle_hash::MerkleHashAsString;
use crate::view::StatusMessage;

use utoipa::ToSchema;

#[serde_as]
#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct MerkleHashes {
    #[serde_as(as = "HashSet<MerkleHashAsString>")]
    pub hashes: HashSet<MerkleHash>,
}

#[serde_as]
#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct MerkleHashesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde_as(as = "HashSet<MerkleHashAsString>")]
    pub hashes: HashSet<MerkleHash>,
}
