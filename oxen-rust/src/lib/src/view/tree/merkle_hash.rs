use serde::{Deserialize, Serialize};

use crate::model::merkle_tree::merkle_hash::MerkleHashAsString;
use crate::model::MerkleHash;
use crate::view::StatusMessage;

use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct MerkleHashResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(with = "MerkleHashAsString")]
    pub hash: MerkleHash,
}
