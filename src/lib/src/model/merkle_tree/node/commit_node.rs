use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;

use crate::error::OxenError;
use crate::model::Commit;
use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct CommitNode {
    pub hash: MerkleHash,
    pub dtype: MerkleTreeNodeType,
    pub parent_ids: Vec<MerkleHash>,
    pub message: String,
    pub author: String,
    pub email: String,
    pub timestamp: OffsetDateTime,
}

impl CommitNode {
    pub fn to_commit(&self) -> Commit {
        Commit {
            id: self.hash.to_string(),
            parent_ids: self.parent_ids.iter().map(|id| id.to_string()).collect(),
            email: self.email.to_owned(),
            author: self.author.to_owned(),
            message: self.message.to_owned(),
            timestamp: self.timestamp.to_owned(),
            root_hash: None,
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<CommitNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing commit: {e}")))
    }
}

impl Default for CommitNode {
    fn default() -> Self {
        CommitNode {
            hash: MerkleHash::new(0),
            dtype: MerkleTreeNodeType::Commit,
            parent_ids: vec![],
            message: "".to_string(),
            author: "".to_string(),
            email: "".to_string(),
            timestamp: OffsetDateTime::now_utc(),
        }
    }
}

impl MerkleTreeNodeIdType for CommitNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn hash(&self) -> MerkleHash {
        self.hash
    }
}

impl TMerkleTreeNode for CommitNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "CommitNode")?;
        writeln!(f, "\tmessage: {}", self.message)?;
        writeln!(f, "\tparent_ids: {:?}", self.parent_ids)?;
        writeln!(f, "\tauthor: {}", self.author)?;
        writeln!(f, "\temail: {}", self.email)?;
        writeln!(f, "\ttimestamp: {}", self.timestamp)?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let parent_ids = self
            .parent_ids
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        write!(
            f,
            "\"{}\" -> {} {} parent_ids {:?}",
            self.message, self.author, self.email, parent_ids
        )
    }
}
