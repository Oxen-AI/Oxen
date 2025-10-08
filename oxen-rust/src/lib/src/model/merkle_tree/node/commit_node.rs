//! Wrapper around the CommitNodeData struct to support old versions of the commit node

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use time::OffsetDateTime;

use crate::core::v_latest::model::merkle_tree::node::commit_node::CommitNodeData as CommitNodeDataV0_25_0;
use crate::core::v_old::v0_19_0::model::merkle_tree::node::commit_node::CommitNodeData as CommitNodeDataV0_19_0;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

pub trait TCommitNode {
    fn node_type(&self) -> &MerkleTreeNodeType;
    fn version(&self) -> MinOxenVersion;
    fn hash(&self) -> &MerkleHash;
    fn parent_ids(&self) -> &Vec<MerkleHash>;
    fn message(&self) -> &str;
    fn author(&self) -> &str;
    fn email(&self) -> &str;
    fn timestamp(&self) -> &OffsetDateTime;
}

pub struct CommitNodeOpts {
    pub hash: MerkleHash,
    pub parent_ids: Vec<MerkleHash>,
    pub email: String,
    pub author: String,
    pub message: String,
    pub timestamp: OffsetDateTime,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum ECommitNode {
    // This is for backwards compatibility to load older versions from disk
    V0_25_0(CommitNodeDataV0_25_0),
    V0_19_0(CommitNodeDataV0_19_0),
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct CommitNode {
    node: ECommitNode,
}

impl CommitNode {
    pub fn new(repo: &LocalRepository, opts: CommitNodeOpts) -> Result<CommitNode, OxenError> {
        match repo.min_version() {
            MinOxenVersion::V0_19_0 => Ok(CommitNode {
                node: ECommitNode::V0_19_0(CommitNodeDataV0_19_0 {
                    hash: opts.hash,
                    parent_ids: opts.parent_ids,
                    email: opts.email,
                    author: opts.author,
                    message: opts.message,
                    timestamp: opts.timestamp,
                    node_type: MerkleTreeNodeType::Commit,
                }),
            }),
            MinOxenVersion::LATEST | MinOxenVersion::V0_25_0 => Ok(CommitNode {
                node: ECommitNode::V0_25_0(CommitNodeDataV0_25_0 {
                    hash: opts.hash,
                    parent_ids: opts.parent_ids,
                    email: opts.email,
                    author: opts.author,
                    message: opts.message,
                    timestamp: opts.timestamp,
                    node_type: MerkleTreeNodeType::Commit,
                }),
            }),
            _ => Err(OxenError::basic_str(
                "CommitNode not supported in this version",
            )),
        }
    }

    pub fn from_commit(commit: Commit) -> CommitNode {
        CommitNode {
            node: ECommitNode::V0_25_0(CommitNodeDataV0_25_0 {
                hash: MerkleHash::from_str(&commit.id).unwrap(),
                parent_ids: commit
                    .parent_ids
                    .iter()
                    .map(|id| MerkleHash::from_str(id).unwrap())
                    .collect(),
                email: commit.email.clone(),
                author: commit.author.clone(),
                message: commit.message.clone(),
                timestamp: commit.timestamp,
                node_type: MerkleTreeNodeType::Commit,
            }),
        }
    }

    pub fn to_commit(&self) -> Commit {
        Commit {
            id: self.hash().to_string(),
            parent_ids: self.parent_ids().iter().map(|id| id.to_string()).collect(),
            email: self.email().to_owned(),
            author: self.author().to_owned(),
            message: self.message().to_owned(),
            timestamp: self.timestamp().to_owned(),
        }
    }

    pub fn get_opts(&self) -> CommitNodeOpts {
        match self.node {
            ECommitNode::V0_25_0(ref commit) => CommitNodeOpts {
                hash: commit.hash,
                parent_ids: commit.parent_ids.clone(),
                email: commit.email.clone(),
                author: commit.author.clone(),
                message: commit.message.clone(),
                timestamp: commit.timestamp,
            },
            ECommitNode::V0_19_0(ref commit) => CommitNodeOpts {
                hash: commit.hash,
                parent_ids: commit.parent_ids.clone(),
                email: commit.email.clone(),
                author: commit.author.clone(),
                message: commit.message.clone(),
                timestamp: commit.timestamp,
            },
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<CommitNode, OxenError> {
        // In order to support versions that didn't have the enum,
        // if it fails we will fall back to the old struct, then populate the enum
        let commit: CommitNode = match rmp_serde::from_slice(data) {
            Ok(node) => node,
            Err(_) => {
                // This is a fallback for old versions of the commit node
                log::debug!("Deserializing old commit node version");
                let commit: CommitNodeDataV0_19_0 = rmp_serde::from_slice(data)?;
                log::debug!("Deserialized old commit node version: {commit:?}");
                Self {
                    node: ECommitNode::V0_19_0(commit),
                }
            }
        };
        Ok(commit)
    }

    fn node(&self) -> &dyn TCommitNode {
        match self.node {
            ECommitNode::V0_25_0(ref commit) => commit,
            ECommitNode::V0_19_0(ref commit) => commit,
        }
    }

    pub fn version(&self) -> MinOxenVersion {
        self.node().version()
    }

    pub fn hash(&self) -> &MerkleHash {
        self.node().hash()
    }

    pub fn parent_ids(&self) -> &Vec<MerkleHash> {
        self.node().parent_ids()
    }

    pub fn message(&self) -> &str {
        self.node().message()
    }

    pub fn author(&self) -> &str {
        self.node().author()
    }

    pub fn email(&self) -> &str {
        self.node().email()
    }

    pub fn timestamp(&self) -> &OffsetDateTime {
        self.node().timestamp()
    }
}

impl Default for CommitNode {
    fn default() -> Self {
        CommitNode {
            node: ECommitNode::V0_25_0(CommitNodeDataV0_25_0 {
                hash: MerkleHash::new(0),
                node_type: MerkleTreeNodeType::Commit,
                parent_ids: vec![],
                message: "".to_string(),
                author: "".to_string(),
                email: "".to_string(),
                timestamp: OffsetDateTime::now_utc(),
            }),
        }
    }
}

impl MerkleTreeNodeIdType for CommitNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        *self.node().node_type()
    }

    fn hash(&self) -> MerkleHash {
        *self.node().hash()
    }
}

impl TMerkleTreeNode for CommitNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "CommitNode")?;
        writeln!(f, "\tmessage: {}", self.message())?;
        writeln!(f, "\tparent_ids: {:?}", self.parent_ids())?;
        writeln!(f, "\tauthor: {}", self.author())?;
        writeln!(f, "\temail: {}", self.email())?;
        writeln!(f, "\ttimestamp: {}", self.timestamp())?;
        Ok(())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for CommitNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let parent_ids = self
            .parent_ids()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        write!(
            f,
            "({}) \"{}\" -> {} {} parent_ids {:?}",
            self.version(),
            self.message(),
            self.author(),
            self.email(),
            parent_ids
        )
    }
}
