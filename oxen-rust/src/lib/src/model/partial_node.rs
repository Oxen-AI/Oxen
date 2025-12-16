use crate::model::MerkleHash;
use crate::util;
use filetime::FileTime;

// TODO: Either deprecate this struct, or update it to include schema

// Reduced form of the FileNode, used to save space
#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub struct PartialNode {
    pub hash: MerkleHash,
    pub last_modified: FileTime,
    pub size: u64,
}

impl PartialNode {
    pub fn from(
        hash: MerkleHash,
        last_modified_seconds: i64,
        last_modified_nanoseconds: u32,
        size: u64,
    ) -> Self {
        let last_modified =
            util::fs::last_modified_time(last_modified_seconds, last_modified_nanoseconds);
        PartialNode {
            hash,
            last_modified,
            size,
        }
    }
}
