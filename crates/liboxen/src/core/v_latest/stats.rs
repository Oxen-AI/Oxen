use crate::constants::DEFAULT_BRANCH_NAME;
use crate::error::OxenError;
use crate::model::DataTypeStat;
use crate::model::EntryDataType;
use crate::model::LocalRepository;
use crate::model::RepoStats;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::repositories;
use std::collections::HashMap;
use std::str::FromStr;

pub fn get_stats(repo: &LocalRepository) -> Result<RepoStats, OxenError> {
    let mut data_size: u64 = 0;
    let mut data_types: HashMap<EntryDataType, DataTypeStat> = HashMap::new();

    match repositories::revisions::get(repo, DEFAULT_BRANCH_NAME) {
        Ok(Some(commit)) => {
            let Some(commit_node) = repositories::tree::get_root(repo, &commit)? else {
                log::error!("Error getting root dir for main branch commit");
                return Ok(RepoStats {
                    data_size: 0,
                    data_types: HashMap::new(),
                });
            };
            let dir_node = repositories::tree::get_root_dir(&commit_node)?;

            if let EMerkleTreeNode::Directory(dir_node) = &dir_node.node {
                data_size = dir_node.num_bytes();
                for data_type_count in dir_node.data_types() {
                    let data_type = EntryDataType::from_str(&data_type_count.data_type).unwrap();
                    let count = data_type_count.count;
                    let size = dir_node
                        .data_type_sizes()
                        .get(&data_type_count.data_type)
                        .unwrap();
                    let data_type_stat = DataTypeStat {
                        data_size: *size,
                        data_type: data_type.to_owned(),
                        file_count: count,
                    };
                    data_types.insert(data_type, data_type_stat);
                }
            }
        }
        _ => {
            log::debug!("Error getting main branch commit");
        }
    }

    Ok(RepoStats {
        data_size,
        data_types,
    })
}
