use crate::api;
use crate::core::v0_19_0::structs::PullProgress;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::CommitEntry;
use crate::model::LocalRepository;
use crate::model::MetadataEntry;
use crate::model::RemoteRepository;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core;
use crate::model::MerkleHash;
use std::str::FromStr;

pub async fn download_dir(
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    local_path: &Path,
) -> Result<(), OxenError> {
    log::debug!("downloading dir {:?}", entry.filename);
    // Initialize temp repo to download node into
    // TODO: Where should this repo be?
    let tmp_repo = LocalRepository::new(local_path)?;

    // Find and download dir node and its children from remote repo
    let hash = MerkleHash::from_str(&entry.hash)?;
    let dir_node = api::client::tree::download_tree_from(&tmp_repo, remote_repo, &hash).await?;

    // Create local directory to pull entries into
    let directory = PathBuf::from("");
    let pull_progress = PullProgress::new();

    // Recursively pull entries
    r_download_entries(
        remote_repo,
        &tmp_repo.path.join(&entry.filename),
        &dir_node,
        &directory,
        &pull_progress,
    )
    .await?;

    Ok(())
}

async fn r_download_entries(
    remote_repo: &RemoteRepository,
    local_repo_path: &Path,
    node: &MerkleTreeNode,
    directory: &Path,
    pull_progress: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    log::debug!("downloading entries for {:?}", directory);
    for child in &node.children {
        log::debug!("downloading entry {:?}", child.hash);

        let mut new_directory = directory.to_path_buf();
        if let EMerkleTreeNode::Directory(dir_node) = &child.node {
            new_directory.push(&dir_node.name);
        }

        if child.has_children() {
            Box::pin(r_download_entries(
                remote_repo,
                local_repo_path,
                child,
                &new_directory,
                pull_progress,
            ))
            .await?;
        }

        if let EMerkleTreeNode::VNode(_) = &node.node {
            let mut entries: Vec<Entry> = vec![];

            for child in &node.children {
                if let EMerkleTreeNode::File(file_node) = &child.node {
                    entries.push(Entry::CommitEntry(CommitEntry {
                        commit_id: file_node.last_commit_id.to_string(),
                        path: directory.join(&file_node.name),
                        hash: child.hash.to_string(),
                        num_bytes: file_node.num_bytes,
                        last_modified_seconds: file_node.last_modified_seconds,
                        last_modified_nanoseconds: file_node.last_modified_nanoseconds,
                    }));
                }
            }

            log::debug!("downloading {} entries to working dir", entries.len());
            core::v0_10_0::index::puller::pull_entries_to_working_dir(
                remote_repo,
                &entries,
                local_repo_path,
                pull_progress,
            )
            .await?;
        }
    }

    Ok(())
}
