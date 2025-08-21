use crate::core::progress::pull_progress::PullProgress;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::CommitEntry;
use crate::model::LocalRepository;
use crate::model::MetadataEntry;
use crate::model::RemoteRepository;
use crate::{api, repositories};
use std::path::Path;
use std::sync::Arc;

use crate::core;

pub async fn download_dir(
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let remote_path = remote_path.as_ref();
    let local_path = local_path.as_ref();
    log::debug!("downloading dir {:?}", remote_path);
    // Initialize temp repo to download node into
    // TODO: Where should this repo be?
    let tmp_repo = LocalRepository::new(local_path)?;

    // Find and download dir node and its children from remote repo
    let commit_id = &entry.latest_commit.as_ref().unwrap().id;
    let dir_node = api::client::tree::download_tree_from_path(
        &tmp_repo,
        remote_repo,
        commit_id,
        remote_path.to_string_lossy(),
        true,
    )
    .await?;

    // Track Progress
    let pull_progress = Arc::new(PullProgress::new());

    // Recursively pull entries
    r_download_entries(
        remote_repo,
        &tmp_repo.path,
        &dir_node,
        remote_path,
        &pull_progress,
    )
    .await?;

    Ok(())
}

pub async fn download_dir_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let remote_path = remote_path.as_ref();
    let local_path = local_path.as_ref();
    log::debug!("downloading dir {:?}", remote_path);

    // Get tree from local repos
    let commit = &entry.latest_commit.as_ref().unwrap();
    let Some(dir_node) =
        repositories::tree::get_dir_with_children_recursive(local_repo, commit, remote_path)?
    else {
        log::warn!("Dir node not found for path {local_path:?}");
        return Ok(());
    };

    // Recursively pull entries
    let pull_progress = Arc::new(PullProgress::new());
    r_download_entries(
        remote_repo,
        &local_repo.path,
        &dir_node,
        remote_path,
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
    for child in &node.children {

        let mut new_directory = directory.to_path_buf();
        if let EMerkleTreeNode::Directory(dir_node) = &child.node {
            new_directory.push(dir_node.name());
        }

        let has_children = child.has_children();
        if has_children {
            Box::pin(r_download_entries(
                remote_repo,
                local_repo_path,
                child,
                &new_directory,
                pull_progress,
            ))
            .await?;
        }
    }

    if let EMerkleTreeNode::VNode(_) = &node.node {
        let mut entries: Vec<Entry> = vec![];

        for child in &node.children {
            if let EMerkleTreeNode::File(file_node) = &child.node {
                entries.push(Entry::CommitEntry(CommitEntry {
                    commit_id: file_node.last_commit_id().to_string(),
                    path: directory.join(file_node.name()),
                    hash: child.hash.to_string(),
                    num_bytes: file_node.num_bytes(),
                    last_modified_seconds: file_node.last_modified_seconds(),
                    last_modified_nanoseconds: file_node.last_modified_nanoseconds(),
                }));
            }
        }

        log::debug!(
            "r_download_entries downloading {} entries to working dir",
            entries.len()
        );
        core::v_latest::fetch::pull_entries_to_working_dir(
            remote_repo,
            &entries,
            local_repo_path,
            pull_progress,
        )
        .await?;
    }

    Ok(())
}
