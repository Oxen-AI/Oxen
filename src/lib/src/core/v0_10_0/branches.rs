use super::index::{CommitEntryReader, CommitReader, CommitWriter, EntryIndexer};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, RemoteBranch};
use crate::{api, repositories};

use std::collections::HashSet;
use std::path::Path;

pub async fn checkout(repo: &LocalRepository, branch_name: &str) -> Result<(), OxenError> {
    let branch = repositories::branches::get_by_name(repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;
    let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&branch.commit_id))?;

    // Sync changes if needed
    maybe_pull_missing_entries(repo, &commit).await?;

    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_commit(&commit).await
}

pub async fn checkout_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    let commit_id = commit_id.as_ref();
    let commit = repositories::commits::get_by_id(repo, commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(commit_id))?;
    let commit_writer = CommitWriter::new(repo)?;
    commit_writer.set_working_repo_to_commit(&commit).await
}

async fn maybe_pull_missing_entries(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    // If we don't have a remote, there are not missing entries, so return
    let rb = RemoteBranch::default();
    let remote = repo.get_remote(&rb.remote);
    let Some(remote) = remote else {
        log::debug!("No remote, no missing entries to fetch");
        return Ok(());
    };

    match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(remote_repo)) => {
            let indexer = EntryIndexer::new(repo)?;
            indexer
                .pull_all_entries_for_commit(&remote_repo, commit)
                .await?;
        }
        Ok(None) => {
            log::debug!("No remote repo found, no entries to fetch");
        }
        Err(err) => {
            log::error!("Error getting remote repo: {}", err);
        }
    };

    Ok(())
}

pub fn list_entry_versions_for_commit(
    local_repo: &LocalRepository,
    commit_id: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    let commit_reader = CommitReader::new(local_repo)?;

    let root_commit = commit_reader.root_commit()?;
    let mut branch_commits = commit_reader.history_from_base_to_head(&root_commit.id, commit_id)?;

    // Sort on timestamp oldest to newest
    branch_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut result: Vec<(Commit, CommitEntry)> = Vec::new();
    let mut seen_hashes: HashSet<String> = HashSet::new();

    for commit in branch_commits {
        let entry_reader = CommitEntryReader::new(local_repo, &commit)?;
        let entry = entry_reader.get_entry(path)?;

        if let Some(entry) = entry {
            if !seen_hashes.contains(&entry.hash) {
                seen_hashes.insert(entry.hash.clone());
                result.push((commit, entry));
            }
        }
    }

    result.reverse();

    Ok(result)
}
