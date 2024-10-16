use crate::api;
use crate::constants::OBJECTS_DIR;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::core::v0_10_0::commits::merge_objects_dbs;
use crate::core::v0_10_0::index::{puller, CommitEntryReader, ObjectDBReader};
use crate::core::v0_19_0::structs::PullProgress;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::MetadataEntry;
use crate::model::RemoteRepository;
use crate::util;
use std::path::Path;

pub async fn download_dir(
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    local_path: &Path,
) -> Result<(), OxenError> {
    // Download the commit db for the given commit id or branch
    let commit_id = &entry.resource.as_ref().unwrap().commit.as_ref().unwrap().id;
    let home_dir = util::fs::oxen_tmp_dir()?;
    let repo_dir = home_dir
        .join(&remote_repo.namespace)
        .join(&remote_repo.name);
    let repo_cache_dir = repo_dir.join(OXEN_HIDDEN_DIR);
    api::client::commits::download_dir_hashes_db_to_path(remote_repo, commit_id, &repo_cache_dir)
        .await?;

    let local_objects_dir = repo_cache_dir.join(OBJECTS_DIR);
    let tmp_objects_dir =
        api::client::commits::download_objects_db_to_path(remote_repo, &repo_dir).await?;
    log::debug!(
        "trying to merge tmp_objects_dir {:?} into local objects dir {:?}",
        tmp_objects_dir,
        local_objects_dir
    );

    merge_objects_dbs(&local_objects_dir, &tmp_objects_dir)?;

    // Merge it in with the (probably not already extant) local objects db

    let object_reader = ObjectDBReader::new_from_path(repo_dir.clone(), commit_id)?;

    let commit_reader = CommitEntryReader::new_from_path(&repo_dir, commit_id, object_reader)?;
    let entries =
        commit_reader.list_directory(Path::new(&entry.resource.as_ref().unwrap().path))?;

    // Convert entries to [Entry]
    let entries: Vec<Entry> = entries.into_iter().map(Entry::from).collect();

    // Pull all the entries
    let pull_progress = PullProgress::new();
    puller::pull_entries_to_working_dir(remote_repo, &entries, local_path, &pull_progress).await?;

    Ok(())
}
