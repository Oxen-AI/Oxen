use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, PoisonError};
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::Mutex as TokioMutex;

use crate::constants::STAGED_DIR;
use crate::core;
use crate::core::refs::with_ref_manager;
use crate::core::staged::remove_from_cache;
use crate::core::v_latest::workspaces;
use crate::error::OxenError;
use crate::model::merkle_tree::node::file_node::FileNodeOpts;
use crate::model::merkle_tree::node::{
    EMerkleTreeNode, FileNode, MerkleTreeNode, StagedMerkleTreeNode,
};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{
    Branch, Commit, EntryDataType, MerkleHash, NewCommitBody, StagedEntryStatus, Workspace,
};
use crate::repositories;
use crate::util;
use crate::util::progress_bar::FinishOnDropProgressBar;
use crate::view::merge::{MergeConflictFile, Mergeable};

use filetime::FileTime;
use indicatif::ProgressBar;

// Serializes commits of the same workspace so two concurrent commits can't
// tear each other's data-frame export mid-read or wipe the shared staged db.
// Keyed by the workspace repo path; in-process, and entries are dropped once
// no commit holds or waits on the lock.
static COMMIT_LOCKS: LazyLock<StdMutex<HashMap<PathBuf, Arc<TokioMutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn commit_lock_for(key: &Path) -> Arc<TokioMutex<()>> {
    // Poisoning only means a holder panicked; the map of Arc handles is still
    // sound, so recover the guard.
    let mut locks = COMMIT_LOCKS.lock().unwrap_or_else(PoisonError::into_inner);
    locks.entry(key.to_path_buf()).or_default().clone()
}

fn cleanup_commit_lock(key: &Path) {
    let mut locks = COMMIT_LOCKS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(lock) = locks.get(key) {
        // The registry's reference is the only one left — no holder, no
        // waiter — so the entry can be dropped. A late-arriving committer
        // simply gets a fresh mutex, which is equivalent since nobody holds
        // this one.
        if Arc::strong_count(lock) == 1 {
            locks.remove(key);
        }
    }
}

pub async fn commit(
    workspace: &Workspace,
    new_commit: &NewCommitBody,
    branch_name: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let lock_key = workspace.workspace_repo.path.clone();
    let lock = commit_lock_for(&lock_key);
    let result = {
        let _guard = lock.lock().await;
        commit_inner(workspace, new_commit, branch_name.as_ref()).await
    };
    drop(lock);
    cleanup_commit_lock(&lock_key);
    result
}

async fn commit_inner(
    workspace: &Workspace,
    new_commit: &NewCommitBody,
    branch_name: &str,
) -> Result<Commit, OxenError> {
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;

    let branch = match repositories::branches::get_by_name(repo, branch_name) {
        Ok(branch) => branch,
        Err(OxenError::BranchNotFound(_)) => {
            log::debug!("commit creating branch: {branch_name}");
            repositories::branches::create(repo, branch_name, &commit.id)?
        }
        Err(e) => return Err(e),
    };
    log::debug!("commit looking up branch: {:#?}", branch);

    let staged_db_path = util::fs::oxen_hidden_dir(&workspace.workspace_repo.path).join(STAGED_DIR);

    log::debug!("workspaces::commit staged db path: {staged_db_path:?}");
    let commit = {
        let commit_progress_bar = FinishOnDropProgressBar(ProgressBar::new_spinner());

        // Read all the staged entries
        let (dir_entries, _) = core::v_latest::status::read_staged_entries_with_staged_db_manager(
            &workspace.workspace_repo,
            &commit_progress_bar,
        )?;

        let conflicts = list_conflicts(workspace, &dir_entries, &branch)?;
        if !conflicts.is_empty() {
            return Err(OxenError::WorkspaceBehind(Box::new(workspace.clone())));
        }

        let dir_entries = export_tabular_data_frames(workspace, dir_entries).await?;

        repositories::commits::commit_writer::commit_dir_entries(
            &workspace.base_repo,
            dir_entries,
            new_commit,
            branch_name,
        )?
    };

    // Clear the staged db
    log::debug!("Removing staged_db_path: {staged_db_path:?}");
    remove_from_cache(&workspace.workspace_repo.path)?;
    util::fs::remove_dir_all(staged_db_path)?;

    // DEBUG
    // let tree = repositories::tree::get_by_commit(&workspace.base_repo, &commit)?;
    // log::debug!("0.19.0::workspaces::commit tree");
    // tree.print();

    // Update the branch
    let commit_id = commit.id.to_owned();
    with_ref_manager(&workspace.base_repo, |manager| {
        manager.set_branch_commit_id(branch_name, &commit_id)
    })?;

    if workspace.name.is_some() {
        // Named workspaces aren't deleted on commit, instead we
        // update the workspace config to point to the new commit
        repositories::workspaces::update_commit(workspace, &commit_id)?;
    } else {
        // Unnamed workspaces are deleted on commit
        repositories::workspaces::delete(workspace)?;
    }

    Ok(commit)
}

pub fn mergeability(
    workspace: &Workspace,
    branch_name: impl AsRef<str>,
) -> Result<Mergeable, OxenError> {
    let branch_name = branch_name.as_ref();
    let branch = repositories::branches::get_by_name(&workspace.base_repo, branch_name)?;

    let base = &workspace.commit;
    let Some(head) = repositories::commits::get_by_id(&workspace.base_repo, &branch.commit_id)?
    else {
        return Err(OxenError::RevisionNotFound(
            branch.commit_id.as_str().into(),
        ));
    };

    log::debug!("workspaces::mergeability base: {base:?}");
    log::debug!("workspaces::mergeability head: {head:?}");

    // Get commits between the base and head
    let commits =
        repositories::merge::list_commits_between_commits(&workspace.base_repo, base, &head)?;

    log::debug!("workspaces::mergeability commits: {commits:?}");

    // Get conflicts between the base and head
    let staged_db_path = util::fs::oxen_hidden_dir(&workspace.workspace_repo.path).join(STAGED_DIR);

    log::debug!("workspaces::commit staged db path: {staged_db_path:?}");

    // Read all the staged entries
    let commit_progress_bar = ProgressBar::new_spinner();
    let (dir_entries, _) = core::v_latest::status::read_staged_entries_with_staged_db_manager(
        &workspace.workspace_repo,
        &commit_progress_bar,
    )?;

    let conflicts = list_conflicts(workspace, &dir_entries, &branch)?;
    if !conflicts.is_empty() {
        return Ok(Mergeable {
            is_mergeable: false,
            conflicts: conflicts
                .into_iter()
                .map(|path| MergeConflictFile {
                    path: path.to_string_lossy().to_string(),
                })
                .collect(),
            commits,
        });
    }

    Ok(Mergeable {
        is_mergeable: true,
        conflicts: vec![],
        commits,
    })
}

fn list_conflicts(
    workspace: &Workspace,
    dir_entries: &HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
    branch: &Branch,
) -> Result<Vec<PathBuf>, OxenError> {
    let workspace_commit = &workspace.commit;
    let Some(branch_commit) =
        repositories::commits::get_by_id(&workspace.base_repo, &branch.commit_id)?
    else {
        return Err(OxenError::RevisionNotFound(
            branch.commit_id.as_str().into(),
        ));
    };
    log::debug!(
        "checking if workspace is behind: {:?} {} == {}",
        branch.name,
        branch_commit,
        workspace_commit
    );
    if branch.commit_id == workspace_commit.id {
        // Nothing has changed on the branch since the workspace was created
        return Ok(vec![]);
    }

    // Check to see if any of the staged entries have been updated on the target branch
    // If so, mark the commit as behind
    // Otherwise, we can just commit the changes
    let Some(branch_commit) =
        repositories::commits::get_by_id(&workspace.base_repo, &branch.commit_id)?
    else {
        return Err(OxenError::RevisionNotFound(
            branch.commit_id.as_str().into(),
        ));
    };
    let Some(branch_tree) =
        repositories::tree::get_root_with_children(&workspace.base_repo, &branch_commit)?
    else {
        return Err(OxenError::RevisionNotFound(
            branch.commit_id.as_str().into(),
        ));
    };
    let Some(workspace_tree) =
        repositories::tree::get_root_with_children(&workspace.base_repo, workspace_commit)?
    else {
        return Err(OxenError::RevisionNotFound(
            workspace.commit.id.as_str().into(),
        ));
    };

    let mut conflicts = vec![];
    for (path, entries) in dir_entries {
        for entry in entries {
            let EMerkleTreeNode::File(_) = &entry.node.node else {
                // Only check files for conflicts
                continue;
            };

            log::debug!("checking if workspace is behind: {path:?} -> {entry}");
            let file_path = entry.node.maybe_path()?;
            log::debug!("checking if branch tree has file: {file_path:?}");
            let Some(branch_node) = branch_tree.get_by_path(&file_path)? else {
                log::debug!("branch node not found: {file_path:?}");
                continue;
            };
            let Some(workspace_node) = workspace_tree.get_by_path(&file_path)? else {
                log::debug!("workspace node not found: {file_path:?}");
                continue;
            };
            log::debug!("comparing hashes: {path:?} -> {entry}");
            log::debug!("branch node hash: {:?}", branch_node.hash);
            log::debug!("workspace node hash: {:?}", workspace_node.hash);
            if branch_node.hash == workspace_node.hash {
                log::debug!("branch node hashes match: {path:?} -> {entry}");
                continue;
            }
            log::debug!("got conflict: {file_path:?}");
            conflicts.push(file_path.to_path_buf());
        }
    }

    Ok(conflicts)
}

async fn export_tabular_data_frames(
    workspace: &Workspace,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
) -> Result<HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, OxenError> {
    // Export all the workspace data frames and add them to the commit
    let mut new_dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>> = HashMap::new();
    for (dir_path, entries) in dir_entries {
        for dir_entry in entries {
            log::debug!(
                "workspace commit checking if we want to export tabular data frame: {:?} -> {}",
                dir_path,
                dir_entry.node
            );
            match &dir_entry.node.node {
                EMerkleTreeNode::File(file_node) => {
                    // TODO: This is hacky - because we don't know if a file node is the full path or relative to the dir_path
                    // need a better way to distinguish
                    let mut node_path = PathBuf::from(file_node.name());
                    if !node_path.starts_with(&dir_path)
                        || (dir_path == Path::new("") && node_path.components().count() == 1)
                    {
                        node_path = dir_path.join(node_path);
                    }

                    // Only recompute the metadata if the file is tabular and indexed (editable df and eval)
                    if *file_node.data_type() == EntryDataType::Tabular
                        && repositories::workspaces::data_frames::is_indexed(workspace, &node_path)?
                    {
                        log::debug!(
                            "Exporting tabular data frame: {:?} -> {:?}",
                            node_path,
                            file_node.name()
                        );

                        let exported_path =
                            workspaces::data_frames::extract_file_node_to_working_dir(
                                workspace, &dir_path, file_node,
                            )?;

                        log::debug!("exported path: {exported_path:?}");

                        // Update the metadata in the new staged merkle tree node
                        let entry_status = dir_entry.status.clone();
                        let new_staged_merkle_tree_node = compute_staged_merkle_tree_node(
                            workspace,
                            &exported_path,
                            entry_status.clone(),
                            file_node.data_type().clone(),
                        )
                        .await?;

                        log::debug!(
                            "export_tabular_data_frames new_staged_merkle_tree_node: {new_staged_merkle_tree_node:?}"
                        );

                        // Drop the entry only when the re-export is identical
                        // (content and metadata) to the BASE commit's file node
                        // — i.e. the staged edits net to nothing — so a
                        // rewritten-but-unchanged file isn't committed. Compare
                        // against the base node, not the staged `file_node`: the
                        // staged node already carries the edit (e.g. a
                        // metadata-only change bumps its combined hash), so
                        // comparing against it would wrongly skip real edits.
                        let base_node = repositories::tree::get_file_by_path(
                            &workspace.base_repo,
                            &workspace.commit,
                            &node_path,
                        )?;
                        if entry_status == StagedEntryStatus::Modified
                            && let Some(base_node) = &base_node
                            && new_staged_merkle_tree_node.node.file()?.combined_hash()
                                == base_node.combined_hash()
                        {
                            log::debug!(
                                "export_tabular_data_frames export identical to base, skipping: {node_path:?}"
                            );
                            continue;
                        }
                        new_dir_entries
                            .entry(dir_path.to_path_buf())
                            .or_default()
                            .push(new_staged_merkle_tree_node);
                    } else {
                        // A staged table that exists but fails the indexed
                        // gate was written by an older version and may hold
                        // edits this code cannot export. The staged entry is
                        // the BASE file node, so committing it would silently
                        // discard those edits — fail instead so the user can
                        // re-index (dropping the stale edits explicitly) or
                        // unstage.
                        if *file_node.data_type() == EntryDataType::Tabular
                            && repositories::workspaces::data_frames::has_staged_table(
                                workspace, &node_path,
                            )?
                        {
                            return Err(OxenError::basic_str(format!(
                                "Cannot commit workspace: the staged data frame {node_path:?} \
                                 was indexed by an older version of oxen. Re-index the data \
                                 frame (discarding its staged edits) or unstage it, then \
                                 commit again."
                            )));
                        }
                        new_dir_entries
                            .entry(dir_path.to_path_buf())
                            .or_default()
                            .push(dir_entry);
                    }
                }
                _ => {
                    new_dir_entries
                        .entry(dir_path.to_path_buf())
                        .or_default()
                        .push(dir_entry);
                }
            }
        }
    }
    Ok(new_dir_entries)
}

async fn compute_staged_merkle_tree_node(
    workspace: &Workspace,
    path: &PathBuf,
    status: StagedEntryStatus,
    data_type: EntryDataType,
) -> Result<StagedMerkleTreeNode, OxenError> {
    // This logic is copied from add.rs but add has some optimizations that make it hard to be reused here
    let metadata = util::fs::metadata(path)?;
    let mtime = FileTime::from_last_modification_time(&metadata);
    let hash = util::hasher::get_hash_given_metadata(path, &metadata)?;
    let num_bytes = metadata.len();
    let hash = MerkleHash::new(hash);

    // Use the committed node's data type for the guard below: an empty export
    // can mime-detect as non-tabular.
    let mime_type = util::fs::file_mime_type(path);
    log::debug!("compute_staged_merkle_tree_node path: {path:?}");
    let mut metadata = repositories::metadata::get_file_metadata(path, &data_type)?;
    log::debug!("compute_staged_merkle_tree_node metadata: {metadata:?}");

    // A tabular file we cannot parse must never be committed: a FileNode with
    // data_type Tabular and no metadata makes every subsequent read of the
    // file fail with "File node does not have metadata". This happens when
    // the exported data frame is empty (e.g. all rows were staged as removed
    // — an empty jsonl/csv has no schema to infer). Failing the commit keeps
    // the last good version readable.
    if data_type == EntryDataType::Tabular && metadata.is_none() {
        return Err(OxenError::TabularExportMissingMetadata(path.clone()));
    }

    // Here we give priority to the staged schema, as it can contained metadata that was changed during the
    if let Ok(Some(staged_schema)) =
        core::v_latest::data_frames::schemas::get_staged_schema_with_staged_db_manager(
            &workspace.workspace_repo,
            path,
        )
        && let Some(GenericMetadata::MetadataTabular(metadata)) = &mut metadata
    {
        metadata
            .tabular
            .schema
            .update_metadata_from_schema(&staged_schema);
    }

    // Compute the metadata hash and combined hash
    let metadata_hash = util::hasher::get_metadata_hash(&metadata)?;
    let combined_hash = util::hasher::get_combined_hash(Some(metadata_hash), hash.to_u128())?;
    let combined_hash = MerkleHash::new(combined_hash);

    // Copy file to the version store
    log::debug!("compute_staged_merkle_tree_node writing file to version store");
    let file_size = tokio::fs::metadata(path).await?.len();
    let file = File::open(path).await?;
    let reader = BufReader::new(file);
    let version_store = workspace.base_repo.version_store();
    version_store
        .store_version_from_reader(&hash.to_string(), Box::new(reader), file_size)
        .await?;

    let file_extension = path.extension().unwrap_or_default().to_string_lossy();
    let relative_path = util::fs::path_relative_to_dir(path, &workspace.workspace_repo.path)?;
    let relative_path_str = relative_path.to_str().unwrap();
    let file_node = FileNode::new(FileNodeOpts {
        name: relative_path_str.to_string(),
        hash,
        combined_hash,
        metadata_hash: Some(MerkleHash::new(metadata_hash)),
        num_bytes,
        last_modified_seconds: mtime.unix_seconds(),
        last_modified_nanoseconds: mtime.nanoseconds(),
        data_type,
        metadata,
        mime_type: mime_type.clone(),
        extension: file_extension.to_string(),
    })?;

    Ok(StagedMerkleTreeNode {
        status,
        node: MerkleTreeNode::from_file(file_node),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_commit_lock_registry_shares_and_cleans_up() -> Result<(), OxenError> {
        let key = PathBuf::from("test-commit-lock-registry");

        // Two lookups for the same key must return the same underlying mutex.
        let lock_a = commit_lock_for(&key);
        let lock_b = commit_lock_for(&key);
        let guard = lock_a.lock().await;
        assert!(
            lock_b.try_lock().is_err(),
            "second handle should contend on the same mutex"
        );
        drop(guard);
        assert!(lock_b.try_lock().is_ok());

        // A different key gets an independent mutex.
        let other = commit_lock_for(Path::new("test-commit-lock-registry-other"));
        let _guard = lock_a.lock().await;
        assert!(other.try_lock().is_ok());

        // Once all handles are dropped, cleanup removes the entry.
        drop(_guard);
        drop(lock_a);
        drop(lock_b);
        cleanup_commit_lock(&key);
        let registry = COMMIT_LOCKS.lock().unwrap();
        assert!(!registry.contains_key(&key));

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_commits_to_same_workspace_are_serialized() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let head = repositories::commits::head_commit(&repo)?;
            let workspace = repositories::workspaces::create_with_name(
                &repo,
                &head,
                "concurrent-commit-test",
                Some("concurrent-commit-test-ws".to_string()),
                true,
            )
            .await?;

            // Stage two files in the same workspace.
            for name in ["file1.txt", "file2.txt"] {
                let path = workspace.workspace_repo.path.join(name);
                util::fs::write_to_path(&path, format!("content of {name}"))?;
                repositories::workspaces::files::add(&workspace, &path).await?;
            }

            let body_one = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "concurrent commit one".to_string(),
            };
            let body_two = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "concurrent commit two".to_string(),
            };

            // Without the per-workspace lock these interleave: one commit
            // wipes the staged db (or rewrites the data-frame exports) while
            // the other is mid-commit. With the lock they serialize: the
            // first to acquire commits everything staged, the second finds a
            // clean staged db and reports "No changes to commit".
            let (result_one, result_two) = tokio::join!(
                commit(&workspace, &body_one, "main"),
                commit(&workspace, &body_two, "main"),
            );

            let ok_count = [result_one.is_ok(), result_two.is_ok()]
                .iter()
                .filter(|ok| **ok)
                .count();
            assert_eq!(
                ok_count, 1,
                "exactly one commit should land, got: {result_one:?} / {result_two:?}"
            );

            // Both staged files must be present at the branch head.
            let branch = repositories::branches::get_by_name(&repo, "main")?;
            let head = repositories::commits::get_by_id(&repo, &branch.commit_id)?
                .expect("branch head commit should exist");
            for name in ["file1.txt", "file2.txt"] {
                assert!(
                    repositories::tree::get_file_by_path(&repo, &head, Path::new(name))?.is_some(),
                    "{name} should be committed at the branch head"
                );
            }

            Ok(())
        })
        .await
    }
}
