use std::collections::HashMap;
use std::path::PathBuf;

use crate::constants::STAGED_DIR;
use crate::core;
use crate::core::db;
use crate::core::refs::RefWriter;
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

use filetime::FileTime;
use indicatif::ProgressBar;
use rocksdb::{DBWithThreadMode, SingleThreaded};

pub fn commit(
    workspace: &Workspace,
    new_commit: &NewCommitBody,
    branch_name: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;

    let mut branch = repositories::branches::get_by_name(repo, branch_name)?;
    log::debug!("commit looking up branch: {:#?}", &branch);

    if branch.is_none() {
        log::debug!("commit creating branch: {}", branch_name);
        branch = Some(repositories::branches::create(
            repo,
            branch_name,
            &commit.id,
        )?);
    }

    let branch = branch.unwrap();

    let workspace_commit = &workspace.commit;
    if branch.commit_id != workspace_commit.id {
        return Err(OxenError::WorkspaceBehind(Branch {
            name: branch_name.to_string(),
            commit_id: workspace_commit.id.to_string(),
        }));
    }

    let staged_db_path = util::fs::oxen_hidden_dir(&workspace.workspace_repo.path).join(STAGED_DIR);

    log::debug!(
        "0.19.0::workspaces::commit staged db path: {:?}",
        staged_db_path
    );
    let opts = db::key_val::opts::default();
    let commit = {
        let staged_db: DBWithThreadMode<SingleThreaded> =
            DBWithThreadMode::open(&opts, dunce::simplified(&staged_db_path))?;

        let commit_progress_bar = ProgressBar::new_spinner();

        // Read all the staged entries
        let (dir_entries, _) = core::v_latest::status::read_staged_entries(
            &workspace.workspace_repo,
            &staged_db,
            &commit_progress_bar,
        )?;

        let dir_entries = export_tabular_data_frames(workspace, dir_entries)?;

        repositories::commits::commit_writer::commit_dir_entries(
            &workspace.base_repo,
            dir_entries,
            new_commit,
            branch_name,
            &commit_progress_bar,
        )?
    };

    // Clear the staged db
    log::debug!("Removing staged_db_path: {staged_db_path:?}");
    util::fs::remove_dir_all(staged_db_path)?;

    // DEBUG
    // let tree = repositories::tree::get_by_commit(&workspace.base_repo, &commit)?;
    // log::debug!("0.19.0::workspaces::commit tree");
    // tree.print();

    // Update the branch
    let ref_writer = RefWriter::new(&workspace.base_repo)?;
    let commit_id = commit.id.to_owned();

    ref_writer.set_branch_commit_id(branch_name, &commit_id)?;

    // Cleanup workspace on commit
    repositories::workspaces::delete(workspace)?;
    if let Some(workspace_name) = &workspace.name {
        repositories::workspaces::create_with_name(
            &workspace.base_repo,
            &commit,
            workspace.id.clone(),
            Some(workspace_name.clone()),
            true,
        )?;
    }

    Ok(commit)
}

fn export_tabular_data_frames(
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
                        || (dir_path == PathBuf::from("") && node_path.components().count() == 1)
                    {
                        node_path = dir_path.join(node_path);
                    }
                    if *file_node.data_type() == EntryDataType::Tabular {
                        log::debug!(
                            "Exporting tabular data frame: {:?} -> {:?}",
                            node_path,
                            file_node.name()
                        );
                        let exported_path = if repositories::workspaces::data_frames::is_indexed(
                            workspace, &node_path,
                        )? {
                            workspaces::data_frames::extract_file_node_to_working_dir(
                                workspace, &dir_path, file_node,
                            )?
                        } else {
                            workspace.workspace_repo.path.join(node_path)
                        };

                        log::debug!("exported path: {:?}", exported_path);

                        // Update the metadata in the new staged merkle tree node
                        let new_staged_merkle_tree_node = compute_staged_merkle_tree_node(
                            workspace,
                            &exported_path,
                            dir_entry.status,
                        )?;

                        log::debug!(
                            "export_tabular_data_frames new_staged_merkle_tree_node: {:?}",
                            new_staged_merkle_tree_node
                        );
                        new_dir_entries
                            .entry(dir_path.to_path_buf())
                            .or_default()
                            .push(new_staged_merkle_tree_node);
                    } else {
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

fn compute_staged_merkle_tree_node(
    workspace: &Workspace,
    path: &PathBuf,
    status: StagedEntryStatus,
) -> Result<StagedMerkleTreeNode, OxenError> {
    // This logic is copied from add.rs but add has some optimizations that make it hard to be reused here
    let metadata = util::fs::metadata(path)?;
    let mtime = FileTime::from_last_modification_time(&metadata);
    let hash = util::hasher::get_hash_given_metadata(path, &metadata)?;
    let num_bytes = metadata.len();
    let hash = MerkleHash::new(hash);

    // Get the data type of the file
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, &mime_type);
    log::debug!("compute_staged_merkle_tree_node path: {:?}", path);
    let mut metadata = repositories::metadata::get_file_metadata(path, &data_type)?;
    log::debug!("compute_staged_merkle_tree_node metadata: {:?}", metadata);

    // Here we give priority to the staged schema, as it can contained metadata that was changed during the
    if let Ok(Some(staged_schema)) =
        core::v_latest::data_frames::schemas::get_staged(&workspace.workspace_repo, path)
    {
        if let Some(GenericMetadata::MetadataTabular(metadata)) = &mut metadata {
            metadata
                .tabular
                .schema
                .update_metadata_from_schema(&staged_schema);
        }
    }

    // Compute the metadata hash and combined hash
    let metadata_hash = util::hasher::get_metadata_hash(&metadata)?;
    let combined_hash = util::hasher::get_combined_hash(Some(metadata_hash), hash.to_u128())?;
    let combined_hash = MerkleHash::new(combined_hash);

    // Copy the file to the versioned directory
    let dst_dir = util::fs::version_dir_from_hash(&workspace.base_repo.path, hash.to_string());
    if !dst_dir.exists() {
        util::fs::create_dir_all(&dst_dir).unwrap();
    }

    let relative_path = util::fs::path_relative_to_dir(path, &workspace.workspace_repo.path)?;
    let dst = dst_dir.join("data");

    log::debug!("compute_staged_merkle_tree_node copying file to {:?}", dst);

    util::fs::copy(path, &dst).unwrap();
    let file_extension = path.extension().unwrap_or_default().to_string_lossy();
    let relative_path_str = relative_path.to_str().unwrap();
    let file_node = FileNode::new(
        &workspace.base_repo,
        FileNodeOpts {
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
        },
    )?;

    Ok(StagedMerkleTreeNode {
        status,
        node: MerkleTreeNode::from_file(file_node),
    })
}
