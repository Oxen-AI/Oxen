use std::path::Path;

use crate::core::staged::get_staged_db_manager;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::Schema;
use crate::model::StagedEntryStatus;
use crate::model::Workspace;
use crate::model::merkle_tree::node::{MerkleTreeNode, StagedMerkleTreeNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::repositories;

/// Rename `before_column` to `after_column` on the data frame's staged schema, carrying that
/// column's metadata across the rename. The staged schema takes priority over the committed one,
/// since it holds metadata changed earlier in the same editing session.
pub fn update_schema(
    workspace: &Workspace,
    path: &Path,
    before_column: &str,
    after_column: &str,
) -> Result<(), OxenError> {
    // Read the committed node before taking the staged-db write lock.
    let committed_node =
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?;

    let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
    staged_db_manager.edit_staged_node(path, |staged| {
        let (mut file_node, status) = match staged {
            Some(staged) => (staged.node.file()?, staged.status),
            None => (
                committed_node.ok_or_else(|| OxenError::path_does_not_exist(path))?,
                StagedEntryStatus::Modified,
            ),
        };

        let Some(GenericMetadata::MetadataTabular(m)) = file_node.get_mut_metadata() else {
            return Err(OxenError::NotADataFrame(path.into()));
        };
        // Renaming the field in place carries its metadata to the new name.
        if let Some(field) = m
            .tabular
            .schema
            .fields
            .iter_mut()
            .find(|f| f.name == before_column)
        {
            field.name = after_column.to_string();
        }
        m.tabular.schema.recompute_hash();
        file_node.recompute_metadata_hashes()?;

        Ok(StagedMerkleTreeNode {
            status,
            node: MerkleTreeNode::from_file(file_node),
        })
    })?;

    Ok(())
}

/// Set the metadata on the data frame's schema itself, as opposed to on one of
/// its columns — the workspace-staged equivalent of `oxen schemas add -m '{...}'`.
/// The value replaces any existing schema metadata wholesale.
pub fn add_schema_metadata(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: &Path,
    metadata: &serde_json::Value,
) -> Result<Schema, OxenError> {
    super::stage_schema_metadata_update(repo, workspace, file_path, |schema| {
        schema.metadata = Some(metadata.to_owned());
        Ok(())
    })
}
