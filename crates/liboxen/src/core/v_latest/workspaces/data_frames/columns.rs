use polars::frame::DataFrame;

use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::DataFrameError;
use crate::core::db::data_frames::columns;
use crate::core::db::data_frames::df_db::{self, with_df_db_manager};
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::staged::staged_db_manager::get_staged_db_manager;
use crate::core::v_latest::workspaces;
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{LocalRepository, MerkleHash, Schema, StagedEntryStatus, Workspace};

use crate::view::data_frames::columns::{ColumnToDelete, ColumnToUpdate, NewColumn};
use crate::{repositories, util};

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub fn add(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    new_column: &NewColumn,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    log::debug!("add_column() got db_path: {db_path:?}");
    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| columns::add_column(conn, new_column))
    })?;

    workspaces::files::track_modified_data_frame(workspace, file_path)?;

    Ok(result)
}

pub fn delete(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_delete: &ColumnToDelete,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    log::debug!("delete_column() got db_path: {db_path:?}");
    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| columns::delete_column(conn, column_to_delete))
    })?;

    workspaces::files::track_modified_data_frame(workspace, file_path)?;

    Ok(result)
}

pub async fn update(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    log::debug!("update_column() got db_path: {db_path:?}");
    let result = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            let table_schema = schema_without_oxen_cols(conn, TABLE_NAME)?;
            columns::update_column(conn, column_to_update, &table_schema)
        })
    })?;

    let column_after_name = column_to_update
        .new_name
        .clone()
        .unwrap_or(column_to_update.name.clone());

    let og_schema = repositories::data_frames::schemas::get_by_path(
        &workspace.base_repo,
        &workspace.commit,
        file_path,
    )?;

    repositories::workspaces::data_frames::schemas::update_schema(
        workspace,
        file_path,
        &og_schema.ok_or_else(|| OxenError::basic_str("Original schema not found"))?,
        &column_to_update.name,
        &column_after_name,
    )?;

    repositories::workspaces::files::add(workspace, file_path).await?;

    Ok(result)
}

pub fn add_column_metadata(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column: impl AsRef<str>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
    {
        let path = file_path.as_ref();
        let path = util::fs::path_relative_to_dir(path, &workspace.workspace_repo.path)?;
        let column = column.as_ref();

        let staged_merkle_tree_node = staged_db_manager.read_from_staged_db(&path)?;
        let mut staged_nodes: HashMap<PathBuf, StagedMerkleTreeNode> = HashMap::new();

        let mut file_node = if let Some(staged_merkle_tree_node) = staged_merkle_tree_node {
            staged_merkle_tree_node.node.file()?
        } else {
            // Get the FileNode from the CommitMerkleTree
            let commit = workspace.commit.clone();
            let node = repositories::tree::get_node_by_path(repo, &commit, &path)?
                .ok_or_else(|| OxenError::basic_str("Node does not exist at the specified path"))?;
            let mut parent_id = node.parent_id;
            let mut dir_path = path.clone();

            // Add parent nodes to staged nodes
            while let Some(current_parent_id) = parent_id {
                if current_parent_id == MerkleHash::new(0) {
                    break;
                }
                let mut parent_node = MerkleTreeNode::from_hash(repo, &current_parent_id)?;
                parent_id = parent_node.parent_id;
                let EMerkleTreeNode::Directory(mut dir_node) = parent_node.node.clone() else {
                    continue;
                };

                // if parent() returns None, we've reached the root
                let Some(parent_dir) = dir_path.parent() else {
                    break;
                };
                dir_path = parent_dir.to_path_buf();
                dir_node.set_name(dir_path.to_string_lossy());
                parent_node.node = EMerkleTreeNode::Directory(dir_node);
                let staged_parent_node = StagedMerkleTreeNode {
                    status: StagedEntryStatus::Modified,
                    node: parent_node,
                };
                staged_nodes.insert(dir_path.clone(), staged_parent_node);
            }

            let Some(file_node) = repositories::tree::get_file_by_path(repo, &commit, &path)?
            else {
                return Err(OxenError::path_does_not_exist(&path));
            };
            file_node
        };

        // Sync the workspace columns into the metadata before staging anything,
        // so a failure here leaves no partially-staged parent nodes behind.
        sync_workspace_columns_into_metadata(workspace, &path, file_node.get_mut_metadata())?;

        // Stage parent nodes
        staged_db_manager.upsert_staged_nodes(&staged_nodes)?;

        // Update the column metadata
        let mut results = HashMap::new();
        match file_node.get_mut_metadata() {
            Some(GenericMetadata::MetadataTabular(m)) => {
                log::debug!("add_column_metadata: {m:?}");
                let mut column_found = false;
                for f in m.tabular.schema.fields.iter_mut() {
                    log::debug!("add_column_metadata: checking column {f:?} == {column}");

                    if f.name == column {
                        log::debug!("add_column_metadata: found column {f:?}");
                        f.metadata = Some(metadata.to_owned());
                        column_found = true;
                    }
                }
                if !column_found {
                    Err(DataFrameError::ColumnNameNotFound(column.to_string()))?;
                }
                results.insert(path.clone(), m.tabular.schema.clone());
            }
            _ => {
                return Err(OxenError::path_does_not_exist(path));
            }
        }

        // Stage the file node with the updated metadata
        let mut staged_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Modified,
            node: MerkleTreeNode::from_file(file_node.clone()),
        };

        let oxen_metadata = &file_node.metadata();
        let oxen_metadata_hash = util::hasher::get_metadata_hash(oxen_metadata)?;
        let combined_hash =
            util::hasher::get_combined_hash(Some(oxen_metadata_hash), file_node.hash().to_u128())?;

        let mut file_node = staged_entry.node.file()?;

        file_node.set_name(path.to_string_lossy().as_ref());
        file_node.set_combined_hash(&MerkleHash::new(combined_hash));
        file_node.set_metadata_hash(Some(MerkleHash::new(oxen_metadata_hash)));

        staged_entry.node = MerkleTreeNode::from_file(file_node);

        staged_db_manager.upsert_staged_node(&path, &staged_entry, None)?;

        Ok(results)
    }
}

/// Reconcile the file node's tabular metadata schema with the workspace's
/// staged DuckDB table, so metadata reflects the edited state (columns a
/// workspace edit added/removed/retyped) that the committed schema does not
/// know yet. The staged table is authoritative: fields are rebuilt in its
/// order with its dtypes, dropping columns no longer present and adding new
/// ones, while any per-column metadata already attached is carried over by
/// matching name. A data frame that isn't indexed is left untouched.
fn sync_workspace_columns_into_metadata(
    workspace: &Workspace,
    path: &Path,
    file_node_metadata: &mut Option<GenericMetadata>,
) -> Result<(), OxenError> {
    let Some(GenericMetadata::MetadataTabular(metadata_tabular)) = file_node_metadata else {
        log::warn!("Metadata is not of type MetadataTabular or is None");
        return Ok(());
    };

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    if !db_path.exists() {
        // Never indexed: nothing to sync.
        return Ok(());
    }
    // Only reconcile when the staged table actually exists. A db file without
    // it yields an empty schema, and rebuilding against that would wipe the
    // committed metadata — so treat "no table" as a no-op, and let any real
    // DB error propagate.
    let table_schema = with_df_db_manager(&db_path, |manager| {
        manager.with_conn(|conn| {
            if !df_db::table_exists(conn, TABLE_NAME)? {
                return Ok(None);
            }
            Ok(Some(schema_without_oxen_cols(conn, TABLE_NAME)?))
        })
    })?;
    let Some(table_schema) = table_schema else {
        return Ok(());
    };

    let old_fields = std::mem::take(&mut metadata_tabular.tabular.schema.fields);
    metadata_tabular.tabular.schema.fields = table_schema
        .fields
        .into_iter()
        .map(|table_field| {
            let carried = old_fields.iter().find(|f| f.name == table_field.name);
            Field {
                name: table_field.name,
                dtype: table_field.dtype,
                metadata: carried.and_then(|f| f.metadata.clone()),
                changes: None,
            }
        })
        .collect();
    Ok(())
}
