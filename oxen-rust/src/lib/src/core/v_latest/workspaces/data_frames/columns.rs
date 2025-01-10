use polars::frame::DataFrame;
use rocksdb::DB;

use crate::constants::TABLE_NAME;
use crate::core::db;
use crate::core::db::data_frames::workspace_df_db::schema_without_oxen_cols;
use crate::core::db::data_frames::{column_changes_db, columns, df_db};
use crate::core::v_latest::data_frames;
use crate::core::v_latest::workspaces;
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{LocalRepository, MerkleHash, Schema, StagedEntryStatus, Workspace};
use crate::repositories::workspaces::data_frames::columns::get_column_diff;
use rmp_serde::Serializer;
use serde::Serialize;

use crate::view::data_frames::columns::{
    ColumnToDelete, ColumnToRestore, ColumnToUpdate, NewColumn,
};
use crate::view::data_frames::{ColumnChange, DataFrameColumnChange};
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
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("add_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;
    let result = columns::add_column(&conn, new_column)?;

    let column_after = ColumnChange {
        column_name: new_column.name.clone(),
        column_data_type: Some(new_column.data_type.to_owned()),
    };

    columns::record_column_change(
        &column_changes_path,
        "added".to_owned(),
        None,
        Some(column_after),
    )?;

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
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("delete_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;
    let column_data_type =
        table_schema
            .get_field(&column_to_delete.name)
            .ok_or(OxenError::Basic(
                "A column with the given name does not exist".into(),
            ))?;

    let result = columns::delete_column(&conn, column_to_delete)?;

    let column_before = ColumnChange {
        column_name: column_to_delete.name.clone(),
        column_data_type: Some(column_data_type.dtype.clone()),
    };

    columns::record_column_change(
        &column_changes_path,
        "deleted".to_owned(),
        Some(column_before),
        None,
    )?;

    workspaces::files::track_modified_data_frame(workspace, file_path)?;

    Ok(result)
}

pub fn update(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_update: &ColumnToUpdate,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);
    log::debug!("update_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let result = columns::update_column(&conn, column_to_update, &table_schema)?;

    let column_data_type = table_schema.get_field(&column_to_update.name).unwrap();

    let column_after_name = column_to_update
        .new_name
        .clone()
        .unwrap_or(column_to_update.name.clone());

    let column_after_data_type = column_to_update
        .new_data_type
        .clone()
        .unwrap_or(column_data_type.dtype.clone());

    let column_before = ColumnChange {
        column_name: column_to_update.name.clone(),
        column_data_type: Some(column_data_type.dtype.clone()),
    };

    let column_after = ColumnChange {
        column_name: column_after_name.clone(),
        column_data_type: Some(column_after_data_type),
    };

    columns::record_column_change(
        &column_changes_path,
        "modified".to_string(),
        Some(column_before),
        Some(column_after),
    )?;

    let og_schema = repositories::data_frames::schemas::get_by_path(
        &workspace.base_repo,
        &workspace.commit,
        file_path,
    )?;

    repositories::workspaces::data_frames::schemas::update_schema(
        workspace,
        file_path,
        &og_schema.ok_or(OxenError::basic_str("Original schema not found"))?,
        &column_to_update.name,
        &column_after_name,
    )?;

    workspaces::files::add(workspace, file_path)?;

    Ok(result)
}

pub fn restore(
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column_to_restore: &ColumnToRestore,
) -> Result<DataFrame, OxenError> {
    let file_path = file_path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let column_changes_path =
        repositories::workspaces::data_frames::column_changes_path(workspace, file_path);

    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(&column_changes_path))?;

    log::debug!("restore_column() got db_path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let og_schema = repositories::data_frames::schemas::get_by_path(
        &workspace.base_repo,
        &workspace.commit,
        file_path,
    )?;

    if let Some(change) =
        column_changes_db::get_data_frame_column_change(&db, &column_to_restore.name)?
    {
        match change.operation.as_str() {
            "added" => {
                log::debug!("restore_column() column is added, deleting");
                let column_to_delete = ColumnToDelete {
                    name: change
                        .column_after
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore an add, the column after object has to be defined".into(),
                        ))?
                        .column_name
                        .clone(),
                };
                let result = columns::delete_column(&conn, &column_to_delete)?;
                columns::revert_column_changes(
                    &db,
                    &change
                        .column_after
                        .ok_or(OxenError::Basic(
                            "To restore an add, the column after object has to be defined".into(),
                        ))?
                        .column_name,
                )?;
                workspaces::files::add(workspace, file_path)?;
                Ok(result)
            }
            "deleted" => {
                log::debug!("restore_column() column was removed, adding it back");
                let new_column = NewColumn {
                    name: change
                        .column_before
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore a delete, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_name,
                    data_type: change
                        .column_before
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore a delete, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_data_type
                        .ok_or(OxenError::Basic(
                            "Column data type is required but was None".into(),
                        ))?,
                };
                let result = columns::add_column(&conn, &new_column)?;
                columns::revert_column_changes(
                    &db,
                    &change
                        .column_before
                        .ok_or(OxenError::Basic(
                            "To restore a delete, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_name,
                )?;
                workspaces::files::add(workspace, file_path)?;
                Ok(result)
            }
            "modified" => {
                log::debug!("restore_column() column was modified, reverting changes");
                let new_data_type = change
                    .column_before
                    .clone()
                    .ok_or(OxenError::Basic(
                        "To restore a modify, the column before object has to be defined".into(),
                    ))?
                    .column_data_type
                    .ok_or(OxenError::Basic(
                        "column_data_type is None, cannot unwrap".into(),
                    ))?;
                let column_to_update = ColumnToUpdate {
                    name: change
                        .column_after
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore a modify, the column after object has to be defined".into(),
                        ))?
                        .column_name,
                    new_data_type: Some(new_data_type.to_owned()),
                    new_name: Some(
                        change
                            .column_before
                            .clone()
                            .ok_or(OxenError::Basic(
                                "To restore a modify, the column before object has to be defined"
                                    .into(),
                            ))?
                            .column_name,
                    ),
                };

                let table_schema = schema_without_oxen_cols(&conn, TABLE_NAME)?;
                let result = columns::update_column(&conn, &column_to_update, &table_schema)?;
                columns::revert_column_changes(
                    &db,
                    &change
                        .column_after
                        .clone()
                        .ok_or(OxenError::Basic(
                            "To restore a modify, the column before object has to be defined"
                                .into(),
                        ))?
                        .column_name,
                )?;
                let og_schema =
                    og_schema.ok_or(OxenError::basic_str("Original schema not found"))?;

                repositories::data_frames::schemas::restore_schema(
                    &workspace.workspace_repo,
                    file_path,
                    &og_schema,
                    &change
                        .column_before
                        .clone()
                        .ok_or(OxenError::basic_str(
                            "To restore a modify, the column before object has to be defined",
                        ))?
                        .column_name,
                    &change
                        .column_after
                        .clone()
                        .ok_or(OxenError::basic_str(
                            "To restore a modify, the column after object has to be defined",
                        ))?
                        .column_name,
                )?;
                workspaces::files::add(workspace, file_path)?;
                Ok(result)
            }
            _ => Err(OxenError::UnsupportedOperation(
                change.operation.clone().into(),
            )),
        }
    } else {
        Err(OxenError::ColumnNameNotFound(
            format!("Column to restore not found: {}", column_to_restore.name).into(),
        ))
    }
}

pub fn add_column_metadata(
    repo: &LocalRepository,
    workspace: &Workspace,
    file_path: impl AsRef<Path>,
    column: impl AsRef<str>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let db = data_frames::schemas::get_staged_db(&workspace.workspace_repo)?;
    let path = file_path.as_ref();
    let path = util::fs::path_relative_to_dir(path, &workspace.workspace_repo.path)?;
    let column = column.as_ref();

    let key = path.to_string_lossy();

    let staged_merkle_tree_node = db.get(key.as_bytes())?;
    let mut staged_nodes: HashMap<PathBuf, StagedMerkleTreeNode> = HashMap::new();

    let mut file_node = if let Some(staged_merkle_tree_node) = staged_merkle_tree_node {
        let staged_merkle_tree_node: StagedMerkleTreeNode =
            rmp_serde::from_slice(&staged_merkle_tree_node)?;
        staged_merkle_tree_node.node.file()?
    } else {
        // Get the FileNode from the CommitMerkleTree
        let commit = workspace.commit.clone();
        let node = repositories::tree::get_node_by_path(repo, &commit, &path)?.ok_or(
            OxenError::basic_str("Node does not exist at the specified path"),
        )?;
        let mut parent_id = node.parent_id;
        let mut dir_path = path.clone();
        while let Some(current_parent_id) = parent_id {
            if current_parent_id == MerkleHash::new(0) {
                break;
            }
            let mut parent_node = MerkleTreeNode::from_hash(repo, &current_parent_id)?;
            parent_id = parent_node.parent_id;
            let EMerkleTreeNode::Directory(mut dir_node) = parent_node.node.clone() else {
                continue;
            };
            dir_path = dir_path.parent().unwrap().to_path_buf();
            dir_node.set_name(dir_path.to_string_lossy());
            parent_node.node = EMerkleTreeNode::Directory(dir_node);
            let staged_parent_node = StagedMerkleTreeNode {
                status: StagedEntryStatus::Modified,
                node: parent_node,
            };
            staged_nodes.insert(dir_path.clone(), staged_parent_node);
        }

        let Some(file_node) = repositories::tree::get_file_by_path(repo, &commit, &path)? else {
            return Err(OxenError::path_does_not_exist(&path));
        };
        file_node
    };
    let column_diff = get_column_diff(workspace, &file_path)?;

    update_column_names_in_metadata(&column_diff, file_node.get_mut_metadata());

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
                return Err(OxenError::ColumnNameNotFound(column.to_string().into()));
            }
            results.insert(path.clone(), m.tabular.schema.clone());
        }
        _ => {
            return Err(OxenError::path_does_not_exist(path));
        }
    }

    let mut staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Modified,
        node: MerkleTreeNode::from_file(file_node.clone()),
    };

    for (path, staged_node) in staged_nodes.iter() {
        let key = path.to_string_lossy();
        let mut buf = Vec::new();
        staged_node
            .serialize(&mut Serializer::new(&mut buf))
            .unwrap();
        db.put(key.as_bytes(), &buf)?;
    }

    let oxen_metadata = &file_node.metadata();
    let oxen_metadata_hash = util::hasher::get_metadata_hash(oxen_metadata)?;
    let combined_hash =
        util::hasher::get_combined_hash(Some(oxen_metadata_hash), file_node.hash().to_u128())?;

    let mut file_node = staged_entry.node.file()?;

    file_node.set_name(path.to_str().unwrap());
    file_node.set_combined_hash(MerkleHash::new(combined_hash));
    file_node.set_metadata_hash(Some(MerkleHash::new(oxen_metadata_hash)));

    staged_entry.node = MerkleTreeNode::from_file(file_node);

    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    db.put(key.as_bytes(), &buf)?;

    Ok(results)
}

pub fn update_column_names_in_metadata(
    column_changes: &[DataFrameColumnChange],
    file_node_metadata: &mut Option<GenericMetadata>,
) {
    if let Some(GenericMetadata::MetadataTabular(metadata_tabular)) = file_node_metadata {
        for change in column_changes {
            if change.operation == "modified" {
                let column_before = change.column_before.as_ref().unwrap();
                let column_after = change.column_after.as_ref().unwrap();
                if column_before.column_name != column_after.column_name {
                    for field in &mut metadata_tabular.tabular.schema.fields {
                        if field.name == column_before.column_name {
                            field.name = column_after.column_name.clone();
                            log::debug!(
                                "Updated column name from {} to {}",
                                column_before.column_name,
                                column_after.column_name
                            );
                        }
                    }
                }
            } else if change.operation == "added" {
                let column_after = change.column_after.as_ref().unwrap();
                // Create a new field and add it to the schema
                metadata_tabular.tabular.schema.fields.push(Field {
                    name: column_after.column_name.clone(),
                    // Assuming you have a default data type or can derive it from column_after
                    dtype: column_after.column_data_type.clone().unwrap_or_default(),
                    changes: None, // or some default metadata if needed
                    metadata: None,
                });
            }
        }
    } else {
        log::warn!("Metadata is not of type MetadataTabular or is None");
    }
}
