use std::path::Path;

use crate::core::staged::with_staged_db_manager;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataTabular;
use crate::model::Schema;
use crate::model::StagedEntryStatus;
use crate::model::Workspace;
use crate::repositories;

/// Updates the staged schema by changing the column name from `before_column` to `after_column`
/// and updating the metadata from the original schema.
pub fn update_schema(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    og_schema: &Schema,
    before_column: &str,
    after_column: &str,
) -> Result<(), OxenError> {
    let staged_schema =
        crate::core::v_latest::data_frames::schemas::get_staged_schema_with_staged_db_manager(
            &workspace.workspace_repo,
            &path,
        )?;
    let ref_schema = if let Some(schema) = staged_schema {
        schema
    } else {
        og_schema.clone()
    };

    // Here we give priority to the staged schema, as it can contained metadata that was changed during the
    // df editing process.
    let mut schema = ref_schema.clone();
    if let Some(ref_schema) = ref_schema.fields.iter().find(|f| f.name == before_column) {
        for field in &mut schema.fields {
            if field.name == before_column {
                field.name = after_column.to_string();
                field.metadata = ref_schema.metadata.clone();
                break;
            }
        }
    }

    with_staged_db_manager(&workspace.workspace_repo, |staged_db_manager| {
        let data = staged_db_manager.read_from_staged_db(&path)?;

        let mut file_node: FileNode;

        if let Some(data) = data {
            file_node = data.node.file()?;
        } else {
            file_node = repositories::tree::get_file_by_path(
                &workspace.base_repo,
                &workspace.commit,
                path.as_ref(),
            )?
            .ok_or(OxenError::basic_str("File not found"))?;
        }

        if let Some(GenericMetadata::MetadataTabular(tabular_metadata)) = &file_node.metadata() {
            file_node.set_metadata(Some(GenericMetadata::MetadataTabular(
                MetadataTabular::new(
                    tabular_metadata.tabular.width,
                    tabular_metadata.tabular.height,
                    schema,
                ),
            )));
        } else {
            return Err(OxenError::basic_str("Expected tabular metadata"));
        }

        staged_db_manager.upsert_file_node(path, StagedEntryStatus::Modified, &file_node)?;
        Ok(())
    })?;

    Ok(())
}
