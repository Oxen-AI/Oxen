use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::{df_db, workspace_df_db};
use crate::error::OxenError;
use crate::model::diff::tabular_diff::{
    TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};
use crate::model::Workspace;
use crate::repositories;
use std::path::Path;

pub fn diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DiffResult, OxenError> {
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let path = path.as_ref();
    // Get commit for the branch head
    log::debug!(
        "diff_workspace_df {:?} got repo at path {:?}",
        path,
        repo.path
    );

    let file_node = repositories::tree::get_file_by_path(repo, commit, path)?
        .ok_or(OxenError::entry_does_not_exist(path))?;

    log::debug!("diff_workspace_df got file_node {}", file_node);

    if !is_indexed(workspace, path)? {
        return Err(OxenError::basic_str("Dataset is not indexed"));
    };

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

    let conn = df_db::get_connection(db_path)?;

    let diff_df = workspace_df_db::df_diff(&conn)?;

    if diff_df.is_empty() {
        return Ok(DiffResult::Tabular(TabularDiff::empty()));
    }

    let row_mods = AddRemoveModifyCounts::from_diff_df(&diff_df)?;

    let schema = workspace_df_db::schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let schemas = TabularDiffSchemas {
        left: schema.clone(),
        right: schema.clone(),
        diff: schema.clone(),
    };

    let diff_summary = TabularDiffSummary {
        modifications: TabularDiffMods {
            row_counts: row_mods,
            col_changes: TabularSchemaDiff::empty(),
        },
        schemas,
        dupes: TabularDiffDupes::empty(),
    };

    let diff_result = TabularDiff {
        contents: diff_df,
        parameters: TabularDiffParameters::empty(),
        summary: diff_summary,
    };

    Ok(DiffResult::Tabular(diff_result))
}

pub fn is_indexed(workspace: &Workspace, path: &Path) -> Result<bool, OxenError> {
    log::debug!("checking dataset is indexed for {:?}", path);
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("getting conn at path {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let table_exists = df_db::table_exists(&conn, TABLE_NAME)?;
    log::debug!("dataset_is_indexed() got table_exists: {:?}", table_exists);
    Ok(table_exists)
}
