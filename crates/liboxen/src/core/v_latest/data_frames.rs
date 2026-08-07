use crate::core::db::data_frames::df_db::with_hardened_query_conn;
use crate::core::df::{sql, tabular};
use crate::core::staged::get_staged_db_manager;
use crate::error::OxenError;
use crate::model::ParsedResource;
use crate::model::data_frame::{DataFrameSchemaSize, DataFrameSlice, DataFrameSliceSchemas};
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{Commit, DataFrameSize, EntryDataType, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::repositories;
use polars::prelude::IntoLazy as _;

use std::path::Path;

pub mod schemas;

pub async fn get_slice(
    repo: &LocalRepository,
    resource: &ParsedResource,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrameSlice, OxenError> {
    let workspace = resource.workspace.as_ref();
    let commit = match workspace {
        Some(ws) => ws.commit.clone(),
        None => resource
            .commit
            .clone()
            .ok_or_else(|| OxenError::basic_str("Commit not found"))?,
    };

    let (staged_repo, base_repo) = match workspace {
        Some(ws) => (&ws.workspace_repo, repo),
        None => (repo, repo),
    };

    let file_node = match workspace {
        Some(ws) => {
            let staged_db_manager = get_staged_db_manager(staged_repo)?;
            // Try staged DB first
            if let Some(staged_node) = staged_db_manager.read_from_staged_db(&path)? {
                match staged_node.node.node {
                    EMerkleTreeNode::File(f) => Ok(f),
                    _ => Err(OxenError::NotAFile(path.as_ref().to_path_buf().into())),
                }?
            } else {
                // Fall back to commit tree using workspace's commit
                let commit = &ws.commit;
                repositories::tree::get_file_by_path(base_repo, commit, &path)?
                    .ok_or_else(|| OxenError::path_does_not_exist(path.as_ref()))?
            }
        }
        None => {
            let commit = resource
                .commit
                .as_ref()
                .ok_or_else(|| OxenError::basic_str("Commit not found"))?;
            repositories::tree::get_file_by_path(base_repo, commit, &path)?
                .ok_or_else(|| OxenError::path_does_not_exist(path.as_ref()))?
        }
    };

    log::debug!("get_slice file_node {file_node:?}");

    let metadata = match file_node.metadata() {
        Some(GenericMetadata::MetadataTabular(metadata)) => metadata.tabular.clone(),
        // Only a tabular node is expected to carry tabular metadata, so any other node type means
        // the caller asked for a data frame view of a file that is not one. A tabular node with no
        // metadata is the damaged case, and names itself separately.
        _ if *file_node.data_type() != EntryDataType::Tabular => {
            return Err(OxenError::InvalidFileType(
                format!(
                    "{} is a {} file, which cannot be read as a data frame",
                    path.as_ref().display(),
                    file_node.data_type()
                )
                .into(),
            ));
        }
        _ => {
            return Err(OxenError::TabularFileMissingMetadata(
                path.as_ref().to_path_buf().into(),
            ));
        }
    };
    log::debug!("get_slice metadata {metadata:?}");

    let source_schema = metadata.schema;
    let data_frame_size = DataFrameSize {
        width: metadata.width,
        height: metadata.height,
    };

    let handle_sql_result = handle_sql_querying(repo, &commit, path, opts, &data_frame_size).await;
    if let Ok(response) = handle_sql_result {
        return Ok(response);
    }
    // Read the data frame from the version path
    let version_store = repo.version_store();
    let df = tabular::read_version_df(
        &version_store,
        &file_node.hash().to_string(),
        file_node.extension(),
        opts,
    )
    .await?;
    log::debug!("get_slice df {:?}", df.height());

    // Check what the view height is
    let view_height = if opts.has_filter_transform() {
        df.height()
    } else {
        data_frame_size.height
    };

    // Update the schema metadata from the source schema
    let mut slice_schema = Schema::from_polars(df.schema());
    slice_schema.update_metadata_from_schema(&source_schema);
    log::debug!("get_slice slice_schema {slice_schema:?}");
    // Return a DataFrameSlice
    Ok(DataFrameSlice {
        schemas: DataFrameSliceSchemas {
            source: DataFrameSchemaSize {
                size: data_frame_size,
                schema: source_schema,
            },
            slice: DataFrameSchemaSize {
                size: DataFrameSize {
                    width: df.width(),
                    height: view_height,
                },
                schema: slice_schema,
            },
        },
        slice: df,
        total_entries: view_height,
    })
}

async fn handle_sql_querying(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
    data_frame_size: &DataFrameSize,
) -> Result<DataFrameSlice, OxenError> {
    let path = path.as_ref();

    if let Some(sql) = opts.sql.clone() {
        // Finding the queryable workspace opens DuckDB to check the index, and the query reads it.
        // Both are sync DB/filesystem work, so run them as one blocking unit off the worker.
        let query_repo = repo.clone();
        let query_commit = commit.clone();
        let query_path = path.to_path_buf();
        let (workspace, df) = tokio::task::spawn_blocking(move || -> Result<_, OxenError> {
            let workspace =
                crate::core::v_latest::workspaces::data_frames::get_queryable_data_frame_workspace(
                    &query_repo,
                    &query_path,
                    &query_commit,
                )?;
            let db_path =
                repositories::workspaces::data_frames::duckdb_path(&workspace, &query_path);
            // No opts: `collect_with_opts` below paginates this path in polars. Passing
            // them here would apply the page twice, in SQL and then again on the page.
            let df = with_hardened_query_conn(&db_path, |conn| sql::query_df(conn, sql, None))?;
            Ok((workspace, df))
        })
        .await??;
        log::debug!("handle_sql_querying got df {df:?}");
        let paginated_df = tabular::collect_with_opts(df.clone().lazy(), opts.clone()).await?;

        // Reading the stored schema is a sync repo/tree read; keep it off the worker too.
        let schema_repo = repo.clone();
        let schema_commit = workspace.commit.clone();
        let schema_path = path.to_path_buf();
        let stored_schema = tokio::task::spawn_blocking(move || {
            repositories::data_frames::schemas::get_by_path(
                &schema_repo,
                &schema_commit,
                &schema_path,
            )
        })
        .await??;
        let source_schema =
            stored_schema.unwrap_or_else(|| Schema::from_polars(paginated_df.schema()));

        let mut slice_schema = Schema::from_polars(df.schema());
        slice_schema.update_metadata_from_schema(&source_schema);

        return Ok(DataFrameSlice {
            schemas: DataFrameSliceSchemas {
                source: DataFrameSchemaSize {
                    size: data_frame_size.clone(),
                    schema: source_schema,
                },
                slice: DataFrameSchemaSize {
                    size: DataFrameSize {
                        width: paginated_df.width(),
                        height: paginated_df.height(),
                    },
                    schema: slice_schema,
                },
            },
            slice: paginated_df,
            total_entries: df.height(),
        });
    }

    Err(OxenError::basic_str("Could not query data frame"))
}
