use crate::constants::DUCKDB_DF_TABLE_NAME;
use crate::core::db::data_frames::df_db;
use crate::core::df::tabular::transform_new;
use crate::core::df::{sql, tabular};
use crate::error::OxenError;
use crate::model::data_frame::{DataFrameSchemaSize, DataFrameSlice, DataFrameSliceSchemas};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::metadata_tabular::MetadataTabularImpl;
use crate::model::{Commit, DataFrameSize, LocalRepository, Schema, Workspace};
use crate::opts::DFOpts;
use crate::{repositories, util};
use polars::prelude::IntoLazy as _;

use std::path::Path;

pub mod schemas;

pub fn get_slice(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrameSlice, OxenError> {
    // Get the file node
    let file_node = repositories::tree::get_file_by_path(repo, commit, &path)?
        .ok_or(OxenError::path_does_not_exist(path.as_ref()))?;

    let metadata: Result<MetadataTabularImpl, OxenError> = match file_node.metadata {
        Some(metadata) => match metadata {
            GenericMetadata::MetadataTabular(metadata) => Ok(metadata.tabular),
            _ => {
                return Err(OxenError::basic_str("Metadata is not tabular"));
            }
        },
        None => {
            return Err(OxenError::basic_str("File node does not have metadata"));
        }
    };
    let metadata = metadata?;
    log::debug!("get_slice metadata {:?}", metadata);

    let source_schema = metadata.schema;
    let data_frame_size = DataFrameSize {
        width: metadata.width,
        height: metadata.height,
    };

    let handle_sql_result = handle_sql_querying(repo, commit, path, opts, &data_frame_size);
    if let Ok(response) = handle_sql_result {
        return Ok(response);
    }
    // Read the data frame from the version path
    let version_path = util::fs::version_path_from_hash(repo, file_node.hash.to_string());
    let df = tabular::read_df_with_extension(version_path, file_node.extension, opts)?;

    // Check what the view height is
    let view_height = if opts.has_filter_transform() {
        df.height()
    } else {
        data_frame_size.height
    };

    // Update the schema metadata from the source schema
    let mut slice_schema = Schema::from_polars(&df.schema());
    slice_schema.update_metadata_from_schema(&source_schema);
    log::debug!("get_slice slice_schema {:?}", slice_schema);
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

fn handle_sql_querying(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
    data_frame_size: &DataFrameSize,
) -> Result<DataFrameSlice, OxenError> {
    let path = path.as_ref();
    let mut workspace: Option<Workspace> = None;

    if opts.sql.is_some() {
        match crate::core::v0_19_0::workspaces::data_frames::get_queryable_data_frame_workspace(
            repo, path, commit,
        ) {
            Ok(found_workspace) => {
                workspace = Some(found_workspace);
            }
            Err(e) => return Err(e),
        }
    }

    if let (Some(sql), Some(workspace)) = (opts.sql.clone(), workspace) {
        let db_path = repositories::workspaces::data_frames::duckdb_path(&workspace, path);
        let mut conn = df_db::get_connection(db_path)?;

        let mut slice_schema = df_db::get_schema(&conn, DUCKDB_DF_TABLE_NAME)?;
        let df = sql::query_df(sql, &mut conn)?;

        let paginated_df = transform_new(df.clone().lazy(), opts.clone())?.collect()?;

        let source_schema = if let Some(schema) =
            repositories::data_frames::schemas::get_by_path(repo, &workspace.commit, path)?
        {
            schema
        } else {
            Schema::from_polars(&paginated_df.schema())
        };

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
