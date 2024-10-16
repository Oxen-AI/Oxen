use crate::constants::DUCKDB_DF_TABLE_NAME;
use crate::core::db::data_frames::df_db;
use crate::core::df::{sql, tabular};
use crate::core::v0_10_0::cache::cachers;
use crate::core::v0_10_0::index;
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::error::OxenError;
use crate::model::data_frame::schema::Schema;
use crate::model::data_frame::{DataFrameSchemaSize, DataFrameSlice, DataFrameSliceSchemas};
use crate::model::{Commit, CommitEntry, DataFrameSize, LocalRepository, Workspace};
use crate::opts::DFOpts;
use crate::repositories;
use crate::util;

pub mod schemas;

use std::path::Path;

pub fn get_slice(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrameSlice, OxenError> {
    let path = path.as_ref();
    let entry_reader = CommitEntryReader::new(repo, commit)?;
    let entry = entry_reader
        .get_entry(path)?
        .ok_or(OxenError::path_does_not_exist(path))?;

    let version_path = util::fs::version_path_for_commit_id(repo, &commit.id, path)?;
    log::debug!("view_from Reading version file {:?}", version_path);

    let data_frame_size = cachers::df_size::get_cache_for_version(repo, commit, &version_path)?;
    log::debug!("view_from got data frame size {:?}", data_frame_size);

    let handle_sql_result = handle_sql_querying(repo, commit, path, opts, &entry, &data_frame_size);
    if let Ok(response) = handle_sql_result {
        return Ok(response);
    }

    let height = if opts.slice.is_some() {
        log::debug!("Scanning df with slice: {:?}", opts.slice);
        let slice = opts.slice.as_ref().unwrap();
        let (_, end) = slice.split_once("..").unwrap();

        end.parse::<usize>().unwrap()
    } else {
        data_frame_size.height
    };

    log::debug!("Scanning df with height: {}", height);

    let mut df = tabular::scan_df(&version_path, opts, height)?;

    // Try to get the schema from the merkle tree
    let og_schema = if let Some(schema) =
        repositories::data_frames::schemas::get_by_path(repo, commit, path)?
    {
        schema
    } else {
        match df.collect_schema() {
            Ok(schema) => Ok(Schema::from_polars(&schema.to_owned())),
            Err(e) => {
                log::error!("Error reading df: {}", e);
                Err(OxenError::basic_str("Error reading df schema"))
            }
        }?
    };

    log::debug!("view_from Done getting schema {:?}", version_path);

    let source_schema = DataFrameSchemaSize {
        size: data_frame_size.clone(),
        schema: og_schema.clone(),
    };

    log::debug!("view_from BEFORE TRANSFORM LAZY {}", data_frame_size.height);

    // Transformation and pagination logic...
    let df_view = tabular::transform_lazy(df, opts.clone())?;

    // Have to do the pagination after the transform
    let lf = tabular::transform_slice_lazy(df_view, opts.clone())?;
    log::debug!("done transform_slice_lazy: {:?}", lf.describe_plan());
    let df = lf.collect()?;

    let view_height = if opts.has_filter_transform() {
        df.height()
    } else {
        data_frame_size.height
    };

    let mut slice_schema = Schema::from_polars(&df.schema());
    log::debug!("OG schema {:?}", og_schema);
    log::debug!("Pre-Slice schema {:?}", slice_schema);
    slice_schema.update_metadata_from_schema(&og_schema);
    log::debug!("Slice schema {:?}", slice_schema);

    Ok(DataFrameSlice {
        total_entries: view_height,
        schemas: DataFrameSliceSchemas {
            source: source_schema,
            slice: DataFrameSchemaSize {
                size: DataFrameSize {
                    width: df.width(),
                    height: view_height,
                },
                schema: slice_schema,
            },
        },
        slice: df,
    })
}

fn handle_sql_querying(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
    entry: &CommitEntry,
    data_frame_size: &DataFrameSize,
) -> Result<DataFrameSlice, OxenError> {
    let path = path.as_ref();
    let mut workspace: Option<Workspace> = None;

    if opts.sql.is_some() {
        match index::workspaces::data_frames::get_queryable_data_frame_workspace(repo, path, commit)
        {
            Ok(found_workspace) => {
                // Assign the found workspace to the workspace variable
                workspace = Some(found_workspace);
            }
            Err(e) => return Err(e),
        }
    }

    if let (Some(sql), Some(workspace)) = (opts.sql.clone(), workspace) {
        let db_path = repositories::workspaces::data_frames::duckdb_path(&workspace, &entry.path);
        let mut conn = df_db::get_connection(db_path)?;

        let mut slice_schema = df_db::get_schema(&conn, DUCKDB_DF_TABLE_NAME)?;
        let df = sql::query_df(sql, &mut conn)?;

        let source_schema = if let Some(schema) =
            repositories::data_frames::schemas::get_by_path(repo, &workspace.commit, path)?
        {
            schema
        } else {
            Schema::from_polars(&df.schema())
        };

        slice_schema.update_metadata_from_schema(&source_schema);

        return Ok(DataFrameSlice {
            total_entries: df.height(),
            schemas: DataFrameSliceSchemas {
                source: DataFrameSchemaSize {
                    size: data_frame_size.clone(),
                    schema: source_schema,
                },
                slice: DataFrameSchemaSize {
                    size: DataFrameSize {
                        width: df.width(),
                        height: df.height(),
                    },
                    schema: slice_schema,
                },
            },
            slice: df,
        });
    }

    Err(OxenError::basic_str("Could not query data frame"))
}
