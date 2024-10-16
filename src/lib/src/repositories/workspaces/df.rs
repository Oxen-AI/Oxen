//! # oxen df
//!
//! Interact with Remote DataFrames
//!

use std::path::Path;

use polars::prelude::DataFrame;

use crate::api;
use crate::config::UserConfig;
use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::DFOpts;
use crate::view::StatusMessage;

/// Interact with Remote DataFrames
/// Interact with Remote DataFrames
pub async fn df(
    repo: &LocalRepository,
    workspace_id: &str,
    input: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    // Special case where we are writing data
    if let Some(row) = &opts.add_row {
        add_row(repo, workspace_id, input.as_ref(), row).await
    } else if let Some(uuid) = &opts.delete_row {
        delete_row(repo, workspace_id, input, uuid).await
    } else {
        let remote_repo = api::client::repositories::get_default_remote(repo).await?;
        let output = opts.output.clone();
        let workspace_id = UserConfig::identifier()?;
        let val =
            api::client::workspaces::data_frames::get(&remote_repo, workspace_id, input, opts)
                .await;

        match val {
            Ok(val) => {
                if let Some(data_frame) = val.data_frame {
                    let mut df = data_frame.view.to_df();
                    if let Some(output) = output {
                        println!("Writing {output:?}");
                        tabular::write_df(&mut df, output)?;
                    }

                    println!(
                        "Full shape: ({}, {})\n",
                        data_frame.source.size.height, data_frame.source.size.width
                    );
                    println!("Slice {df:?}");
                    Ok(df)
                } else {
                    handle_unindexed_error()
                }
            }
            Err(_) => handle_unindexed_error(),
        }
    }
}

fn handle_unindexed_error() -> Result<DataFrame, OxenError> {
    println!(
        "Dataset not indexed for remote editing. Use `oxen df --index <path>` to index it, or `oxen df <path> --committed` to view the committed resource in view-only mode.\n"
    );
    Err(OxenError::basic_str("No dataset staged for this resource."))
}

// TODO: Only difference between this and `df` is for `get` operations - everything above
// the "else" can be factored into a shared method
pub async fn staged_df<P: AsRef<Path>>(
    repo: &LocalRepository,
    workspace_id: &str,
    input: P,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    // Special case where we are writing data
    if let Some(row) = &opts.add_row {
        add_row(repo, workspace_id, input.as_ref(), row).await
    } else if let Some(uuid) = &opts.delete_row {
        delete_row(repo, workspace_id, input, uuid).await
    } else {
        let remote_repo = api::client::repositories::get_default_remote(repo).await?;
        let output = opts.output.clone();
        let val =
            api::client::workspaces::data_frames::get(&remote_repo, &workspace_id, input, opts)
                .await;

        if let Ok(val) = val {
            if let Some(data_frame) = val.data_frame {
                let mut df = data_frame.view.to_df();
                if let Some(output) = output {
                    println!("Writing {output:?}");
                    tabular::write_df(&mut df, output)?;
                }

                println!(
                    "Full shape: ({}, {})\n",
                    data_frame.source.size.height, data_frame.source.size.width
                );
                println!("Slice {df:?}");
                return Ok(df);
            }
        }

        println!(
            "Dataset not indexed for remote editing. Use `oxen df --index <path>` to index it, or `oxen df <path> --committed` to view the committed resource in view-only mode.\n"
        );
        Err(OxenError::basic_str("No dataset staged for this resource."))
    }
}

pub async fn add_row(
    repo: &LocalRepository,
    workspace_id: &str,
    path: &Path,
    data: &str,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::client::repositories::get_default_remote(repo).await?;

    // let data = format!(r#"{{"data": {}}}"#, data);
    let data = data.to_string();
    let (df, row_id) =
        api::client::workspaces::data_frames::rows::add(&remote_repo, workspace_id, path, data)
            .await?;

    if let Some(row_id) = row_id {
        println!("\nAdded row: {row_id:?}");
    }

    println!("{:?}", df);
    Ok(df)
}

pub async fn delete_row(
    repository: &LocalRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::client::repositories::get_default_remote(repository).await?;
    let df = api::client::workspaces::data_frames::rows::delete(
        &remote_repo,
        workspace_id,
        path.as_ref(),
        row_id,
    )
    .await?;
    Ok(df)
}

pub async fn get_row(
    repository: &LocalRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::client::repositories::get_default_remote(repository).await?;
    let df_json = api::client::workspaces::data_frames::rows::get(
        &remote_repo,
        workspace_id,
        path.as_ref(),
        row_id,
    )
    .await?;
    let df = df_json.data_frame.view.to_df();
    println!("{:?}", df);
    Ok(df)
}

pub async fn index(
    repository: &LocalRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<StatusMessage, OxenError> {
    let remote_repo = api::client::repositories::get_default_remote(repository).await?;
    api::client::workspaces::data_frames::index(&remote_repo, workspace_id, path.as_ref()).await
}
