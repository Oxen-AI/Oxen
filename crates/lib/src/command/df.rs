//! # oxen df
//!
//! Interact with DataFrames
//!

use std::path::Path;

use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::DFOpts;
use crate::{repositories, util};

/// Interact with DataFrames
pub async fn df(input: &Path, opts: DFOpts) -> Result<(), OxenError> {
    let mut df = tabular::show_path(input, opts.clone()).await?;

    if let Some(write) = opts.write {
        println!("Writing {write:?}");
        tabular::write_df(&mut df, &write)?;
    }

    if let Some(output) = opts.output {
        println!("Writing {output:?}");
        tabular::write_df(&mut df, &output)?;
    }

    Ok(())
}

pub async fn df_revision(
    repo: &LocalRepository,
    input: &Path,
    revision: &str,
    opts: DFOpts,
) -> Result<(), OxenError> {
    let commit = repositories::revisions::get(repo, revision)?.ok_or(OxenError::basic_str(
        format!("Revision {} not found", revision),
    ))?;
    let Some(root) = repositories::tree::get_node_by_path_with_children(repo, &commit, input)?
    else {
        return Err(OxenError::basic_str(format!(
            "Merkle tree for revision {} not found",
            revision
        )));
    };

    let mut df = tabular::show_node(repo.clone(), &root, opts.clone()).await?;

    if let Some(output) = opts.output {
        println!("Writing {output:?}");
        tabular::write_df(&mut df, &output)?;
    }

    Ok(())
}

/// Get a human readable schema for a DataFrame
pub fn schema(input: &Path, flatten: bool, opts: DFOpts) -> Result<String, OxenError> {
    tabular::schema_to_string(input, flatten, &opts)
}

/// Add a row to a dataframe
pub async fn add_row(path: &Path, data: &str) -> Result<(), OxenError> {
    if util::fs::is_tabular(path) {
        let mut opts = DFOpts::empty();
        opts.add_row = Some(data.to_string());
        opts.output = Some(path.to_path_buf());
        df(path, opts).await
    } else {
        let err = format!("{} is not a tabular file", path.display());
        Err(OxenError::basic_str(err))
    }
}

/// Add a column to a dataframe
pub async fn add_column(path: &Path, data: &str) -> Result<(), OxenError> {
    if util::fs::is_tabular(path) {
        let mut opts = DFOpts::empty();
        opts.add_col = Some(data.to_string());
        opts.output = Some(path.to_path_buf());
        df(path, opts).await
    } else {
        let err = format!("{} is not a tabular file", path.display());
        Err(OxenError::basic_str(err))
    }
}
