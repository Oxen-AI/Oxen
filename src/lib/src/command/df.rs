//! # oxen df
//!
//! Interact with DataFrames
//!

use std::path::Path;

use crate::core::df::tabular;
use crate::error::OxenError;
use crate::opts::DFOpts;
use crate::util;

/// Interact with DataFrames
pub fn df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    if opts.show_size {
        let size = tabular::get_size(input);
        println!("{:?}", size);
        return Ok(());
    }

    let mut df = tabular::show_path(input, opts.clone())?;

    if let Some(write) = opts.write {
        println!("Writing {write:?}");
        tabular::write_df(&mut df, write)?;
    }

    if let Some(output) = opts.output {
        println!("Writing {output:?}");
        tabular::write_df(&mut df, output)?;
    }

    Ok(())
}

/// Get a human readable schema for a DataFrame
pub fn schema<P: AsRef<Path>>(input: P, flatten: bool, opts: DFOpts) -> Result<String, OxenError> {
    tabular::schema_to_string(input, flatten, &opts)
}

/// Add a row to a dataframe
pub fn add_row(path: &Path, data: &str) -> Result<(), OxenError> {
    if util::fs::is_tabular(path) {
        let mut opts = DFOpts::empty();
        opts.add_row = Some(data.to_string());
        opts.output = Some(path.to_path_buf());
        df(path, opts)
    } else {
        let err = format!("{} is not a tabular file", path.display());
        Err(OxenError::basic_str(err))
    }
}

/// Add a column to a dataframe
pub fn add_column(path: &Path, data: &str) -> Result<(), OxenError> {
    if util::fs::is_tabular(path) {
        let mut opts = DFOpts::empty();
        opts.add_col = Some(data.to_string());
        opts.output = Some(path.to_path_buf());
        df(path, opts)
    } else {
        let err = format!("{} is not a tabular file", path.display());
        Err(OxenError::basic_str(err))
    }
}
