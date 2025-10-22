use crate::error::OxenError;
use crate::opts::DFOpts;
use minus::Pager;
/// # Pretty Print
/// The default Polars DataFrame print implementation puts characters
/// that do not fit well in our docs, so this transform helps cleanup the output.
///
/// ## Polars Example
/// ┌────────┬──────────┬───────────────────┐
/// │ prompt ┆ category ┆ .oxen.diff.status │
/// │ ---    ┆ ---      ┆ ---               │
/// │ str    ┆ str      ┆ str               │
/// ╞════════╪══════════╪═══════════════════╡
/// │ 20*20  ┆ math     ┆ added             │
/// └────────┴──────────┴───────────────────┘
///
/// ## Oxen Example
/// +--------+----------+-------------------+
/// | prompt ┆ category ┆ .oxen.diff.status |
/// | ---    ┆ ---      ┆ ---               |
/// | str    ┆ str      ┆ str               |
/// +--------+----------+-------------------+
/// | 20*20  ┆ math     ┆ added             |
/// +--------+----------+-------------------+
use polars::prelude::*;
use std::cmp;
use std::env;
use std::fmt::Write;

fn write_to_pager(output: &mut Pager, text: &str) -> Result<(), OxenError> {
    match writeln!(output, "{text}") {
        Ok(_) => Ok(()),
        Err(_) => Err(OxenError::basic_str("Could not write to pager")),
    }
}

pub fn df_to_str(df: &DataFrame) -> String {
    let default_fmt = format!("{df:?}");

    default_fmt
        .replace(['┌', '└', '┬', '┴', '┐', '┘'], "+")
        .replace('─', "-")
        .replace('│', "|")
        .replace(['╞', '╡'], "+")
        .replace('═', "-")
        .replace('╪', "+")
        .replace('┆', "|")
}

pub fn df_to_pager(df: &DataFrame, opts: &DFOpts) -> Result<Pager, OxenError> {
    let height = df.height();
    let max_rows = height + 10;
    env::set_var("POLARS_FMT_MAX_ROWS", max_rows.to_string());

    let page_size: usize = opts.page_size.unwrap_or(10);
    let start: usize = if let Some(page) = opts.page {
        cmp::max(1, (page - 1) * page_size + 1)
    } else {
        1
    };

    let default_fmt = format!("{df:?}");
    let pretty_fmt = default_fmt
        .replace(['┌', '└', '┬', '┴', '┐', '┘'], "+")
        .replace('─', "-")
        .replace('│', "|")
        .replace(['╞', '╡'], "+")
        .replace('═', "-")
        .replace('╪', "+")
        .replace('┆', "|");

    let mut lines = pretty_fmt.lines();
    let first_line = lines.clone().nth(1).unwrap();

    let mut output = Pager::new();

    for _line in 0..5 {
        write_to_pager(&mut output, lines.next().unwrap())?;
    }

    if start > height {
        write_to_pager(&mut output, first_line)?;
        write_to_pager(&mut output, first_line)?;
        env::set_var("POLARS_FMT_MAX_ROWS", "10");
        return Ok(output);
    }

    for _line in 0..start {
        lines.next();
    }

    for line in 0..height - start + 1 {
        if line % page_size == 0 {
            write_to_pager(&mut output, first_line)?;
        }
        write_to_pager(&mut output, lines.next().unwrap())?;
    }

    write_to_pager(&mut output, first_line)?;
    env::set_var("POLARS_FMT_MAX_ROWS", "10");

    Ok(output)
}
