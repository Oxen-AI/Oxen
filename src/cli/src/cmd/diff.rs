use async_trait::async_trait;
use clap::{Arg, Command};
use colored::ColoredString;
use colored::Colorize;
use minus::Pager;
use std::fmt::Write;
use std::path::PathBuf;

use liboxen::core::df::pretty_print;
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::model::diff::tabular_diff::TabularDiffMods;
use liboxen::model::diff::{ChangeType, DiffResult, TextDiff};
use liboxen::opts::DiffOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "diff";
pub struct DiffCmd;

fn write_to_pager(output: &mut Pager, text: &str) -> Result<(), OxenError> {
    match writeln!(output, "{}", text) {
        Ok(_) => Ok(()),
        Err(_) => Err(OxenError::basic_str("Could not write to pager")),
    }
}

#[async_trait]
impl RunCmd for DiffCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Compare two files against each other or against versions. The two resource paramaters can be specified by filepath or `file:revision` syntax.")
            .arg(Arg::new("RESOURCE1")
                .required(true)
                .help("First resource, in format `file` or `file:revision`")
                .index(1)
            )
            .arg(Arg::new("RESOURCE2")
                .required(false)
                .help("Second resource, in format `file` or `file:revision`. If left blank, RESOURCE1 will be compared with HEAD.")
                .index(2))
            .arg(Arg::new("keys")
                .required(false)
                .long("keys")
                .short('k')
                .help("Comma-separated list of columns to compare on. If not specified, all columns are used for keys.")
                .use_value_delimiter(true)
                .action(clap::ArgAction::Set))
            .arg(Arg::new("compares")
                .required(false)
                .long("compares")
                .short('c')
                .help("Comma-separated list of columns to compare changes between. If not specified, all columns  that are not keys are compares.")
                .use_value_delimiter(true)
                .action(clap::ArgAction::Set))
            .arg(Arg::new("output")
                .required(false)
                .long("output")
                .short('o')
                .help("Output directory path to write the results of the comparison. Will write both match.csv (rows with same keys and compares) and diff.csv (rows with different compares between files.")
                .action(clap::ArgAction::Set))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let opts = DiffCmd::parse_args(args);
        let output = opts.output.clone();

        let mut diff_result = repositories::diffs::diff(opts)?;

        DiffCmd::print_diff_result(&diff_result)?;
        DiffCmd::maybe_save_diff_output(&mut diff_result, output)?;

        Ok(())
    }
}

impl DiffCmd {
    pub fn parse_args(args: &clap::ArgMatches) -> DiffOpts {
        let resource1 = args.get_one::<String>("RESOURCE1").expect("required");
        let resource2 = args.get_one::<String>("RESOURCE2");

        let (file1, revision1) = DiffCmd::parse_file_and_revision(resource1);

        let file1 = PathBuf::from(file1);

        let (file2, revision2) = match resource2 {
            Some(resource) => {
                let (file, revision) = DiffCmd::parse_file_and_revision(resource);
                (Some(PathBuf::from(file)), revision)
            }
            None => (None, None),
        };

        let keys: Vec<String> = match args.get_many::<String>("keys") {
            Some(values) => values.cloned().collect(),
            None => Vec::new(),
        };

        // We changed the external name to compares, need to refactor internals still
        let maybe_targets = args.get_many::<String>("compares");

        let targets = match maybe_targets {
            Some(values) => values.cloned().collect(),
            None => Vec::new(),
        };

        let output = args.get_one::<String>("output").map(PathBuf::from);

        DiffOpts {
            repo_dir: None,
            path_1: file1,
            path_2: file2,
            keys,
            targets,
            revision_1: revision1,
            revision_2: revision2,
            output,
        }
    }

    fn parse_file_and_revision(file_revision: &str) -> (String, Option<String>) {
        let parts: Vec<&str> = file_revision.split(':').collect();
        if parts.len() == 2 {
            (parts[0].to_string(), Some(parts[1].to_string()))
        } else {
            (parts[0].to_string(), None)
        }
    }

    pub fn print_diff_result(results: &Vec<DiffResult>) -> Result<(), OxenError> {
        let mut p = Pager::new();

        for result in results {
            match result {
                DiffResult::Tabular(diff) => {
                    // println!("{:?}", ct.summary);
                    write_to_pager(
                        &mut p,
                        &format!(
                            "--- from file: {}\n+++ to file: {}\n",
                            diff.filename1.as_ref().unwrap(),
                            diff.filename2.as_ref().unwrap()
                        ),
                    )?;
                    DiffCmd::print_column_changes(&mut p, &diff.summary.modifications)?;
                    DiffCmd::print_row_changes(&mut p, &diff.summary.modifications)?;
                    write_to_pager(&mut p, pretty_print::df_to_str(&diff.contents).as_str())?;
                }
                DiffResult::Text(diff) => {
                    DiffCmd::print_text_diff(&mut p, diff)?;
                }
            }
            write_to_pager(&mut p, "\n\n".to_string().as_str())?;
        }

        match minus::page_all(p) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error while paging: {}", e);
            }
        }

        Ok(())
    }

    fn print_row_changes(p: &mut Pager, mods: &TabularDiffMods) -> Result<(), OxenError> {
        let mut outputs: Vec<ColoredString> = vec![];

        if mods.row_counts.modified + mods.row_counts.added + mods.row_counts.removed == 0 {
            println!();
            return Ok(());
        }

        outputs.push("\nRow changes: \n".into());
        if mods.row_counts.modified > 0 {
            outputs.push(format!("   Δ {} (modified)\n", mods.row_counts.modified).yellow());
        }

        if mods.row_counts.added > 0 {
            outputs.push(format!("   + {} (added)\n", mods.row_counts.added).green());
        }

        if mods.row_counts.removed > 0 {
            outputs.push(format!("   - {} (removed)\n", mods.row_counts.removed).red());
        }

        for output in outputs {
            write_to_pager(p, &output)?;
        }

        write_to_pager(p, "\n".to_string().as_str())?;

        Ok(())
    }

    // TODO: Truncate to "and x more"
    fn print_column_changes(p: &mut Pager, mods: &TabularDiffMods) -> Result<(), OxenError> {
        let mut outputs: Vec<ColoredString> = vec![];

        if !mods.col_changes.added.is_empty() || !mods.col_changes.added.is_empty() {
            outputs.push("Column changes:\n".into());
        }

        for col in &mods.col_changes.added {
            outputs.push(format!("   + {} ({})\n", col.name, col.dtype).green());
        }

        for col in &mods.col_changes.removed {
            outputs.push(format!("   - {} ({})\n", col.name, col.dtype).red());
        }

        for output in outputs {
            write_to_pager(p, &format!("{output}"))?;
        }

        Ok(())
    }

    fn print_text_diff(p: &mut Pager, diff: &TextDiff) -> Result<(), OxenError> {
        write_to_pager(
            p,
            &format!(
                "--- from file: {}\n+++ to file: {}\n",
                diff.filename1.as_ref().unwrap_or(&"<no file1>".to_string()),
                diff.filename2.as_ref().unwrap_or(&"<no file1>".to_string())
            ),
        )?;

        for line in &diff.lines {
            match line.modification {
                ChangeType::Unchanged => write_to_pager(p, line.text.to_string().as_str())?,
                ChangeType::Added => write_to_pager(p, &format!("+ {}", line.text.green()))?,
                ChangeType::Removed => write_to_pager(p, &format!("- {}", line.text.red()))?,
                ChangeType::Modified => write_to_pager(p, line.text.to_string().as_str())?,
            }
        }
        Ok(())
    }

    pub fn maybe_save_diff_output(
        result: &mut Vec<DiffResult>,
        output: Option<PathBuf>,
    ) -> Result<(), OxenError> {
        for result in result {
            if let Some(ref file_path) = output {
                match result {
                    DiffResult::Tabular(result) => {
                        let mut df = result.contents.clone();
                        tabular::write_df(&mut df, file_path.clone())?;
                    }
                    DiffResult::Text(_) => {
                        println!("Saving to disk not supported for text output");
                    }
                }
            }
        }
        // Save to disk if we have an output

        Ok(())
    }
}
