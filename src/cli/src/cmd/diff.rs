use async_trait::async_trait;
use clap::{Arg, Command};
use colored::ColoredString;
use colored::Colorize;
use std::path::PathBuf;

use liboxen::core::df::pretty_print;
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::model::diff::tabular_diff::TabularDiffMods;
use liboxen::model::diff::{ChangeType, DiffResult, TextDiff};
use liboxen::opts::DiffOpts;
use liboxen::repositories;
use liboxen::util;

use crate::cmd::RunCmd;
pub const NAME: &str = "diff";
pub struct DiffCmd;

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

        // If the user specifies two files without revisions, we will compare the files on disk
        let mut diff_result =
            if opts.revision_1.is_none() && opts.revision_2.is_none() && opts.path_2.is_some() {
                // If we do not have revisions set, just compare the files on disk
                repositories::diffs::diff(
                    opts.path_1,
                    opts.path_2,
                    opts.keys,
                    opts.targets,
                    None,
                    opts.revision_1,
                    opts.revision_2,
                )?
            } else {
                // If we have revisions set, pass in the repo_dir to be able
                // to compare the files at those revisions within the .oxen repo
                let repo_dir = util::fs::get_repo_root_from_current_dir().unwrap();
                repositories::diffs::diff(
                    opts.path_1,
                    opts.path_2,
                    opts.keys,
                    opts.targets,
                    Some(repo_dir),
                    opts.revision_1,
                    opts.revision_2,
                )?
            };

        DiffCmd::print_diff_result(&diff_result)?;
        DiffCmd::maybe_save_diff_output(&mut diff_result, opts.output)?;

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
            path_1: file1,
            path_2: file2,
            keys,
            targets,
            repo_dir: None,
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

    pub fn print_diff_result(result: &DiffResult) -> Result<(), OxenError> {
        match result {
            DiffResult::Tabular(result) => {
                // println!("{:?}", ct.summary);
                DiffCmd::print_column_changes(&result.summary.modifications)?;
                DiffCmd::print_row_changes(&result.summary.modifications)?;
                println!("{}", pretty_print::df_to_str(&result.contents));
            }
            DiffResult::Text(diff) => {
                DiffCmd::print_text_diff(diff);
            }
        }

        Ok(())
    }

    fn print_row_changes(mods: &TabularDiffMods) -> Result<(), OxenError> {
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
            print!("{output}");
        }

        println!();

        Ok(())
    }

    // TODO: Truncate to "and x more"
    fn print_column_changes(mods: &TabularDiffMods) -> Result<(), OxenError> {
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
            print!("{output}");
        }

        Ok(())
    }

    fn print_text_diff(diff: &TextDiff) {
        for line in &diff.lines {
            match line.modification {
                ChangeType::Unchanged => println!("{}", line.text),
                ChangeType::Added => println!("{}", line.text.green()),
                ChangeType::Removed => println!("{}", line.text.red()),
                ChangeType::Modified => println!("{}", line.text.yellow()),
            }
        }
    }

    pub fn maybe_save_diff_output(
        result: &mut DiffResult,
        output: Option<PathBuf>,
    ) -> Result<(), OxenError> {
        match result {
            DiffResult::Tabular(result) => {
                let mut df = result.contents.clone();
                // Save to disk if we have an output
                if let Some(file_path) = output {
                    tabular::write_df(&mut df, file_path.clone())?;
                }
            }
            DiffResult::Text(_) => {
                println!("Saving to disk not supported for text output");
            }
        }

        Ok(())
    }
}
