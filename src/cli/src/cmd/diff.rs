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
pub const DIFFSEP: &str = "..";
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
            .about("Show changes between commits, commit and working tree, etc")
            .arg(
                Arg::new("commits_or_files")
                    .help(format!("Commits, commit ranges (commit1{DIFFSEP}commit2)"))
                    .num_args(0..)
                    .action(clap::ArgAction::Append)
                    .value_name("revision | commit | branch"),
            )
            .arg(
                Arg::new("paths")
                    .help("Limit diff to specific paths")
                    .num_args(0..)
                    .last(true)
                    .action(clap::ArgAction::Append)
                    .value_name("path"),
            )
            .arg(
                Arg::new("keys")
                    .long("keys")
                    .short('k')
                    .help("Comma-separated list of columns to compare on")
                    .use_value_delimiter(true)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("compares")
                    .long("compares")
                    .short('c')
                    .help("Comma-separated list of columns to compare changes between")
                    .use_value_delimiter(true)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("output")
                    .long("output")
                    .short('o')
                    .help("Output directory path to write the results")
                    .action(clap::ArgAction::Set),
            )
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
        let commits_or_files: Vec<String> = args
            .get_many::<String>("commits_or_files")
            .map(|values| values.cloned().collect())
            .unwrap_or_default();

        let paths: Vec<String> = args
            .get_many::<String>("paths")
            .map(|values| values.cloned().collect())
            .unwrap_or_default();

        // Parse the different forms of diff commands
        let (file1, file2, revision1, revision2) = match commits_or_files.len() {
            0 => {
                // oxen diff [--] [<path>…​] - compare working tree with HEAD
                let path = if !paths.is_empty() {
                    PathBuf::from(&paths[0])
                } else {
                    PathBuf::from("")
                };
                (path, None, Some("HEAD".to_string()), None)
            }
            1 => {
                let arg = &commits_or_files[0];
                if arg.contains(DIFFSEP) {
                    // oxen diff <commit>..<commit> [--] [<path>…​]
                    let parts: Vec<&str> = arg.split(DIFFSEP).collect();
                    if parts.len() == 2 {
                        let path = if !paths.is_empty() {
                            PathBuf::from(&paths[0])
                        } else {
                            PathBuf::from("")
                        };
                        (
                            path.clone(),
                            Some(path),
                            Some(parts[0].to_string()),
                            Some(parts[1].to_string()),
                        )
                    } else {
                        // Invalid range format, treat as single commit
                        let path = if !paths.is_empty() {
                            PathBuf::from(&paths[0])
                        } else {
                            PathBuf::from("")
                        };
                        (path, None, Some(arg.clone()), Some("HEAD".to_string()))
                    }
                } else {
                    // oxen diff <revision1> [--] [<path>…​] - compare revision1 with HEAD
                    let path = if !paths.is_empty() {
                        PathBuf::from(&paths[0])
                    } else {
                        PathBuf::from("")
                    };
                    (path, None, Some("HEAD".to_string()), Some(arg.clone()))
                }
            }
            2 => {
                // Check if both arguments are local files
                let arg1_path = PathBuf::from(&commits_or_files[0]);
                let arg2_path = PathBuf::from(&commits_or_files[1]);

                if arg1_path.exists() && arg2_path.exists() {
                    // oxen diff file1 file2 - compare two local files
                    (arg1_path, Some(arg2_path), None, None)
                } else {
                    // oxen diff revision1 revision2 [--] [<path>…​] - compare two revisions
                    let path = if !paths.is_empty() {
                        PathBuf::from(&paths[0])
                    } else {
                        PathBuf::from("")
                    };
                    (
                        path.clone(),
                        Some(path),
                        Some(commits_or_files[0].clone()),
                        Some(commits_or_files[1].clone()),
                    )
                }
            }
            _ => {
                // Too many arguments, use first two as revisions
                let path = if !paths.is_empty() {
                    PathBuf::from(&paths[0])
                } else {
                    PathBuf::from("")
                };
                (
                    path.clone(),
                    Some(path),
                    Some(commits_or_files[0].clone()),
                    Some(commits_or_files[1].clone()),
                )
            }
        };

        let keys: Vec<String> = args
            .get_many::<String>("keys")
            .map(|values| values.cloned().collect())
            .unwrap_or_default();

        let targets: Vec<String> = args
            .get_many::<String>("compares")
            .map(|values| values.cloned().collect())
            .unwrap_or_default();

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
            ..Default::default()
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
