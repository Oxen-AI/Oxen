use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use std::path::PathBuf;

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::CleanOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;
use crate::util;

pub const NAME: &str = "clean";
pub struct CleanCmd;

#[async_trait]
impl RunCmd for CleanCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Remove untracked files and directories from the working tree")
            .long_about(
                "Remove untracked files and directories from the working tree.\n\n\
                 Without `-f`, runs as a dry-run and prints what would be removed.\n\
                 Pass `-f` to actually delete.\n\n\
                 `.oxenignore`-matched files are always preserved.",
            )
            .arg(
                Arg::new("force")
                    .long("force")
                    .short('f')
                    .help("Actually remove the files. Without this flag, clean runs as a dry-run.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("paths")
                    .num_args(0..)
                    .help("Scope cleanup to these paths. Defaults to the whole working tree."),
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let paths: Vec<PathBuf> = args
            .get_many::<String>("paths")
            .map(|vals| {
                vals.map(|p| -> Result<PathBuf, OxenError> {
                    let current_dir = std::env::current_dir()?;
                    let joined = current_dir.join(p);
                    util::fs::canonicalize(&joined).or_else(|_| Ok(joined))
                })
                .collect::<Result<Vec<PathBuf>, OxenError>>()
            })
            .transpose()?
            .unwrap_or_default();

        let opts = CleanOpts {
            paths,
            force: args.get_flag("force"),
        };

        let result = repositories::clean::clean(&repository, &opts).await?;

        if result.files.is_empty() && result.dirs.is_empty() {
            println!("Nothing to clean.");
        } else if !result.applied {
            println!(
                "\nDry run: {} file(s), {} dir(s) would be removed. Re-run with -f to apply.",
                result.files.len(),
                result.dirs.len()
            );
        }

        Ok(())
    }
}
