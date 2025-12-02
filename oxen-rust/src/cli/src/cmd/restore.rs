use crate::helpers::check_repo_migration_needed;
use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::RestoreOpts;
use liboxen::repositories;
use std::path::PathBuf;

use crate::cmd::RunCmd;
pub const NAME: &str = "restore";
pub struct RestoreCmd;

pub fn restore_args() -> Command {
    // Setups the CLI args for the restore command
    Command::new(NAME)
        .about("Restore specified paths in the working tree with some contents from a restore source.")
        .arg(
            Arg::new("paths")
                .required(true)
                .action(clap::ArgAction::Append),
        )
        .arg_required_else_help(true)
        .arg(
            Arg::new("source")
                .long("source")
                .help("Restores a specific revision of the file. Can supply commit id or branch name")
                .action(clap::ArgAction::Set)
                .requires("paths"),
        )
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Restore content in staging area. By default, if --staged is given, the contents are restored from HEAD. Use --source to restore from a different commit.")
                .action(clap::ArgAction::SetTrue)
                .requires("paths"),
        )
        .arg(
            Arg::new("combine-chunks")
                .long("combine-chunks")
                .help("Build complete file from file chunks and restore to working directory")
                .action(clap::ArgAction::SetTrue)
                .requires("paths"),
        )
}

#[async_trait]
impl RunCmd for RestoreCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        restore_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;
        let paths: Vec<PathBuf> = args
            .get_many::<String>("paths")
            .expect("Must supply paths")
            .map(|p| -> Result<PathBuf, OxenError> {
                let path = PathBuf::from(p);
                Ok(path)
            })
            .collect::<Result<Vec<PathBuf>, OxenError>>()?;

        for path in paths {
            let opts = if let Some(source) = args.get_one::<String>("source") {
                RestoreOpts {
                    path: path.clone(),
                    staged: args.get_flag("staged"),
                    is_remote: false,
                    source_ref: Some(String::from(source)),
                }
            } else {
                RestoreOpts {
                    path: path.clone(),
                    staged: args.get_flag("staged"),
                    is_remote: false,
                    source_ref: None,
                }
            };

            if args.get_flag("combine-chunks") {
                repositories::restore::combine_chunks(&repo, opts).await?;
            } else {
                repositories::restore::restore(&repo, opts).await?;
            }
        }

        Ok(())
    }
}
