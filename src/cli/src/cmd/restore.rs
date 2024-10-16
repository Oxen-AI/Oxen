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

#[async_trait]
impl RunCmd for RestoreCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
        .about("Restore specified paths in the working tree with some contents from a restore source.")
        .arg(Arg::new("PATH")
            .help("The files or directory to restore")
        )
        .arg_required_else_help(true)
        .arg(
            Arg::new("source")
                .long("source")
                .help("Restores a specific revision of the file. Can supply commit id or branch name")
                .action(clap::ArgAction::Set)
                .requires("PATH"),
        )
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Restore content in staging area. By default, if --staged is given, the contents are restored from HEAD. Use --source to restore from a different commit.")
                .action(clap::ArgAction::SetTrue)
                .requires("PATH"),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let path = args.get_one::<String>("PATH").expect("required");

        let opts = if let Some(source) = args.get_one::<String>("source") {
            RestoreOpts {
                path: PathBuf::from(path),
                staged: args.get_flag("staged"),
                is_remote: false,
                source_ref: Some(String::from(source)),
            }
        } else {
            RestoreOpts {
                path: PathBuf::from(path),
                staged: args.get_flag("staged"),
                is_remote: false,
                source_ref: None,
            }
        };

        let repository = LocalRepository::from_current_dir()?;

        check_repo_migration_needed(&repository)?;
        repositories::restore::restore(&repository, opts)?;

        Ok(())
    }
}
