use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::RmOpts;
use liboxen::repositories;
use std::path::PathBuf;

use crate::cmd::RunCmd;
pub const NAME: &str = "rm";
pub struct RmCmd;

#[async_trait]
impl RunCmd for RmCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Removes the specified files from the index")
            .arg(
                Arg::new("files")
                    .required(true)
                    .action(clap::ArgAction::Append),
            )
            .arg(
                Arg::new("staged")
                    .long("staged")
                    .help("Removes the file from the staging area.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("recursive")
                    .long("recursive")
                    .short('r')
                    .help("Recursively removes directory.")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let opts = RmOpts {
            // The path will get overwritten for each file that is removed
            path: paths.first().unwrap().to_path_buf(),
            staged: args.get_flag("staged"),
            recursive: args.get_flag("recursive"),
        };

        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        for path in paths {
            let path_opts = RmOpts::from_path_opts(&path, &opts);
            repositories::rm(&repository, &path_opts)?;
        }

        Ok(())
    }
}
