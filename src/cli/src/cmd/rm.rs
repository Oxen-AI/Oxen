use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;

use crate::util;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::RmOpts;
use liboxen::repositories;
use std::path::PathBuf;

use crate::cmd::RunCmd;
pub const NAME: &str = "rm";
pub struct RmCmd;

pub fn rm_args() -> Command {
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

#[async_trait]
impl RunCmd for RmCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        rm_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(|p| -> Result<PathBuf, OxenError> {
                let current_dir = std::env::current_dir().map_err(|e| {
                    log::warn!("Failed to get current directory: {}", e);
                    OxenError::basic_str(format!("Failed to get current directory: {}", e))
                })?;
                let joined_path = current_dir.join(p);
                util::fs::canonicalize(&joined_path).or_else(|_| Ok(joined_path))
            })
            .collect::<Result<Vec<PathBuf>, OxenError>>()?;

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
