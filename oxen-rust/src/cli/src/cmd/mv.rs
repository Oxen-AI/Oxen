use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;

use crate::util;
use liboxen::model::LocalRepository;
use liboxen::opts::RmOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const MV: &str = "mv";

pub struct MvCmd;

#[async_trait]
impl RunCmd for MvCmd {
    fn name(&self) -> &str {
        MV
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(MV)
            .about("Adds the specified files or directories")
            .arg(
                Arg::new("source")
                    .index(1)
                    .required(true)
                    .action(clap::ArgAction::Append),
            )
            .arg(
                Arg::new("dest")
                    .index(2)
                    .required(true)
                    .action(clap::ArgAction::Append),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let source: &String = args.get_one("source").unwrap();
        let dest: &String = args.get_one("dest").unwrap();

        let source = PathBuf::from(source);
        let dest = PathBuf::from(dest);

        // Recursively look up from the current dir for .oxen directory
        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        let rm_opts = RmOpts {
            // The path will get overwritten for each file that is removed
            path: source.clone(),
            staged: false,
            recursive: true,
        };

        util::fs::rename(&source, &dest)?;
        repositories::add(&repo, dest).await?;
        repositories::rm(&repo, &rm_opts)?;

        Ok(())
    }
}
