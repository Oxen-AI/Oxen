use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::util;
use std::path::Path;

use crate::cmd::RunCmd;
pub const NAME: &str = "save";
pub struct SaveCmd;

#[async_trait]
impl RunCmd for SaveCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .arg(
                Arg::new("PATH")
                    .help("Path of the local repository to save")
                    .required(true)
                    .index(1), // This represents the position of the argument in the command line command.
            )
            .arg(
                Arg::new("output")
                    .help("Name of the output .tar.gz archive")
                    .short('o')
                    .long("output")
                    .required(true),
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo_str = args.get_one::<String>("PATH").expect("Required");
        let output_str = args.get_one::<String>("output").expect("Required");

        let output_path = Path::new(output_str);
        let repo_path = Path::new(repo_str);
        let repo_dir =
            util::fs::get_repo_root(repo_path).ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;
        let repo = LocalRepository::from_dir(&repo_dir)?;

        repositories::save(&repo, output_path)?;

        Ok(())
    }
}
