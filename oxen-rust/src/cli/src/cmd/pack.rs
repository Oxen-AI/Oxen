use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;

use crate::cmd::RunCmd;
pub const NAME: &str = "pack";
pub struct PackCmd;

#[async_trait]
impl RunCmd for PackCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Pack your bags, let's see how well we can compress data. TODO: This is not a real command, just an experiment.")
            .arg(
                Arg::new("files")
                    .required(true)
                    .action(clap::ArgAction::Append),
            )
            // Number of commits back to pack
            .arg(
                Arg::new("number")
                    .long("number")
                    .short('n')
                    .help("How many commits back to pack")
                    .default_value("1")
                    .action(clap::ArgAction::Set),
            )
            // Chunk size
            .arg(
                Arg::new("chunk_size")
                    .long("chunk_size")
                    .short('c')
                    .help("Chunk size in KB")
                    .default_value("4")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let _paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let _n = args
            .get_one::<String>("number")
            .expect("Must supply number")
            .parse::<usize>()
            .expect("number must be a valid integer.");

        let _chunk_size = args
            .get_one::<String>("chunk_size")
            .expect("Must supply chunk size")
            .parse::<usize>()
            .expect("Chunk size must be a valid integer.");

        if _paths.len() != 1 {
            return Err(OxenError::basic_str("Must supply exactly one file"));
        }
        Ok(())
    }
}
