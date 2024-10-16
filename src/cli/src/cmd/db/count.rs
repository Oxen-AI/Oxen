use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::error::OxenError;

use crate::cmd::RunCmd;
pub const NAME: &str = "count";

pub struct DbCountCmd;

#[async_trait]
impl RunCmd for DbCountCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("List the full key value database.")
            .arg(Arg::new("PATH").help("The path of the database."))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(path) = args.get_one::<String>("PATH") else {
            return Err(OxenError::basic_str("Must supply path"));
        };

        let count = command::db::count(PathBuf::from(path))?;

        println!("There are {} entries in the database", count);

        Ok(())
    }
}
