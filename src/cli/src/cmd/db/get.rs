use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::{command, error::OxenError};

use crate::cmd::RunCmd;
pub const NAME: &str = "get";
pub struct DbGetCmd;

#[async_trait]
impl RunCmd for DbGetCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Get a value from the database given a key.")
            .arg(Arg::new("PATH").help("The path of the database."))
            .arg(Arg::new("KEY").help("The key to get the value for."))
            .arg(
                Arg::new("dtype")
                    .short('d')
                    .long("dtype")
                    .help("The data type of the key.")
                    .default_value("str"),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let error = "Usage: oxen db get <PATH> <KEY>";
        let Some(path) = args.get_one::<String>("PATH") else {
            return Err(OxenError::basic_str(error));
        };
        let Some(key) = args.get_one::<String>("KEY") else {
            return Err(OxenError::basic_str(error));
        };

        let dtype = args.get_one::<String>("dtype").map(|x| x.as_str());
        let value = command::db::get(path, key, dtype)?;
        println!("{}", value);

        Ok(())
    }
}
