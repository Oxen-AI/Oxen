use async_trait::async_trait;
use clap::{arg, Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "rm";

pub struct SchemasRmCmd;

#[async_trait]
impl RunCmd for SchemasRmCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Remove a schema from the list of committed or added schemas.")
            .arg(arg!(<NAME_OR_HASH> ... "Name, hash, or path of the schema you want to remove."))
            .arg(
                Arg::new("staged")
                    .long("staged")
                    .help("Removed a staged schema")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let repository = LocalRepository::from_current_dir()?;

        let Some(schema_ref) = args.get_one::<String>("NAME_OR_HASH") else {
            return Err(OxenError::basic_str(
                "Must supply a name, hash, or path of the schema you want to remove.",
            ));
        };

        let staged = args.get_flag("staged");
        repositories::data_frames::schemas::rm(&repository, schema_ref, staged)?;

        Ok(())
    }
}
