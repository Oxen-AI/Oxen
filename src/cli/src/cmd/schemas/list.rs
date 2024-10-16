use std::collections::HashMap;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::data_frame::schema::Schema;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "list";

pub struct SchemasListCmd;

#[async_trait]
impl RunCmd for SchemasListCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME).about("List the committed schemas.").arg(
            Arg::new("staged")
                .long("staged")
                .help("List the staged schemas")
                .action(clap::ArgAction::SetTrue),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let staged = args.get_flag("staged");

        let repository = LocalRepository::from_current_dir()?;
        let schemas = if staged {
            repositories::data_frames::schemas::list_staged(&repository)?
        } else {
            let mut schemas = HashMap::new();
            if let Some(commit) = repositories::commits::head_commit_maybe(&repository)? {
                schemas = repositories::data_frames::schemas::list(&repository, &commit)?
            }
            schemas
        };

        if schemas.is_empty() && staged {
            eprintln!("{}", OxenError::no_schemas_staged());
        } else if schemas.is_empty() {
            eprintln!("{}", OxenError::no_schemas_committed());
        } else {
            let result = Schema::schemas_to_string(schemas);
            println!("{result}");
        }

        Ok(())
    }
}
