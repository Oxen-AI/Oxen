use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;

pub const NAME: &str = "df";
pub struct WorkspaceDfCmd;

#[async_trait]
impl RunCmd for WorkspaceDfCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
        .about("Interact with remote data frames. Supported types: csv, tsv, ndjson, jsonl, parquet.")
        .arg(Arg::new("WORKSPACE_ID").help("The workspace id to use."))
        .subcommand(
            Command::new("index")
                .about("Index the data frame for querying.")
                .arg(Arg::new("PATH").help("The path of the data frame file.")),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(workspace_id) = args.get_one::<String>("WORKSPACE_ID") else {
            return Err(OxenError::basic_str("Must supply a workspace id."));
        };
        if let Some(subcommand) = args.subcommand() {
            match subcommand {
                ("index", sub_m) => {
                    let Some(path) = sub_m.get_one::<String>("PATH") else {
                        return Err(OxenError::basic_str("Must supply a DataFrame to process."));
                    };
                    let repository = LocalRepository::from_current_dir()?;
                    match repositories::workspaces::df::index(&repository, workspace_id, path).await
                    {
                        Ok(_) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }
                _ => return Err(OxenError::basic_str("Command not yet implemented.")),
            }
        }

        return Err(OxenError::basic_str("Command not yet implemented."));
    }
}
