use async_trait::async_trait;
use clap::{arg, Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "show";

pub struct SchemasShowCmd;

#[async_trait]
impl RunCmd for SchemasShowCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Show a schema in detail by the file path.")
            .arg(arg!(<PATH> ... "Path of the schema you want to show."))
            .arg(
                Arg::new("staged")
                    .long("staged")
                    .help("Show the staged schema")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("verbose")
                    .long("verbose")
                    .short('v')
                    .help("Show the schema in verbose format")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let repository = LocalRepository::from_current_dir()?;

        let Some(path) = args.get_one::<String>("PATH") else {
            return Err(OxenError::basic_str(
                "Must supply a path of the schema you want to show.",
            ));
        };
        let verbose = args.get_flag("verbose");
        let staged = args.get_flag("staged");

        let result = repositories::data_frames::schemas::show(&repository, path, staged, verbose)?;
        println!("{result}");

        Ok(())
    }
}
