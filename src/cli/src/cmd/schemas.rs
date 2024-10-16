use async_trait::async_trait;
use clap::{arg, Arg, Command};
use std::collections::HashMap;

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "schemas";

pub mod add;
pub use add::SchemasAddCmd;

pub mod list;
pub use list::SchemasListCmd;

pub mod rm;
pub use rm::SchemasRmCmd;

pub mod show;
pub use show::SchemasShowCmd;

pub struct SchemasCmd;

#[async_trait]
impl RunCmd for SchemasCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command = Command::new(NAME)
            .about("Manage schemas that are created from committing tabular data")
            .arg(arg!([SCHEMA_REF] "Name, hash, or path of the schema you want to view in more detail."))
            .arg(
                Arg::new("staged")
                    .long("staged")
                    .help("Show the staged schema")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("flatten")
                    .long("flatten")
                    .help("Print the schema in a flattened format")
                    .action(clap::ArgAction::SetTrue),
            )
        ;

        // These are all the subcommands for the schemas command
        // including `add`, `name`, `list`, and `rm`
        let sub_commands = self.get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let sub_commands = self.get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                eprintln!("Unknown schema subcommand {name}");
                return Err(OxenError::basic_str(format!(
                    "Unknown schema subcommand {name}"
                )));
            };

            // Calling await within an await is making it complain?
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
            })?;
        } else if let Some(schema_ref) = args.get_one::<String>("SCHEMA_REF") {
            let repository = LocalRepository::from_current_dir()?;
            let staged = args.get_flag("staged");
            let verbose = !args.get_flag("flatten"); // default to verbose
            let val =
                repositories::data_frames::schemas::show(&repository, schema_ref, staged, verbose)?;
            println!("{val}");
        } else {
            // Fall back to list schemas
            let Some(cmd) = sub_commands.get("list") else {
                eprintln!("Unknown schema subcommand list");
                return Err(OxenError::basic_str("Unknown schema subcommand list"));
            };
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(cmd.run(args))
            })?;
        }
        Ok(())
    }
}

impl SchemasCmd {
    fn get_subcommands(&self) -> HashMap<String, Box<dyn RunCmd>> {
        let commands: Vec<Box<dyn RunCmd>> = vec![
            Box::new(SchemasAddCmd),
            Box::new(SchemasListCmd),
            Box::new(SchemasRmCmd),
            Box::new(SchemasShowCmd),
        ];
        let mut runners: HashMap<String, Box<dyn RunCmd>> = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }
}
