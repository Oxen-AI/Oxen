use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;

use crate::cmd::RunCmd;
pub const NAME: &str = "moo";
pub struct MooCmd;

#[async_trait]
impl RunCmd for MooCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Moo! 🐂")
            .arg(
                Arg::new("number")
                    .long("number")
                    .short('n')
                    .help("How long is the moo?")
                    .default_value("2")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("loud")
                    .long("loud")
                    .short('l')
                    .help("Make the MOO louder.")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let n = args
            .get_one::<String>("number")
            .expect("Must supply number")
            .parse::<usize>()
            .expect("number must be a valid integer.");

        let loud = args.get_flag("loud");
        if loud {
            // Print the moo loudly with -n number of o's
            println!("M{}!", "O".repeat(n));
        } else {
            // Print the moo with -n number of o's
            println!("m{}", "o".repeat(n));
        }

        Ok(())
    }
}
