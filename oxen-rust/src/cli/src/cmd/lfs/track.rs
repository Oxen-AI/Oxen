use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "track";
pub struct LfsTrackCmd;

#[async_trait]
impl RunCmd for LfsTrackCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Track a file pattern with Oxen LFS")
            .arg(
                Arg::new("pattern")
                    .help("File glob pattern to track (e.g. \"*.bin\", \"datasets/**\")")
                    .required(false),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;

        match args.get_one::<String>("pattern") {
            Some(pattern) => {
                lfs::gitattributes::track_pattern(&repo_root, pattern)?;
                println!("Tracking \"{pattern}\"");
            }
            None => {
                // No pattern: list currently tracked patterns.
                let patterns = lfs::gitattributes::list_tracked_patterns(&repo_root)?;
                if patterns.is_empty() {
                    println!("No patterns tracked by Oxen LFS.");
                } else {
                    println!("Patterns tracked by Oxen LFS:");
                    for p in &patterns {
                        println!("    {p}");
                    }
                }
            }
        }
        Ok(())
    }
}
