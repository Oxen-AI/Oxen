use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
use std::path::Path;

use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "load";
pub struct LoadCmd;

#[async_trait]
impl RunCmd for LoadCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Load a repository backup from a .tar.gz archive")
            .arg(Arg::new("SRC_PATH")
                .help("Path to the .tar.gz archive to load")
                .required(true)
                .index(1))
            .arg(Arg::new("DEST_PATH")
                    .help("Path in which to unpack the repository")
                    .required(true)
                    .index(2))
            .arg(
                Arg::new("no-working-dir")
                .long("no-working-dir")
                .help("Don't unpack version files to local working directory (space-saving measure for server repos)")
                .action(clap::ArgAction::SetTrue)
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Match on both SRC_PATH and DEST_PATH
        let src_path_str = args.get_one::<String>("SRC_PATH").expect("required");
        let dest_path_str = args.get_one::<String>("DEST_PATH").expect("required");
        let no_working_dir = args.get_flag("no-working-dir");

        let src_path = Path::new(src_path_str);
        let dest_path = Path::new(dest_path_str);

        // Call into liboxen
        repositories::load(src_path, dest_path, no_working_dir).await?;
        Ok(())
    }
}
