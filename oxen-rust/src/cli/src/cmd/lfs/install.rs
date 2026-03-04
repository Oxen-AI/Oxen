use async_trait::async_trait;
use clap::{Arg, ArgAction, Command};

use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "install";
pub struct LfsInstallCmd;

#[async_trait]
impl RunCmd for LfsInstallCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Configure Git's global filter driver for Oxen LFS")
            .arg(
                Arg::new("uninstall")
                    .long("uninstall")
                    .help("Remove the global filter driver configuration")
                    .action(ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        if args.get_flag("uninstall") {
            lfs::install::uninstall_global_filter()?;
            println!("Oxen LFS global filter uninstalled.");
        } else {
            let oxen_bin = lfs::install::current_exe_path()?;
            let oxen_path = std::path::Path::new(&oxen_bin);
            lfs::install::install_global_filter(oxen_path)?;
            println!("Oxen LFS global filter installed (using {oxen_bin}).");
        }
        Ok(())
    }
}
