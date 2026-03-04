use async_trait::async_trait;
use clap::Command;

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "env";
pub struct LfsEnvCmd;

#[async_trait]
impl RunCmd for LfsEnvCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Show Oxen LFS environment and diagnostic info")
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        println!("oxen lfs environment");
        println!("  oxen version: {}", liboxen::constants::OXEN_VERSION);

        let repo_root = std::env::current_dir()?;
        let oxen_dir = repo_root.join(OXEN_HIDDEN_DIR);

        if oxen_dir.exists() {
            let config = lfs::config::LfsConfig::load(&oxen_dir)?;
            println!(
                "  remote: {}",
                config.remote_url.as_deref().unwrap_or("(not set)")
            );
            println!("  versions dir: {}", oxen_dir.join("versions").display());

            let patterns = lfs::gitattributes::list_tracked_patterns(&repo_root)?;
            if patterns.is_empty() {
                println!("  tracked patterns: (none)");
            } else {
                println!("  tracked patterns:");
                for p in &patterns {
                    println!("    {p}");
                }
            }
        } else {
            println!("  Oxen LFS not initialized in this repository.");
        }

        Ok(())
    }
}
