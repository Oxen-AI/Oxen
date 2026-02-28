use async_trait::async_trait;
use clap::Command;

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "status";
pub struct LfsStatusCmd;

#[async_trait]
impl RunCmd for LfsStatusCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Show status of Oxen LFS tracked files")
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let oxen_dir = repo_root.join(OXEN_HIDDEN_DIR);
        let versions_dir = oxen_dir.join("versions");

        if !versions_dir.exists() {
            println!("Oxen LFS not initialized. Run `oxen lfs init` first.");
            return Ok(());
        }

        let statuses = lfs::status::get_status(&repo_root, &versions_dir).await?;

        if statuses.is_empty() {
            println!("No LFS tracked files found.");
            return Ok(());
        }

        for s in &statuses {
            let local_indicator = if s.local { "local" } else { "missing" };
            println!(
                "{} ({}, {} bytes, {})",
                s.path.display(),
                s.pointer.oid,
                s.pointer.size,
                local_indicator,
            );
        }

        Ok(())
    }
}
