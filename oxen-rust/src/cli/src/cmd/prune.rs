use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;

pub const NAME: &str = "prune";
pub struct PruneCmd;

pub fn prune_args() -> Command {
    Command::new(NAME)
        .about("Remove orphaned nodes and version files not referenced by any commit")
        .arg(
            Arg::new("dry-run")
                .long("dry-run")
                .short('n')
                .help("Show what would be removed without actually removing it")
                .action(clap::ArgAction::SetTrue),
        )
}

#[async_trait]
impl RunCmd for PruneCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        prune_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let dry_run = args.get_flag("dry-run");

        if dry_run {
            println!("Running prune in dry-run mode (no files will be deleted)...");
        } else {
            println!("Running prune...");
        }

        let stats = repositories::prune::prune(&repository, dry_run).await?;

        println!("\nPrune Statistics:");
        println!("  Nodes:");
        println!("    Scanned:  {}", stats.nodes_scanned);
        println!("    Kept:     {}", stats.nodes_kept);
        println!("    Removed:  {}", stats.nodes_removed);
        println!("  Version Files:");
        println!("    Scanned:  {}", stats.versions_scanned);
        println!("    Kept:     {}", stats.versions_kept);
        println!("    Removed:  {}", stats.versions_removed);
        println!(
            "  Disk Space Freed: {}",
            bytesize::ByteSize::b(stats.bytes_freed)
        );

        if dry_run {
            println!("\nThis was a dry run. No files were actually deleted.");
            println!("Run without --dry-run to perform the actual prune.");
        }

        Ok(())
    }
}
