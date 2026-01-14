use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;
use liboxen::api;
use liboxen::constants::DEFAULT_REMOTE_NAME;
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
        .arg(
            Arg::new("remote")
                .long("remote")
                .short('r')
                .help("Prune the remote repository instead of the local one")
                .value_name("REMOTE")
                .num_args(0..=1)
                .default_missing_value(DEFAULT_REMOTE_NAME),
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
        let remote_name = args.get_one::<String>("remote");

        let stats = if let Some(remote_name) = remote_name {
            // Remote prune
            let remote = repository
                .get_remote(remote_name)
                .ok_or_else(|| OxenError::remote_not_set(remote_name))?;
            let remote_repo = api::client::repositories::get_by_remote(&remote)
                .await?
                .ok_or_else(|| OxenError::remote_not_found(remote.clone()))?;

            if dry_run {
                println!(
                    "Running remote prune in dry-run mode on {} (no files will be deleted)...",
                    remote_repo.url()
                );
            } else {
                println!("Running remote prune on {}...", remote_repo.url());
            }

            repositories::prune::prune_remote(&remote_repo, dry_run).await?
        } else {
            // Local prune
            if dry_run {
                println!("Running prune in dry-run mode (no files will be deleted)...");
            } else {
                println!("Running prune...");
            }

            repositories::prune::prune(&repository, dry_run).await?
        };

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
