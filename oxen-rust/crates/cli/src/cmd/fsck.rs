use async_trait::async_trait;
use clap::{ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;

pub const NAME: &str = "fsck";
pub struct FsckCmd;

pub fn fsck_args() -> Command {
    Command::new(NAME)
        .about("Check repository integrity and scan for corrupted version files")
        .arg(
            clap::Arg::new("clean")
                .long("clean")
                .help("Remove corrupted version files instead of just reporting them")
                .action(clap::ArgAction::SetTrue),
        )
}

#[async_trait]
impl RunCmd for FsckCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        fsck_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let clean = args.get_flag("clean");
        let dry_run = !clean;

        if dry_run {
            println!("Scanning version files for corruption...");
        } else {
            println!("Scanning and cleaning corrupted version files...");
        }

        let version_store = repository.version_store()?;
        let result = version_store.clean_corrupted_versions(dry_run).await?;

        println!("\nResults:");
        println!("  Scanned:   {}", result.scanned);
        println!("  Corrupted: {}", result.corrupted);
        if !dry_run {
            println!("  Cleaned:   {}", result.cleaned);
        }
        println!("  Errors:    {}", result.errors);
        println!("  Elapsed:   {:.2}s", result.elapsed.as_secs_f64());

        if dry_run && result.corrupted > 0 {
            println!("\nRun with --clean to remove corrupted version files.");
        }

        Ok(())
    }
}
