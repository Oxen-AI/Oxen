use async_trait::async_trait;
use clap::{ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::repositories::fsck::rebuild_dir_hash_db;

use crate::cmd::RunCmd;

pub const NAME: &str = "fsck";
pub struct FsckCmd;

pub fn fsck_args() -> Command {
    Command::new(NAME)
        .about("Check and repair repository integrity")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("clean")
                .about("Scan version files and remove corrupted ones")
                .arg(
                    clap::Arg::new("dry-run")
                        .long("dry-run")
                        .short('n')
                        .help("Report corrupted version files without removing them")
                        .action(clap::ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("rebuild-dir-hashes")
                .about("Rebuild a commit's dir_hash_db from its merkle tree")
                .arg(
                    clap::Arg::new("commit")
                        .long("commit")
                        .short('c')
                        .value_name("COMMIT_ID")
                        .help("Commit ID to rebuild (defaults to HEAD)")
                        .conflicts_with("branch"),
                )
                .arg(
                    clap::Arg::new("branch")
                        .long("branch")
                        .short('b')
                        .value_name("NAME")
                        .help("Branch whose tip commit to rebuild (defaults to HEAD)"),
                ),
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

        match args.subcommand() {
            Some(("clean", sub_args)) => run_clean(&repository, sub_args).await,
            Some(("rebuild-dir-hashes", sub_args)) => run_rebuild_dir_hashes(&repository, sub_args),
            _ => unreachable!("clap enforces subcommand_required(true)"),
        }
    }
}

async fn run_clean(repository: &LocalRepository, args: &ArgMatches) -> Result<(), OxenError> {
    let dry_run = args.get_flag("dry-run");

    if dry_run {
        println!("Scanning version files for corruption (dry run)...");
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
        println!("\nRun without --dry-run to remove corrupted version files.");
    }

    Ok(())
}

fn run_rebuild_dir_hashes(
    repository: &LocalRepository,
    args: &ArgMatches,
) -> Result<(), OxenError> {
    let commit = if let Some(commit_id) = args.get_one::<String>("commit") {
        repositories::commits::get_by_id(repository, commit_id)?
            .ok_or_else(|| OxenError::RevisionNotFound(commit_id.clone().into()))?
    } else if let Some(branch_name) = args.get_one::<String>("branch") {
        let branch = repositories::branches::get_by_name(repository, branch_name)?;
        repositories::commits::get_by_id(repository, &branch.commit_id)?
            .ok_or_else(|| OxenError::RevisionNotFound(branch.commit_id.clone().into()))?
    } else {
        repositories::commits::head_commit(repository)?
    };

    println!("Rebuilding dir_hash_db for commit {}...", commit.id);
    let stats = rebuild_dir_hash_db(repository, &commit)?;

    println!("\nResults:");
    println!("  Commit:       {}", stats.commit_id);
    println!("  Dirs written: {}", stats.dirs_written);

    Ok(())
}
