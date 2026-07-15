use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::storage::ContentFormat;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "storage";
pub struct StorageCmd;

pub fn storage_args() -> Command {
    Command::new(NAME)
        .about("Inspect and migrate the repository's content storage format")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("status")
                .about("Show the content storage format and version representations"),
        )
        .subcommand(
            Command::new("migrate")
                .about("Migrate the repository between the legacy and block-v1 content formats")
                .arg(
                    Arg::new("to")
                        .long("to")
                        .value_name("FORMAT")
                        .value_parser(["block-v1", "legacy"])
                        .required(true)
                        .help("Target content format"),
                ),
        )
        .subcommand(Command::new("rebuild-index").about(
            "Rebuild the local chunk index by scanning stored blocks \
             (recovers a lost or corrupted index; blocks and manifests are untouched)",
        ))
}

#[async_trait]
impl RunCmd for StorageCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        storage_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), anyhow::Error> {
        let mut repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        match args.subcommand() {
            Some(("status", _)) => run_status(&repository).await?,
            Some(("migrate", sub_args)) => run_migrate(&mut repository, sub_args).await?,
            Some(("rebuild-index", _)) => run_rebuild_index(&repository).await?,
            _ => unreachable!("clap enforces subcommand_required(true)"),
        }
        Ok(())
    }
}

async fn run_rebuild_index(repository: &LocalRepository) -> Result<(), OxenError> {
    println!("Rebuilding the chunk index from stored blocks...");
    let chunks = repositories::storage::rebuild_chunk_index(repository).await?;
    println!("Rebuilt chunk index: {chunks} chunks indexed");
    Ok(())
}

async fn run_status(repository: &LocalRepository) -> Result<(), OxenError> {
    let status = repositories::storage::status(repository).await?;
    let format = match status.content_format {
        ContentFormat::Legacy => "legacy",
        ContentFormat::BlockV1 => "block-v1",
    };
    println!("Content format:   {format}");
    println!("Stored versions:  {}", status.total_versions);
    println!("Chunked versions: {}", status.chunked_versions);
    Ok(())
}

async fn run_migrate(repository: &mut LocalRepository, args: &ArgMatches) -> Result<(), OxenError> {
    let target = args
        .get_one::<String>("to")
        .map(String::as_str)
        .unwrap_or_default();

    let stats = match target {
        "block-v1" => {
            println!("Migrating repository to the block-v1 content format...");
            repositories::storage::migrate_to_block_v1(repository).await?
        }
        "legacy" => {
            println!("Migrating repository back to the legacy content format...");
            println!("(blocks stay on disk until garbage collection)");
            repositories::storage::migrate_to_legacy(repository).await?
        }
        other => {
            return Err(OxenError::basic_str(format!(
                "unknown content format: {other}"
            )));
        }
    };

    println!("\nResults:");
    println!("  Migrated:         {}", stats.migrated);
    println!("  Already migrated: {}", stats.already_migrated);
    println!("  Kept whole-file:  {}", stats.skipped_small);
    if stats.skipped_orphaned > 0 {
        println!("  Skipped non-version dirs: {}", stats.skipped_orphaned);
    }
    println!(
        "  Bytes migrated:   {}",
        bytesize::ByteSize(stats.bytes_migrated)
    );
    Ok(())
}
