use std::path::Path;

use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::{error::OxenError, model::LocalRepository};

use crate::cmd::RunCmd;
use crate::helpers::migrations;

pub const NAME: &str = "migrate";

pub fn migrate_args(name: &'static str, desc: &'static str) -> Command {
    Command::new(name)
        .about(desc)
        .arg(
            Arg::new("PATH")
                .help("Directory in which to apply the migration")
                .required(true),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .short('a')
                .help("Run the migration for all oxen repositories in this directory")
                .action(clap::ArgAction::SetTrue),
        )
}

pub fn subcommands(name: &'static str, desc: &'static str) -> Command {
    let migrations = migrations();

    let mut cmd = Command::new(name).about(desc).subcommand_required(true);

    for (_, migration) in migrations {
        cmd = cmd.subcommand(migrate_args(migration.name(), migration.description()))
    }

    cmd
}

pub struct MigrateCmd;

#[async_trait]
impl RunCmd for MigrateCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Run a named migration on a server repository or set of repositories")
            .subcommand_required(true)
            .subcommand(subcommands("up", "Apply a named migration forward."))
            .subcommand(subcommands("down", "Apply a named migration backward."))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let migrations = migrations();

        if let Some((direction, sub_matches)) = args.subcommand() {
            if let Some((migration, sub_matches)) = sub_matches.subcommand() {
                let migration = migrations
                    .get(migration)
                    .ok_or(OxenError::basic_str(format!(
                        "Unknown migration: {migration}"
                    )))?;
                let path_str = sub_matches.get_one::<String>("PATH").expect("required");
                let path = Path::new(path_str);

                let all = sub_matches.get_flag("all");

                if direction == "up" {
                    let repo = LocalRepository::from_dir(path)?;
                    if migration.is_needed(&repo)? {
                        migration.up(path, all)?;
                    } else {
                        println!("Migration already applied: {}", migration.name());
                    }
                } else if direction == "down" {
                    migration.down(path, all)?;
                } else {
                    return Err(OxenError::basic_str(format!(
                        "Unknown direction: {direction}"
                    )));
                }
            }
        }

        Ok(())
    }
}
