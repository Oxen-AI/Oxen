use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::command::migrate::Migrate;
use liboxen::{command::migrate::Direction, error::OxenError, model::LocalRepository};

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
        .arg(
            Arg::new("run-optional")
                .long("run-optional")
                .help(
                    "Run an optional migration that wouldn't run by default. The \
                     migration is invoked iff it is applicable to the repo in this \
                     direction; otherwise the command prints a notice and exits \
                     successfully without changes.",
                )
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

        if let Some((direction, sub_matches)) = args.subcommand()
            && let Some((migration, sub_matches)) = sub_matches.subcommand()
        {
            let Some(migration) = migrations.get(migration) else {
                return Err(OxenError::UnknownMigration(migration.to_string()));
            };

            let direction = Direction::from_str(direction)?;

            let path_str = sub_matches.get_one::<String>("PATH").expect("required");
            let path = Path::new(path_str);

            let all = sub_matches.get_flag("all");
            let run_optional = sub_matches.get_flag("run-optional");

            try_apply_migration(
                &**migration,
                MigrationOptions {
                    path,
                    all,
                    direction,
                    run_optional,
                },
            )?;
        }

        Ok(())
    }
}

struct MigrationOptions<'a> {
    path: &'a Path,
    all: bool,
    direction: Direction,
    run_optional: bool,
}

/// Attempt to apply the specified migration to the repository in the options.
///
/// Returns:
///     - Ok(true): Migration applied successfully.
///     - Ok(false): Not applied and no error was encountered.
///     - Err(...): Not applied and an error was encountered that prevented it from being applied.
fn try_apply_migration<'a>(
    migration: &dyn Migrate,
    opts: MigrationOptions<'a>,
) -> Result<bool, OxenError> {
    let MigrationOptions {
        path,
        all,
        direction,
        run_optional,
    } = opts;
    match direction {
        Direction::Up => {
            let repo = LocalRepository::from_dir(path)?;
            if migration.is_needed(&repo)? {
                migration.up(path, all)?;
                Ok(true)
            } else if migration.is_applicable(Direction::Up, &repo)? {
                if run_optional {
                    migration.up(path, all)?;
                    Ok(true)
                } else {
                    println!(
                        "Nothing to do: '{}' (up) is applicable, but not needed. You must run with --run-optional to apply it.",
                        migration.name()
                    );
                    Ok(false)
                }
            } else {
                println!(
                    "Nothing to do: '{}' (up) is not needed nor is it applicable.",
                    migration.name()
                );
                Ok(false)
            }
        }
        Direction::Down => {
            // down migrations are run unconditionally
            migration.down(path, all)?;
            Ok(true)
        }
    }
}
