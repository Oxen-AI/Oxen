use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::command::migrate::{
    ALL_MIGRATIONS, MigrationResults, all_migrations, run_on_all_repos, try_apply_migration,
};
use liboxen::{command::migrate::Direction, error::OxenError, model::LocalRepository};

use crate::cmd::RunCmd;

pub const NAME: &str = "migrate";

pub fn migrate_args(name: &'static str, desc: &'static str) -> Command {
    Command::new(name)
        .about(desc)
        .arg(
            Arg::new("PATH")
                .help(
                    "Directory in which to apply the migration. Must be the root \
                    of the repository, unless --all is specified.",
                )
                .required(true),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .short('a')
                .help(
                    "Run the migration for all oxen repositories in the directory. \
                    Expects the directory to contain \"namespace/repository\" format. \
                    The direct directories inside must be namespaces. Each directory \
                    inside the namespace must be a repository. (Skips directories \
                    that don't have a .oxen/ directory in them).",
                )
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
    let mut cmd = Command::new(name).about(desc).subcommand_required(true);
    for migration in ALL_MIGRATIONS {
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
        if let Some((direction, sub_matches)) = args.subcommand()
            && let Some((migration, sub_matches)) = sub_matches.subcommand()
        {
            let Some(migration) = all_migrations(migration) else {
                return Err(OxenError::UnknownMigration(migration.to_string()));
            };

            let direction = Direction::from_str(direction)?;

            let path_str = sub_matches.get_one::<String>("PATH").expect("required");
            let path = Path::new(path_str);

            let all = sub_matches.get_flag("all");
            let run_optional = sub_matches.get_flag("run-optional");

            if all {
                let MigrationResults { executed, errored } =
                    run_on_all_repos(false, migration, direction, run_optional, path)?;

                if errored.is_empty() {
                    println!(
                        "\u{2705} Migration completed: successfully migrated {} repositories",
                        executed.len(),
                    );
                } else {
                    println!(
                        "\u{274C} [ERROR] Migration completed with {} success(es) and {} failure(s):",
                        executed.len(),
                        errored.len(),
                    );
                    for (repo_path, error) in errored {
                        println!("\t[FAIL] \"{}\" due to: {error}", repo_path.display());
                    }
                    return Err(OxenError::MigrationFailed);
                }
            } else {
                let mr = try_apply_migration(
                    migration,
                    direction,
                    run_optional,
                    LocalRepository::from_dir(path)?,
                )?;
                println!("{}", mr.as_hint(false));
            }
        }

        Ok(())
    }
}
