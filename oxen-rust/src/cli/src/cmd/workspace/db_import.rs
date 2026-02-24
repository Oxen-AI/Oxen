use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::db_import::DbImportConfig;
use liboxen::model::{Commit, LocalRepository, NewCommitBody};
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "db-import";
pub struct WorkspaceDbImportCmd;

#[async_trait]
impl RunCmd for WorkspaceDbImportCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Import data from an external database into the repository")
            .arg(
                Arg::new("connection-url")
                    .long("connection-url")
                    .short("u")
                    .required(true)
                    .help("Database connection URL (e.g. postgres://user@host/db, sqlite:///path/to/db)"),
            )
            .arg(
                Arg::new("password")
                    .long("password")
                    .short("p")
                    .help("Database password (if not included in the connection URL)"),
            )
            .arg(
                Arg::new("query")
                    .long("query")
                    .short("q")
                    .required(true)
                    .help("SQL SELECT query to execute"),
            )
            .arg(
                Arg::new("output")
                    .long("output")
                    .short("o")
                    .required(true)
                    .help("Output path in the repository (e.g. data/users.csv)"),
            )
            .arg(
                Arg::new("format")
                    .long("format")
                    .short("f")
                    .help("Output format: csv, parquet, tsv, jsonl (default: inferred from output extension)"),
            )
            .arg(
                Arg::new("batch-size")
                    .long("batch-size")
                    .short("bs")
                    .value_parser(clap::value_parser!(usize))
                    .help("Number of rows per batch (default: 10000)"),
            )
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .help("Target branch to commit to (default: current branch)"),
            )
            .arg(
                Arg::new("message")
                    .long("message")
                    .short('m')
                    .help("Commit message. A commit message is always included: default is automatically generated."),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
      let (config, commit_body) = parse_args(args)?;

      println!(
          "Importing from {} database into {}",
          config.db_type,
          output_path.display()
      );

      let commit = repositories::workspaces::db_import::import_db(
          &repo,
          &branch_name,
          &config,
          &commit_body,
      )
      .await?;

      println!(
          "Successfully imported and committed as {} on branch {}",
          commit.id, branch_name
      );

      Ok(())
    }


    async fn parse_args(args: &clap::ArgMatches)  -> Result<(DbImportConfig, NewCommitBody), OxenError> {
        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        let connection_url = args
            .get_one::<String>("connection-url")
            .ok_or_else(|| OxenError::basic_str("--connection-url (-u) is required"))?;

        let password = args.get_one::<String>("password").cloned();

        let query = args
            .get_one::<String>("query")
            .ok_or_else(|| OxenError::basic_str("--query (-q) is required"))?;

        let output = args
            .get_one::<String>("output")
            .ok_or_else(|| OxenError::basic_str("--output (-o) is required"))?;
        let output_path = PathBuf::from(output);

        let format = if let Some(fmt) = args.get_one::<String>("format") {
            fmt.clone()
        } else {
            output_path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("csv")
                .to_string()
        };

        let batch_size = args.get_one::<usize>("batch-size").copied();

        let branch_name = match args.get_one::<String>("branch") {
            Some(name) => name.clone(),
            None => match repositories::branches::current_branch(&repo)? {
                Some(branch) => branch.name,
                None => {
                    return Err(OxenError::basic_str(
                        "No current branch. Use --branch to specify a target branch.",
                    ));
                }
            },
        };

        let commit_message = args
            .get_one::<String>("message")
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "Import from {} to {}",
                    config.db_type,
                    output_path.display()
                )
            });

        let config = DbImportConfig::new(
            connection_url,
            password,
            query,
            &output_path,
            &format,
            batch_size,
        )?;

        let cfg = UserConfig::get()?;

        let commit_body = NewCommitBody {
            message: commit_message,
            author: cfg.name,
            email: cfg.email,
        };

        Ok((config, commit_body))
    }
}
