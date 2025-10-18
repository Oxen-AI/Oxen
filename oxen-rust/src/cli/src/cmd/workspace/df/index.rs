use std::path::Path;
use std::time::Instant;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::DFOpts;

use crate::cmd::RunCmd;
pub const NAME: &str = "index";

pub struct WorkspaceDFIndexCmd;

#[async_trait]
impl RunCmd for WorkspaceDFIndexCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Index the data frame for querying.")
            .arg(Arg::new("path").help("Path to the data frame you want to index or query."))
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .help("The workspace id to use."),
            )
            .arg(
                Arg::new("column")
                    .help("The column to index for embeddings.")
                    .long("column")
                    .short('c'),
            )
            .arg(
                Arg::new("embeddings")
                    .help("This is a flag to indicate that the embeddings should be indexed.")
                    .long("embeddings")
                    .short('e')
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("use-background-thread")
                    .help("Use a background thread to index the embeddings.")
                    .long("use-background-thread")
                    .short('b')
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(workspace_id) = args.get_one::<String>("workspace-id") else {
            return Err(OxenError::basic_str("Must supply a workspace id."));
        };

        let Some(path) = args.get_one::<String>("path") else {
            return Err(OxenError::basic_str(
                "Must supply a path to the data frame you want to index.",
            ));
        };

        // Time the indexing

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;

        let df = api::client::workspaces::data_frames::get(
            &remote_repo,
            &workspace_id,
            &path,
            &DFOpts::empty(),
        )
        .await?;

        if !df.is_indexed {
            let start = Instant::now();
            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;
            println!("{:?} indexed in {:?}", path, start.elapsed());
        } else {
            log::debug!("Data frame is already indexed.");
        }

        if args.get_flag("embeddings") {
            let Some(column) = args.get_one::<String>("column") else {
                return Err(OxenError::basic_str(
                    "Must supply a column to index for embeddings.",
                ));
            };
            let use_background_thread = args.get_flag("use-background-thread");
            println!("Indexing embeddings for column: {column}");
            let path = Path::new(&path);
            api::client::workspaces::data_frames::embeddings::index(
                &remote_repo,
                workspace_id,
                path,
                column,
                use_background_thread,
            )
            .await?;
        }

        Ok(())
    }
}
