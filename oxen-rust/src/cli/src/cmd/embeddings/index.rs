use async_trait::async_trait;
use clap::{Arg, Command, arg};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "index";

pub struct EmbeddingsIndexCmd;

#[async_trait]
impl RunCmd for EmbeddingsIndexCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Index the embeddings for a data frame.")
            .arg(arg!([PATH] "Path to the data frame you want to index or query."))
            .arg(
                Arg::new("column")
                    .long("column")
                    .short('c')
                    .help("The column that you want to index the embeddings for."),
            )
            .arg(
                Arg::new("use-background-thread")
                    .long("use-background-thread")
                    .short('b')
                    .help("Run the indexing asynchronously in the background."),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let path = args.get_one::<String>("PATH");
        let column = args.get_one::<String>("column");

        let err_msg = "Must supply a path to the data frame.";
        let Some(path) = path else {
            return Err(OxenError::basic_str(err_msg));
        };

        let err_msg = "Must supply a column name.";
        let Some(column) = column else {
            return Err(OxenError::basic_str(err_msg));
        };

        let use_background_thread = args.get_flag("use-background-thread");

        let repository = LocalRepository::from_current_dir()?;
        let commit = repositories::commits::head_commit(&repository)?;
        if !repositories::workspaces::data_frames::is_queryable_data_frame_indexed(
            &repository,
            path,
            &commit,
        )? {
            // If not, proceed to create a new workspace and index the data frame.
            // create the workspace id from the file path + commit id
            let workspace_id = format!("{}-{}", path, commit.id);
            let workspace =
                repositories::workspaces::create(&repository, &commit, workspace_id, false)?;
            repositories::workspaces::data_frames::index(&repository, &workspace, path)?;
            repositories::workspaces::data_frames::embeddings::index(
                &workspace,
                path,
                column,
                use_background_thread,
            )
        } else {
            Err(OxenError::basic_str("Data frame is already indexed."))
        }
    }
}
