use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::constants::{DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::DFOpts;

use crate::cmd::RunCmd;
pub const NAME: &str = "get";

pub struct WorkspaceDFGetCmd;

#[async_trait]
impl RunCmd for WorkspaceDFGetCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Fetch the remote data frame.")
            .arg(
                Arg::new("path")
                    .help("Path to the data frame you want to get.")
                    .required(true),
            )
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .help("The workspace id to use."),
            )
            .arg(
                Arg::new("page")
                    .long("page")
                    .help("The page number to get."),
            )
            .arg(
                Arg::new("page_size")
                    .long("page_size")
                    .help("The page size to get."),
            )
            .arg(
                Arg::new("output")
                    .long("output")
                    .short('o')
                    .help("The path to save the data frame to."),
            )
            .arg(
                Arg::new("sql")
                    .long("sql")
                    .short('s')
                    .help("The SQL query to run on the data frame."),
            )
            .arg(
                Arg::new("find-embedding-where")
                    .long("find-embedding-where")
                    .help("Find the embeddings that match the where clause.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("sort-by-similarity-to")
                    .long("sort-by-similarity-to")
                    .help("The column of embeddings to sort by.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("output-column")
                    .long("output-column")
                    .help(
                        "The column to output the similarity scores to. Defaults to 'similarity'.",
                    )
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(path) = args.get_one::<String>("path") else {
            return Err(OxenError::basic_str(
                "Must supply a path to the data frame you want to get.",
            ));
        };
        let Some(workspace_id) = args.get_one::<String>("workspace-id") else {
            return Err(OxenError::basic_str("Must supply a workspace id."));
        };

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        let opts = self.parse_args(args)?;

        if let Some(output) = &opts.output {
            println!("Downloading data frame to {:?}", output);
            api::client::workspaces::data_frames::download(
                &remote_repo,
                &workspace_id,
                &path,
                &opts,
            )
            .await?;
            let df = tabular::read_df(output, opts.clone()).await?;
            println!("{:?}", df);
            return Ok(());
        }

        let start = std::time::Instant::now();
        match api::client::workspaces::data_frames::get(&remote_repo, &workspace_id, &path, &opts)
            .await
        {
            Ok(response) => {
                if let Some(data_frame) = response.data_frame {
                    let df = data_frame.view.to_df().await;
                    let df = tabular::strip_excluded_cols(df)?;
                    println!("{:?}", df);
                    println!("Query took: {:?}", start.elapsed());
                } else {
                    return Err(OxenError::basic_str(
                        format!("No data frame found. Index the data frame before querying.\n\n  oxen workspace df index {path} -w {workspace_id}\n")));
                }
            }
            Err(e) => {
                return Err(OxenError::basic_str(format!("{:?}", e)));
            }
        }

        Ok(())
    }
}

impl WorkspaceDFGetCmd {
    fn parse_args(&self, args: &clap::ArgMatches) -> Result<DFOpts, OxenError> {
        let mut opts = DFOpts::empty();
        if let Some(sql) = args.get_one::<String>("sql") {
            opts.sql = Some(sql.to_string());
        }
        if let Some(find_embedding_where) = args.get_one::<String>("find-embedding-where") {
            opts.find_embedding_where = Some(find_embedding_where.to_string());
        }
        if let Some(sort_by_similarity_to) = args.get_one::<String>("sort-by-similarity-to") {
            opts.sort_by_similarity_to = Some(sort_by_similarity_to.to_string());
        }
        if let Some(output_column) = args.get_one::<String>("output-column") {
            opts.output_column = Some(output_column.to_string());
        }
        if let Some(output) = args.get_one::<String>("output") {
            opts.output = Some(PathBuf::from(output));
        }
        if let Some(page) = args.get_one::<usize>("page") {
            opts.page = Some(*page);
        } else {
            opts.page = Some(DEFAULT_PAGE_NUM);
        }
        if let Some(page_size) = args.get_one::<usize>("page_size") {
            opts.page_size = Some(*page_size);
        } else {
            opts.page_size = Some(DEFAULT_PAGE_SIZE);
        }
        Ok(opts)
    }
}
