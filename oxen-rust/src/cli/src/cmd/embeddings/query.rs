use std::path::PathBuf;

use async_trait::async_trait;
use clap::{arg, Arg, Command};
use liboxen::constants::{DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::{EmbeddingQueryOpts, PaginateOpts};
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "query";

pub struct EmbeddingsQueryCmd;

#[async_trait]
impl RunCmd for EmbeddingsQueryCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Sort a data frame by the cosine similarity to a query vector.")
            .arg(arg!([PATH] "Path to the data frame you want to sort."))
            .arg(
                Arg::new("column")
                    .long("column")
                    .short('c')
                    .help("The column that you want to query the embeddings for."),
            )
            .arg(
                Arg::new("query")
                    .long("query")
                    .short('q')
                    .help("Formatted as key=value where we find rows that match this criteria, and grab the embedding vectors for those rows."),
            )
            .arg(
                Arg::new("name")
                    .long("name")
                    .short('n')
                    .help("The name of the new column to add to the data frame for the similarity scores. Defaults to 'similarity'."),
            )
            .arg(
                Arg::new("output")
                    .long("output")
                    .short('o')
                    .help("File path to save the output data frame to."),
            )
            .arg(
                Arg::new("page_size")
                    .long("page-size")
                    .help("The number of rows to return per page."),
            )
            .arg(
                Arg::new("page_number")
                    .long("page-number")
                    .help("The page number to return."),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let path = args.get_one::<String>("PATH");
        let column = args.get_one::<String>("column");

        let Some(path) = path else {
            return Err(OxenError::basic_str(
                "Must supply a path to the data frame.",
            ));
        };

        let Some(column) = column else {
            return Err(OxenError::basic_str("Must supply a column name."));
        };

        let Some(query) = args.get_one::<String>("query") else {
            return Err(OxenError::basic_str("Must supply a query."));
        };

        let page_size = args
            .get_one::<usize>("page_size")
            .unwrap_or(&DEFAULT_PAGE_SIZE);
        let page_number = args
            .get_one::<usize>("page_number")
            .unwrap_or(&DEFAULT_PAGE_NUM);

        let default_name = String::from("similarity");
        let name = args.get_one::<String>("name").unwrap_or(&default_name);
        let opts = EmbeddingQueryOpts {
            path: PathBuf::from(path),
            column: column.to_string(),
            query: query.to_string(),
            name: name.to_string(),
            pagination: PaginateOpts {
                page_size: *page_size,
                page_num: *page_number,
            },
        };

        if opts.parse_query().is_err() {
            return Err(OxenError::basic_str(
                "Query must be in the format key=value",
            ));
        }

        let repository = LocalRepository::from_current_dir()?;
        let commit = repositories::commits::head_commit(&repository)?;
        let workspace_id = format!("{}-{}", path, commit.id);
        let Some(workspace) = repositories::workspaces::get(&repository, &workspace_id)? else {
            return Err(OxenError::basic_str(format!(
                "Workspace not found: {workspace_id}"
            )));
        };

        let start = std::time::Instant::now();
        let mut df =
            liboxen::repositories::workspaces::data_frames::embeddings::query(&workspace, &opts)?;
        println!("{df}");
        println!("Query took: {:?}", start.elapsed());

        let Some(output) = args.get_one::<String>("output") else {
            return Ok(());
        };

        println!("Writing to {output}");
        tabular::write_df(&mut df, output)?;

        Ok(())
    }
}
