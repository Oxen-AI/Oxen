use std::path::PathBuf;

use async_trait::async_trait;
use clap::{arg, Arg, ArgMatches, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util::fs;

use crate::cmd::RunCmd;
pub const NAME: &str = "df";
pub struct DFCmd;

#[async_trait]
impl RunCmd for DFCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
        .about("View and transform data frames. Supported types: csv, tsv, ndjson, jsonl, parquet.")
        .arg(arg!(<PATH> ... "The DataFrame you want to process. If in the schema subcommand the schema ref."))
        .arg_required_else_help(true)
        .arg(
            Arg::new("write")
                .long("write")
                .short('w')
                .help("Write transformed data back to the file")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .help("Output file to store the transformed data")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("full")
                .long("full")
                .short('l')
                .help("Display non-truncated data frame")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("delimiter")
                .long("delimiter")
                .short('d')
                .help("The delimiter to use when reading the file. Default is ','")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("filter")
                .long("filter")
                .short('f')
                .help("A filter to apply to the data frame. Format: 'column op value' ie: 'category == dog'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("columns")
                .long("columns")
                .short('c')
                .help("A comma separated set of columns names to look at. Ex file,x,y")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("output-column")
                .long("output-column")
                .help("The column to output the results to.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("item")
                .long("item")
                .help("Select a specific row item from column to view it fully. Format: 'column:idx' ie: 'my_col_name:3'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("row")
                .long("row")
                .help("Select a specific row to view it fully. Format: '3'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("vstack")
                .long("vstack")
                .help("Combine row data from different files. The number of columns must match.")
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("slice")
                .long("slice")
                .help("A continuous slice of the data you want to look at. Format: 'start..end' Ex) '10..25' will take 15 elements, starting at 10 and ending at 25.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("page")
                .long("page")
                .help("Page number when paginating through the data frame. Default page = 1")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("page-size")
                .long("page-size")
                .help("Paginated through the data frame. Default page-size = 10")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("head")
                .long("head")
                .help("Grab the first N entries of the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("tail")
                .long("tail")
                .help("Grab the last N entries of the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("take")
                .long("take")
                .short('t')
                .help("A comma separated set of row indices to look at. Ex 1,22,313")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("sort")
                .long("sort")
                .short('s')
                .help("Sort the output by a column name. Is run at the end of all the other transforms.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("find-embedding-where")
                .long("find-embedding-where")
                .help("Find the embedding where clause.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("sort-by-similarity-to")
                .long("sort-by-similarity-to")
                .help("Sort the output by similarity to a column.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("sql")
                .long("sql")
                .help("Run a sql query on the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("text2sql")
                .long("text2sql")
                .help("Run a text query that translates to sql on the data frame.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("What remote host to run the query against. Ie: hub.oxen.ai")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("revision")
                .long("revision")
                .help("What version of the data frame to use. Ex: oxen df <path> --revision <commit_id>")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("randomize")
                .long("randomize")
                .help("Randomize the order of the table")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("reverse")
                .long("reverse")
                .help("Reverse the order of the table")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("unique")
                .long("unique")
                .short('u')
                .help("Unique the output by a set of column names. Takes a comma separated set of column names ie: \"text,label\".")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("schema")
                .long("schema")
                .help("Print the full list of columns and data types within the schema in a dataframe.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("schema-flat")
                .long("schema-flat")
                .help("Print the full list of columns and data types within the schema.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("add-col")
                .long("add-col")
                .help("Add a column with a default value to the data table. If used with --add-row, row is added first, then column. Format 'name:val:dtype'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("at")
                .long("at")
                .help("Where to add the new column at. Should be an index. Ie: oxen df add-col 'name:val:dtype' --at 1")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("add-row")
                .long("add-row")
                .help("Add a row and cast to the values data types to match the current schema. If used with --add-col, row is added first, then column. Format 'comma,separated,vals'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("rename-col")
                .long("rename-col")
                .help("Rename a column in the data frame. Format: 'old_name:new_name'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("delete-row")
                .long("delete-row")
                .help("Delete a row from a data frame. Currently only works with remote data frames with the value from _id column.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("quote")
                .long("quote")
                .help("The quote character to use when reading the file. Default is '\"'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("bearer_token")
                .long("bearer-token")
                .help("Bearer token for authentication. If not provided, the config file will be used.")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let mut opts = DFCmd::parse_df_args(args);
        let Some(path) = args.get_one::<String>("PATH") else {
            return Err(OxenError::basic_str("Must supply a DataFrame to process."));
        };
        opts.path = Some(PathBuf::from(path));

        if let Some(revision) = args.get_one::<String>("revision") {
            let repo = LocalRepository::from_current_dir()?;
            command::df::df_revision(&repo, path, revision, opts)?;
        } else if args.get_flag("schema") || args.get_flag("schema-flat") {
            let flatten = args.get_flag("schema-flat");
            let result = command::df::schema(path, flatten, opts)?;
            println!("{result}");
        } else {
            command::df(path, opts)?;
        }

        Ok(())
    }
}

impl DFCmd {
    pub fn parse_df_args(args: &ArgMatches) -> liboxen::opts::DFOpts {
        let vstack: Option<Vec<PathBuf>> = if let Some(vstack) = args.get_many::<String>("vstack") {
            let values: Vec<PathBuf> = vstack.map(std::path::PathBuf::from).collect();
            Some(values)
        } else {
            None
        };

        let write_path: Option<PathBuf> = if args.get_flag("write") {
            args.get_one::<String>("DF_SPEC")
                .map(std::path::PathBuf::from)
        } else {
            None
        };

        let repo_dir: Option<PathBuf> = if args.get_one::<String>("sql").is_some()
            || args.get_one::<String>("text2sql").is_some()
        {
            fs::get_repo_root_from_current_dir()
        } else {
            None
        };

        let page_specified: bool = args.get_one::<String>("page").is_some()
            | args.get_one::<String>("page-size").is_some();

        liboxen::opts::DFOpts {
            add_col: args.get_one::<String>("add-col").map(String::from),
            add_row: args.get_one::<String>("add-row").map(String::from),
            rename_col: args.get_one::<String>("rename-col").map(String::from),
            at: args
                .get_one::<String>("at")
                .map(|x| x.parse::<usize>().expect("at must be valid int")),
            bearer_token: args.get_one::<String>("bearer_token").map(String::from),
            columns: args.get_one::<String>("columns").map(String::from),
            delete_row: args.get_one::<String>("delete-row").map(String::from),
            delimiter: args.get_one::<String>("delimiter").map(String::from),
            embedding: None, // Not really feasible to provide an embedding from the CLI
            filter: args.get_one::<String>("filter").map(String::from),
            find_embedding_where: args
                .get_one::<String>("find-embedding-where")
                .map(String::from),
            head: args
                .get_one::<String>("head")
                .map(|x| x.parse::<usize>().expect("head must be valid int")),
            host: args.get_one::<String>("host").map(String::from),
            item: args.get_one::<String>("item").map(String::from),
            output: args
                .get_one::<String>("output")
                .map(std::path::PathBuf::from),
            output_column: args.get_one::<String>("output-column").map(String::from),
            page: args
                .get_one::<String>("page")
                .map(|x| x.parse::<usize>().expect("page must be valid int")),
            page_size: args
                .get_one::<String>("page-size")
                .map(|x| x.parse::<usize>().expect("page-size must be valid int")),
            path: None,
            quote_char: args.get_one::<String>("quote").map(String::from),
            repo_dir,
            row: args
                .get_one::<String>("row")
                .map(|x| x.parse::<usize>().expect("row must be valid int")),
            should_page: args.get_flag("full") || page_specified,
            should_randomize: args.get_flag("randomize"),
            should_reverse: args.get_flag("reverse"),
            slice: args.get_one::<String>("slice").map(String::from),
            sort_by: args.get_one::<String>("sort").map(String::from),
            sort_by_similarity_to: args
                .get_one::<String>("sort-by-similarity-to")
                .map(String::from),
            sql: args.get_one::<String>("sql").map(String::from),
            tail: args
                .get_one::<String>("tail")
                .map(|x| x.parse::<usize>().expect("tail must be valid int")),
            take: args.get_one::<String>("take").map(String::from),
            text2sql: args.get_one::<String>("text2sql").map(String::from),
            unique: args.get_one::<String>("unique").map(String::from),
            vstack,
            write: write_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ArgMatches;
    use std::ffi::OsString;

    fn create_df_args_with_bearer_token(
        path: &str,
        host: Option<&str>,
        bearer_token: Option<&str>,
        sql: Option<&str>,
    ) -> ArgMatches {
        let mut args = vec![
            OsString::from("df"),
            OsString::from(path),
        ];

        if let Some(h) = host {
            args.push(OsString::from("--host"));
            args.push(OsString::from(h));
        }

        if let Some(token) = bearer_token {
            args.push(OsString::from("--bearer-token"));
            args.push(OsString::from(token));
        }

        if let Some(q) = sql {
            args.push(OsString::from("--sql"));
            args.push(OsString::from(q));
        }

        DFCmd.args().try_get_matches_from(args).unwrap()
    }

    #[test]
    fn test_df_cmd_args_with_bearer_token() {
        let cmd = DFCmd;
        let command = cmd.args();

        // Test that --bearer-token argument is present
        let bearer_token_arg = command.get_arguments().find(|arg| {
            arg.get_id() == "bearer_token"
        });
        assert!(bearer_token_arg.is_some());

        let arg = bearer_token_arg.unwrap();
        assert_eq!(arg.get_long(), Some("bearer-token"));
        assert!(arg.get_help().unwrap().to_string().contains("Bearer token"));
    }

    #[test]
    fn test_df_cmd_parse_args_with_bearer_token() {
        let args = create_df_args_with_bearer_token(
            "data.csv",
            Some("test.example.com"),
            Some("test_bearer_token_123"),
            Some("SELECT * FROM data"),
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("PATH").unwrap(), "data.csv");
        assert_eq!(args.get_one::<String>("host").unwrap(), "test.example.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "test_bearer_token_123");
        assert_eq!(args.get_one::<String>("sql").unwrap(), "SELECT * FROM data");
    }

    #[test]
    fn test_df_cmd_parse_args_without_bearer_token() {
        let args = create_df_args_with_bearer_token(
            "data.csv",
            None,
            None,
            None,
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("PATH").unwrap(), "data.csv");
        assert!(args.get_one::<String>("host").is_none());
        assert!(args.get_one::<String>("bearer_token").is_none());
        assert!(args.get_one::<String>("sql").is_none());
    }

    #[test]
    fn test_df_cmd_args_help_contains_bearer_token() {
        let cmd = DFCmd;
        let help_text = cmd.args().render_help().to_string();
        
        assert!(help_text.contains("--bearer-token"));
        assert!(help_text.contains("Bearer token for authentication"));
        assert!(help_text.contains("config file will be used"));
    }

    #[test]
    fn test_df_cmd_bearer_token_is_optional() {
        // Should be able to create args without bearer token
        let args = create_df_args_with_bearer_token("data.csv", None, None, None);
        assert!(args.get_one::<String>("bearer_token").is_none());

        // Should be able to create args with bearer token
        let args = create_df_args_with_bearer_token("data.csv", None, Some("token123"), None);
        assert!(args.get_one::<String>("bearer_token").is_some());
    }

    #[test]
    fn test_df_cmd_all_options_together() {
        let args = create_df_args_with_bearer_token(
            "remote_data.csv",
            Some("custom.host.com"),
            Some("bearer_token_xyz"),
            Some("SELECT * FROM remote_data WHERE id > 10"),
        );

        assert_eq!(args.get_one::<String>("PATH").unwrap(), "remote_data.csv");
        assert_eq!(args.get_one::<String>("host").unwrap(), "custom.host.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "bearer_token_xyz");
        assert_eq!(args.get_one::<String>("sql").unwrap(), "SELECT * FROM remote_data WHERE id > 10");
    }

    #[test]
    fn test_df_cmd_parse_df_args_includes_bearer_token() {
        let args = create_df_args_with_bearer_token(
            "test.csv",
            Some("test.host.com"),
            Some("test_token_123"),
            None,
        );

        let opts = DFCmd::parse_df_args(&args);
        assert_eq!(opts.bearer_token, Some("test_token_123".to_string()));
        assert_eq!(opts.host, Some("test.host.com".to_string()));
    }
}
