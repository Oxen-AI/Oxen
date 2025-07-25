use async_trait::async_trait;
use clap::{arg, Arg, Command};
use std::path::{Component, Path, PathBuf};

use liboxen::api;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::opts::fetch_opts::FetchOpts;
use liboxen::opts::CloneOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::{check_remote_version, check_remote_version_blocking};

pub const NAME: &str = "clone";
pub struct CloneCmd;

#[async_trait]
impl RunCmd for CloneCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Clone a repository by its URL")
            .arg_required_else_help(true)
            .arg(arg!(<URL> "URL of the repository you want to clone"))
            .arg(arg!([DESTINATION] "Optional name of the directory to clone into").required(false))
            .arg(
                Arg::new("filter")
                    .long("filter")
                    .help("Filter down the set of directories you want to clone. Useful if you have a large repository and only want to make changes to a specific subset of files.")
                    .action(clap::ArgAction::Append),
            )
            .arg(
                Arg::new("depth")
                    .long("depth")
                    .help("Used in combination with --subtree. The depth at which to clone the subtree. If not provided, the entire subtree will be cloned.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("all")
                    .long("all")
                    .help("This downloads the full commit history, all the data files, and all the commit databases. Useful if you want to have the entire history locally or push to a new remote.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .help("The branch you want to pull to when you clone.")
                    .default_value(DEFAULT_BRANCH_NAME)
                    .default_missing_value(DEFAULT_BRANCH_NAME)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .help("Clone the repo in 'remote mode', pulling the metadata but not the file contents")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let url = args.get_one::<String>("URL").expect("required");
        let all = args.get_flag("all");
        let branch = args
            .get_one::<String>("branch")
            .expect("Must supply a branch");
        let filters: Vec<PathBuf> = args
            .get_many::<String>("filter")
            .unwrap_or_default()
            .map(PathBuf::from)
            .collect();
        let depth: Option<i32> = args
            .get_one::<String>("depth")
            .map(|s| s.parse().expect("Invalid depth, must be an integer"));
        let is_remote = args.get_flag("remote");

        let current_dir = std::env::current_dir().expect("Could not get current working directory");
        let dst: PathBuf = match args.get_one::<String>("DESTINATION") {
            Some(dir_name) => {
                let path = Path::new(dir_name);

                if path.is_absolute()
                    || path.components().any(|c| matches!(c, Component::ParentDir))
                {
                    return Err(OxenError::basic_str(
                        "Invalid destination: absolute paths or '..' not allowed",
                    ));
                }

                let joined = current_dir.join(path);
                if !joined.starts_with(&current_dir) {
                    return Err(OxenError::basic_str(
                        "Invalid destination: path escapes base directory",
                    ));
                }
                joined
            }
            None => {
                // Get the name of the repo from the url
                let repo_name = url.split('/').next_back().unwrap_or("repository");
                current_dir.join(repo_name)
            }
        };

        let opts = CloneOpts {
            url: url.to_string(),
            dst,
            fetch_opts: FetchOpts {
                branch: branch.to_string(),
                subtree_paths: filters_to_subtree_paths(&filters, depth),
                depth,
                all,
                ..FetchOpts::new()
            },
            is_remote,
        };

        let (scheme, host) = api::client::get_scheme_and_host_from_url(&opts.url)?;

        // TODO: Do I need to worry about this for remote repo?
        check_remote_version_blocking(scheme.clone(), host.clone()).await?;
        check_remote_version(scheme, host).await?;

        repositories::clone(&opts).await?;

        Ok(())
    }
}

fn filters_to_subtree_paths(filters: &[PathBuf], depth: Option<i32>) -> Option<Vec<PathBuf>> {
    if filters.is_empty() {
        if depth.is_some() {
            // If the user specifies a depth, default to the root
            Some(vec![PathBuf::from(".")])
        } else {
            None
        }
    } else {
        Some(filters.to_vec())
    }
}
