use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::api;
use liboxen::error::OxenError;
use std::path::PathBuf;

use liboxen::opts::DownloadOpts;
use liboxen::repositories;

use crate::helpers::check_remote_version_blocking;
use liboxen::constants::{DEFAULT_HOST, DEFAULT_REMOTE_NAME};

use crate::cmd::RunCmd;
pub const NAME: &str = "download";
pub struct DownloadCmd;

#[async_trait]
impl RunCmd for DownloadCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
        .about("Download a specific file from the remote repository")
        .arg(
            Arg::new("paths")
                .required(true)
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .help("Output file to store the downloaded data")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("Host to download from, for example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .help("Remote to download from, for example: 'origin'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("revision")
                .long("revision")
                .help("The branch or commit id to download the data from. Defaults to main branch. If a branch is specified, it will download the latest commit from that branch.")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let opts = DownloadOpts {
            paths: args
                .get_many::<String>("paths")
                .expect("Must supply paths")
                .map(PathBuf::from)
                .collect(),
            dst: args
                .get_one::<String>("output")
                .map(PathBuf::from)
                .unwrap_or(PathBuf::from(".")),
            remote: args
                .get_one::<String>("remote")
                .map(String::from)
                .unwrap_or(DEFAULT_REMOTE_NAME.to_string()),
            host: args
                .get_one::<String>("host")
                .map(String::from)
                .unwrap_or(DEFAULT_HOST.to_string()),
            revision: args.get_one::<String>("revision").map(String::from),
        };

        let paths = &opts.paths;
        if paths.is_empty() {
            return Err(OxenError::basic_str("Must supply a path to download."));
        }

        check_remote_version_blocking(opts.clone().host).await?;

        // Check if the first path is a valid remote repo
        let name = paths[0].to_string_lossy();
        if let Some(remote_repo) =
            api::client::repositories::get_by_name_host_and_remote(&name, &opts.host, &opts.remote)
                .await?
        {
            // Download from the remote without having to have a local repo directory
            let remote_paths = paths[1..].to_vec();
            let commit_id = opts.remote_commit_id(&remote_repo).await?;
            for path in remote_paths {
                repositories::download(&remote_repo, &path, &opts.dst, &commit_id).await?;
            }
        } else {
            eprintln!("Repository does not exist {}", name);
        }

        Ok(())
    }
}
