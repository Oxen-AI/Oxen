use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::api;
use liboxen::constants::DEFAULT_HOST;
use liboxen::constants::DEFAULT_REMOTE_NAME;
use liboxen::error::OxenError;
use liboxen::opts::UploadOpts;
use liboxen::repositories;

use std::path::PathBuf;

use crate::helpers::check_remote_version_blocking;

use crate::cmd::RunCmd;
pub const NAME: &str = "upload";
pub struct UploadCmd;

#[async_trait]
impl RunCmd for UploadCmd {
    fn name(&self) -> &str {
        NAME
    }
    fn args(&self) -> Command {
        Command::new(NAME)
        .about("Upload a specific file to the remote repository.")
        .arg(
            Arg::new("paths")
                .required(true)
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("dst")
                .long("destination")
                .short('d')
                .help("The destination directory to upload the data to. Defaults to the root './' of the repository.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("branch")
                .long("branch")
                .short('b')
                .help("The branch to upload the data to. Defaults to main branch.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("message")
                .help("The message for the commit. Should be descriptive about what changed.")
                .long("message")
                .short('m')
                .required(true)
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("Host to upload the data to, for example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .help("Remote to up the data to, for example: 'origin'")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let opts = UploadOpts {
            paths: args
                .get_many::<String>("paths")
                .expect("Must supply paths")
                .map(PathBuf::from)
                .collect(),
            dst: args
                .get_one::<String>("dst")
                .map(PathBuf::from)
                .unwrap_or(PathBuf::from(".")),
            message: args
                .get_one::<String>("message")
                .map(String::from)
                .expect("Must supply a commit message"),
            branch: args.get_one::<String>("branch").map(String::from),
            remote: args
                .get_one::<String>("remote")
                .map(String::from)
                .unwrap_or(DEFAULT_REMOTE_NAME.to_string()),
            host: args
                .get_one::<String>("host")
                .map(String::from)
                .unwrap_or(DEFAULT_HOST.to_string()),
        };

        // `oxen upload $namespace/$repo_name $path`
        let paths = &opts.paths;
        if paths.is_empty() {
            return Err(OxenError::basic_str(
                "Must supply repository and a file to upload.",
            ));
        }

        check_remote_version_blocking(opts.clone().host).await?;

        // Check if the first path is a valid remote repo
        let name = paths[0].to_string_lossy();
        if let Some(remote_repo) =
            api::client::repositories::get_by_name_host_and_remote(&name, &opts.host, &opts.remote)
                .await?
        {
            // Remove the repo name from the list of paths
            let remote_paths = paths[1..].to_vec();
            let opts = UploadOpts {
                paths: remote_paths,
                ..opts
            };

            repositories::workspaces::upload(&remote_repo, &opts).await?;
        } else {
            eprintln!("Repository does not exist {}", name);
        }

        Ok(())
    }
}
