use async_trait::async_trait;

use clap::arg;
use clap::{Arg, Command};

use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_HOST, DEFAULT_SCHEME};
use liboxen::error::OxenError;
use liboxen::api;
use std::path::PathBuf;

use liboxen::repositories;

use crate::helpers::check_remote_version_blocking;

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
        .arg(arg!(<ID> "ID of the repository you want to download from ie. ox/my-repo"))
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
            Arg::new("revision")
                .long("revision")
                .help("The branch or commit id to download the data from. Defaults to main branch. If a branch is specified, it will download the latest commit from that branch.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("The host of the repository you want to download from. Defaults to hub.oxen.ai")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("scheme")
                .long("scheme")
                .help("The scheme of the repository you want to download from. Defaults to https")
                .value_parser(["http", "https"])
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let id = args
            .get_one::<String>("ID")
            .expect("Must supply a repository id");
        // Check that the id format is namespace/repo-name
        if id.chars().filter(|&c| c == '/').count() != 1 {
            return Err(OxenError::basic_str(
                "Invalid repository ID format. Must be namespace/repo-name",
            ));
        }
        let paths: Vec<PathBuf> = args
            .get_many::<String>("paths")
            .expect("Must supply paths")
            .map(PathBuf::from)
            .collect();
        if paths.is_empty() {
            return Err(OxenError::basic_str("Must supply a path to download."));
        }
        let dst = args
            .get_one::<String>("output")
            .map(PathBuf::from)
            .unwrap_or(PathBuf::from("."));
        let revision = args
            .get_one::<String>("revision")
            .map(String::from)
            .unwrap_or(DEFAULT_BRANCH_NAME.to_string());
        let host = args
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string());
        let scheme = args
            .get_one::<String>("scheme")
            .map(String::from)
            .unwrap_or(DEFAULT_SCHEME.to_string());

        check_remote_version_blocking(scheme.clone(), host.clone()).await?;

        // If remote flag is set and there's a remote-mode repository in scope, run workspace download
        /*if let Ok(repo) = LocalRepository::from_current_dir() {
            if args.get_flag("remote") && repo.is_remote_mode() {
                let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
                let cwd = std::env::current_dir()?;
                for path in paths {
                    api::client::workspaces::files::download(
                        &remote_repo,
                        &repo.workspace_name.clone().unwrap(),
                        path.to_str().unwrap(),
                        Some(&dst),
                    )
                    .await?;
                }

                return Ok(());
            }

            // Error if remote flag set in regular repo
            if args.get_flag("remote") && !repo.is_remote_mode() {
                // TODO: Better error message
                return Err(OxenError::basic_str(
                    "Error: 'remote' flag can only be used in remote-mode repositories",
                ));
            }
        }*/

        // Check if the first path is a valid remote repo
        if let Some(remote_repo) =
            api::client::repositories::get_by_name_host_and_scheme(id, &host, &scheme).await?
        {
            // Download from the remote without having to have a local repo directory
            for path in paths {
                repositories::download(&remote_repo, &path, &dst, &revision).await?;
            }
        } else {
            eprintln!("Repository does not exist {}", id);
        }

        Ok(())
    }
}
