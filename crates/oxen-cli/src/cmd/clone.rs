use async_trait::async_trait;
use clap::{Arg, Command, arg};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use liboxen::api;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::core::db::merkle_node::MerkleNodeBackend;
use liboxen::error::OxenError;
use liboxen::opts::CloneOpts;
use liboxen::opts::FetchOpts;
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
            .arg(
                arg!([DESTINATION] "Optional path of the directory to clone into. Relative paths resolve against the current directory.")
                    .required(false),
            )
            .arg(
                Arg::new("filter")
                    .long("filter")
                    .help("Filter down the set of directories you want to clone. Useful if you have a large repository and only want to make changes to a specific subset of files.")
                    .action(clap::ArgAction::Append),
            )
            .arg(
                Arg::new("depth")
                    .long("depth")
                    .help("Used in combination with --filter. The depth at which to clone a subtree. If not provided, the entire subtree will be cloned.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("merkle-backend")
                    .long("merkle-backend")
                    .help("Which engine backs the local repo's Merkle node store (default: lmdb; filesystem is deprecated and rejected)")
                    .value_parser(["filesystem", "lmdb"])
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("all")
                    .long("all")
                    .short('a')
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
                Arg::new("vfs")
                    .long("vfs")
                    .help("Configure the repo to be stored on a virtual file system")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .help("Clone the repo in 'remote mode', pulling the metadata but not the file contents")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
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
            .map(|s| s.parse::<i32>().map_err(OxenError::ParseIntError))
            .transpose()?;
        let merkle_node_backend = args
            .get_one::<String>("merkle-backend")
            .map(|s| MerkleNodeBackend::from_str(s))
            .transpose()?;
        crate::helpers::reject_deprecated_merkle_backend(merkle_node_backend)?;
        let is_vfs = args.get_flag("vfs");
        let is_remote = args.get_flag("remote");

        let current_dir = std::env::current_dir()?;
        let dst = resolve_destination(
            &current_dir,
            args.get_one::<String>("DESTINATION").map(String::as_str),
            url,
        );

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
            is_vfs,
            is_remote,
            merkle_node_backend,
        };

        let (scheme, host) = api::client::get_scheme_and_host_from_url(&opts.url)?;

        // TODO: Do I need to worry about this for remote repo?
        check_remote_version_blocking(scheme.clone(), host.clone()).await?;
        check_remote_version(scheme, host).await?;

        repositories::clone(&opts).await?;

        Ok(())
    }
}

/// Resolve where the clone lands. A relative destination resolves against
/// `current_dir`; an absolute one is used as given. With no destination the
/// directory is named after the last segment of the URL.
fn resolve_destination(current_dir: &Path, destination: Option<&str>, url: &str) -> PathBuf {
    match destination {
        Some(dir_name) => current_dir.join(dir_name),
        None => current_dir.join(repo_name_from_url(url)),
    }
}

/// Last non-empty segment of `url`, so a trailing slash does not yield an empty name.
fn repo_name_from_url(url: &str) -> &str {
    url.rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or("repository")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cwd() -> PathBuf {
        PathBuf::from("/home/ox/work")
    }

    #[test]
    fn test_resolve_destination_relative() {
        assert_eq!(
            resolve_destination(
                &cwd(),
                Some("test_repo"),
                "https://hub.oxen.ai/ox/test_repo"
            ),
            PathBuf::from("/home/ox/work/test_repo")
        );
    }

    #[test]
    fn test_resolve_destination_nested_relative() {
        assert_eq!(
            resolve_destination(
                &cwd(),
                Some("data/test_repo"),
                "https://hub.oxen.ai/ox/test_repo"
            ),
            PathBuf::from("/home/ox/work/data/test_repo")
        );
    }

    #[test]
    fn test_resolve_destination_absolute() {
        assert_eq!(
            resolve_destination(
                &cwd(),
                Some("/var/data/test_repo"),
                "https://hub.oxen.ai/ox/test_repo"
            ),
            PathBuf::from("/var/data/test_repo")
        );
    }

    #[test]
    fn test_resolve_destination_parent_dir() {
        assert_eq!(
            resolve_destination(
                &cwd(),
                Some("../test_repo"),
                "https://hub.oxen.ai/ox/test_repo"
            ),
            PathBuf::from("/home/ox/work/../test_repo")
        );
    }

    #[test]
    fn test_resolve_destination_defaults_to_repo_name() {
        assert_eq!(
            resolve_destination(&cwd(), None, "https://hub.oxen.ai/ox/test_repo"),
            PathBuf::from("/home/ox/work/test_repo")
        );
    }

    #[test]
    fn test_resolve_destination_ignores_trailing_slash_in_url() {
        assert_eq!(
            resolve_destination(&cwd(), None, "https://hub.oxen.ai/ox/test_repo/"),
            PathBuf::from("/home/ox/work/test_repo")
        );
    }

    #[test]
    fn test_repo_name_from_url_without_any_segments() {
        assert_eq!(repo_name_from_url("///"), "repository");
    }
}
