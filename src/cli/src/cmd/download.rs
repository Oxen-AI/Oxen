use async_trait::async_trait;

use clap::arg;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_HOST, DEFAULT_SCHEME};
use liboxen::error::OxenError;
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
        .arg(
            Arg::new("bearer_token")
                .long("bearer-token")
                .help("Bearer token for authentication. If not provided, the config file will be used.")
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
        let bearer_token = args.get_one::<String>("bearer_token");

        check_remote_version_blocking(scheme.clone(), host.clone()).await?;

        // Check if the first path is a valid remote repo
        if let Some(remote_repo) =
            api::client::repositories::get_by_name_host_and_scheme(id, &host, &scheme).await?
        {
            // Download from the remote without having to have a local repo directory
            for path in paths {
                repositories::download_with_bearer_token(&remote_repo, &path, &dst, &revision, bearer_token.as_ref().map(|s| s.as_str())).await?;
            }
        } else {
            eprintln!("Repository does not exist {}", id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ArgMatches;
    use std::ffi::OsString;

    fn create_download_args_with_bearer_token(
        id: &str,
        paths: Vec<&str>,
        host: Option<&str>,
        bearer_token: Option<&str>,
        scheme: Option<&str>,
    ) -> ArgMatches {
        let mut args = vec![
            OsString::from("download"),
            OsString::from(id),
        ];

        for path in paths {
            args.push(OsString::from(path));
        }

        if let Some(h) = host {
            args.push(OsString::from("--host"));
            args.push(OsString::from(h));
        }

        if let Some(token) = bearer_token {
            args.push(OsString::from("--bearer-token"));
            args.push(OsString::from(token));
        }

        if let Some(s) = scheme {
            args.push(OsString::from("--scheme"));
            args.push(OsString::from(s));
        }

        DownloadCmd.args().try_get_matches_from(args).unwrap()
    }

    #[test]
    fn test_download_cmd_args_with_bearer_token() {
        let cmd = DownloadCmd;
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
    fn test_download_cmd_parse_args_with_bearer_token() {
        let args = create_download_args_with_bearer_token(
            "ox/test-repo",
            vec!["test.txt"],
            Some("test.example.com"),
            Some("test_bearer_token_123"),
            Some("https"),
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("ID").unwrap(), "ox/test-repo");
        assert_eq!(args.get_one::<String>("host").unwrap(), "test.example.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "test_bearer_token_123");
        assert_eq!(args.get_one::<String>("scheme").unwrap(), "https");
    }

    #[test]
    fn test_download_cmd_parse_args_without_bearer_token() {
        let args = create_download_args_with_bearer_token(
            "ox/test-repo",
            vec!["test.txt"],
            None,
            None,
            None,
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("ID").unwrap(), "ox/test-repo");
        assert!(args.get_one::<String>("host").is_none());
        assert!(args.get_one::<String>("bearer_token").is_none());
        assert!(args.get_one::<String>("scheme").is_none());
    }

    #[test]
    fn test_download_cmd_args_help_contains_bearer_token() {
        let cmd = DownloadCmd;
        let help_text = cmd.args().render_help().to_string();
        
        assert!(help_text.contains("--bearer-token"));
        assert!(help_text.contains("Bearer token for authentication"));
        assert!(help_text.contains("config file will be used"));
    }

    #[test]
    fn test_download_cmd_bearer_token_is_optional() {
        // Should be able to create args without bearer token
        let args = create_download_args_with_bearer_token("ox/test", vec!["file.txt"], None, None, None);
        assert!(args.get_one::<String>("bearer_token").is_none());

        // Should be able to create args with bearer token
        let args = create_download_args_with_bearer_token("ox/test", vec!["file.txt"], None, Some("token123"), None);
        assert!(args.get_one::<String>("bearer_token").is_some());
    }

    #[test]
    fn test_download_cmd_all_options_together() {
        let args = create_download_args_with_bearer_token(
            "namespace/repo-name",
            vec!["file1.txt", "file2.txt"],
            Some("custom.host.com"),
            Some("bearer_token_xyz"),
            Some("https"),
        );

        assert_eq!(args.get_one::<String>("ID").unwrap(), "namespace/repo-name");
        assert_eq!(args.get_one::<String>("host").unwrap(), "custom.host.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "bearer_token_xyz");
        assert_eq!(args.get_one::<String>("scheme").unwrap(), "https");
    }
}
