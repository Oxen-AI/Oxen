use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::api;
use liboxen::constants::DEFAULT_HOST;
use liboxen::constants::DEFAULT_REMOTE_NAME;
use liboxen::constants::DEFAULT_SCHEME;
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
            Arg::new("scheme")
                .long("scheme")
                .help("Scheme for the host to upload the data to, for example: 'https'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .help("Remote to upload the data to, for example: 'origin'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("bearer_token")
                .long("bearer-token")
                .help("Bearer token for authentication. If not provided, the config file will be used.")
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
            scheme: args
                .get_one::<String>("scheme")
                .map(String::from)
                .unwrap_or(DEFAULT_SCHEME.to_string()),
            bearer_token: None,
        };
        let bearer_token = args.get_one::<String>("bearer_token");

        // `oxen upload $namespace/$repo_name $path`
        let paths = &opts.paths;
        if paths.is_empty() {
            return Err(OxenError::basic_str(
                "Must supply repository and a file to upload.",
            ));
        }

        check_remote_version_blocking(&opts.scheme, opts.clone().host).await?;

        // Check if the first path is a valid remote repo
        let name = paths[0].to_string_lossy();
        if let Some(remote_repo) = api::client::repositories::get_by_name_host_and_remote(
            &name,
            &opts.host,
            &opts.scheme,
            &opts.remote,
        )
        .await?
        {
            // Remove the repo name from the list of paths
            let remote_paths = paths[1..].to_vec();
            let opts = UploadOpts {
                paths: remote_paths,
                bearer_token: bearer_token.cloned(),
                ..opts
            };

            repositories::workspaces::upload(&remote_repo, &opts).await?;
        } else {
            eprintln!("Repository does not exist {}", name);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ArgMatches;
    use std::ffi::OsString;

    fn create_upload_args_with_bearer_token(
        paths: Vec<&str>,
        message: &str,
        host: Option<&str>,
        bearer_token: Option<&str>,
        scheme: Option<&str>,
        branch: Option<&str>,
    ) -> ArgMatches {
        let mut args = vec![
            OsString::from("upload"),
        ];

        for path in paths {
            args.push(OsString::from(path));
        }

        args.push(OsString::from("--message"));
        args.push(OsString::from(message));

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

        if let Some(b) = branch {
            args.push(OsString::from("--branch"));
            args.push(OsString::from(b));
        }

        UploadCmd.args().try_get_matches_from(args).unwrap()
    }

    #[test]
    fn test_upload_cmd_args_with_bearer_token() {
        let cmd = UploadCmd;
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
    fn test_upload_cmd_parse_args_with_bearer_token() {
        let args = create_upload_args_with_bearer_token(
            vec!["ox/test-repo", "test.txt"],
            "Test commit message",
            Some("test.example.com"),
            Some("test_bearer_token_123"),
            Some("https"),
            Some("main"),
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("message").unwrap(), "Test commit message");
        assert_eq!(args.get_one::<String>("host").unwrap(), "test.example.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "test_bearer_token_123");
        assert_eq!(args.get_one::<String>("scheme").unwrap(), "https");
        assert_eq!(args.get_one::<String>("branch").unwrap(), "main");
    }

    #[test]
    fn test_upload_cmd_parse_args_without_bearer_token() {
        let args = create_upload_args_with_bearer_token(
            vec!["ox/test-repo", "test.txt"],
            "Test commit message",
            None,
            None,
            None,
            None,
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("message").unwrap(), "Test commit message");
        assert!(args.get_one::<String>("host").is_none());
        assert!(args.get_one::<String>("bearer_token").is_none());
        assert!(args.get_one::<String>("scheme").is_none());
        assert!(args.get_one::<String>("branch").is_none());
    }

    #[test]
    fn test_upload_cmd_args_help_contains_bearer_token() {
        let cmd = UploadCmd;
        let help_text = cmd.args().render_help().to_string();
        
        assert!(help_text.contains("--bearer-token"));
        assert!(help_text.contains("Bearer token for authentication"));
        assert!(help_text.contains("config file will be used"));
    }

    #[test]
    fn test_upload_cmd_bearer_token_is_optional() {
        // Should be able to create args without bearer token
        let args = create_upload_args_with_bearer_token(
            vec!["ox/test", "file.txt"], 
            "Test message", 
            None, 
            None, 
            None, 
            None
        );
        assert!(args.get_one::<String>("bearer_token").is_none());

        // Should be able to create args with bearer token
        let args = create_upload_args_with_bearer_token(
            vec!["ox/test", "file.txt"], 
            "Test message", 
            None, 
            Some("token123"), 
            None, 
            None
        );
        assert!(args.get_one::<String>("bearer_token").is_some());
    }

    #[test]
    fn test_upload_cmd_all_options_together() {
        let args = create_upload_args_with_bearer_token(
            vec!["namespace/repo-name", "file1.txt", "file2.txt"],
            "Upload multiple files",
            Some("custom.host.com"),
            Some("bearer_token_xyz"),
            Some("https"),
            Some("feature-branch"),
        );

        assert_eq!(args.get_one::<String>("message").unwrap(), "Upload multiple files");
        assert_eq!(args.get_one::<String>("host").unwrap(), "custom.host.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "bearer_token_xyz");
        assert_eq!(args.get_one::<String>("scheme").unwrap(), "https");
        assert_eq!(args.get_one::<String>("branch").unwrap(), "feature-branch");
    }
}
