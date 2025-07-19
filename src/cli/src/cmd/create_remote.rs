use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::constants::{DEFAULT_HOST, DEFAULT_SCHEME};
use liboxen::error::OxenError;
use liboxen::model::file::{FileContents, FileNew};
use liboxen::model::RepoNew;

use crate::cmd::RunCmd;
pub const NAME: &str = "create-remote";
pub struct CreateRemoteCmd;

#[async_trait]
impl RunCmd for CreateRemoteCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
        .about("Creates a remote repository with the name on the host. Default behavior is to create a remote on the hub.oxen.ai remote.")
        .arg(
            Arg::new("name")
                .long("name")
                .short('n')
                .help("The namespace/name of the remote repository you want to create. For example: 'ox/my_repo'")
                .required(true)
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("The host you want to create the remote repository on. For example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("scheme")
                .long("scheme")
                .help("The scheme for the url of the remote repository. For example: 'https' or 'http'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("add_readme")
                .long("add_readme")
                .help("If present, it will create a README file and initial commit in the remote repo.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("is_public")
                .long("is_public")
                .short('p')
                .help("If present, it will create a public remote repository.")
                .action(clap::ArgAction::SetTrue),
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
        let Some(namespace_name) = args.get_one::<String>("name") else {
            return Err(OxenError::basic_str(
                "Must supply a namespace/name for the remote repository.",
            ));
        };
        // Default the host to the oxen.ai hub
        let host = args
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string());
        // Default scheme
        let scheme = args
            .get_one::<String>("scheme")
            .map(String::from)
            .unwrap_or(DEFAULT_SCHEME.to_string());

        // The format is namespace/name
        let parts: Vec<&str> = namespace_name.split('/').collect();
        if parts.len() != 2 {
            return Err(OxenError::basic_str(
                "Invalid name format. Must be namespace/name",
            ));
        }

        let namespace = parts[0];
        let name = parts[1];
        let empty = !args.get_flag("add_readme");
        let is_public = args.get_flag("is_public");
        let bearer_token = args.get_one::<String>("bearer_token");

        if empty {
            let mut repo_new = RepoNew::from_namespace_name(namespace, name);
            repo_new.host = Some(host);
            repo_new.is_public = Some(is_public);
            repo_new.scheme = Some(scheme);
            let remote_repo = if let Some(token) = bearer_token {
                api::client::repositories::create_empty_with_bearer_token(repo_new, token).await?
            } else {
                api::client::repositories::create_empty(repo_new).await?
            };
            println!("🎉 Remote successfully created for '{}/{}'\n\nIf this is a brand new repository:\n\n  oxen clone {}\n\nTo push an existing local repository to a new remote:\n\n  oxen config --set-remote origin {}\n",
                namespace, name, remote_repo.remote.url, remote_repo.remote.url
            );
        } else {
            // Creating a remote with an initial commit and a README
            let config = UserConfig::get()?;
            let user = config.to_user();
            let readme_body = format!(
                "
Welcome to Oxen.ai 🐂 🌾

## Getting Started

Clone the repository to your local machine:

```bash
oxen clone https://{}/{}/{}
```

## Adding Data

You can add files to it with

```
oxen add <path>
```

Then commit them with

```
oxen commit -m <message>
```

## Pushing Data

Push your changes to the remote with

```
oxen push origin main
```

## Learn More

For the complete developer documentation, visit https://docs.oxen.ai/

Happy Mooooooving of data 🐂
",
                host, namespace, name
            );

            let files: Vec<FileNew> = vec![FileNew {
                path: PathBuf::from("README.md"),
                contents: FileContents::Text(format!("# {}\n{}", name, readme_body)),
                user,
            }];
            let mut repo = RepoNew::from_files(namespace, name, files);
            repo.host = Some(host);
            repo.is_public = Some(is_public);
            repo.scheme = Some(scheme);

            let remote_repo = if let Some(token) = bearer_token {
                api::client::repositories::create_with_bearer_token(repo, token).await?
            } else {
                api::client::repositories::create(repo).await?
            };
            println!("🎉 Remote successfully created for '{}/{}'\n\nClone your repository with:\n\n  oxen clone {}\n",
                namespace, name, remote_repo.remote.url
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ArgMatches;
    use std::ffi::OsString;

    fn create_args_with_bearer_token(
        name: &str,
        host: Option<&str>,
        bearer_token: Option<&str>,
        add_readme: bool,
        is_public: bool,
    ) -> ArgMatches {
        let mut args = vec![
            OsString::from("create-remote"),
            OsString::from("--name"),
            OsString::from(name),
        ];

        if let Some(h) = host {
            args.push(OsString::from("--host"));
            args.push(OsString::from(h));
        }

        if let Some(token) = bearer_token {
            args.push(OsString::from("--bearer-token"));
            args.push(OsString::from(token));
        }

        if add_readme {
            args.push(OsString::from("--add_readme"));
        }

        if is_public {
            args.push(OsString::from("--is_public"));
        }

        CreateRemoteCmd.args().try_get_matches_from(args).unwrap()
    }

    #[test]
    fn test_create_remote_cmd_args_with_bearer_token() {
        let cmd = CreateRemoteCmd;
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
    fn test_create_remote_cmd_parse_args_with_bearer_token() {
        let args = create_args_with_bearer_token(
            "ox/test-repo",
            Some("test.example.com"),
            Some("test_bearer_token_123"),
            false,
            true,
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("name").unwrap(), "ox/test-repo");
        assert_eq!(args.get_one::<String>("host").unwrap(), "test.example.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "test_bearer_token_123");
        assert!(!args.get_flag("add_readme"));
        assert!(args.get_flag("is_public"));
    }

    #[test]
    fn test_create_remote_cmd_parse_args_without_bearer_token() {
        let args = create_args_with_bearer_token(
            "ox/test-repo",
            None,
            None,
            true,
            false,
        );

        // Verify args are parsed correctly
        assert_eq!(args.get_one::<String>("name").unwrap(), "ox/test-repo");
        assert!(args.get_one::<String>("host").is_none());
        assert!(args.get_one::<String>("bearer_token").is_none());
        assert!(args.get_flag("add_readme"));
        assert!(!args.get_flag("is_public"));
    }

    #[test]
    fn test_create_remote_cmd_parse_args_name_validation() {
        // Test valid namespace/name format
        let args = create_args_with_bearer_token("ox/test-repo", None, None, false, false);
        assert_eq!(args.get_one::<String>("name").unwrap(), "ox/test-repo");

        // Test that the command accepts the name (validation happens in run method)
        assert!(args.get_one::<String>("name").is_some());
    }

    #[test]
    fn test_create_remote_cmd_args_help_contains_bearer_token() {
        let cmd = CreateRemoteCmd;
        let help_text = cmd.args().render_help().to_string();
        
        assert!(help_text.contains("--bearer-token"));
        assert!(help_text.contains("Bearer token for authentication"));
        assert!(help_text.contains("config file will be used"));
    }

    #[test]
    fn test_create_remote_cmd_bearer_token_is_optional() {
        // Should be able to create args without bearer token
        let args = create_args_with_bearer_token("ox/test", None, None, false, false);
        assert!(args.get_one::<String>("bearer_token").is_none());

        // Should be able to create args with bearer token
        let args = create_args_with_bearer_token("ox/test", None, Some("token123"), false, false);
        assert!(args.get_one::<String>("bearer_token").is_some());
    }

    #[test]
    fn test_create_remote_cmd_all_options_together() {
        let args = create_args_with_bearer_token(
            "namespace/repo-name",
            Some("custom.host.com"),
            Some("bearer_token_xyz"),
            true,
            true,
        );

        assert_eq!(args.get_one::<String>("name").unwrap(), "namespace/repo-name");
        assert_eq!(args.get_one::<String>("host").unwrap(), "custom.host.com");
        assert_eq!(args.get_one::<String>("bearer_token").unwrap(), "bearer_token_xyz");
        assert!(args.get_flag("add_readme"));
        assert!(args.get_flag("is_public"));
    }
}
