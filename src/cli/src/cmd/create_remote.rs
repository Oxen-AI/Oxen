use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::constants::DEFAULT_HOST;
use liboxen::error::OxenError;
use liboxen::model::file::FileNew;
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
            .unwrap_or("https".to_string());

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

        if empty {
            let mut repo_new = RepoNew::from_namespace_name(namespace, name);
            repo_new.host = Some(host);
            repo_new.is_public = Some(is_public);
            repo_new.scheme = Some(scheme);
            let remote_repo = api::client::repositories::create_empty(repo_new).await?;
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
                contents: format!("# {}\n{}", name, readme_body),
                user,
            }];
            let mut repo = RepoNew::from_files(namespace, name, files);
            repo.host = Some(host);
            repo.is_public = Some(is_public);
            repo.scheme = Some(scheme);

            let remote_repo = api::client::repositories::create(repo).await?;
            println!("🎉 Remote successfully created for '{}/{}'\n\nClone your repository with:\n\n  oxen clone {}\n",
                namespace, name, remote_repo.remote.url
            );
        }

        Ok(())
    }
}
