use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::config::{AuthConfig, UserConfig};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::StorageOpts;

use crate::cmd::RunCmd;
pub const NAME: &str = "config";
pub struct ConfigCmd;

#[async_trait]
impl RunCmd for ConfigCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Sets the user configuration in ~/.oxen/user_config.toml")
            .arg(
                Arg::new("name")
                    .long("name")
                    .short('n')
                    .help("Set the name you want your commits to be saved as.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("email")
                    .long("email")
                    .short('e')
                    .help("Set the email you want your commits to be saved as.")
                    .action(clap::ArgAction::Set),
            )
            // Note: we differ from git here
            .arg(
                Arg::new("set-remote")
                    .long("set-remote")
                    .number_of_values(2)
                    .value_names(["NAME", "URL"])
                    .help("Set a remote for your current working repository.")
                    .action(clap::ArgAction::Set),
            )
            // "delete-remote" is easier to read than "remove-remote"
            .arg(
                Arg::new("delete-remote")
                    .long("delete-remote")
                    .value_name("REMOTE_NAME")
                    .help("Delete a remote from the current working repository.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("storage-backend")
                    .long("storage-backend")
                    .help("Set the location to store version files. --storage-backend local --storage-backend-path <path> or --storage-backend s3 --storage-backend-bucket <bucket> --storage-backend-path <prefix>")
                    .default_value("local")
                    .default_missing_value("local")
                    .value_parser(["local", "s3"])
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("storage-backend-path")
                    .long("storage-backend-path")
                    .help("Set the path for local storage backend or the prefix for s3 storage backend. Must specify type.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("storage-backend-bucket")
                    .long("storage-backend-bucket")
                    .help("Set the bucket for s3 storage backend. Must specify type and path together.")
                    .requires_if("s3", "storage-backend")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("auth-token")
                    .long("auth")
                    .short('a')
                    .number_of_values(2)
                    .value_names(["HOST", "TOKEN"])
                    .help("Set the authentication token for a specific oxen-server host.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("default-host")
                    .long("default-host")
                    .help("Sets the default host used to check version numbers. If empty, the CLI will not do a version check.")
                    .action(clap::ArgAction::Set),
            )
            .arg_required_else_help(true)
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Non-Repo Dependent
        if let Some(name) = args.get_one::<String>("name") {
            match self.set_user_name(name) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        if let Some(email) = args.get_one::<String>("email") {
            match self.set_user_email(email) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        if let Some(auth) = args.get_many::<String>("auth-token") {
            if let [host, token] = auth.collect::<Vec<_>>()[..] {
                match self.set_auth_token(host, token) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            } else {
                eprintln!("invalid arguments for --auth");
            }
        }

        if let Some(default_host) = args.get_one::<String>("default-host") {
            match self.set_default_host(default_host) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        // Repo Dependent
        if let Some(remote) = args.get_many::<String>("set-remote") {
            let mut repo = LocalRepository::from_current_dir()?;
            if let [name, url] = remote.collect::<Vec<_>>()[..] {
                match self.set_remote(&mut repo, name, url) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            } else {
                eprintln!("invalid arguments for --set-remote");
            }
        }

        if let Some(name) = args.get_one::<String>("delete-remote") {
            let mut repo = LocalRepository::from_current_dir()?;
            match self.delete_remote(&mut repo, name) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        }

        if let Some(path) = args
            .get_one::<String>("storage-backend-path")
            .map(String::from)
        {
            let backend = args
                .get_one::<String>("storage-backend")
                .ok_or_else(|| {
                    OxenError::basic_str(
                        "storage-backend must be specified when storage-backend-path is provided",
                    )
                })?
                .to_string();
            let bucket = args
                .get_one::<String>("storage-backend-bucket")
                .map(String::from);
            let storage_opts = StorageOpts::from_args(Some(backend), Some(path), bucket)?;

            let mut repo = LocalRepository::from_current_dir()?;
            self.set_version_store(&mut repo, storage_opts)
                .await
                .map_err(|e| OxenError::basic_str(format!("{e}")))?;
        }

        Ok(())
    }
}

impl ConfigCmd {
    fn strip_host(host: &str) -> Result<String, OxenError> {
        if host.contains("://") {
            Ok(url::Url::parse(host)?
                .host_str()
                .ok_or_else(|| OxenError::basic_str("Unable to parse host."))?
                .to_string())
        } else {
            Ok(host.to_string())
        }
    }

    pub fn set_remote(
        &self,
        repo: &mut LocalRepository,
        name: &str,
        url: &str,
    ) -> Result<(), OxenError> {
        command::config::set_remote(repo, name, url)?;

        Ok(())
    }

    pub fn delete_remote(&self, repo: &mut LocalRepository, name: &str) -> Result<(), OxenError> {
        command::config::delete_remote(repo, name)?;

        Ok(())
    }

    pub async fn set_version_store(
        &self,
        repo: &mut LocalRepository,
        storage_opts: Option<StorageOpts>,
    ) -> Result<(), OxenError> {
        if let Some(storage_opts) = storage_opts {
            command::config::set_version_store(repo, &storage_opts).await?;
        }

        Ok(())
    }

    pub fn set_auth_token(&self, host: &str, token: &str) -> Result<(), OxenError> {
        let host = Self::strip_host(host)?;
        let mut config = AuthConfig::get_or_create()?;
        config.add_host_auth_token(host.as_ref(), token);
        config.save_default()?;
        println!("Authentication token set for host: {host}");
        Ok(())
    }

    pub fn set_default_host(&self, host: &str) -> Result<(), OxenError> {
        let host = Self::strip_host(host)?;
        let mut config = AuthConfig::get_or_create()?;
        if host.is_empty() {
            config.default_host = None;
        } else {
            config.default_host = Some(host.clone());
        }
        config.save_default()?;
        println!("Default host set to: {host}");
        Ok(())
    }

    pub fn set_user_name(&self, name: &str) -> Result<(), OxenError> {
        let mut config = UserConfig::get_or_create()?;
        config.name = String::from(name);
        config.save_default()?;
        Ok(())
    }

    pub fn set_user_email(&self, email: &str) -> Result<(), OxenError> {
        let mut config = UserConfig::get_or_create()?;
        config.email = String::from(email);
        config.save_default()?;
        Ok(())
    }
}
