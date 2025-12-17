use async_trait::async_trait;
use clap::{Arg, Command, ValueEnum};
use liboxen::api;
use liboxen::constants::{DEFAULT_NOTEBOOK_BASE_IMAGE, DEFAULT_REMOTE_NAME};
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, RemoteRepository};

use crate::cmd::RunCmd;
use liboxen::opts::NotebookOpts;

pub const NAME: &str = "notebook";

#[derive(Clone, Debug, ValueEnum)]
pub enum NotebookAction {
    Start,
    Stop,
    Images,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum NotebookMode {
    Edit,
    Script,
}

pub struct NotebookCmd;

#[async_trait]
impl RunCmd for NotebookCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Launch a notebook environment")
            .arg(
                Arg::new("action")
                    .help("Whether you want to start or stop a notebook (start|stop)")
                    .required(true)
                    .value_parser(clap::value_parser!(NotebookAction))
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("notebook")
                    .long("notebook")
                    .short('n')
                    .help("File path to the notebook to start the notebook, or the notebook id if running")
                    .required(true)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .help("Branch to use")
                    .default_value("main")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("base_image")
                    .long("base-image")
                    .help("Base Docker image to use")
                    .default_value(DEFAULT_NOTEBOOK_BASE_IMAGE)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("gpu")
                    .long("gpu")
                    .help("GPU model to use (A10G, H100, A100-40GB, A100-80GB)")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("cpu_cores")
                    .long("cpu-cores")
                    .help("Number of CPU cores to allocate")
                    .default_value("1")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("memory_mb")
                    .long("memory-mb")
                    .help("Amount of memory to allocate in MB")
                    .default_value("1024")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("timeout_secs")
                    .long("timeout-secs")
                    .help("Timeout in seconds")
                    .default_value("3600")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("build_script")
                    .long("build-script")
                    .help("Path to build script to run before starting notebook")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .short('r')
                    .help("Base Docker image to use")
                    .default_value(DEFAULT_REMOTE_NAME)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("mode")
                    .long("mode")
                    .help("Notebook mode (edit|script)")
                    .value_parser(clap::value_parser!(NotebookMode))
                    .default_value("edit")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("args")
                    .help("Additional arguments to pass to the notebook/script")
                    .num_args(0..)
                    .action(clap::ArgAction::Append),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let action = args
            .get_one::<NotebookAction>("action")
            .expect("Must supply action");
        let notebook = args
            .get_one::<String>("notebook")
            .expect("Must supply notebook path or id");
        let branch = args
            .get_one::<String>("branch")
            .expect("Must supply branch");
        let remote_name = args
            .get_one::<String>("remote")
            .expect("Must supply remote");
        let base_image = args
            .get_one::<String>("base_image")
            .expect("Must supply base_image");
        let mode = args
            .get_one::<NotebookMode>("mode")
            .expect("Must supply mode");
        let args_vec: Vec<String> = args
            .get_many::<String>("args")
            .unwrap_or_default()
            .map(|s| s.to_string())
            .collect();
        let gpu_model = args.get_one::<String>("gpu");
        let cpu_cores = args
            .get_one::<String>("cpu_cores")
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let memory_mb = args
            .get_one::<String>("memory_mb")
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let timeout_secs = args
            .get_one::<String>("timeout_secs")
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let build_script = args.get_one::<String>("build_script");

        log::debug!("{action:?} notebook with:");
        log::debug!("  Notebook: {notebook}");
        log::debug!("  Branch: {branch}");
        log::debug!("  Base Image: {base_image}");
        log::debug!("  Mode: {mode:?}");
        log::debug!("  Script Args: {args_vec:?}");
        log::debug!("  GPU Model: {gpu_model:?}");
        log::debug!("  CPU Cores: {cpu_cores}");
        log::debug!("  Memory MB: {memory_mb}");
        log::debug!("  Timeout Secs: {timeout_secs}");
        log::debug!("  Build Script: {build_script:?}");

        let repository = LocalRepository::from_current_dir()?;

        // Get the remote repo
        let remote = repository
            .get_remote(remote_name)
            .ok_or(OxenError::remote_not_set(remote_name))?;
        let remote_repo = api::client::repositories::get_by_remote(&remote)
            .await?
            .ok_or(OxenError::remote_not_found(remote.clone()))?;

        let mut opts = NotebookOpts {
            notebook: notebook.to_owned(),
            branch: branch.to_owned(),
            base_image: base_image.to_owned(),
            mode: match mode {
                NotebookMode::Edit => String::from("edit"),
                NotebookMode::Script => String::from("script"),
            },
            gpu_model: gpu_model.map(|s| s.to_owned()),
            cpu_cores,
            memory_mb,
            timeout_secs,
            notebook_base_image_id: None,
            build_script: build_script.map(|s| s.to_owned()),
            script_args: if args_vec.is_empty() {
                None
            } else {
                Some(args_vec.join(" "))
            },
        };

        log::debug!("notebook opts: {opts:?}");

        match action {
            NotebookAction::Start => {
                self.start_notebook(&remote_repo, &mut opts).await?;
            }
            NotebookAction::Stop => {
                self.stop_notebook(&remote_repo, &opts).await?;
            }
            NotebookAction::Images => {
                self.list_base_images(&remote_repo).await?;
            }
        }

        Ok(())
    }
}

impl NotebookCmd {
    pub async fn start_notebook(
        &self,
        repository: &RemoteRepository,
        opts: &mut NotebookOpts,
    ) -> Result<(), OxenError> {
        let base_images = api::client::notebooks::list_base_images(repository).await?;
        let base_image_id = base_images
            .iter()
            .find(|i| i.image_definition == opts.base_image);

        let Some(base_image_id) = base_image_id else {
            let error = format!("Base image not supported: {}\n\nTo see a list of supported images run:\n\n  oxen notebook images -n {}\n", opts.base_image, opts.notebook);
            return Err(OxenError::basic_str(error));
        };

        opts.base_image = base_image_id.id.to_owned();

        let notebook = api::client::notebooks::create(repository, opts).await?;
        // api::client::notebooks::run(repository, &notebook).await?;
        let url = format!(
            "https://oxen.ai/{}/{}/notebooks/{}",
            repository.namespace, repository.name, notebook.id
        );
        println!("✅ Notebook {} successfully started", notebook.id);
        println!("\nVisit the notebook at:\n\n  {}\n\nTo stop the notebook run:\n\n  oxen notebook stop -n {}\n", url, notebook.id);
        Ok(())
    }

    pub async fn stop_notebook(
        &self,
        repository: &RemoteRepository,
        opts: &NotebookOpts,
    ) -> Result<(), OxenError> {
        let notebook = api::client::notebooks::get(repository, &opts.notebook).await?;
        api::client::notebooks::stop(repository, &notebook).await?;
        println!("🛑 Notebook {} successfully stopped", notebook.id);
        Ok(())
    }

    pub async fn list_base_images(&self, repository: &RemoteRepository) -> Result<(), OxenError> {
        let base_images = api::client::notebooks::list_base_images(repository).await?;

        for i in base_images {
            println!("{}", i.image_definition);
        }

        Ok(())
    }
}
