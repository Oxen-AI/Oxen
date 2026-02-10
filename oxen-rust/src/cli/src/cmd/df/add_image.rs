use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;

pub const NAME: &str = "add-image";

pub struct AddImageCmd;

#[async_trait]
impl RunCmd for AddImageCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Add image(s) to a data frame, copying external images into the repo and staging all files.")
            .arg(
                Arg::new("IMAGE_PATH")
                    .help("Path(s) to image file(s) to add")
                    .required(true)
                    .num_args(1..),
            )
            .arg(
                Arg::new("file")
                    .long("file")
                    .short('f')
                    .help("The data frame file to add the image path(s) to")
                    .required(true),
            )
            .arg(
                Arg::new("dest")
                    .long("dest")
                    .help("Destination directory or path in the repo for external images"),
            )
            .arg(
                Arg::new("extension")
                    .long("extension")
                    .help("Override the data frame format (e.g. csv, tsv, parquet)"),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;

        let current_dir = std::env::current_dir()
            .map_err(|e| OxenError::basic_str(format!("Failed to get current directory: {e}")))?;

        // Collect image paths, resolving relative to CWD
        let image_paths: Vec<PathBuf> = args
            .get_many::<String>("IMAGE_PATH")
            .ok_or_else(|| OxenError::basic_str("At least one IMAGE_PATH is required"))?
            .map(|p| {
                let path = PathBuf::from(p);
                if path.is_absolute() {
                    path
                } else {
                    current_dir.join(path)
                }
            })
            .collect();

        // Get df path relative to repo root
        let df_arg = args
            .get_one::<String>("file")
            .ok_or_else(|| OxenError::basic_str("--file is required"))?;
        let df_path = PathBuf::from(df_arg);
        let df_repo_relative = if df_path.is_absolute() {
            liboxen::util::fs::path_relative_to_dir(&df_path, &repo.path)?
        } else {
            // Resolve relative to CWD, then make repo-relative
            let abs_df = current_dir.join(&df_path);
            // If the file doesn't exist yet, we can't canonicalize.
            // Just compute the relative path.
            if abs_df.exists() {
                let canonical = abs_df.canonicalize().map_err(|e| {
                    OxenError::basic_str(format!("Could not canonicalize {abs_df:?}: {e}"))
                })?;
                let repo_canonical = repo.path.canonicalize().map_err(|e| {
                    OxenError::basic_str(format!(
                        "Could not canonicalize repo path {:?}: {e}",
                        repo.path
                    ))
                })?;
                liboxen::util::fs::path_relative_to_dir(&canonical, &repo_canonical)?
            } else {
                let repo_canonical = repo.path.canonicalize().map_err(|e| {
                    OxenError::basic_str(format!(
                        "Could not canonicalize repo path {:?}: {e}",
                        repo.path
                    ))
                })?;
                let abs_cwd_canonical = current_dir.canonicalize().map_err(|e| {
                    OxenError::basic_str(format!("Could not canonicalize CWD: {e}"))
                })?;
                let cwd_relative =
                    liboxen::util::fs::path_relative_to_dir(&abs_cwd_canonical, &repo_canonical)?;
                cwd_relative.join(&df_path)
            }
        };

        let dest = args.get_one::<String>("dest").map(PathBuf::from);
        let dest_ref = dest.as_deref();

        let extension_override = args.get_one::<String>("extension").map(|s| s.as_str());

        let result = command::df::add_images(
            &repo,
            &df_repo_relative,
            &image_paths,
            dest_ref,
            extension_override,
        )
        .await?;

        println!(
            "Added {} image(s) to data frame '{}'",
            result.len(),
            df_repo_relative.display()
        );

        Ok(())
    }
}
