use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::model::LocalRepository;
use std::path::PathBuf;

use liboxen::opts::InfoOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::path_relative_to_repo;
pub const NAME: &str = "info";
pub struct InfoCmd;

#[async_trait]
impl RunCmd for InfoCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Get metadata information about a file such as the oxen hash, data type, etc.")
            .arg(Arg::new("path").required(false))
            .arg(
                Arg::new("revision")
                    .long("revision")
                    .short('r')
                    .help("Read metadata as of this branch or commit instead of the working tree.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("verbose")
                    .long("verbose")
                    .short('v')
                    .help("If present, will print all the field names when printing as tab separated list.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("json")
                    .long("json")
                    .help("If present, will print the metadata info as json.")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
        // Parse args
        let path = args.get_one::<String>("path").map(PathBuf::from);
        let revision = args.get_one::<String>("revision").map(String::from);

        if path.is_none() {
            return Err(anyhow::anyhow!("Must supply path."));
        }

        let path = path.unwrap();
        let verbose = args.get_flag("verbose");
        let output_as_json = args.get_flag("json");

        let opts = InfoOpts {
            path: path.clone(),
            revision,
            verbose,
            output_as_json,
        };

        // Look up from the current dir for .oxen directory
        let repository = LocalRepository::from_current_dir()?;
        // With a revision, read metadata from that commit's merkle tree (which is keyed by
        // repo-relative paths); without one, describe the working-tree file on disk at the path
        // as given.
        let metadata = match &opts.revision {
            Some(revision) => {
                let repo_path = path_relative_to_repo(&repository, &path)?;
                repositories::metadata::get_cli_at_revision(&repository, &repo_path, revision)?
            }
            None => repositories::metadata::get_cli(&repository, &path, &path)?,
        };

        if opts.output_as_json {
            let json = serde_json::to_string(&metadata)?;
            println!("{json}");
        } else {
            // hash size data_type mime_type extension last_updated_commit_id
            if opts.verbose {
                println!("hash\tsize\tdata_type\tmime_type\textension\tlast_updated_commit_id");
            }

            let mut last_updated_commit_id = String::from("None");
            if let Some(commit) = metadata.last_updated {
                last_updated_commit_id = commit.id;
            }

            println!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                metadata.hash,
                metadata.size,
                metadata.data_type,
                metadata.mime_type,
                metadata.extension,
                last_updated_commit_id
            );
        }

        Ok(())
    }
}
