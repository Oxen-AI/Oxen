use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use std::path::PathBuf;

use crate::helpers::check_repo_migration_needed;

use crate::cmd::RunCmd;
pub const NAME: &str = "status";
pub struct StatusCmd;

#[async_trait]
impl RunCmd for StatusCmd {
    fn name(&self) -> &str {
        NAME
    }
    fn args(&self) -> Command {
        Command::new(NAME)
            .about("View the repository status, including staged, untracked, modified, and removed files")
            .arg(
                Arg::new("skip")
                    .long("skip")
                    .short('s')
                    .help("Allows you to skip and paginate through the file list preview.")
                    .default_value("0")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("limit")
                    .long("limit")
                    .short('l')
                    .help("Allows you to view more file list preview.")
                    .default_value("10")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("print_all")
                    .long("print_all")
                    .short('a')
                    .help("If present, does not truncate the output of status at all.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(Arg::new("path").required(false))
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let directory = args.get_one::<String>("path").map(PathBuf::from);

        let skip = args
            .get_one::<String>("skip")
            .expect("Must supply skip")
            .parse::<usize>()
            .expect("skip must be a valid integer.");
        let limit = args
            .get_one::<String>("limit")
            .expect("Must supply limit")
            .parse::<usize>()
            .expect("limit must be a valid integer.");
        let print_all = args.get_flag("print_all");

        let is_remote = false;
        let opts = StagedDataOpts {
            skip,
            limit,
            print_all,
            is_remote,
        };

        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let directory = directory.unwrap_or(repository.path.clone());
        let repo_status = repositories::status_from_dir(&repository, &directory)?;

        if let Some(current_branch) = repositories::branches::current_branch(&repository)? {
            println!(
                "On branch {} -> {}\n",
                current_branch.name, current_branch.commit_id
            );
        } else if let Some(head) = repositories::commits::head_commit_maybe(&repository)? {
            println!(
                "You are in 'detached HEAD' state.\nHEAD is now at {} {}\n",
                head.id, head.message
            );
        }

        repo_status.print_with_params(&opts);

        Ok(())
    }
}
