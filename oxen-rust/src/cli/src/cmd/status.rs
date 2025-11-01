use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use glob::glob;
use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use std::collections::HashSet;
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
                Arg::new("ignore")
                    .long("ignore")
                    .short('i')
                    .help("Ignore files at this path.")
                    .action(clap::ArgAction::Set)
                    .required(false),
            )
            .arg(
                Arg::new("print_all")
                    .long("print_all")
                    .short('a')
                    .help("If present, does not truncate the output of status at all.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("no-cache")
                    .long("no-cache")
                    .help("Skip filesystem cache and perform full scan")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("paths")
                    .num_args(0..)
                    .trailing_var_arg(true)  // Collect all remaining args as paths
                    .help("Specify one or more paths")
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
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
        let no_cache = args.get_flag("no-cache");

        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let paths = args
            .get_many::<String>("paths")
            .map(|vals| vals.map(|v| repository.path.join(v)).collect())
            .unwrap_or_else(|| vec![repository.path.clone()]);
        let is_remote = false;
        let opts = StagedDataOpts {
            paths,
            skip,
            limit,
            print_all,
            is_remote,
            ignore: parse_ignore_files(args.get_one::<String>("ignore")),
        };
        log::debug!("status opts: {:?}", opts);

        // Use the watcher-enabled status function unless --no-cache is specified
        let repo_status = if no_cache {
            log::debug!("Using direct scan (--no-cache specified)");
            repositories::status::status_from_opts(&repository, &opts)?
        } else {
            // Try to use watcher cache by default
            log::debug!("Attempting to use watcher cache");
            liboxen::core::v_latest::status::status_with_cache(&repository, &opts, true).await?
        };

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

fn parse_ignore_files(paths: Option<&String>) -> Option<HashSet<PathBuf>> {
    let paths_str = paths?;

    match glob(paths_str) {
        Ok(paths) => {
            let mut results: HashSet<PathBuf> = HashSet::new();
            for path in paths.flatten() {
                results.insert(path);
            }
            Some(results)
        }
        Err(err) => {
            log::error!("Err: {err:?}");
            None
        }
    }
}
