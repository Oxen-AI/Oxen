use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::repositories::ResetMode;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "reset";
pub struct ResetCmd;

#[async_trait]
impl RunCmd for ResetCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Move the current branch to another commit")
            .arg_required_else_help(true)
            .arg(Arg::new("revision").help("Commit id or branch name to move the branch to"))
            .arg(
                Arg::new("mixed")
                    .long("mixed")
                    .help("Move the branch and leave the working tree alone, so the dropped files can be committed again. This is the default.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("hard")
                    .long("hard")
                    .help("Move the branch and restore the working tree to the target commit, discarding changes that disagree with it")
                    .conflicts_with("mixed")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("soft")
                    .long("soft")
                    .help("Not supported yet")
                    .hide(true)
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
        if args.get_flag("soft") {
            return Err(anyhow::anyhow!(
                "`oxen reset --soft` is not supported yet. `oxen reset` leaves the files in the working tree, so `oxen add` and `oxen commit` can recreate the commit.",
            ));
        }

        let revision = args
            .get_one::<String>("revision")
            .ok_or_else(|| anyhow::anyhow!("Err: Usage `oxen reset <revision>`"))?;
        let mode = if args.get_flag("hard") {
            ResetMode::Hard
        } else {
            ResetMode::Mixed
        };

        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        let commit = repositories::reset(&repo, revision, mode).await?;
        println!("Reset to {} {}", commit.id, commit.message);

        Ok(())
    }
}
