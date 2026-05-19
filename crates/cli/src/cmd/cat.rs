use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::{repositories, util};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::cmd::RunCmd;

pub const NAME: &str = "cat";
pub struct CatCmd;

fn normalize_repo_path(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    if path.is_absolute() {
        return util::fs::path_relative_to_dir(path, &repo.path);
    }

    let current_dir = std::env::current_dir()?;
    util::fs::path_relative_to_dir(current_dir.join(path), &repo.path)
}

#[async_trait]
impl RunCmd for CatCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Print raw file contents from a revision.")
            .arg(Arg::new("path").required(true))
            .arg(
                Arg::new("revision")
                    .long("revision")
                    .short('r')
                    .help("The branch, commit id, or HEAD revision to read from.")
                    .default_value("HEAD")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        let path = args.get_one::<String>("path").expect("Must supply path");
        let revision = args
            .get_one::<String>("revision")
            .expect("Must supply revision");
        let repo_path = normalize_repo_path(&repository, PathBuf::from(path))?;

        let mut stream = repositories::revisions::get_version_stream_from_revision(
            &repository,
            revision,
            repo_path,
        )
        .await?;
        let mut stdout = tokio::io::stdout();

        while let Some(chunk) = stream.next().await {
            stdout.write_all(&chunk?).await?;
        }
        stdout.flush().await?;

        Ok(())
    }
}
