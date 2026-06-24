use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::model::LocalRepository;
use liboxen::{repositories, util};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::cmd::RunCmd;

pub const NAME: &str = "cat";
pub struct CatCmd;

#[async_trait]
impl RunCmd for CatCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Print the raw bytes of a file at a revision to stdout")
            .arg(
                Arg::new("path")
                    .required(true)
                    .help("Path to the file, relative to the current directory"),
            )
            .arg(
                Arg::new("revision")
                    .long("revision")
                    .short('r')
                    .help("The branch name or commit id to read from. Defaults to HEAD.")
                    .default_value("HEAD")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
        let repository = LocalRepository::from_current_dir()?;
        let path = args
            .get_one::<String>("path")
            .ok_or_else(|| anyhow::anyhow!("Must supply a path"))?;
        let revision = args
            .get_one::<String>("revision")
            .ok_or_else(|| anyhow::anyhow!("Must supply a revision"))?;

        // Resolve the user-supplied path to a repo-relative path. Joining an absolute path
        // onto the current dir yields the absolute path, so this handles both absolute and
        // current-dir-relative inputs. Don't canonicalize — the file need not exist in the
        // working tree, only in the revision.
        let current_dir = std::env::current_dir()?;
        let repo_path = util::fs::path_relative_to_dir(current_dir.join(path), &repository.path)?;

        let mut stream = repositories::revisions::get_version_stream_from_revision(
            &repository,
            revision,
            repo_path,
        )
        .await?;

        // Unbounded streamed write: use the large-buffer convention and flush explicitly, since
        // `BufWriter`'s `Drop` does not flush.
        let mut stdout = tokio::io::BufWriter::with_capacity(10 * 1024 * 1024, tokio::io::stdout());
        while let Some(chunk) = stream.next().await {
            stdout.write_all(&chunk?).await?;
        }
        stdout.flush().await?;

        Ok(())
    }
}
