use std::path::Path;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "init";
pub struct LfsInitCmd;

#[async_trait]
impl RunCmd for LfsInitCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Initialize Oxen LFS in the current Git repository")
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .help("Oxen remote URL for push/pull of large files"),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;

        // Verify we are in a git repository.
        let git_dir = repo_root.join(".git");
        if !git_dir.exists() {
            return Err(OxenError::basic_str(
                "Not a git repository. Run `git init` first.",
            ));
        }

        // Create .oxen/ directory.
        let oxen_dir = repo_root.join(OXEN_HIDDEN_DIR);
        std::fs::create_dir_all(&oxen_dir)?;

        // Create versions/ directory.
        let versions_dir = oxen_dir.join("versions");
        std::fs::create_dir_all(&versions_dir)?;

        // Save LFS config.
        let remote_url = args.get_one::<String>("remote").cloned();
        let config = lfs::config::LfsConfig { remote_url };
        config.save(&oxen_dir)?;

        // Install hooks.
        lfs::hooks::install_hooks(&git_dir)?;

        // Add .oxen/ to .gitignore.
        ensure_gitignore(&repo_root)?;

        println!("Oxen LFS initialized in {}", repo_root.display());
        Ok(())
    }
}

/// Ensure `.oxen/` is listed in `.gitignore`.
fn ensure_gitignore(repo_root: &Path) -> Result<(), OxenError> {
    let gitignore = repo_root.join(".gitignore");
    let pattern = format!("{OXEN_HIDDEN_DIR}/");

    let existing = if gitignore.exists() {
        std::fs::read_to_string(&gitignore)?
    } else {
        String::new()
    };

    if existing.lines().any(|l| l.trim() == pattern) {
        return Ok(());
    }

    let mut content = existing;
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&pattern);
    content.push('\n');

    std::fs::write(&gitignore, content)?;
    Ok(())
}
