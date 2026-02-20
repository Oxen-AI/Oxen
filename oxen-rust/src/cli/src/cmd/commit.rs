use async_trait::async_trait;
use clap::{Arg, Command};
use std::io::Write;
use tempfile::TempDir;

use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "commit";
pub struct CommitCmd;

#[async_trait]
impl RunCmd for CommitCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Commit the staged files to the repository.")
            .arg(
                Arg::new("message")
                    .help("The message for the commit. Should be descriptive about what changed.")
                    .long("message")
                    .short('m')
                    .required(false)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("allow_empty")
                    .help("Allow creating a commit with no changes")
                    .long("allow-empty")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let message = match args.get_one::<String>("message") {
            Some(msg) => msg.clone(),
            None => get_message_from_editor(UserConfig::get().ok().as_ref())?,
        };

        let allow_empty = args.get_flag("allow_empty");

        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        println!("Committing with message: {message}");

        if allow_empty {
            repositories::commits::commit_allow_empty(&repo, &message)?;
        } else {
            repositories::commit(&repo, &message)?;
        }

        Ok(())
    }
}

fn resolve_editor(maybe_config: Option<&UserConfig>) -> Option<String> {
    // 1. Check UserConfig
    if let Some(config) = maybe_config {
        if let Some(ref editor) = config.editor {
            if !editor.is_empty() {
                return Some(editor.to_string());
            }
        }
    }

    // 2. Fall back to VISUAL env var
    if let Ok(editor) = std::env::var("VISUAL") {
        if !editor.is_empty() {
            return Some(editor);
        }
    }

    // 3. Fall back to EDITOR env var
    if let Ok(editor) = std::env::var("EDITOR") {
        if !editor.is_empty() {
            return Some(editor);
        }
    }

    None
}

fn get_message_from_editor(maybe_config: Option<&UserConfig>) -> Result<String, OxenError> {
    let editor = resolve_editor(maybe_config).ok_or_else(|| {
        OxenError::basic_str(
            "No editor is configured and no commit message was provided via -m.\n\n\
             To set your preferred editor, run:\n    \
             oxen config --editor <EDITOR>\n\n\
             Or manually add the following to ~/.config/oxen/user_config.toml:\n    \
             editor = \"vim\"",
        )
    })?;

    // Create a temp file with a comment template
    // NOTE: when temp_dir is dropped the directory it made will be deleted
    let temp_dir = TempDir::new()?;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_path = temp_dir
        .path()
        .join(format!("oxen_commit_msg_{timestamp}.txt"));

    let template = "\n# Please enter the commit message for your changes.\n\
                     # Lines starting with '#' will be ignored, and an empty message aborts the commit.\n";

    {
        let mut file = std::fs::File::create(&temp_path)
            .map_err(|e| OxenError::basic_str(format!("Failed to create temp file: {e}")))?;
        file.write_all(template.as_bytes())
            .map_err(|e| OxenError::basic_str(format!("Failed to write to temp file: {e}")))?;
    }

    // Spawn the editor
    // Split the editor string to support commands like "code --wait"
    let parts: Vec<&str> = editor.split_whitespace().collect();
    if parts.is_empty() {
        return Err(OxenError::basic_str(
            "Must supply valid editor path, not an empty/whitespace-only string.",
        ));
    }
    let status = std::process::Command::new(parts[0])
        .args(&parts[1..])
        .arg(&temp_path)
        .status()
        .map_err(|e| OxenError::basic_str(format!("Failed to open editor '{editor}': {e}")))?;

    if !status.success() {
        return Err(OxenError::basic_str(format!(
            "Editor '{editor}' exited with non-zero status."
        )));
    }

    // Read the file and strip comments
    let contents = std::fs::read_to_string(&temp_path)
        .map_err(|e| OxenError::basic_str(format!("Failed to read temp file: {e}")))?;
    let _ = std::fs::remove_file(&temp_path);

    let message: String = contents
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");
    let message = message.trim().to_string();

    if message.is_empty() {
        return Err(OxenError::basic_str(
            "Aborting commit due to empty commit message.",
        ));
    }

    Ok(message)
}
