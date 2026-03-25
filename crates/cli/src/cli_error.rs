use colored::Colorize;
use liboxen::error::OxenError;

/// Format an OxenError for CLI output.
///
/// This adds CLI-specific context on top of OxenError's Display impl:
/// colored "Error:" prefix, actionable hints telling the user what command
/// to run, and suppression of internal details for truly internal errors.
pub fn format_error(err: &OxenError) -> String {
    let prefix = "Error:".red().bold();

    match err {
        // --- Repo not found: most common new-user error ---
        OxenError::LocalRepoNotFound(_) => {
            let hint = hint("Run `oxen init` to create a new repository here.");
            format!("{prefix} {err}\n{hint}")
        }

        // --- Auth / config ---
        OxenError::UserConfigNotFound(_) => {
            // The inner message already contains the config command hint,
            // just add the colored prefix.
            format!("{prefix} {err}")
        }
        OxenError::Authentication(_) => {
            let hint =
                hint("Check your token with `oxen config --auth <HOST> <TOKEN>` and try again.");
            format!("{prefix} {err}\n{hint}")
        }

        // --- Remote issues ---
        OxenError::RemoteRepoNotFound(remote) => {
            let hint = hint(&format!(
                "Verify the remote URL is correct. Check your remotes with `oxen remote -v`.\nCurrent remote: {}",
                remote.url
            ));
            format!("{prefix} Remote repository not found: {remote}\n{hint}")
        }
        OxenError::RemoteAheadOfLocal(_)
        | OxenError::IncompleteLocalHistory(_)
        | OxenError::RemoteBranchLocked(_)
        | OxenError::UpstreamMergeConflict(_) => {
            // These already carry good remediation messages from the constructors
            format!("{prefix} {err}")
        }

        // --- Branch / revision ---
        OxenError::BranchNotFound(_) => {
            let hint = hint("List available branches with `oxen branch --all`.");
            format!("{prefix} {err}\n{hint}")
        }
        OxenError::RevisionNotFound(_) => {
            let hint = hint(
                "Check available branches with `oxen branch --all` or commits with `oxen log`.",
            );
            format!("{prefix} {err}\n{hint}")
        }

        // --- Nothing to commit ---
        OxenError::NothingToCommit(_) => {
            let hint = hint("Stage changes with `oxen add <path>` before committing.");
            format!("{prefix} {err}\n{hint}")
        }

        // --- HEAD not found (empty repo) ---
        OxenError::HeadNotFound(_) | OxenError::NoCommitsFound(_) => {
            let hint =
                hint("This repository has no commits yet. Add files and create your first commit.");
            format!("{prefix} {err}\n{hint}")
        }

        // --- Path / resource not found ---
        OxenError::PathDoesNotExist(_)
        | OxenError::ResourceNotFound(_)
        | OxenError::ParsedResourceNotFound(_)
        | OxenError::CommitEntryNotFound(_) => {
            let hint = hint("Check the path and current branch with `oxen status`.");
            format!("{prefix} {err}\n{hint}")
        }

        // --- Update / migration ---
        OxenError::OxenUpdateRequired(_) | OxenError::MigrationRequired(_) => {
            // These already carry detailed update/migration instructions
            format!("{prefix} {err}")
        }

        // --- Network errors ---
        OxenError::HTTP(req_err) => {
            let base = format!("{prefix} Network error: could not reach remote server.");
            if req_err.is_connect() || req_err.is_timeout() {
                let hint =
                    hint("Check your internet connection and that the remote host is reachable.");
                format!("{base}\n{hint}")
            } else if req_err.is_status() {
                if let Some(status) = req_err.status() {
                    let detail = format!("Server returned HTTP {status}.");
                    format!("{base}\n  {detail}")
                } else {
                    base
                }
            } else {
                let hint = hint(
                    "Check your internet connection and remote configuration with `oxen remote -v`.",
                );
                format!("{base}\n{hint}")
            }
        }

        // --- IO errors: add context about what kind ---
        OxenError::IO(io_err) => match io_err.kind() {
            std::io::ErrorKind::PermissionDenied => {
                let hint = hint("Check file permissions and try again.");
                format!("{prefix} Permission denied: {io_err}\n{hint}")
            }
            std::io::ErrorKind::NotFound => {
                format!("{prefix} File or directory not found: {io_err}")
            }
            _ => format!("{prefix} {err}"),
        },

        // --- Internal / low-level errors: hide noisy details ---
        OxenError::DB(_)
        | OxenError::ArrowError(_)
        | OxenError::BinCodeError(_)
        | OxenError::RedisError(_)
        | OxenError::R2D2Error(_)
        | OxenError::RmpDecodeError(_) => {
            let detail = format!("{err}");
            let hint = hint("This is an internal error. Run with RUST_LOG=debug for more details.");
            format!("{prefix} An internal error occurred.\n  {detail}\n{hint}")
        }

        // --- Everything else: colored prefix + the Display message ---
        _ => format!("{prefix} {err}"),
    }
}

fn hint(msg: &str) -> String {
    format!("  {} {}", "hint:".cyan().bold(), msg)
}
