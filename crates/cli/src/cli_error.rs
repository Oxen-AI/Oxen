use colored::Colorize;
use liboxen::error::OxenError;

/// Print an OxenError to stderr with colored prefix and optional hints.
pub fn print_error(err: &OxenError) {
    let prefix = "Error:".red().bold();

    match err {
        OxenError::LocalRepoNotFound(_) => {
            eprintln!("{prefix} {err}");
            print_hint("Run `oxen init` to create a new repository here.");
        }
        OxenError::Authentication(_) => {
            eprintln!("{prefix} {err}");
            print_hint("Check your token with `oxen config --auth <HOST> <TOKEN>` and try again.");
        }
        OxenError::RemoteRepoNotFound(remote) => {
            eprintln!("{prefix} {err}");
            print_hint(&format!(
                "Verify the remote URL is correct. Check your remotes with `oxen remote -v`.\nCurrent remote: {}",
                remote.url
            ));
        }
        OxenError::BranchNotFound(_) => {
            eprintln!("{prefix} {err}");
            print_hint("List available branches with `oxen branch --all`.");
        }
        OxenError::RevisionNotFound(_) => {
            eprintln!("{prefix} {err}");
            print_hint(
                "Check available branches with `oxen branch --all` or commits with `oxen log`.",
            );
        }
        OxenError::NothingToCommit(_) => {
            eprintln!("{prefix} {err}");
            print_hint("Stage changes with `oxen add <path>` before committing.");
        }
        OxenError::HeadNotFound(_) | OxenError::NoCommitsFound(_) => {
            eprintln!("{prefix} {err}");
            print_hint(
                "This repository has no commits yet. Add files and create your first commit.",
            );
        }
        OxenError::PathDoesNotExist(_)
        | OxenError::ResourceNotFound(_)
        | OxenError::ParsedResourceNotFound(_)
        | OxenError::CommitEntryNotFound(_) => {
            eprintln!("{prefix} {err}");
            print_hint("Check the path and current branch with `oxen status`.");
        }
        OxenError::HTTP(req_err) => {
            eprintln!("{prefix} Network error: could not reach remote server.");
            if req_err.is_connect() || req_err.is_timeout() {
                print_hint("Check your internet connection and that the remote host is reachable.");
            } else if req_err.is_status() {
                if let Some(status) = req_err.status() {
                    eprintln!("  Server returned HTTP {status}.");
                }
            } else {
                print_hint(
                    "Check your internet connection and remote configuration with `oxen remote -v`.",
                );
            }
        }
        OxenError::IO(io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("{prefix} Permission denied: {io_err}");
            print_hint("Check file permissions and try again.");
        }
        OxenError::IO(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("{prefix} File or directory not found: {io_err}");
        }
        OxenError::DB(_)
        | OxenError::ArrowError(_)
        | OxenError::BinCodeError(_)
        | OxenError::RedisError(_)
        | OxenError::R2D2Error(_)
        | OxenError::RmpDecodeError(_) => {
            eprintln!("{prefix} An internal error occurred.");
            eprintln!("  {err}");
            print_hint("This is an internal error. Run with RUST_LOG=debug for more details.");
        }
        _ => {
            eprintln!("{prefix} {err}");
        }
    }
}

fn print_hint(msg: &str) {
    eprintln!("  {} {}", "hint:".cyan().bold(), msg);
}
