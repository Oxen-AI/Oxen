use colored::Colorize;
use liboxen::error::OxenError;

/// Print an OxenError to stderr with colored prefix and optional hints.
pub fn print_error(err: &OxenError) {
    eprintln!("{} {}", "Error:".red().bold(), err);

    if let Some(hint) = err.hint() {
        eprintln!("  {} {}", "hint:".cyan().bold(), hint);
    }
}
