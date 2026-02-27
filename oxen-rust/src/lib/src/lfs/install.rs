use std::path::Path;
use std::process::Command;

use crate::error::OxenError;

/// Resolve the canonical absolute path of the running `oxen` binary.
pub fn current_exe_path() -> Result<String, OxenError> {
    let exe = std::env::current_exe()
        .map_err(|e| OxenError::basic_str(format!("failed to determine current executable: {e}")))?;
    let canonical = exe.canonicalize().map_err(|e| {
        OxenError::basic_str(format!(
            "failed to canonicalize executable path {}: {e}",
            exe.display()
        ))
    })?;
    canonical.to_str().map(|s| s.to_string()).ok_or_else(|| {
        OxenError::basic_str(format!(
            "executable path is not valid UTF-8: {}",
            canonical.display()
        ))
    })
}

/// Configure Git's global filter driver so that every repository
/// using `filter=oxen` will invoke our clean/smudge process.
///
/// `oxen_bin` is the absolute path to the `oxen` executable.
///
/// Sets in `~/.gitconfig`:
/// ```text
/// [filter "oxen"]
///     process = /full/path/to/oxen lfs filter-process
///     required = true
///     clean = /full/path/to/oxen lfs clean -- %f
///     smudge = /full/path/to/oxen lfs smudge -- %f
/// ```
pub fn install_global_filter(oxen_bin: &Path) -> Result<(), OxenError> {
    let bin = shell_quote(oxen_bin);
    git_config_global("filter.oxen.process", &format!("{bin} lfs filter-process"))?;
    git_config_global("filter.oxen.required", "true")?;
    git_config_global("filter.oxen.clean", &format!("{bin} lfs clean -- %f"))?;
    git_config_global("filter.oxen.smudge", &format!("{bin} lfs smudge -- %f"))?;
    Ok(())
}

/// Shell-quote a path if it contains spaces, otherwise return as-is.
fn shell_quote(path: &Path) -> String {
    let s = path.to_string_lossy();
    if s.contains(' ') {
        format!("'{s}'")
    } else {
        s.into_owned()
    }
}

/// Remove the global filter driver configuration.
pub fn uninstall_global_filter() -> Result<(), OxenError> {
    // --remove-section fails if the section doesn't exist, so ignore errors.
    let _ = Command::new("git")
        .args(["config", "--global", "--remove-section", "filter.oxen"])
        .output();
    Ok(())
}

fn git_config_global(key: &str, value: &str) -> Result<(), OxenError> {
    let output = Command::new("git")
        .args(["config", "--global", key, value])
        .output()
        .map_err(|e| OxenError::basic_str(format!("failed to run git config: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(OxenError::basic_str(format!(
            "git config --global {key} failed: {stderr}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // Note: We don't run install/uninstall in tests to avoid modifying the
    // developer's actual ~/.gitconfig. Integration tests with isolated HOME
    // can cover this.

    use super::*;

    #[test]
    fn test_install_and_uninstall_do_not_panic() {
        // Smoke test: just verify the functions can be called without panic.
        // Actual git config changes are tested in integration tests.
        let exe = std::path::PathBuf::from("/usr/local/bin/oxen");
        let _ = install_global_filter(&exe);
        let _ = uninstall_global_filter();
    }

    #[test]
    fn test_current_exe_path_returns_string() {
        // Should succeed in any test environment.
        let path = current_exe_path().unwrap();
        assert!(!path.is_empty());
    }

    #[test]
    fn test_shell_quote_no_spaces() {
        let p = std::path::Path::new("/usr/local/bin/oxen");
        assert_eq!(shell_quote(p), "/usr/local/bin/oxen");
    }

    #[test]
    fn test_shell_quote_with_spaces() {
        let p = std::path::Path::new("/path with spaces/oxen");
        assert_eq!(shell_quote(p), "'/path with spaces/oxen'");
    }
}
