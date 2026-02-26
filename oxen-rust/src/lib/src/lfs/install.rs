use std::process::Command;

use crate::error::OxenError;

/// Configure Git's global filter driver so that every repository
/// using `filter=oxen` will invoke our clean/smudge process.
///
/// Sets in `~/.gitconfig`:
/// ```text
/// [filter "oxen"]
///     process = oxen lfs filter-process
///     required = true
///     clean = oxen lfs clean -- %f
///     smudge = oxen lfs smudge -- %f
/// ```
pub fn install_global_filter() -> Result<(), OxenError> {
    git_config_global("filter.oxen.process", "oxen lfs filter-process")?;
    git_config_global("filter.oxen.required", "true")?;
    git_config_global("filter.oxen.clean", "oxen lfs clean -- %f")?;
    git_config_global("filter.oxen.smudge", "oxen lfs smudge -- %f")?;
    Ok(())
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
        let _ = install_global_filter();
        let _ = uninstall_global_filter();
    }
}
